## FunctionDef _indexes_from_metadata(metadata, transforms, vocabulary)
**_indexes_from_metadata**: The function of _indexes_from_metadata is to collect and return a list of SQLAlchemy Index objects from a transformed metadata object, optionally filtering for vocabulary or non‑vocabulary tables.

**parameters**:  
· **metadata** – a SQLAlchemy `MetaData` instance that contains the full set of Table objects for the PEDSnet schema.  
· **transforms** – a list of Transform classes (subclasses of `pedsnetdcc.abstract_transform.Transform`). Each class will be applied in sequence to modify the supplied metadata.  
· **vocabulary** – a boolean flag indicating whether the returned indexes should be taken from vocabulary tables (`True`) or from non‑vocabulary tables (`False`). Default is `False`.

**Code Description**:  
1. The function iterates over the `transforms` list. For each Transform class `t`, it calls `t.modify_metadata(metadata)`. The `modify_metadata` method (defined in `abstract_transform.py`) walks through all non‑vocabulary tables in the supplied `metadata` and invokes the Transform’s `modify_table` hook, allowing the Transform to add or alter columns, indexes, constraints, etc. The method returns the same `metadata` object, now potentially mutated.  
2. After all transforms have been applied, the function creates an empty list `indexes`.  
3. It then iterates over every `(name, table)` pair in `metadata.tables`.  
   - If `vocabulary` is `True`, the function checks whether the table name is present in the constant `VOCAB_TABLES`. When the name is in that set, the table’s `indexes` collection is extended onto the `indexes` list.  
   - If `vocabulary` is `False`, the function adds indexes from tables whose names are **not** in `VOCAB_TABLES`.  
   This conditional logic ensures that callers can request indexes exclusively for the vocabulary schema or for the site‑specific (non‑vocabulary) schema.  
4. Finally, the accumulated list of `sqlalchemy.Index` objects is returned.

**Reference Relationships**:  
- **Callees**: The function calls `t.modify_metadata(metadata)` for each Transform in the `transforms` argument. This method is implemented in `pedsnetdcc.abstract_transform.Transform` and is the primary way the function obtains a transformed metadata representation.  
- **Callers**:  
  * `pedsnetdcc/indexes.py::_indexes_sql` invokes `_indexes_from_metadata` to obtain the set of indexes that will be rendered as CREATE or DROP statements for a given model version.  
  * Any other module that needs a concrete list of Index objects after applying the project's standard transforms could also call this function directly, though the current codebase uses it only via `_indexes_sql`.  

**Note**:  
- The function does not copy the metadata; it mutates the original object in‑place through each Transform’s `modify_metadata`. Callers should be aware that the supplied `metadata` will be altered after the function returns.  
- The `vocabulary` flag controls filtering based on the global `VOCAB_TABLES` constant; ensure that this constant accurately reflects the set of vocabulary tables for the target model version.  
- If a Transform adds indexes to vocabulary tables, those indexes will only be returned when `vocabulary=True`. Conversely, indexes added to non‑vocabulary tables are omitted when `vocabulary=True`.  
- The order of indexes in the returned list follows the order of tables in `metadata.tables` and the order in which each table stores its `indexes` collection; no additional sorting is performed.  

**Output Example**:  
```python
# Assume we have a stock metadata object for model version '2.2.0'
metadata = stock_metadata('2.2.0')

# Transforms list contains AgeTransform and SiteNameTransform, each adds an index.
transforms = [AgeTransform, SiteNameTransform]

# Retrieve indexes for non‑vocabulary tables
idx_list = _indexes_from_metadata(metadata, transforms, vocabulary=False)

# idx_list is a list of sqlalchemy.Index objects, e.g.
# [Index('ix_person_age', person.c.age),
#  Index('ix_encounter_site_id', encounter.c.site_id), ...]
len(idx_list)   # -> number of indexes added by the transforms to non‑vocabulary tables
```
## FunctionDef _indexes_sql(model_version, vocabulary, drop)
**_indexes_sql**: The function of _indexes_sql is to generate a list of SQL statements that either create (ADD) or remove (DROP) indexes for a transformed PEDSnet schema, optionally limited to vocabulary tables.

**parameters**:  
· **model_version** – the PEDSnet model version for which the schema metadata should be retrieved (type: `str`).  
· **vocabulary** – when `True`, the generated statements target only the vocabulary tables; when `False` (default), they target the site‑specific (non‑vocabulary) tables (type: `bool`).  
· **drop** – when `True`, the function produces `DROP INDEX` statements; when `False` (default), it produces `CREATE INDEX` statements (type: `bool`).  

**Code Description**:  
1. The function decides which SQLAlchemy schema operation to use based on the `drop` flag:  
   * If `drop` is `True`, `sqlalchemy.schema.DropIndex` is assigned to the local variable `func`.  
   * Otherwise, `sqlalchemy.schema.CreateIndex` is assigned to `func`.  
2. It obtains a fresh, untransformed metadata object for the requested `model_version` by calling `stock_metadata(model_version)`.  
3. The metadata is passed, together with the project's list of transform classes (`pedsnetdcc.transform_runner.TRANSFORMS`) and the `vocabulary` flag, to the helper `_indexes_from_metadata`. This helper applies each transform to the metadata, then extracts all `sqlalchemy.Index` objects from either vocabulary or non‑vocabulary tables, returning them as a list.  
4. For each `Index` object returned, the function:  
   * Instantiates the chosen operation (`func`) with the index (`func(x)`).  
   * Compiles the operation to a PostgreSQL‑dialect SQL string via `.compile(dialect=sqlalchemy.dialects.postgresql.dialect())`.  
   * Converts the compiled object to a plain string and removes any leading whitespace with `.lstrip()`.  
5. The list comprehension collects all compiled SQL strings into a Python list, which is then returned to the caller.

**Reference Relationships**:  
* **Callees**:  
  – `_indexes_from_metadata` – extracts the transformed index objects.  
  – `stock_metadata` – provides the base `MetaData` for the specified model version.  
  – `sqlalchemy.schema.CreateIndex` / `sqlalchemy.schema.DropIndex` – generate the appropriate DDL operation objects.  
  – `sqlalchemy.dialects.postgresql.dialect` – supplies the PostgreSQL dialect for correct statement rendering.  
* **Callers**:  
  – `_process_indexes` – builds a `StatementSet` from the returned SQL strings and executes them against a database.  
  – Unit‑test methods in `pedsnetdcc/tests/indexes_test.py` (`test_add_indexes`, `test_drop_indexes`, `test_add_indexes_for_vocabulary`) invoke `_indexes_sql` directly to verify the generated statements.  
  – Any external script that needs to create or drop indexes for a specific model version may also call this function.

**Note**:  
* The function does not perform any database connection; it only returns the raw DDL strings. Execution is delegated to callers such as `_process_indexes`.  
* The `vocabulary` flag relies on the global `VOCAB_TABLES` constant used inside `_indexes_from_metadata`; ensure that this constant accurately reflects the vocabulary tables for the target model version.  
* Because the index objects are compiled with the PostgreSQL dialect, the returned statements are valid only for PostgreSQL databases.  
* The order of statements in the returned list follows the order in which indexes appear in the transformed metadata; no additional sorting is applied.  
* If a transform adds or removes indexes, those changes are automatically reflected in the output, as the metadata is transformed before extraction.

**Output Example**:  
```python
# Example call for model version 2.2.0, non‑vocabulary tables, CREATE statements
sql_statements = _indexes_sql('2.2.0')
# sql_statements might contain:
[
    "CREATE INDEX obs_otcn_89a4742c38ecb8ba35_ix ON observation (observation_type_concept_name)",
    "CREATE INDEX dea_s_4906dc6995505fc71431f_ix ON death (site)",
    "CREATE INDEX vis_vsaim_f1537dca8da9ab914_ix ON visit_occurrence (visit_start_age_in_months)",
    # ... additional CREATE INDEX statements ...
]
# Example call for the same version, vocabulary tables, DROP statements
drop_sql = _indexes_sql('2.2.0', vocabulary=True, drop=True)
# drop_sql might contain:
[
    "DROP INDEX idx_concept_class_id",
    "DROP INDEX idx_concept_synonym_id",
    # ... additional DROP INDEX statements ...
]
```
## FunctionDef _check_stmt_err(stmt, force)
**_check_stmt_err**: The function of _check_stmt_err is to examine a database statement for errors and decide whether to ignore benign errors based on the `force` flag, otherwise re‑raise the error.

**parameters**:  
· stmt: an instance of `pedsnetdcc.db.Statement` representing a single SQL statement that has been executed. The object contains the original SQL text (`stmt.sql`) and any exception raised during execution (`stmt.err`).  
· force: a boolean flag indicating whether benign errors (such as “object does not exist” when dropping, or “object already exists” when creating) should be suppressed.

**Code Description**:  
1. The function first checks `stmt.err`. If it is `None`, the statement completed without error and the function returns immediately.  
2. It determines the type of operation by inspecting the beginning of the SQL string:  
   * `dropping` is `True` when the statement starts with the keyword `DROP`.  
   * `creating` is `True` when the statement starts with the keyword `CREATE`.  
3. Two specific PostgreSQL error conditions are identified using the error’s `pgcode` attribute and the `psycopg2_errorcodes` lookup:  
   * **does_not_exist** – true when the error code maps to `UNDEFINED_OBJECT` (SQLSTATE 42P01), which occurs when an object that is being dropped does not exist.  
   * **already_exists** – true when the error code maps to `DUPLICATE_TABLE` (SQLSTATE 42P07), which occurs when an object that is being created already exists.  
4. If the statement is a `DROP` and `force` is `True` and the error is `does_not_exist`, the function treats the error as benign and returns without raising.  
5. If the statement is a `CREATE` and `force` is `True` and the error is `already_exists`, the function also treats the error as benign and returns.  
6. For any other situation (no error, non‑benign error, or `force` is `False`), the original exception stored in `stmt.err` is raised, propagating it to the caller.

**Relationship with Callers**:  
`_check_stmt_err` is invoked by the index‑management functions in `indexes.py` (`_process_indexes`, `drop_unneeded_indexes`, `add_vocab_indexes`, `drop_vocab_unneeded_indexes`). After a batch of statements is executed in parallel, each caller iterates over the resulting `Statement` objects and calls `_check_stmt_err` to verify that no unexpected errors occurred. If a benign error is encountered and the caller passed `force=True`, the error is silently ignored, allowing the index operation to continue. Otherwise, the caller logs the fatal error and re‑raises it, causing the whole index‑processing routine to abort.

**Note**:  
- The function only distinguishes the two benign error cases listed above; any other PostgreSQL error, including connection problems (`psycopg2.OperationalError`), will be re‑raised regardless of the `force` flag.  
- The check relies on the SQL string starting exactly with `DROP` or `CREATE`; statements with leading whitespace or comments will not be recognized as dropping/creating and may be treated as non‑benign.  
- The function does not return a value; its effect is either a silent return (no error or ignored benign error) or an exception raise.

**Output Example**:  
When a `DROP INDEX` statement fails because the index does not exist and `force=True`:

```python
_check_stmt_err(stmt, force=True)   # returns None (error ignored)
```

When a `CREATE INDEX` statement fails because the index already exists and `force=False`:

```python
_check_stmt_err(stmt, force=False)  # raises psycopg2.errors.DuplicateTable
```
## FunctionDef _process_indexes(conn_str, model_version, force, vocabulary, drop)
**_process_indexes**: The function of _process_indexes is to generate, execute, and verify a batch of ADD or DROP INDEX SQL statements for a transformed PEDSnet schema, optionally limited to vocabulary tables, and to handle errors according to the `force` flag.

**parameters**:  
· **conn_str** (str): PostgreSQL connection string used for all statement executions.  
· **model_version** (str): Version identifier of the PEDSnet data model whose metadata is used to derive the index definitions.  
· **force** (bool): When True, benign errors such as “object does not exist” on DROP or “object already exists” on CREATE are ignored; otherwise any error causes the function to raise an exception.  
· **vocabulary** (bool): When True, index statements are generated only for the vocabulary schema; when False, statements target the site (non‑vocabulary) schema.  
· **drop** (bool): When True, the function generates DROP INDEX statements; when False, it generates CREATE INDEX statements.

**Code Description**:  
1. **Determine operation name** – The boolean `drop` is examined to set a human‑readable `operation` variable (`'removal'` for DROP, `'creation'` for CREATE). This value is used only for logging.  
2. **Prepare logging context** – A dictionary `log_dict` is built by merging the function‑specific context (`model_version`, `vocabulary`, `force`) with connection details obtained from `get_conn_info_dict(conn_str)`. The combined dictionary is logged at INFO level to announce the start of the index operation, including the chosen `operation`. The start time is captured with `time.time()`.  
3. **Create a StatementSet** – An empty `StatementSet` instance `stmts` is instantiated. This collection will hold one `Statement` object per generated SQL command.  
4. **Generate SQL statements** – The helper `_indexes_sql(model_version, vocabulary, drop)` is called. It returns a list of raw SQL strings that either create or drop indexes, based on the supplied flags.  
5. **Wrap each SQL string in a Statement** – For every SQL string returned, a `Statement` object is created (`Statement(stmt)`) and added to `stmts` via `stmts.add`. The `Statement` class stores the SQL, a default message, and a unique immutable identifier.  
6. **Parallel execution** – `stmts.parallel_execute(conn_str)` is invoked. `StatementSet.parallel_execute` launches a pool of worker processes (up to 25, or one per statement if fewer) that each call `Statement.execute(conn_str)`. Execution is performed in autocommit mode, allowing DDL statements such as CREATE/DROP INDEX. Results and any errors are collected back into the `StatementSet`.  
7. **Error inspection** – After all statements have finished, the function iterates over the executed `Statement` objects. For each `stmt`, `_check_stmt_err(stmt, force)` is called. This helper examines `stmt.err`; if an error is present it decides whether the error is benign (based on the SQL command type and PostgreSQL error codes) and the `force` flag. Benign errors are ignored; any other error is re‑raised, causing the function to abort.  
8. **Logging on failure** – If `_check_stmt_err` raises, the surrounding `except` block logs a fatal error entry containing the offending SQL and the error message, logs the aborted operation with elapsed time (computed by `secs_since(start_time)`), and re‑raises the exception to propagate it to the caller.  
9. **Successful completion logging** – If the loop finishes without raising, an INFO log entry records the successful completion of the index `operation` together with the total elapsed time.  
10. **Return value** – The function returns `True` to indicate that all statements were executed and verified successfully.

**Reference relationships**:  
- **Callers**: `add_indexes(conn_str, model_version, force=False, vocabulary=False)` and `drop_indexes(conn_str, model_version, force=False, vocabulary=False)` are thin wrappers that invoke `_process_indexes` with `drop=False` (add) or `drop=True` (drop) respectively.  
- **Callees**:  
  * `_indexes_sql` – produces the raw CREATE/DROP INDEX statements based on the model version and vocabulary flag.  
  * `Statement` – encapsulates each SQL command for execution and result tracking.  
  * `StatementSet` – aggregates the `Statement` objects and provides the `parallel_execute` method for concurrent execution.  
  * `_check_stmt_err` – validates each executed statement and applies the `force` logic.  
  * `combine_dicts` and `get_conn_info_dict` – build the structured logging dictionary.  
  * `secs_since` – formats the elapsed time for log messages.  
  * Module‑level `logger` – records start, error, and completion messages.  

**Note**:  
- The function does not commit or roll back transactions; each `Statement` runs in its own autocommit context, which is required for DDL operations.  
- Errors other than the specific PostgreSQL codes for “object does not exist” (42P01) and “object already exists” (42P07) are always propagated, regardless of the `force` flag.  
- The `vocabulary` flag influences which tables are considered when extracting indexes from the transformed metadata; ensure the flag matches the intended schema.  
- Because execution is parallel, the order of log messages may not reflect the original statement order, but each log entry includes the statement’s unique `id_` for correlation.  
- The function returns `True` only after all statements have been executed and verified; any exception aborts the process before the return.  

**Output Example**:  
```python
result = _process_indexes(
    conn_str="host=pg host=localhost port=5432 dbname=pedsnet user=loader",
    model_version="2.2.0",
    force=True,
    vocabulary=False,
    drop=False
)
print(result)   # -> True
```
## FunctionDef add_indexes(conn_str, model_version, force, vocabulary)
**add_indexes**: The function of add_indexes is to execute CREATE INDEX statements for a transformed PEDSnet schema.

**parameters**:  
· conn_str: PostgreSQL connection string used for all statement executions (type: str)  
· model_version: Identifier of the PEDSnet data model version whose metadata defines the indexes (type: str)  
· force: When True, benign errors such as “object already exists” are ignored; defaults to False (type: bool)  
· vocabulary: When True, the function targets the vocabulary schema; when False, it targets the site (non‑vocabulary) schema (type: bool)

**Code Description**:  
1. The function is a thin wrapper that delegates all work to the internal helper `_process_indexes`.  
2. It passes the received arguments unchanged, with the `drop` flag set to `False` to indicate that CREATE INDEX statements (rather than DROP INDEX) should be generated and executed.  
3. `_process_indexes` performs the following steps (summarized):  
   - Determines the human‑readable operation name (“creation”).  
   - Builds a logging dictionary that merges the function‑specific context (`model_version`, `vocabulary`, `force`) with connection details obtained from `get_conn_info_dict`.  
   - Logs the start of the index creation operation and records the start time.  
   - Instantiates an empty `StatementSet`.  
   - Calls `_indexes_sql(model_version, vocabulary, drop=False)` to obtain a list of raw CREATE INDEX SQL strings appropriate for the given model version and schema selection.  
   - Wraps each SQL string in a `Statement` object and adds it to the `StatementSet`.  
   - Executes all statements in parallel via `StatementSet.parallel_execute(conn_str)`. Each statement runs in its own autocommit session, which is required for DDL.  
   - Iterates over the executed statements and invokes `_check_stmt_err(stmt, force)` for each. If an error is found and it is not considered benign (or `force` is False), the error is logged, the operation is marked as aborted, and the exception is re‑raised.  
   - If no unhandled errors occur, logs a successful completion message with the elapsed time.  
   - Returns `True` to signal successful execution of all index creation statements.  
4. The wrapper simply returns the boolean result from `_process_indexes`.

**Reference relationships**:  
- **Callers**: `add_indexes` is invoked by several higher‑level workflows that build or modify a PEDSnet database:  
  * `merge_site_data` (pedsnetdcc/merge_site_data.py) – after merging site tables, it adds indexes to the DCC schema.  
  * `merge_data_to_schema` (pedsnetdcc/merge_site_data.py) – similar to `merge_site_data` but for an arbitrary target schema.  
  * `run_subset_by_cohort` (pedsnetdcc/subset_by_cohort.py) – adds indexes to a subset schema when `index_create` is True.  
  * `run_transformation` (pedsnetdcc/transform_runner.py) – adds indexes to transformed tables after other post‑transform steps.  
- **Callee**: The only direct callee is `_process_indexes` (pedsnetdcc/indexes.py/_process_indexes), which implements the full index generation, execution, and error‑handling logic. `_process_indexes` in turn calls `_indexes_sql`, `Statement`, `StatementSet`, `_check_stmt_err`, and various logging utilities.

**Note**:  
- The `force` flag controls whether benign errors (e.g., “index already exists”) are suppressed; without `force`, such errors raise an exception and abort the operation.  
- The `vocabulary` flag determines which set of tables the indexes apply to: `False` for site (core) tables, `True` for vocabulary tables. Ensure the flag matches the intended schema, otherwise the generated SQL will target the wrong objects.  
- Execution is parallel; log entries may appear out of order, but each includes a unique statement identifier for correlation.  
- The function does not manage transactions; each CREATE INDEX runs in its own autocommit context, which is required for DDL statements in PostgreSQL.  
- The function returns `True` only after all statements have been successfully executed and verified; any exception prevents the return.

**Output Example**:  
```python
result = add_indexes(
    conn_str="host=localhost port=5432 dbname=pedsnet user=loader",
    model_version="2.2.0",
    force=False,
    vocabulary=False
)
print(result)   # -> True
```
## FunctionDef drop_indexes(conn_str, model_version, force, vocabulary)
**drop_indexes**: The function of drop_indexes is to remove (DROP) all indexes defined for a transformed PEDSnet schema, optionally limited to either the site (non‑vocabulary) tables or the vocabulary tables.

**parameters**:  
· **conn_str** (str): PostgreSQL connection string used to connect to the target database.  
· **model_version** (str): Identifier of the PEDSnet data model version whose metadata supplies the index definitions.  
· **force** (bool, default False): When True, benign errors such as “object does not exist” are ignored; otherwise any error aborts the operation.  
· **vocabulary** (bool, default False): When True, the function operates on the vocabulary schema; when False, it operates on the site (non‑vocabulary) schema.

**Code Description**:  
1. **Thin wrapper** – The body of `drop_indexes` consists of a single call to the internal helper `_process_indexes`.  
2. **Argument forwarding** – It passes the received `conn_str`, `model_version`, `force`, and `vocabulary` arguments unchanged, and sets the `drop` flag to `True` to indicate that DROP INDEX statements should be generated.  
3. **Return value** – The result of `_process_indexes` is returned directly. `_process_indexes` returns `True` only after all generated DROP statements have been executed successfully and any errors have been handled according to the `force` flag.  

**Reference relationships**:  
- **Callers**:  
  * `pedsnetdcc/tests/indexes_test.py/IndexesDatabaseTest.test_drop` – verifies that indexes on non‑vocabulary tables are removed and that a second call raises an error unless `force=True`.  
  * `pedsnetdcc/tests/indexes_test.py/IndexesDatabaseTest.test_add` – uses `drop_indexes` (both with default and `vocabulary=True`) to clear the database before testing index creation.  
  * `pedsnetdcc/tests/indexes_test.py/IndexesDatabaseTest.test_drop_force` – calls `drop_indexes` with `force=True` after manually dropping a single index to ensure no exception is raised.  
  * `pedsnetdcc/transform_runner.py/undo_transformation` – invokes `drop_indexes` with `force=True` as part of the rollback process after a transformation.  
- **Callee**:  
  * `_process_indexes` – performs the heavy lifting: generates the appropriate DROP INDEX SQL statements via `_indexes_sql`, wraps each statement in a `Statement` object, executes them in parallel, checks for errors with `_check_stmt_err`, logs progress, and finally returns `True`.  

**Note**:  
- The function does not commit or roll back a transaction; each DROP statement runs in its own autocommit context, which is required for DDL operations.  
- If `force=False` and a DROP statement fails because the index does not exist (PostgreSQL error code 42P01), the error propagates as a `DatabaseError` (or the underlying `psycopg2.ProgrammingError`). Setting `force=True` suppresses such benign errors.  
- The `vocabulary` flag determines which set of tables the index definitions are drawn from. Ensure the flag matches the intended schema; using the wrong flag may leave indexes in the unintended schema untouched.  
- Because execution is parallel, log entries may appear out of order, but each log record includes a unique statement identifier for correlation.  
- The function returns `True` only after all statements have been successfully executed and verified; any unhandled exception aborts the process before the return.  

**Output Example**:  
```python
result = drop_indexes(
    conn_str="host=localhost port=5432 dbname=pedsnet user=loader",
    model_version="2.2.0",
    force=False,
    vocabulary=False
)
print(result)   # -> True
```
## FunctionDef drop_unneeded_indexes(conn_str, model_version, force, vocabulary)
**drop_unneeded_indexes**: The function of drop_unneeded_indexes is to drop indexes that are no longer required in a transformed PEDSnet schema.

**parameters**:  
· conn_str (str): PostgreSQL connection string in libpq format.  
· model_version (str): Identifier of the PEDSnet data model version (e.g., “2.2.0”).  
· force (bool, optional, default =False): When True, benign errors such as “object does not exist” are ignored.  
· vocabulary (bool, optional, default =False): When True, the statements target the vocabulary schema; otherwise they target the clinical (non‑vocabulary) schema.

**Code Description**:  
1. The function sets the local variable `operation` to the literal string `'removal'`. This value is used only for log messages.  
2. A log dictionary `log_dict` is built by merging the supplied `model_version`, `vocabulary`, and `force` flags with connection‑metadata obtained from `get_conn_info_dict(conn_str)`. The combined dictionary is logged at INFO level to mark the start of the index‑removal step. The start time is recorded with `time.time()`.  
3. An empty `StatementSet` instance (`stmts`) is created. `StatementSet` is a mutable set that can hold objects exposing an `execute(conn_str, resq=None, logq=None)` method; in this project the objects are `Statement` instances.  
4. The helper `_special_drop_indexes_sql(model_version, vocabulary)` is called. It returns a list of raw SQL strings, each being a `DROP INDEX …` command generated from the model metadata for the given version and schema type. For every SQL string returned, a `Statement` object is instantiated (`Statement(stmt)`) and added to the `StatementSet`.  
5. The entire set of statements is executed in parallel by invoking `stmts.parallel_execute(conn_str)`. `parallel_execute` spawns a pool of worker processes (up to 25, or one per statement) that call each statement’s `execute` method, collect results, and forward log records through a dedicated logging thread.  
6. After parallel execution finishes, the function iterates over each `Statement` in `stmts`. For each statement it calls `_check_stmt_err(stmt, force)`. This helper examines `stmt.err`; if an error is present it determines whether the error is benign (e.g., dropping a non‑existent index) and, when `force` is True, suppresses it. Otherwise the original exception is re‑raised. If an exception is raised, the function logs a fatal error, logs the elapsed time, and propagates the exception, aborting further processing.  
7. If all statements complete without raising, a final INFO log entry records the successful completion of the index removal, including the elapsed time calculated by `secs_since(start_time)`.  
8. The function returns the boolean value `True` to indicate successful execution.

**Reference relationships**:  
*Callers* – `drop_unneeded_indexes` is invoked by higher‑level workflow functions that manage schema transformations:  
- `merge_site_data` (pedsnetdcc/merge_site_data.py) – called after creating primary keys, NOT NULL constraints, and indexes, unless the caller disables index dropping with `nodrop`.  
- `merge_data_to_schema` – same usage as `merge_site_data` but for an arbitrary target schema.  
- `run_subset_by_cohort` – used when the user requests index creation for a cohort subset; after `add_indexes` it calls `drop_unneeded_indexes` to clean up unnecessary indexes.  
- `run_transformation` (pedsnetdcc/transform_runner.py) – part of the standard transformation pipeline; after adding indexes it drops those that are no longer needed.  

*Callees* – Within its own body the function relies on:  
- `_special_drop_indexes_sql` – generates the list of DROP INDEX statements based on the model version and the `vocabulary` flag.  
- `Statement` – wraps each raw SQL string so that it can be executed and its result captured.  
- `StatementSet` – aggregates the `Statement` objects and provides the `parallel_execute` method for concurrent execution.  
- `_check_stmt_err` – validates each statement’s execution result, optionally ignoring benign errors when `force=True`.  
- `combine_dicts` and `get_conn_info_dict` – utilities used to build structured log dictionaries.  
- `secs_since` – formats the elapsed time for log messages.  
- The module‑level `logger` – records start, error, and completion messages.

**Note**:  
- The function only generates DROP statements; it never creates indexes.  
- Errors other than “object does not exist” (for DROP) or “object already exists” (for CREATE) are always re‑raised, regardless of the `force` flag.  
- The `vocabulary` flag determines which set of transforms is used to locate indexes; the same function is used for both clinical and vocabulary schemas.  
- Parallel execution uses separate processes; therefore all `Statement` objects must be picklable (they are).  
- If the underlying `_special_drop_indexes_sql` returns an empty list (no indexes to drop), the function still logs start and finish messages and returns `True` without performing any database work.  
- The function does not modify the connection string; it expects the caller to have set the appropriate `search_path` beforehand.

**Output Example**:  
```python
result = drop_unneeded_indexes(
    conn_str="host=pg host=5432 dbname=pedsnet user=loader password=secret",
    model_version="2.2.0",
    force=True,
    vocabulary=False
)
print(result)   # -> True
```
## FunctionDef add_vocab_indexes(conn_str, model_version, force, vocabulary)
**add_vocab_indexes**: The function of add_vocab_indexes is to create and execute the necessary CREATE INDEX statements for the vocabulary schema of a transformed PEDSnet database model.

**parameters**:  
· conn_str: database connection string (str) used to open a PostgreSQL connection for executing the generated SQL.  
· model_version: version identifier of the PEDSnet data model (str) that determines which tables and columns are indexed.  
· force (optional, default False): when True, benign errors such as “object already exists” or “object does not exist” are ignored; see issue #10 in the project repository. (bool)  
· vocabulary (optional, default True): when True the function generates indexes for the vocabulary tables; when False it would target the non‑vocabulary (clinical) tables. (bool)

**Code Description**:  
1. The function records the operation type as the literal string `'creation'`.  
2. It builds a logging dictionary (`log_dict`) that merges the supplied `model_version`, `vocabulary`, and `force` flags with connection‑metadata extracted by `get_conn_info_dict(conn_str)`. This dictionary is used for all subsequent log entries.  
3. An informational log entry is written indicating the start of the index‑creation operation, including the operation name (`creation`). The start time is captured with `time.time()`.  
4. A `StatementSet` instance (`stmts`) is created to hold the individual SQL statements that will be run in parallel.  
5. The helper `_special_add_indexes_sql(model_version, vocabulary)` is called. It returns a list of PostgreSQL `CREATE INDEX` strings appropriate for the supplied model version and the vocabulary flag. For each returned SQL string, a `Statement` object is instantiated and added to the `StatementSet`.  
6. `stmts.parallel_execute(conn_str)` launches a pool of worker processes (up to 25, or one per statement) that each execute one of the `Statement` objects against the database. The parallel execution handles logging from child processes via a dedicated logging thread.  
7. After all statements have finished, the function iterates over the `StatementSet`. For each `Statement` it calls `_check_stmt_err(stmt, force)`. This helper inspects `stmt.err`; if an error is present it is either ignored (when `force` is True and the error corresponds to a benign “does not exist” or “already exists” condition) or re‑raised, causing the whole function to abort. Any unexpected error is logged with the offending SQL and the elapsed time before the exception is propagated.  
8. If no exception is raised, a final informational log entry records the successful completion of the index‑creation step together with the elapsed time calculated by `secs_since(start_time)`.  
9. The function returns the boolean value `True` to signal successful execution.

**Reference Relationships**:  
- **Callers**: The primary caller is `run_vocab_indexes` in `pedsnetdcc/transform_runner.py`. That routine prepares the overall vocabulary‑index update workflow, invokes `add_vocab_indexes` to add the required indexes, and then calls `drop_vocab_unneeded_indexes` to clean up obsolete ones. No other functions in the repository directly call `add_vocab_indexes`.  
- **Callees**:  
  * `_special_add_indexes_sql` – generates the raw CREATE INDEX DDL strings based on the model version and the `vocabulary` flag.  
  * `Statement` – wraps each generated SQL string so that it can be executed and its result metadata captured.  
  * `StatementSet` – aggregates the `Statement` objects and provides the `parallel_execute` method for concurrent execution.  
  * `_check_stmt_err` – validates each executed statement, applying the `force` logic to suppress benign errors.  
  * `secs_since` – formats the elapsed time for logging.  
  * Utility functions `combine_dicts` and `get_conn_info_dict` – used to build structured log dictionaries containing both operational context and connection details.  

**Note**:  
- The function always generates index statements for the vocabulary schema because the default value of `vocabulary` is True; callers must explicitly pass `vocabulary=False` if they intend to target the clinical tables.  
- Errors that are not classified as “object does not exist” (SQLSTATE 42P01) for DROP statements or “object already exists” (SQLSTATE 42P07) for CREATE statements are never ignored, even when `force=True`.  
- The parallel execution pool size is automatically limited to a maximum of 25 worker processes; this limit cannot be overridden from within `add_vocab_indexes`.  
- Logging is performed using the module‑level `logger`; child‑process logs are routed through a multiprocessing queue to avoid interleaved output.  
- The function returns `True` only after all statements have been executed without raising an exception; it does not return any information about the created indexes themselves.  

**Output Example**:  
When called with a valid connection string, model version “2.3.0”, and default flags, the function completes successfully and returns:

```python
result = add_vocab_indexes(conn_str, "2.3.0")
print(result)   # True
```
## FunctionDef drop_vocab_unneeded_indexes(conn_str, model_version, force, vocabulary)
**drop_vocab_unneeded_indexes**: The function of drop_vocab_unneeded_indexes is to drop indexes that are not required for the vocabulary schema of a transformed PEDSnet data model.

**parameters**:  
· conn_str (str): PostgreSQL connection string used to create database connections.  
· model_version (str): Identifier of the PEDSnet model version (e.g., “2.3.0”).  
· force (bool, optional, default False): When True, benign errors such as “object does not exist” are ignored; otherwise any error aborts the operation.  
· vocabulary (bool, optional, default True): Determines whether the generated statements target the vocabulary schema (True) or the clinical (non‑vocabulary) schema (False).  

**Code Description**:  
1. **Operation label** – The variable `operation` is set to the literal string `'removal'`; it is used only for log messages.  
2. **Logging preparation** –  
   * `combine_dicts` merges a dictionary containing the function arguments (`model_version`, `vocabulary`, `force`) with the connection‑info dictionary returned by `get_conn_info_dict(conn_str)`.  
   * The combined dictionary (`log_dict`) is used to enrich all subsequent log entries with context such as user, host, database name, and search_path.  
   * An INFO‑level log entry records the start of the vocabulary‑unneeded‑index removal, including the operation label.  
   * `start_time` captures the current epoch time for later elapsed‑time reporting.  
3. **Statement collection** –  
   * A new `StatementSet` instance (`stmts`) is created to hold the individual DROP INDEX statements.  
   * The helper `_special_drop_indexes_sql(model_version, vocabulary)` generates a list of raw SQL strings that drop the indexes identified for the given model version and schema type.  
   * For each SQL string, a `Statement` object is instantiated (`Statement(stmt)`) and added to the `StatementSet` via `stmts.add`. The `Statement` class encapsulates the SQL, a default message, and execution metadata.  
4. **Parallel execution** – `stmts.parallel_execute(conn_str)` runs all collected statements concurrently in separate worker processes. The `StatementSet.parallel_execute` method handles pool sizing (up to 25 workers), task/result/log queues, and collection of the executed `Statement` objects.  
5. **Error handling** – After execution finishes, the function iterates over each `Statement` in `stmts`. For each statement it calls `_check_stmt_err(stmt, force)`.  
   * `_check_stmt_err` inspects `stmt.err`. If the error is `None`, the statement succeeded.  
   * If an error is present, the function determines whether it is a benign “object does not exist” error for a DROP statement (or “object already exists” for a CREATE statement) and, when `force` is True, suppresses it.  
   * If the error is not benign or `force` is False, the error is logged at ERROR level and re‑raised, causing the surrounding try/except block to abort further processing.  
6. **Final logging** – Whether successful or aborted, the function logs a concluding INFO message that includes the elapsed time (`secs_since(start_time)`) and the operation label.  
7. **Return value** – If the function reaches the end without raising an exception, it returns `True` to indicate successful completion.  

**Reference relationships**:  
* **Callers** – `run_vocab_indexes` in `pedsnetdcc/transform_runner.py` invokes `drop_vocab_unneeded_indexes` after adding necessary vocabulary indexes. The caller supplies the same connection string, model version, and optional `force` flag, and expects a boolean success indicator.  
* **Callees** –  
  * `_special_drop_indexes_sql` (same module) – produces the list of DROP INDEX statements based on the model version and the `vocabulary` flag.  
  * `Statement` (pedsnetdcc/db.py) – wraps each generated SQL string for execution.  
  * `StatementSet` (pedsnetdcc/db.py) – aggregates the `Statement` objects and runs them in parallel.  
  * `_check_stmt_err` (same module) – validates each executed statement and applies the `force` logic.  
  * Utility functions `combine_dicts`, `get_conn_info_dict`, and `secs_since` – support logging and elapsed‑time calculation.  
  * The module‑level `logger` records start, error, and completion messages.  

**Note**:  
- The function always generates DROP statements; the `vocabulary` parameter only selects the appropriate Transform class inside `_special_drop_indexes_sql`.  
- Because execution is parallel, the order of statement execution is nondeterministic; however, each statement is independent (pure DDL) and does not rely on ordering.  
- Errors other than “object does not exist” for DROP or “object already exists” for CREATE are never suppressed, even when `force=True`.  
- The function assumes that the connection string is valid and that the target database schema matches the supplied `model_version`. Invalid connection strings will cause a connection error in `Statement.execute`, which propagates as a `DatabaseError`.  
- The maximum number of parallel workers is limited to 25 by `StatementSet.parallel_execute`; for more than 25 statements the pool size is capped unless explicitly overridden (not applicable here because the default is used).  

**Output Example**:  
```python
result = drop_vocab_unneeded_indexes(
    conn_str="host=pghost port=5432 dbname=pedsnet user=loader options='-c search_path=vocab'",
    model_version="2.3.0",
    force=True,
    vocabulary=True
)
print(result)   # -> True
```
## FunctionDef _special_drop_indexes_sql(model_version, vocabulary)
**_special_drop_indexes_sql**: The function of _special_drop_indexes_sql is to generate PostgreSQL DROP INDEX statements for a transformed PEDSnet schema based on the supplied model version and vocabulary flag.  

**parameters**:  
· model_version: a string identifying the PEDSnet model version (e.g., “2.2.0”).  
· vocabulary: a boolean that determines whether the generated statements target vocabulary tables (True) or non‑vocabulary (clinical) tables (False).  

**Code Description**:  
1. The function selects the appropriate Transform class that knows how to describe the indexes to be dropped.  
   * If *vocabulary* is True, the tuple *(VocabularyDropIndexTransform,)* is used.  
   * Otherwise, the tuple *(DropIndexTransform,)* is used.  
2. It obtains a fresh, unmodified SQLAlchemy MetaData object for the requested model version by calling **stock_metadata(model_version)**. This metadata contains the complete table and column definitions for the PEDSnet data model.  
3. The helper **_special_indexes_from_metadata** is invoked with three arguments: the metadata, the chosen Transform tuple, and the *vocabulary* flag.  
   * Inside that helper, each Transform’s class method **modify_metadata(metadata)** is called.  
   * For the selected Transform, **modify_metadata** walks the metadata tables, applies **modify_table** to the relevant tables, and returns a list of SQLAlchemy **Index** objects that represent the existing indexes slated for removal.  
4. The list of Index objects returned by the helper is stored in the local variable *indexes*.  
5. For each Index object *x* in *indexes*, a **sqlalchemy.schema.DropIndex** instance is created (assigned to *func*). The DropIndex object is compiled to a PostgreSQL‑specific SQL string using the PostgreSQL dialect. The resulting string is stripped of leading whitespace with **lstrip()** and converted to a plain Python string.  
6. The function returns a list of those compiled DROP INDEX statements.  

**Reference Relationships**:  
*Callers* – The function is called by the high‑level utilities **drop_unneeded_indexes** and **drop_vocab_unneeded_indexes** (both defined in *pedsnetdcc/indexes.py*). Those utilities iterate over the returned SQL strings, wrap each in a **Statement** object, and execute them in parallel against a target database.  

*Callees* –  
- **stock_metadata** (pedsnetdcc/utils.py) supplies the base MetaData for the specified model version.  
- **_special_indexes_from_metadata** (pedsnetdcc/indexes.py) orchestrates the Transform classes and returns the Index objects.  
- **DropIndexTransform** (pedsnetdcc/drop_index_transform.py) and **VocabularyDropIndexTransform** (pedsnetdcc/vocabulary_drop_index_transform.py) provide the concrete logic that identifies which indexes should be dropped for clinical and vocabulary schemas, respectively.  
- **sqlalchemy.schema.DropIndex** and the PostgreSQL dialect are used to render each Index object into a valid DROP INDEX statement.  

**Note**:  
- The function always produces DROP statements; the docstring mentions an *add* option, but the implementation does not support it.  
- The *vocabulary* flag does not affect the internal logic of **_special_indexes_from_metadata**; it only determines which Transform class is selected.  
- The returned SQL strings are already formatted for PostgreSQL; they do not include a terminating semicolon, which is added later by the calling code if needed.  
- If a Transform’s **modify_metadata** method returns an empty list (e.g., when no indexes match the criteria for the given model version), the function returns an empty list, and the caller will execute no statements.  

**Output Example**:  
```python
[
    "DROP INDEX idx_adt_next_id",
    "DROP INDEX idx_adt_prior_id",
    "DROP INDEX idx_fact_relationship_id_1",
    "DROP INDEX idx_fact_relationship_id_2",
    "DROP INDEX idx_procedure_provider_id"
]
```
## FunctionDef _special_indexes_from_metadata(metadata, transforms, vocabulary)
**_special_indexes_from_metadata**: The function of _special_indexes_from_metadata is to collect and return a list of SQLAlchemy Index objects produced by a sequence of Transform classes applied to a given metadata object.

**parameters**  
· **metadata** – a SQLAlchemy MetaData instance that contains the full schema definition (tables, columns, etc.).  
· **transforms** – a list (or tuple) of Transform classes. Each class must implement a class‑method `modify_metadata(metadata)` that returns a list of Index objects.  
· **vocabulary** – a boolean flag indicating whether the caller is interested in indexes for vocabulary tables (`True`) or for non‑vocabulary (clinical) tables (`False`). The flag is passed through to the caller but is not used directly inside this function.

**Code Description**  
1. An empty list named `indexes` is created to accumulate the Index objects returned by each Transform.  
2. The function iterates over the supplied `transforms` collection. For each Transform class `t`, it invokes `t.modify_metadata(metadata)`.  
3. The result of the call – a list of Index objects – is extended onto the `indexes` list using `list.extend`.  
4. After all Transform classes have been processed, the accumulated `indexes` list is returned to the caller.

**Reference Relationships**  
- **Callers**:  
  * `_special_drop_indexes_sql` and `_special_add_indexes_sql` (both defined in `pedsnetdcc/indexes.py`) call `_special_indexes_from_metadata`. They supply the stock metadata for a specific model version, select the appropriate Transform class tuple (e.g., `DropIndexTransform` for dropping non‑vocabulary indexes, `VocabularyAddIndexTransform` for adding vocabulary indexes), and pass the `vocabulary` flag. The list of Index objects returned by this function is then turned into SQL statements (`DROP INDEX` or `CREATE INDEX`).  
- **Callees**:  
  * Each Transform class listed in `transforms` must provide a `modify_metadata` class‑method. In the current project the relevant implementations are:  
    - `DropIndexTransform.modify_metadata` – produces indexes to be dropped from clinical tables.  
    - `AddIndexTransform.modify_metadata` – produces indexes to be added to clinical tables (not shown but analogous).  
    - `VocabularyDropIndexTransform.modify_metadata` – produces indexes to be dropped from vocabulary tables.  
    - `VocabularyAddIndexTransform.modify_metadata` – produces indexes to be added to vocabulary tables.  
  The function does not inspect the `vocabulary` flag itself; it simply forwards the metadata to each Transform, allowing each Transform’s internal logic (which checks `VOCAB_TABLES`) to decide which tables to act upon.

**Note**  
- The function is a thin orchestration helper; it does not modify the supplied `metadata` object. All transformation logic resides in the `modify_metadata` methods of the Transform classes.  
- The `vocabulary` argument is retained for API compatibility with the callers; it is not used inside the function body.  
- The order of Index objects in the returned list follows the order of the `transforms` iterable and the order in which each Transform’s `modify_metadata` returns its indexes.  
- If a Transform returns an empty list, no indexes are added for that Transform.  

**Output Example**  
```python
[
    sqlalchemy.Index('idx_patient_id', Table('patient', metadata, ...).c.patient_id),
    sqlalchemy.Index('idx_encounter_date', Table('encounter', metadata, ...).c.encounter_date),
    sqlalchemy.Index('idx_concept_id', Table('concept', metadata, ...).c.concept_id)
]
```
## FunctionDef _special_add_indexes_sql(model_version, vocabulary)
**_special_add_indexes_sql**: The function of _special_add_indexes_sql is to generate a list of PostgreSQL CREATE INDEX statements for either clinical (non‑vocabulary) tables or vocabulary tables of a given PEDSnet model version.  

**parameters**:  
· **model_version** – a string identifying the PEDSnet data‑model version (e.g., “2.3.0”).  
· **vocabulary** – a boolean flag; when True the function creates statements for vocabulary tables, otherwise for the clinical site tables.  

**Code Description**:  
1. The function selects the appropriate Transform class based on the **vocabulary** flag:  
   * If **vocabulary** is True, the tuple `transforms` contains only `VocabularyAddIndexTransform`.  
   * If **vocabulary** is False, the tuple contains only `AddIndexTransform`.  
2. It obtains a fresh, unmodified SQLAlchemy MetaData object that represents the stock PEDSnet schema for the supplied **model_version** by calling `stock_metadata(model_version)`. This metadata includes all tables, columns, and constraints defined by the remote data‑model service.  
3. The helper `_special_indexes_from_metadata` is invoked with three arguments: the stock metadata, the selected `transforms` tuple, and the original **vocabulary** flag. Inside that helper, each Transform class’s `modify_metadata` class‑method is called; the method iterates over the metadata, creates `sqlalchemy.Index` objects for the columns specified by the Transform, and returns a flat list of those Index objects.  
4. For every `Index` object returned, the function creates a `sqlalchemy.schema.CreateIndex` compiler (`func = sqlalchemy.schema.CreateIndex`). It then compiles the Index into a PostgreSQL‑specific DDL string using `func(x).compile(dialect=sqlalchemy.dialects.postgresql.dialect())`. The resulting string is stripped of leading whitespace with `lstrip()`.  
5. The list comprehension collects all compiled strings and returns them as a plain Python list of SQL statements.  

**Reference Relationships**:  
*Callers* – The primary caller is `add_vocab_indexes` (defined in the same module). That function iterates over the list returned by `_special_add_indexes_sql`, wraps each statement in a `Statement` object, and executes them in parallel against a target database. No other functions in the repository invoke this helper directly.  

*Callees* –  
- `stock_metadata` supplies the base MetaData for the requested model version.  
- `_special_indexes_from_metadata` orchestrates the application of Transform classes to the metadata and aggregates the resulting Index objects.  
- `AddIndexTransform` (for clinical tables) and `VocabularyAddIndexTransform` (for vocabulary tables) each implement `modify_metadata` to produce the appropriate Index objects.  
- `sqlalchemy.schema.CreateIndex` and the PostgreSQL dialect compiler are used to render each Index into a CREATE INDEX DDL string.  

**Note**:  
- The function does not perform any database I/O; it only produces SQL text. Execution is delegated to the caller.  
- The **vocabulary** flag determines which set of column‑to‑index mappings are consulted (clinical mappings in `AddIndexTransform.create_by_table` vs. vocabulary mappings in `VocabularyAddIndexTransform.add_by_table`).  
- The returned statements are already formatted for PostgreSQL; they omit any leading whitespace but retain the full `CREATE INDEX … ON … (column)` syntax.  
- Because the Transform classes generate Index objects in‑memory, the function is safe to call repeatedly; each call creates a fresh metadata instance, ensuring no residual state from previous invocations.  

**Output Example**:  
```python
[
    "CREATE INDEX per_bi_ae1fd5b22b92397ca9_ix ON person (birth_datetime);",
    "CREATE INDEX per_ec_ae1fd5b22b92397ca9_ix ON person (ethnicity_concept_id);",
    "CREATE INDEX per_rc_ae1fd5b22b92397ca9_ix ON person (race_concept_id);",
    "CREATE INDEX per_gc_ae1fd5b22b92397ca9_ix ON person (gender_concept_id);",
    "CREATE INDEX idx_concept_name ON concept (concept_name);",
    "CREATE INDEX idx_standard_concept ON concept (standard_concept);"
]
```
