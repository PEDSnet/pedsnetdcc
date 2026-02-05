## FunctionDef _foreign_keys_from_model_version(model_version, vocabulary, skip_meas_org_meas)
**_foreign_keys_from_model_version**: The function of _foreign_keys_from_model_version is to return a list of SQLAlchemy foreign‑key constraint objects that belong to a specific PEDSnet model version, optionally filtered by vocabulary tables or by a special measurement‑organism rule.

**parameters**:  
· model_version (str): The PEDSnet model version identifier (e.g., “2.2.0”).  
· vocabulary (bool, optional): When True, only foreign keys defined on tables listed in the constant VOCAB_TABLES are returned; when False (default), foreign keys on all non‑vocabulary tables are returned.  
· skip_meas_org_meas (bool, optional): When True and the current table being inspected is measurement_organism, the foreign‑key that points from measurement_organism to measurement is omitted. This flag is used only when processing data tables (vocabulary=False).

**Code Description**:  
1. The function obtains a fresh SQLAlchemy MetaData object that represents the *stock* (untransformed) PEDSnet schema for the requested version by calling **stock_metadata(model_version)**. This call contacts the PEDSnet data‑model service and populates the MetaData with tables, columns, and constraints.  
2. An empty list named foreign_keys is created to collect the matching constraints.  
3. The function iterates over every table in metadata.tables (a mapping of table name → Table object).  
   - If vocabulary is True, the table name must be present in the module‑level constant VOCAB_TABLES to be considered.  
   - If vocabulary is False, the table name must **not** be in VOCAB_TABLES.  
4. For each qualifying table, the function examines the table’s constraints collection. Every constraint that is an instance of sqlalchemy.ForeignKeyConstraint is a candidate foreign‑key definition.  
5. When skip_meas_org_meas is True, an additional check is performed: if the current table name is ‘measurement_organism’ and the first element of the constraint’s elements list references the ‘measurement’ table, that constraint is skipped (continue). This prevents the creation of the foreign key fpk_meas_org_meas in certain migration scenarios.  
6. All remaining foreign‑key constraints are appended to the foreign_keys list.  
7. After the loop finishes, the list of collected sqlalchemy.ForeignKeyConstraint objects is returned to the caller.

**Reference Relationships**:  
- **Callee**: The function relies on **stock_metadata** (pedsnetdcc/utils.py) to retrieve the canonical schema for the given model version. No other internal helpers are invoked.  
- **Callers**:  
  * **add_foreign_keys** (pedsnetdcc/foreign_keys.py) calls _foreign_keys_from_model_version to obtain the constraints that must be added to a live database. The returned constraints are transformed into ADD CONSTRAINT SQL statements and executed in parallel.  
  * **drop_foreign_keys** (pedsnetdcc/foreign_keys.py) also calls _foreign_keys_from_model_version (to retrieve the same set of constraints) and then generates DROP CONSTRAINT SQL statements for removal. Both callers pass the vocabulary flag (and optionally skip_meas_org_meas for add) to control which subset of constraints is processed.  

Thus, _foreign_keys_from_model_version acts as the central extractor of foreign‑key metadata, feeding both creation and deletion workflows.

**Note**:  
- The function always creates a new MetaData instance; repeated calls for the same model version will re‑download the schema unless the caller caches the result.  
- The constant VOCAB_TABLES must be defined elsewhere in the module; it determines which tables are considered “vocabulary”.  
- The function returns raw ForeignKeyConstraint objects, not compiled SQL strings. Callers are responsible for converting them to appropriate DDL statements for the target database dialect (PostgreSQL in this project).  
- The skip_meas_org_meas parameter is relevant only when vocabulary=False; it has no effect when processing vocabulary tables.  

**Output Example**:  
A typical call for a non‑vocabulary extraction might return a list such as:

```
[
    ForeignKeyConstraint(['person_id'], ['person.person_id']),
    ForeignKeyConstraint(['visit_occurrence_id'], ['visit_occurrence.visit_occurrence_id']),
    ForeignKeyConstraint(['condition_concept_id'], ['concept.concept_id']),
    ...
]
```

When called with vocabulary=True, the list would contain constraints like:

```
[
    ForeignKeyConstraint(['vocabulary_concept_id'], ['concept.concept_id']),
    ForeignKeyConstraint(['domain_id'], ['domain.domain_id']),
    ...
]
```
## FunctionDef _check_stmt_err(stmt, force)
**_check_stmt_err**: The function checks a database statement for errors and optionally suppresses benign errors when forced.  

**parameters**:  
· stmt: an instance of `pedsnetdcc.db.Statement` representing the executed SQL statement.  
· force: a boolean flag indicating whether benign errors (such as trying to drop a non‑existent constraint or create an already existing one) should be ignored.  

**Code Description**:  
The function begins by inspecting the `err` attribute of the supplied `Statement` object. If `stmt.err` is `None`, the statement completed without error and the function returns immediately.  

It then determines the nature of the SQL command by searching the original SQL text (`stmt.sql`) for the substrings `'DROP CONSTRAINT'` and `'ADD CONSTRAINT'`. The presence of these substrings sets the local flags `dropping` and `creating`, respectively.  

Two specific PostgreSQL error conditions are identified using the error’s `pgcode` attribute together with `psycopg2_errorcodes.lookup`:  

1. **UNDEFINED_OBJECT** (error code 42704) – occurs when a DROP statement references a constraint that does not exist.  
2. **DUPLICATE_OBJECT** (error code 42710) – occurs when an ADD statement attempts to create a constraint that already exists.  

If the statement is a DROP and `force` is `True` while the error is `UNDEFINED_OBJECT`, the function treats the error as benign and returns without raising.  

Similarly, if the statement is an ADD and `force` is `True` while the error is `DUPLICATE_OBJECT`, the function also suppresses the error and returns.  

For any other error condition, or when `force` is `False`, the function re‑raises the original exception (`stmt.err`). This propagation allows calling code to handle unexpected failures, while still permitting intentional ignoring of known benign cases.  

The function is used by both `add_foreign_keys` and `drop_foreign_keys` in `foreign_keys.py`. After executing a batch of CREATE or DROP constraint statements in parallel, each caller iterates over the resulting `StatementSet` and invokes `_check_stmt_err` for every statement. By passing the caller’s `force` argument, the higher‑level functions can decide whether to tolerate “already exists” or “does not exist” errors, enabling idempotent operations when desired.  

**Note**:  
- The function relies on the `psycopg2_errorcodes` module; ensure it is imported where this function resides.  
- Only the two specific error codes are treated as benign; any other PostgreSQL error will be raised regardless of the `force` flag.  
- The function does not return a value; its purpose is side‑effect (raising or suppressing an exception).  

**Output Example**:  
When a DROP CONSTRAINT statement tries to remove a non‑existent constraint and `force=True`, the function returns silently (no exception). If `force=False`, the same situation raises a `DatabaseError` (or the original `psycopg2` exception) that can be caught by the caller.
## FunctionDef add_foreign_keys(conn_str, model_version, force, vocabulary, skip_meas_org_meas)
**add_foreign_keys**: The function of add_foreign_keys is to create foreign‑key constraints in a PEDSnet database for a specified model version.

**parameters**:  
· conn_str (str): PostgreSQL libpq connection string used to open a database connection.  
· model_version (str): Identifier of the PEDSnet data model version (e.g., “2.2.0”).  
· force (bool, optional, default False): When True, benign errors such as “constraint already exists” are ignored; otherwise any error raises an exception.  
· vocabulary (bool, optional, default False): If True, only foreign keys defined on vocabulary tables are processed; if False, foreign keys on all non‑vocabulary tables are processed.  
· skip_meas_org_meas (bool, optional, default False): When True and processing non‑vocabulary tables, the foreign‑key from `measurement_organism` to `measurement` (named `fpk_meas_org_meas`) is omitted.

**Code Description**:  
1. **Logging preparation** – The function builds a log dictionary that merges the model version, the `vocabulary` and `force` flags with connection‑information extracted by `get_conn_info_dict(conn_str)`. An INFO‑level log entry marks the start of foreign‑key creation and the current time is stored in `start_time`.  

2. **Determine required foreign keys** – It calls the internal helper `_foreign_keys_from_model_version(model_version, vocabulary, skip_meas_org_meas)`. This helper loads the canonical schema for the given model version via `stock_metadata`, iterates over all tables, filters by the `vocabulary` flag, and collects `sqlalchemy.ForeignKeyConstraint` objects. If `skip_meas_org_meas` is True, the constraint linking `measurement_organism` to `measurement` is excluded. The result is a list of foreign‑key constraint objects that need to be added.  

3. **Create executable statements** – An empty `StatementSet` (`stmts`) is instantiated. For each foreign‑key object in the list, the function:
   - Instantiates a PostgreSQL dialect (`sqlalchemy.dialects.postgresql.dialect()`).
   - Compiles an `AddConstraint` DDL expression for the foreign key into a raw SQL string (`sql`).
   - Wraps the SQL string in a `Statement` object (default message “executing SQL”) and adds it to the `StatementSet`.  

4. **Parallel execution** – The populated `StatementSet` is executed in parallel by invoking `stmts.parallel_execute(conn_str)`. This spawns a pool of worker processes (up to 25, or one per statement) that each call `Statement.execute`. The statements run concurrently, and each `Statement` object is updated with execution results, row counts, and any error captured in its `err` attribute.  

5. **Error handling** – After execution, the function iterates over the statements. For each statement it calls `_check_stmt_err(stmt, force)`. `_check_stmt_err` inspects `stmt.err`; if the error corresponds to a known benign condition (`UNDEFINED_OBJECT` for DROP or `DUPLICATE_OBJECT` for ADD) and `force` is True, the error is suppressed. Otherwise the original exception is re‑raised, causing `add_foreign_keys` to abort. If an error is raised, a fatal‑error log entry is written, the elapsed time is logged, and the exception propagates to the caller.  

6. **Completion logging** – If all statements succeed, an INFO‑level log entry records the successful completion and the elapsed time (formatted by `secs_since(start_time)`).  

7. **Return value** – The function returns `True` to indicate successful creation of all foreign keys.

**Reference relationships**:  
- **Callers**: `add_foreign_keys` is invoked by higher‑level workflows that need to enforce referential integrity, including `merge_site_data.merge_site_data`, `merge_site_data.merge_data_to_schema`, `run_subset_by_cohort.run_subset_by_cohort`, and `run_transformation.run_transformation`. In each case the caller prepares the connection string (often adjusting the `search_path` to include the DCC and vocabulary schemas) and passes the model version and optional flags.  
- **Callees**: The function relies on `_foreign_keys_from_model_version` to obtain the constraint metadata, `Statement` and `StatementSet` (from `pedsnetdcc.db`) to build and execute DDL statements, `_check_stmt_err` for conditional error suppression, `combine_dicts` and `get_conn_info_dict` for structured logging, and `secs_since` to format elapsed time.  

**Note**:  
- The function does not perform any schema validation before issuing the `ALTER TABLE … ADD CONSTRAINT` statements; it assumes the target tables already exist in the database.  
- When `force=False`, any attempt to add a constraint that already exists will raise a `psycopg2.ProgrammingError` (or a subclass) and abort the operation. Use `force=True` for idempotent runs.  
- The `vocabulary` flag limits the operation to tables listed in the module‑level constant `VOCAB_TABLES`; all other tables are ignored.  
- `skip_meas_org_meas` is intended for transformation pipelines where the measurement‑organism foreign key is added separately; setting it to True prevents duplicate constraint creation.  
- Parallel execution uses separate processes; all objects placed on the task queue must be picklable. `Statement` objects satisfy this requirement.  
- Logging includes connection details (user, host, dbname, search_path) merged with operation‑specific fields, providing consistent audit trails across the project.  

**Output Example**:  
```python
result = add_foreign_keys(conn_str, "2.2.0", force=True, vocabulary=False)
print(result)   # -> True
```
## FunctionDef drop_foreign_keys(conn_str, model_version, force, vocabulary)
**drop_foreign_keys**: The function of drop_foreign_keys is to remove foreign‑key constraints from a PEDSnet database for a specified model version.

**parameters**:  
· conn_str (str): database connection string in libpq format.  
· model_version (str): identifier of the PEDSnet model version whose foreign keys are to be dropped.  
· force (bool, optional, default False): when True, benign errors such as “constraint does not exist” are ignored; otherwise any error causes an exception.  
· vocabulary (bool, optional, default False): when True, foreign keys on vocabulary tables are dropped; when False, only foreign keys on non‑vocabulary (data) tables are processed.

**Code Description**:  
1. **Logging preparation** – The function builds a dictionary (`log_dict`) that merges the supplied `model_version`, `vocabulary`, and `force` flags with connection‑metadata obtained from `get_conn_info_dict(conn_str)`. An informational log entry marks the start of foreign‑key removal and the current time is recorded in `start_time`.  
2. **Retrieve foreign‑key objects** – It calls the internal helper `_foreign_keys_from_model_version(model_version, vocabulary)` which returns a list of SQLAlchemy `ForeignKeyConstraint` objects that belong to the requested model version and table set (vocabulary or data).  
3. **Create a StatementSet** – An empty `StatementSet` instance (`stmts`) is created to hold the individual DROP statements.  
4. **Generate DROP statements** – For each foreign‑key object `fkey` in the list, the function:
   - Instantiates a PostgreSQL dialect (`pg = sqlalchemy.dialects.postgresql.dialect()`).
   - Compiles a `sqlalchemy.schema.DropConstraint(fkey)` object to a raw SQL string using the dialect.
   - Wraps the SQL string in a `Statement` object and adds it to the `StatementSet`.  
   This results in one `Statement` per constraint, each containing a `DROP CONSTRAINT` command.  
5. **Parallel execution** – The entire set of statements is executed in parallel by invoking `stmts.parallel_execute(conn_str)`. The `StatementSet` implementation spawns a pool of worker processes (up to 25) that each call `Statement.execute`, sending the SQL to the database.  
6. **Error handling** – After execution, the function iterates over the statements in the set. For each statement it calls `_check_stmt_err(stmt, force)`.  
   - `_check_stmt_err` examines `stmt.err`. If the error corresponds to a known benign condition (UNDEFINED_OBJECT for a DROP or DUPLICATE_OBJECT for an ADD) and `force` is True, the error is suppressed.  
   - If the error is not benign or `force` is False, the error is logged (including the offending SQL) and re‑raised, causing `drop_foreign_keys` to abort.  
7. **Completion logging** – If all statements succeed (or benign errors are ignored), an informational log entry records the successful completion and the elapsed time calculated by `secs_since(start_time)`.  
8. **Return value** – The function returns `True` to indicate successful execution without raising an exception.

**Reference relationships**:  
- **Callees**:  
  * `_foreign_keys_from_model_version` – extracts the relevant `ForeignKeyConstraint` objects from the model metadata.  
  * `StatementSet.add` – inserts each generated `Statement` into the set.  
  * `StatementSet.parallel_execute` – runs all statements concurrently.  
  * `_check_stmt_err` – validates each statement’s error status according to the `force` flag.  
  * `combine_dicts`, `get_conn_info_dict`, `secs_since` – utilities used for logging.  
- **Callers**:  
  * `pedsnetdcc/tests/foreign_keys_test.py/ForeignKeysTest.test_drop` – verifies normal dropping and that a second drop raises an exception.  
  * `pedsnetdcc/tests/foreign_keys_test.py/ForeignKeysTest.test_drop_force` – checks that `force=True` suppresses the “constraint does not exist” error.  
  * `pedsnetdcc/tests/foreign_keys_test.py/ForeignKeysTest.test_add` – uses `drop_foreign_keys` as a prerequisite before adding foreign keys.  
  * `pedsnetdcc/transform_runner.py/undo_transformation` – calls `drop_foreign_keys` (with `force=True`) when rolling back a transformation.  

**Note**:  
- The function only generates DROP statements; it does not attempt to verify that the constraints exist before issuing the command. Errors from PostgreSQL are handled by `_check_stmt_err`.  
- When `vocabulary=False` (default) only non‑vocabulary tables are processed; to drop constraints on vocabulary tables set `vocabulary=True`.  
- The `force` flag should be used when the operation must be idempotent (e.g., in undo or repeatable scripts). Without `force`, attempting to drop a constraint that is already absent will raise a `DatabaseError`.  
- Parallel execution relies on the `Statement` and `StatementSet` classes being picklable; all statements must be simple DDL commands that can be run in autocommit mode.  
- The function returns a boolean `True` only after all statements have been processed without raising; any exception aborts the function and propagates to the caller.

**Output Example**:  
```python
result = drop_foreign_keys(conn_str, '2.2.0')
print(result)   # -> True
```
