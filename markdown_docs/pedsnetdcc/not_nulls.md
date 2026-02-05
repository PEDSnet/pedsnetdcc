## FunctionDef _not_null_columns_from_model_version(model_version, vocabulary)
**_not_null_columns_from_model_version**: The function of _not_null_columns_from_model_version is to return a list of SQLAlchemy column objects that are defined as NOT NULL (and are not primary‑key columns) for a given PEDSnet model version, optionally limited to vocabulary or non‑vocabulary tables.

**parameters**:  
· **model_version** (str): The PEDSnet model version identifier (e.g., “2.2.0”).  
· **vocabulary** (bool, optional, default False): When True, only columns belonging to tables listed in the constant **VOCAB_TABLES** are considered; when False, columns from all other tables are considered.

**Code Description**:  
1. The function obtains a fresh, unmodified SQLAlchemy **MetaData** object that represents the complete schema for the requested **model_version** by calling **stock_metadata(model_version)**. This metadata contains a dictionary **metadata.tables** where each key is a table name and each value is a **Table** object with its column definitions.  
2. An empty Python list named **columns** is created to collect the qualifying column objects.  
3. The function iterates over every table in **metadata.tables**:  
   - The conditional expression `((vocabulary and name not in VOCAB_TABLES) or (not vocabulary and name in VOCAB_TABLES))` implements the vocabulary filter. If the current table does not match the requested filter, the loop continues to the next table.  
   - For each remaining table, the inner loop iterates over the column objects (`table.c.values()`).  
   - A column is added to **columns** only if both of the following are true:  
     * `column.nullable` is **False** (the column is defined as NOT NULL in the model), and  
     * `column.primary_key` is **False** (primary‑key columns are excluded because they are handled separately elsewhere in the codebase).  
4. After all tables have been examined, the list **columns**—now containing every non‑primary‑key, NOT NULL column that matches the vocabulary filter—is returned to the caller.

**Reference Relationships**:  
- **Callers**: The function is invoked by **set_not_nulls** and **drop_not_nulls** (both located in *pedsnetdcc/not_nulls.py*). These higher‑level utilities use the returned column list to generate ALTER TABLE statements that either set or drop the NOT NULL constraint on each column.  
- **Callee**: The only external call made inside the function is to **stock_metadata**, which supplies the canonical, version‑specific schema needed for column inspection. No other project components are accessed directly.

**Note**:  
- The function does not cache the metadata; each invocation creates a new **MetaData** instance via **stock_metadata**, which may involve a network request to the data‑model service. Repeated calls with the same **model_version** will therefore incur repeated retrieval overhead.  
- Primary‑key columns are deliberately omitted because they are either inherently NOT NULL or are managed by separate migration logic.  
- The constant **VOCAB_TABLES** must be defined elsewhere in the package; if it is missing or does not contain the expected table names, the vocabulary filtering logic may behave unexpectedly.  
- The returned list contains raw SQLAlchemy **Column** objects; callers should treat them as read‑only descriptors of the schema and avoid mutating them.

**Output Example**:  
A typical call for a non‑vocabulary request might produce a list such as:

```python
[
    Column('gender_concept_id', Integer(), nullable=False, primary_key=False),
    Column('race_concept_id', Integer(), nullable=False, primary_key=False),
    Column('ethnicity_concept_id', Integer(), nullable=False, primary_key=False),
    Column('measurement_concept_id', Integer(), nullable=False, primary_key=False),
    ...
]
```

Each element in the list is a SQLAlchemy **Column** instance representing a NOT NULL column from the PEDSnet model version specified.
## FunctionDef set_not_nulls(conn_str, model_version, vocabulary)
**set_not_nulls**: The function of set_not_nulls is to set the appropriate columns to NOT NULL for a given PEDSnet model version.

**parameters**:  
· **conn_str** (str): database connection string used to create connections for statement execution.  
· **model_version** (str): the PEDSnet model version whose schema defines the columns that must be set NOT NULL.  
· **vocabulary** (bool, optional, default False): when True, the function operates on vocabulary tables only; when False, it operates on all non‑vocabulary tables.

**Code Description**:  
1. **Logging start** – The function logs an INFO message containing the model version and the vocabulary flag, then records the current time (`starttime`).  
2. **Determine target columns** – It calls the internal helper `_not_null_columns_from_model_version(model_version, vocabulary)`. This helper loads the SQLAlchemy metadata for the specified model version (`stock_metadata`) and returns a list of column objects that are defined as NOT NULL in the model (excluding primary‑key columns) and that satisfy the vocabulary filter.  
3. **Prepare statements** – A `StatementSet` instance (`stmts`) is created to hold the ALTER TABLE commands. For each column returned in step 2, a `Statement` object is instantiated with the SQL template `SET_NOT_NULL` (defined elsewhere in the module) where `{table}` and `{column}` are substituted by the column’s table name and column name. Each statement is added to the set via `stmts.add`.  
4. **Parallel execution** – The entire set of ALTER TABLE statements is executed in parallel by invoking `stmts.parallel_execute(conn_str)`. `StatementSet.parallel_execute` spawns a pool of worker processes (up to 25 or the number of statements) that each call `Statement.execute` on the supplied connection string. Execution is performed with autocommit, and any errors are captured in the `err` attribute of each `Statement`.  
5. **Error handling** – After execution, the function iterates over the statements in the set and calls `check_stmt_err(stmt, 'setting columns not null')`. `check_stmt_err` inspects `stmt.err`; if an error is present it logs an ERROR message and raises a `DatabaseError`. This ensures that any failure in setting a NOT NULL constraint aborts the function.  
6. **Logging end** – Upon successful completion of all statements, an INFO message is logged with the elapsed time, calculated by `secs_since(starttime)`.  
7. **Return value** – If no exception is raised, the function returns `True` to indicate success.

**Reference Relationships**:  
- **Callees**:  
  * `_not_null_columns_from_model_version` – provides the list of columns to be altered.  
  * `Statement` – encapsulates each ALTER TABLE command.  
  * `StatementSet.add` – adds each `Statement` to the collection.  
  * `StatementSet.parallel_execute` – runs the statements concurrently.  
  * `check_stmt_err` – validates each statement for execution errors.  
  * `secs_since` – formats the elapsed time for logging.  
- **Callers**:  
  * `merge_site_data` (both `merge_site_data` and `merge_data_to_schema`) invoke `set_not_nulls` after creating tables, unless the `nonull` flag is set.  
  * `run_transformation` calls `set_not_nulls` to apply NOT NULL constraints to transformed tables.  
  * `run_subset_by_cohort` calls `set_not_nulls` on the subset schema when the `nonull` flag is not set.  
  * Unit test `NotNulls.test_not_nulls` uses `set_not_nulls` to verify that NOT NULL constraints can be reapplied after being dropped.

**Note**:  
- The function does not accept a `force` parameter; it always attempts to set NOT NULL constraints and will raise an exception on any failure.  
- Primary‑key columns are never included because they are inherently NOT NULL and are handled elsewhere.  
- The `vocabulary` flag must be used consistently with the same flag in `drop_not_nulls`; mismatched usage may leave columns in an unexpected nullable state.  
- Because the ALTER TABLE statements are executed in parallel, the order of execution is nondeterministic; however, each statement is independent, so this does not affect correctness.  
- The function relies on the global `SET_NOT_NULL` SQL template; any change to that template must preserve the `{table}` and `{column}` placeholders.  
- All logging is performed through the module‑level `logger`; ensure that the logging configuration captures INFO and ERROR levels for full visibility.

**Output Example**:  
```python
result = set_not_nulls(conn_str, '2.3.0')
print(result)   # -> True
```
## FunctionDef drop_not_nulls(conn_str, model_version, vocabulary)
**drop_not_nulls**: The function of drop_not_nulls is to remove NOT NULL constraints from the appropriate columns of a PEDSnet database for a given model version.

**parameters**:  
· **conn_str** (str): database connection string used to create a PostgreSQL connection for executing the ALTER TABLE statements.  
· **model_version** (str): identifier of the PEDSnet data model version (e.g., “2.2.0”) whose schema is inspected to determine which columns currently have NOT NULL constraints.  
· **vocabulary** (bool, optional, default False): when False, only non‑vocabulary tables are processed; when True, only tables listed in the constant **VOCAB_TABLES** are processed.

**Code Description**:  
1. **Logging start** – The function logs an INFO‑level message that it is beginning the drop‑not‑null operation, including the supplied model version and the vocabulary flag. The current epoch time is stored in `starttime` for later elapsed‑time reporting.  
2. **Determine target columns** – It calls the internal helper `_not_null_columns_from_model_version(model_version, vocabulary)`. This helper loads the SQLAlchemy metadata for the requested model version, iterates over every table, applies the vocabulary filter, and collects all columns that are defined as NOT NULL (`column.nullable is False`) but are not primary‑key columns. The result is a list of SQLAlchemy `Column` objects, each exposing the table name (`column.table`) and column name (`column.name`).  
3. **Prepare statements** – An empty `StatementSet` instance (`stmts`) is created. For each column returned in step 2, a `Statement` object is instantiated with an ALTER TABLE command that drops the NOT NULL constraint:  
   ```python
   DROP_NOT_NULL.format(table=column.table, column=column.name)
   ```  
   The formatted SQL string is added to the `StatementSet` via `stmts.add`. No custom message is supplied, so the default “executing SQL” is used.  
4. **Parallel execution** – The entire set of ALTER TABLE statements is executed in parallel by invoking `stmts.parallel_execute(conn_str)`. `StatementSet.parallel_execute` spawns a pool of worker processes (up to 25, or one per statement if fewer) that each call `Statement.execute`. Each worker opens its own database connection, runs the ALTER TABLE command in autocommit mode, captures any error in `stmt.err`, and places the completed `Statement` on a result queue.  
5. **Error handling** – After the parallel run returns, the function iterates over every `Statement` in `stmts` and calls `check_stmt_err(stmt, 'dropping column not nulls')`. `check_stmt_err` inspects `stmt.err`; if an error is present it logs an ERROR message (including elapsed time) and raises a `DatabaseError`. This ensures that any failure aborts the function and propagates the exception to the caller.  
6. **Logging completion** – If no errors were raised, an INFO‑level log entry records that the operation finished, together with the elapsed time calculated by `secs_since(starttime)`.  
7. **Return value** – The function returns `True` to indicate successful completion.

**Reference relationships**:  
- **Callees**:  
  * `_not_null_columns_from_model_version` – provides the list of columns to be altered.  
  * `Statement` – encapsulates each ALTER TABLE command.  
  * `StatementSet` – aggregates the statements and runs them in parallel.  
  * `StatementSet.parallel_execute` – performs the multiprocessing execution.  
  * `check_stmt_err` – validates each statement for errors after execution.  
  * `secs_since` – formats the elapsed time for log messages.  
- **Callers**:  
  * Unit test `pedsnetdcc/tests/not_nulls_test.py.NotNulls.test_not_nulls` invokes `drop_not_nulls` twice (once for non‑vocab tables, once with `vocabulary=True`).  
  * The higher‑level workflow in `pedsnetdcc/not_nulls.py` may also call `drop_not_nulls` directly from command‑line entry points or other migration scripts.  

**Note**:  
- The function only drops NOT NULL constraints; it does not modify column data types or other attributes.  
- Primary‑key columns are deliberately excluded because they are inherently NOT NULL and are handled by separate migration logic.  
- The ALTER TABLE statements are executed with autocommit, so each change is committed immediately; partial failures will leave some columns altered while others remain unchanged. The subsequent error handling will raise an exception, but already‑executed statements are not rolled back.  
- Because execution is parallel, the order of statement execution is nondeterministic; however, dropping NOT NULL constraints does not depend on ordering.  
- The function assumes that the constant `DROP_NOT_NULL` is defined elsewhere in the module (typically as `"ALTER TABLE {table} ALTER COLUMN {column} DROP NOT NULL"`). If this template is missing or malformed, a `KeyError` or SQL syntax error will be raised.  
- The caller must ensure that the database user identified by `conn_str` has sufficient privileges to alter table definitions.  

**Output Example**:  
```python
result = drop_not_nulls("postgresql://user:pwd@host/dbname", "2.2.0")
print(result)   # True
```
