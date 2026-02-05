## FunctionDef _check_stmt_err(stmt, force)
**_check_stmt_err**: The function of _check_stmt_err is to examine a database statement for errors and optionally suppress benign errors when the force flag is set.

**parameters**:  
· stmt: an instance of pedsnetdcc.db.Statement representing a SQL statement that has been executed.  
· force: a bool that indicates whether benign errors (such as “object does not exist” for DROP or “object already exists” for CREATE) should be ignored.

**Code Description**:  
The function begins by checking the err attribute of the supplied stmt. If stmt.err is None, the statement completed without error and the function returns immediately.  

It then determines the type of SQL command by inspecting the beginning of stmt.sql:  
- dropping is True when the statement starts with ‘DROP’.  
- creating is True when the statement starts with ‘CREATE’.  

Two specific PostgreSQL error conditions are detected using the psycopg2_errorcodes module:  

1. does_not_exist identifies error code 42P01 (UNDEFINED_OBJECT), which means the target object (e.g., a table) does not exist.  
2. already_exists identifies error code 42P07 (DUPLICATE_TABLE), which means the target object already exists.  

If the statement is a DROP and force is True and the error is does_not_exist, the function treats the error as benign and returns without raising.  

If the statement is a CREATE and force is True and the error is already_exists, the function also treats the error as benign and returns.  

For any other situation—i.e., when there is no error, the error is not one of the two recognized benign cases, or force is False—the function raises the original exception stored in stmt.err. This propagates database‑level errors such as DatabaseError or connection‑level errors like psycopg2.OperationalError.

**Relationship with Callers**:  
The function is invoked after parallel execution of a set of SQL statements in the following high‑level operations:  
- merge_site_data and merge_data_to_schema (use it to verify the CREATE statements that build merged tables).  
- clear_dcc_data and clear_schema_data (use it to verify the DROP statements that remove existing tables).  

In each caller, the function is wrapped in a try/except block. If _check_stmt_err raises an exception, the caller logs a fatal error, records the elapsed time, and re‑raises the exception, causing the overall operation to fail. When force is True, callers can suppress expected “object does not exist” or “object already exists” errors, allowing the process to continue (e.g., when restarting a partially completed merge).

**Note**:  
- The function does not return a value; it either completes silently or raises an exception.  
- Only the two specific PostgreSQL error codes are treated as benign; all other errors are considered fatal regardless of the force flag.  
- The caller must ensure that stmt.sql is a string beginning with the exact keywords ‘DROP’ or ‘CREATE’ for the suppression logic to apply.  
- The psycopg2_errorcodes module must be imported for the lookup to work; otherwise a missing attribute will cause an AttributeError.

**Output Example**:  
When a DROP statement fails because the table does not exist and force is True, the function returns None and no exception is raised. In all other cases, the function either returns None or raises the original stmt.err exception.
## FunctionDef merge_site_data(model_version, conn_str, addsites, force, notable, nolog, nopk, nonull, noidx, nodrop, norep, nofk, novac)
**merge_site_data**: The function of merge_site_data is to merge data from one or more site schemas into the central DCC schema, then optionally apply logging, primary‑key, NOT‑NULL, index, foreign‑key, and vacuum operations.

**parameters**  
· model_version (str): PEDSnet model version, e.g. “2.2.0”.  
· conn_str (str): libpq‑style PostgreSQL connection string; the user must have privileges to create tables, indexes, constraints, etc.  
· addsites (str): Comma‑separated list of site identifiers to be added to the merge operation.  
· force (bool, default False): When True, benign errors such as “object already exists” or “object does not exist” are ignored during DDL execution.  
· notable (bool, default False): When True, the creation of merged tables is skipped if they already exist.  
· nolog (bool, default False): When True, the tables are left in their default UNLOGGED state; otherwise they are set to LOGGED after the merge.  
· nopk (bool, default False): When True, primary‑key constraints are not added after the merge.  
· nonull (bool, default False): When True, NOT‑NULL constraints are not applied after the merge.  
· noidx (bool, default False): When True, index creation is skipped after the merge.  
· nodrop (bool, default False): When True, unused indexes are not dropped after the merge.  
· norep (bool, default False): When True, index‑replacement tables are not created after the merge.  
· nofk (bool, default False): When True, foreign‑key constraints are not added after the merge.  
· novac (bool, default False): When True, VACUUM (ANALYZE) is not run after the merge.

**Code Description**  
1. **Search path preparation** – The connection string is rewritten with `conn_str_with_search_path` so that the PostgreSQL `search_path` points to the DCC schema (`dcc_pedsnet`). This guarantees that all subsequent DDL statements affect the DCC schema.  

2. **Logging start** – A log dictionary is built by merging the model version, the `force` flag, and connection‑info obtained from `get_conn_info_dict`. The function logs an INFO message “starting merge site data” and records the start time (`start_time`).  

3. **Statement collection** – An empty `StatementSet` (`stmts`) is created to hold the CREATE‑TABLE statements that will be generated for each non‑vocabulary table.  

4. **Metadata acquisition and transformation** –  
   * `stock_metadata(model_version)` loads the canonical PEDSnet schema for the requested version.  
   * Each `Transform` subclass listed in the global `TRANSFORMS` modifies the metadata in‑place via `Transform.modify_metadata`. This step incorporates site‑specific or transformation‑specific column additions, index hints, etc.  

5. **Site list resolution** – The `addsites` argument is split on commas to produce `add_sites`. The final list of sites to be merged (`merge_sites`) is the union of the constant `SITES` (the sites already present in the environment) and `add_sites`, with any empty strings removed.  

6. **SQL generation for each table** – For every table in the metadata that is **not** a vocabulary table:  
   * The column list (`fields`) is built from the table’s column names.  
   * For a predefined set of fact tables, the column `visit_detail_id` is appended if it does not already exist.  
   * A UNION‑ALL SELECT fragment is generated for each site in `merge_sites`. The fragment selects the same fields from the site‑specific schema (`<site>_pedsnet`) and aliases the schema using `_sql_schema_tmpl`.  
   * All site fragments are concatenated, the trailing “UNION ALL” is stripped, and the final CREATE‑TABLE statement is assembled with `_sql_create_tmpl`.  
   * The statement is wrapped in a `Statement` object with a descriptive message and added to `stmts`.  

7. **Execution of merge statements** – If `notable` is False, the collected statements are executed in parallel via `stmts.parallel_execute(conn_str)`. After execution each statement is examined with `_check_stmt_err(stmt, force)`. If an error is not benign (or `force` is False) the error is logged and re‑raised, causing the whole merge to abort.  

8. **Post‑merge optional steps** –  
   * **Logging** – If `nolog` is False, `set_logged` is called to convert the newly created tables from UNLOGGED to LOGGED.  
   * **Primary keys** – If `nopk` is False, `add_primary_keys` adds primary‑key constraints.  
   * **NOT NULL** – If `nonull` is False, `set_not_nulls` sets NOT‑NULL constraints.  
   * **Indexes** – If `noidx` is False, `add_indexes` creates indexes.  
   * **Drop unused indexes** – If `nodrop` is False, `drop_unneeded_indexes` removes indexes that are no longer required.  
   * **Index‑replacement tables** – If `norep` is False, `create_index_replacement_tables` builds auxiliary tables used by certain concept‑group indexes.  
   * **Foreign keys** – The connection string is updated to include both the DCC and VOCAB schemas. If `nofk` is False, `add_foreign_keys` creates foreign‑key constraints.  
   * **Vacuum** – If `novac` is False, `vacuum` runs a VACUUM (and ANALYZE) on all DCC tables.  

9. **Final logging** – An INFO message “finished merge site data” is logged together with the elapsed time calculated by `secs_since(start_time)`.  

10. **Return value** – The function returns `True` when all steps complete without raising an exception; otherwise it propagates the exception and returns `False` is never reached.

**Reference relationships**  
*Callers* – The function is invoked by the CLI entry point `pedsnetdcc/main.py:merge`. When the `undo` flag is not set, `merge` calls `merge_site_data`; when `undo` is set it calls `clear_dcc_data` instead. The test suite `pedsnetdcc/tests/merge_test.py` also calls `merge_site_data` directly to verify that merged tables appear in the `dcc_pedsnet` schema.  

*Callees* – Within its body `merge_site_data` calls the following functions and objects:  
- `conn_str_with_search_path` (utils) – to set the search_path.  
- `combine_dicts`, `get_conn_info_dict` (utils) – for structured logging.  
- `stock_metadata` (utils) – to obtain the base MetaData.  
- `Transform.modify_metadata` (abstract_transform) – applied for each transform in `TRANSFORMS`.  
- `_sql_schema_tmpl`, `_sql_select_tmpl`, `_sql_create_tmpl` – string templates used to build the DDL.  
- `Statement`, `StatementSet` (db) – to encapsulate and execute the CREATE‑TABLE statements.  
- `_check_stmt_err` – to validate each statement’s execution result.  
- `set_logged`, `add_primary_keys`, `set_not_nulls`, `add_indexes`, `drop_unneeded_indexes`, `create_index_replacement_tables`, `add_foreign_keys`, `vacuum` – optional post‑merge steps controlled by the boolean flags.  
- `secs_since` (dict_logging) – to format elapsed time in log messages.  

**Note**  
- The function assumes that all site schemas already contain the transformed tables (the `transform` command must have been run for each site beforehand).  
- The list of tables to be merged excludes vocabulary tables; those remain in the separate `vocabulary` schema and are not touched by this function.  
- The `force` flag only suppresses the two PostgreSQL error codes handled by `_check_stmt_err` (object does not exist for DROP, object already exists for CREATE). All other errors abort the merge.  
- The `notable` flag skips the entire CREATE‑TABLE phase; it is useful when the tables already exist and the user only wants to run the subsequent steps.  
- The function builds the UNION‑ALL query by concatenating raw SQL fragments; therefore the order of sites in `merge_sites` determines the order of rows in the resulting DCC tables.  
- All DDL statements are executed with autocommit (isolation level 0) via the `Statement` class, which is required for commands such as CREATE TABLE, ALTER TABLE, and VACUUM.  

**Output Example**  
```python
result = merge_site_data(
    model_version='2.2.0',
    conn_str="host=pg port=5432 dbname=pedsnet user=loader password=secret",
    addsites='stlouis,midwest',
    force=True,
    notable=False,
    nolog=False,
    nopk=False,
    nonull=False,
    noidx=False,
    nodrop=False,
    norep=False,
    nofk=False,
    novac=False
)
print(result)   # -> True
```
## FunctionDef clear_dcc_data(model_version, conn_str, force)
**clear_dcc_data**: The function of clear_dcc_data is to remove all non‑vocabulary tables from the DCC schema, leaving the schema empty (except for any vocab tables that may exist).

**parameters**: The parameters of this Function.  
· **model_version** (str): PEDSnet model version, e.g. “2.2.0”.  
· **conn_str** (str): libpq connection string used to connect to the PostgreSQL database.  
· **force** (bool, optional, default False): When True, benign errors such as “object does not exist” for a DROP statement are ignored; otherwise any error aborts the operation.

**Code Description**:  
1. **Search‑path preparation** – The function first calls `conn_str_with_search_path(conn_str, DCC_SCHEMA)` to ensure that the connection string includes `search_path=DCC_SCHEMA`. This guarantees that all subsequent SQL statements are executed against the DCC schema regardless of the schema encoded in the original `conn_str`.  

2. **Logging start** – A dictionary containing the model version, the `force` flag, and connection details (obtained via `get_conn_info_dict`) is built with `combine_dicts`. The start of the task (“clear dcc data”) is logged at INFO level, and the current time is stored in `start_time` for later elapsed‑time reporting.  

3. **Statement collection** – An empty `StatementSet` (`stmts`) is created. The function obtains the canonical PEDSnet metadata for the supplied `model_version` by calling `stock_metadata(model_version)`. This metadata includes a mapping of all table names (`metadata.tables`).  

4. **Drop‑statement generation** – For every table name present in the metadata **except** those listed in `VOCAB_TABLES`, a DROP statement is constructed using the template `_sql_drop_tmpl` (which expands to `DROP TABLE IF EXISTS {table_name} CASCADE;`). Each resulting SQL string is wrapped in a `Statement` object and added to the `StatementSet`.  

5. **Parallel execution** – The entire set of DROP statements is executed in parallel by invoking `stmts.parallel_execute(conn_str)`. This method spawns a pool of worker processes (up to 25) that each call `Statement.execute` on their assigned statement, using the same connection string.  

6. **Error handling** – After parallel execution finishes, the function iterates over every `Statement` in `stmts`. For each statement it calls `_check_stmt_err(stmt, force)`.  
   * If the statement succeeded (`stmt.err` is None), the helper returns immediately.  
   * If an error occurred, `_check_stmt_err` determines whether the error is benign (object‑does‑not‑exist for a DROP) and whether `force` is True. If the error is not benign or `force` is False, the helper re‑raises the original exception.  
   * When an exception is caught, the function logs a fatal error (including the offending SQL and error message), logs the elapsed time, and re‑raises the exception, causing the caller to fail.  

7. **Logging completion** – If all statements are processed without raising, a final INFO log entry records the successful completion of the task together with the elapsed time.  

8. **Return value** – The function returns `True` to indicate successful clearing of the DCC data. Any failure results in an exception being raised before the return statement is reached.

**Reference relationships**:  
* **Callers** – `clear_dcc_data` is invoked from `pedsnetdcc/main.py/merge` when the user requests an undo operation (`undo=True`). It is also exercised directly in the unit test `pedsnetdcc/tests/merge_test.py/TestMerge.test_clear`, which creates a DCC schema, populates it with non‑vocab tables, and then calls `clear_dcc_data` to verify that the tables are dropped.  
* **Callees** – The function relies on several utility functions and classes:  
  * `conn_str_with_search_path` (adds the DCC schema to the connection string).  
  * `combine_dicts` and `get_conn_info_dict` (build structured log dictionaries).  
  * `stock_metadata` (provides the list of all tables for the given model version).  
  * `Statement` (encapsulates each DROP command).  
  * `StatementSet` (collects the statements and runs them in parallel).  
  * `_check_stmt_err` (examines each statement for errors and applies the `force` logic).  
  * `secs_since` (formats the elapsed time for log messages).  

**Note**:  
* The function drops **all** tables that are not part of `VOCAB_TABLES`. Vocabulary tables are intentionally preserved because they are required for downstream transformations.  
* The DROP statements use `IF EXISTS` and `CASCADE`, so they succeed even if dependent objects exist; however, any unexpected error (e.g., permission issues) will cause the function to abort unless `force` is True and the error matches the benign “object does not exist” case.  
* Because execution is parallel, each worker creates its own database connection; the connection string must be valid for all workers.  
* The function does not commit or rollback explicitly; each DROP statement runs in autocommit mode (handled by `Statement.execute`).  
* The return value is always `True` when the function reaches the end; callers should rely on exception handling rather than the boolean result to detect failure.  

**Output Example**:  
```python
result = clear_dcc_data('2.2.0', 'host=localhost port=5432 dbname=mydb user=loader password=secret', force=True)
print(result)   # -> True
```
## FunctionDef merge_data_to_schema(model_version, conn_str, schema, altname, skipsites, addsites, force, notable, nolog, nopk, nonull, noidx, nodrop, norep, nofk, novac)
**merge_data_to_schema**: The function of merge_data_to_schema is to merge data from multiple site schemas into a single target DCC schema, applying optional post‑merge steps such as logging, primary‑key creation, NOT NULL constraints, index creation, index cleanup, foreign‑key creation, and vacuuming.

**parameters**:  
· model_version (str): PEDSnet model version, e.g. “2.2.0”.  
· conn_str (str): libpq‑style PostgreSQL connection string.  
· schema (str): name of the target schema that will receive the merged data.  
· altname (str): alternate naming pattern for site schemas; if empty the default pattern “{site}_pedsnet” is used.  
· skipsites (str): comma‑separated list of site identifiers that must be excluded from the merge.  
· addsites (str): comma‑separated list of additional site identifiers to include, even if they are not in the default site list.  
· force (bool, default False): when True, benign errors such as “object already exists” or “object does not exist” are ignored during DDL execution.  
· notable (bool, default False): when True, the creation of merged tables is skipped (the function will not execute the UNION‑ALL CREATE statements).  
· nolog (bool, default False): when True, the step that sets tables to LOGGED is omitted.  
· nopk (bool, default False): when True, primary‑key creation is omitted.  
· nonull (bool, default False): when True, NOT NULL constraints are omitted.  
· noidx (bool, default False): when True, index creation is omitted.  
· nodrop (bool, default False): when True, dropping of unneeded indexes is omitted.  
· norep (bool, default False): when True, creation of index‑replacement tables is omitted.  
· nofk (bool, default False): when True, foreign‑key creation is omitted.  
· novac (bool, default False): when True, the VACUUM ANALYZE step is omitted.  

**Code Description**:  
1. **Search‑path preparation** – The connection string is rewritten with `conn_str_with_search_path` so that the PostgreSQL `search_path` points to the target schema. This guarantees that all subsequent DDL statements operate in the correct namespace.  
2. **Logging start** – A log dictionary is built by merging the model version, the `force` flag, and connection‑info obtained from `get_conn_info_dict`. The function logs a “starting merge site data” message and records the start time.  
3. **Statement collection** – An empty `StatementSet` (`stmts`) is created to hold the CREATE‑TABLE statements that will be generated for each non‑vocabulary table.  
4. **Metadata acquisition and transformation** – `stock_metadata` loads the canonical PEDSnet metadata for the supplied `model_version`. Each `Transform` subclass listed in the global `TRANSFORMS` is applied via `modify_metadata`, allowing site‑specific or transformation‑specific column additions (e.g., the hard‑coded insertion of `visit_detail_id` for certain fact tables).  
5. **Site list resolution** –  
   * `skip_sites` is built by splitting `skipsites`.  
   * `merge_sites` starts as the full `SITES` constant minus the skipped sites.  
   * `add_sites` is built from `addsites` and merged into `merge_sites`.  
   * Empty strings are filtered out, leaving a final list of site identifiers to be processed.  
6. **SQL generation per table** – For every table in the metadata that is **not** a vocabulary table:  
   * The column list (`fields`) is assembled from the table’s column collection.  
   * If the table name matches a hard‑coded list of fact tables, the column `visit_detail_id` is appended when it does not already exist.  
   * For each site in `merge_sites`, the appropriate site schema name is constructed using either `_sql_schema_tmpl` (default) or `_sql_schema_alt_tmpl` (when `altname` is supplied).  
   * A SELECT fragment (`_sql_select_tmpl`) that selects the prepared fields from the site schema is concatenated to a growing UNION‑ALL string.  
   * After all sites are processed, the trailing “UNION ALL” is removed, and a CREATE‑TABLE‑AS statement (`_sql_create_tmpl`) is built, wrapping the UNION query.  
   * The resulting SQL and a descriptive message are wrapped in a `Statement` object and added to `stmts`.  
7. **Execution of merge statements** – If `notable` is False, the entire `StatementSet` is executed in parallel via `stmts.parallel_execute(conn_str)`. After execution each statement is examined with `_check_stmt_err(stmt, force)`. Any non‑benign error causes a logged fatal message and the function raises the original exception, aborting the merge.  
8. **Post‑merge optional steps** –  
   * **Logging** – If `nolog` is False, `set_logged` is called to convert any UNLOGGED tables created during the merge back to LOGGED.  
   * **Primary keys** – If `nopk` is False, `add_primary_keys` adds primary‑key constraints.  
   * **NOT NULL** – If `nonull` is False, `set_not_nulls` applies NOT NULL constraints.  
   * **Indexes** – If `noidx` is False, `add_indexes` creates the required indexes.  
   * **Drop unused indexes** – If `nodrop` is False, `drop_unneeded_indexes` removes indexes that are no longer needed.  
   * **Index‑replacement tables** – If `norep` is False, `create_index_replacement_tables` builds auxiliary tables used by certain concept‑group indexes.  
   * **Foreign keys** – The connection string is updated to include the vocabulary schema in the search path, then, if `nofk` is False, `add_foreign_keys` creates foreign‑key constraints.  
   * **Vacuum** – If `novac` is False, `vacuum` runs a VACUUM ANALYZE on all tables of the model version.  
9. **Logging end** – A final log entry records the completion of the merge task together with the elapsed time.  
10. **Return value** – If the function reaches the end without raising an exception, it returns `True` to indicate successful merge.

**Reference relationships**:  
*Callers* – The function is invoked by the CLI command `merge_schema` in `pedsnetdcc/main.py`. Depending on the `undo` flag, `merge_schema` either calls `merge_data_to_schema` (for a forward merge) or `clear_schema_data` (to drop the merged schema).  
*Callees* – Within its body the function calls:  
- `conn_str_with_search_path` (utils) to adjust the connection string.  
- `combine_dicts` and `get_conn_info_dict` (utils) for structured logging.  
- `stock_metadata` (utils) to obtain the base model definition.  
- `Transform.modify_metadata` (abstract_transform) for each transformation in `TRANSFORMS`.  
- `_check_stmt_err` (merge_site_data) to validate each CREATE statement.  
- `set_logged` (utils) to convert tables to LOGGED.  
- `add_primary_keys` (primary_keys) to add PK constraints.  
- `set_not_nulls` (not_nulls) to enforce NOT NULL.  
- `add_indexes` (indexes) to create indexes.  
- `drop_unneeded_indexes` (indexes) to drop superfluous indexes.  
- `create_index_replacement_tables` (concept_group_tables) to build replacement tables.  
- `add_foreign_keys` (foreign_keys) to add FK constraints (after extending the search path with the vocabulary schema).  
- `vacuum` (utils) to vacuum and analyze tables.  

**Note**:  
- The function assumes that the user specified in `conn_str` has sufficient privileges to create tables, alter constraints, and run VACUUM.  
- The only “benign” error that can be ignored with `force=True` is a duplicate‑object error for CREATE statements or an undefined‑object error for DROP statements; all other errors will abort the process.  
- The hard‑coded addition of `visit_detail_id` applies only to the listed fact tables; if the model version changes and new fact tables are introduced, the list must be updated manually.  
- When `notable` is True the function skips the entire data‑merge step, which can be useful for re‑running only the post‑merge steps (e.g., adding constraints after a partial failure).  
- The function builds the UNION‑ALL query by concatenating raw SELECT strings; therefore the order of sites in the final merged table follows the order of `merge_sites` after set operations.  
- All DDL statements are executed with autocommit (isolation level 0), making them safe for parallel execution but not reversible as a single transaction.  

**Output Example**:  
```python
result = merge_data_to_schema(
    model_version='2.2.0',
    conn_str="host=pg host=5432 dbname=pedsnet user=loader password=secret",
    schema='dcc_pedsnet',
    altname='',
    skipsites='siteA,siteB',
    addsites='siteX',
    force=True,
    notable=False,
    nolog=False,
    nopk=False,
    nonull=False,
    noidx=False,
    nodrop=False,
    norep=False,
    nofk=False,
    novac=False
)
print(result)   # -> True
```
## FunctionDef clear_schema_data(model_version, conn_str, schema, force)
**clear_schema_data**: The function of clear_schema_data is to remove all non‑vocabulary tables from a specified DCC schema, optionally suppressing benign “object does not exist” errors.

**parameters**  
· **model_version** (str): PEDSnet model version, e.g. “2.3.1”.  
· **conn_str** (str): libpq connection string used to connect to the PostgreSQL database.  
· **schema** (str): name of the DCC schema that will be cleared.  
· **force** (bool, default False): when True, ignore harmless errors such as attempting to drop a table that does not exist (see issue #10).

**Code Description**  
1. **Set search_path** – The function first calls `conn_str_with_search_path(conn_str, schema)` to ensure that the connection string operates with the target schema as the first entry in the PostgreSQL `search_path`. This guarantees that subsequent DDL statements affect the intended schema regardless of the schema encoded in the original `conn_str`.  

2. **Prepare logging context** – A dictionary containing the model version, the `force` flag, and connection‑info extracted by `get_conn_info_dict(conn_str)` is built with `combine_dicts`. The start of the task (“clear schema data”) is logged at INFO level, and the current epoch time is stored in `start_time` for later elapsed‑time reporting.  

3. **Create a StatementSet** – An empty `StatementSet` instance (`stmts`) is instantiated. This collection will hold one `Statement` object for each table that must be dropped.  

4. **Obtain model metadata** – `stock_metadata(model_version)` is invoked to retrieve a SQLAlchemy `MetaData` object that describes the full PEDSnet data model for the supplied version. The metadata’s `tables` attribute provides a mapping of table names to `Table` objects.  

5. **Build DROP statements** – The function iterates over the set difference `set(metadata.tables.keys()) - set(VOCAB_TABLES)`. `VOCAB_TABLES` is a constant list of vocabulary tables that must be preserved. For every remaining table name, a `Statement` is created with the SQL string `_sql_drop_tmpl.format(table_name=table_name)` (the template expands to a `DROP TABLE IF EXISTS` command) and added to the `StatementSet`.  

6. **Execute statements in parallel** – `stmts.parallel_execute(conn_str)` launches a pool of worker processes (up to 25) that each execute one of the DROP statements. The parallel execution mechanism handles task queuing, result collection, and centralized logging.  

7. **Error handling** – After execution, the function iterates over each `Statement` in `stmts`. For each statement it calls `_check_stmt_err(stmt, force)`.  
   - If `force` is True and the error corresponds to “object does not exist” (PostgreSQL error code 42P01), `_check_stmt_err` returns silently.  
   - Any other error causes `_check_stmt_err` to raise the original exception.  
   - When an exception is caught, the function logs a fatal error containing the SQL, the error message, and the elapsed time, then re‑raises the exception, causing the overall operation to fail.  

8. **Log completion** – If all statements pass the error check, a final INFO log entry records the successful completion of the task together with the total elapsed time (computed by `secs_since(start_time)`).  

9. **Return value** – The function returns `True` to indicate that the schema was cleared without fatal errors.  

**Reference Relationships**  
- **Callers**: `clear_schema_data` is invoked by `merge_schema` in `pedsnetdcc/main.py` when the `undo` flag is set, i.e., when a user requests to roll back a previous merge operation.  
- **Callees**:  
  * `conn_str_with_search_path` (pedsnetdcc/utils.py) – adjusts the connection string’s search_path.  
  * `combine_dicts` and `get_conn_info_dict` (pedsnetdcc/utils.py) – build log dictionaries and extract connection metadata.  
  * `stock_metadata` (pedsnetdcc/utils.py) – provides the canonical table list for the given model version.  
  * `StatementSet` and its methods `add` and `parallel_execute` (pedsnetdcc/db.py) – manage and run the DROP statements concurrently.  
  * `Statement` (pedsnetdcc/db.py) – encapsulates each individual DROP command.  
  * `_check_stmt_err` (pedsnetdcc/merge_site_data.py) – validates each statement’s error status, applying the `force` logic.  
  * `secs_since` (pedsnetdcc/dict_logging.py) – formats elapsed time for log messages.  
  * `logger` – the module‑level logger used for all informational and error logs.  

**Note**  
- Only tables that are not listed in `VOCAB_TABLES` are dropped; vocabulary tables remain untouched.  
- The `force` flag suppresses only the specific “object does not exist” error for DROP statements; any other error (including permission issues or syntax errors) will abort the operation.  
- The function relies on the global template `_sql_drop_tmpl`; if that template is altered, the generated DROP statements will change accordingly.  
- Parallel execution uses a maximum of 25 worker processes; for a very large number of tables the pool size is capped at this limit unless the internal implementation is modified.  
- The function raises the original exception from the failing `Statement`; callers should be prepared to handle `DatabaseError`, `OperationalError`, or other psycopg2 exceptions.  

**Output Example**  
```python
result = clear_schema_data('2.3.1',
                           'host=localhost port=5432 dbname=mydb user=loader',
                           'dcc_pedsnet',
                           force=True)
print(result)   # -> True
```
