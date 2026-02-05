## FunctionDef _copy_to_measure_table(conn_str, table, concepts)
**_copy_to_measure_table**: The function of _copy_to_measure_table is to copy selected measurement rows from the temporary `measurement_anthro` table into a partitioned measurement table specific to a given measurement type.

**parameters**:  
· conn_str: PostgreSQL connection string used to open a database connection.  
· table: Name of the target measurement partition (e.g., "bmi", "bmiz", "ht_z", "wt_z"). This name is interpolated into the target table identifier `measurement_{table}`.  
· concepts: A comma‑separated string of measurement_concept_id values that should be copied. The string is placed directly into the `IN (…)` clause of the generated SQL.

**Code Description**:  
1. **SQL template construction** – The function defines a multi‑line string `copy_to_sql` that performs an `INSERT INTO measurement_{0}` (where `{0}` is replaced by the `table` argument). The column list enumerates every column present in the measurement tables. The `SELECT … FROM measurement_anthro` clause selects the same columns from the staging table `measurement_anthro` but restricts rows to those whose `measurement_concept_id` appears in the supplied `concepts` list. The statement ends with `ON CONFLICT DO NOTHING` to silently ignore duplicate primary‑key violations.  
2. **Logging message** – `copy_to_msg` is a simple template used to create a human‑readable description such as “copying bmi to measurement_bmi”. This message is passed to the `Statement` object for logging purposes.  
3. **Statement creation** – A `Statement` instance is created with the formatted SQL (`copy_to_sql.format(table, concepts)`) and the formatted message (`copy_to_msg.format(table)`). The `Statement` class (defined in `pedsnetdcc/db.py`) encapsulates the SQL, a descriptive message, and execution metadata.  
4. **Execution** – The `execute` method of the `Statement` object is called with `conn_str`. This opens a new PostgreSQL connection, runs the INSERT…SELECT command, and stores the result metadata (rowcount, any error, etc.) inside the `Statement` instance.  
5. **Error handling** – Immediately after execution, `check_stmt_err` (from `pedsnetdcc/utils.py`) is invoked with the statement and a context string `'insert ' + table + ' measurements'`. If the statement encountered an error, `check_stmt_err` logs the failure and raises a `DatabaseError`. If no error occurred, execution continues.  
6. **Return value** – When the INSERT completes without raising an exception, the function returns `True` to indicate success. The caller can rely on the boolean result because any failure would have caused an exception to be raised earlier.

**Reference Relationships**:  
- **Caller** – `partition_measurement_table` (in `pedsnetdcc/partition_measurement.py`) calls `_copy_to_measure_table` for each measurement type that needs to be moved from the central `measurement_anthro` table to its dedicated partition table. After a successful copy, the caller subsequently invokes `_delete_measure_from_anthro` to remove the moved rows from the staging table.  
- **Callee** – The function uses the `Statement` class to encapsulate and run the SQL command, and it relies on `check_stmt_err` to perform standardized error detection and logging. No other project functions are called directly.  

**Note**:  
- The `concepts` argument must be a correctly formatted comma‑separated list of integer IDs; it is inserted into the SQL string without additional quoting, so malformed input will cause a SQL syntax error.  
- The target table name (`measurement_{table}`) must already exist and have the same column layout as `measurement_anthro`; otherwise the INSERT will fail.  
- Because the statement uses `ON CONFLICT DO NOTHING`, duplicate rows (based on the table’s primary key) are ignored silently; this behavior is intentional to make the operation idempotent.  
- The function returns only a boolean; any database‑level error is raised as a `DatabaseError` by `check_stmt_err`, so callers should be prepared to handle exceptions.  

**Output Example**:  
```python
result = _copy_to_measure_table(conn_str, 'bmi', '3038553')
# If the INSERT succeeds without error, result will be True
print(result)   # -> True
```
## FunctionDef _delete_measure_from_anthro(conn_str, table, concepts)
**_delete_measure_from_anthro**: The function of _delete_measure_from_anthro is to remove rows for specified measurement concept IDs from the `measurement_anthro` table.

**parameters**:  
· **conn_str**: a libpq‑style PostgreSQL connection string used to open a database connection.  
· **table**: a string representing the logical name of the measurement being removed (e.g., `'bmi'`, `'bmiz'`, `'ht_z'`, `'wt_z'`). This value is only used for logging purposes.  
· **concepts**: a string containing a comma‑separated list of integer concept IDs that should be deleted from `measurement_anthro`.

**Code Description**:  
1. **SQL template creation** – The function defines `delete_bmi_sql` as a DELETE statement that targets rows in `measurement_anthro` whose `measurement_concept_id` appears in the supplied list (`{0}` placeholder).  
2. **Log message template** – `delete_bmi_msg` is a human‑readable template that will be interpolated with the `table` argument to describe the operation in logs.  
3. **Statement instantiation** – A `Statement` object is created with the formatted SQL (`delete_bmi_sql.format(concepts)`) and the formatted message (`delete_bmi_msg.format(table)`). The `Statement` class encapsulates the SQL command, a descriptive message, and execution metadata.  
4. **Execution** – The `execute` method of the `Statement` instance is called with `conn_str`. This opens a new PostgreSQL connection, runs the DELETE command in autocommit mode, and stores the result (row count, any error) inside the `Statement` object.  
5. **Error handling** – After execution, `check_stmt_err` is invoked with the statement and a context string (`'remove ' + table + ' measurements'`). `check_stmt_err` inspects `stmt.err`; if an error is present it logs the failure and raises a `DatabaseError`. If no error occurs, execution proceeds.  
6. **Return value** – When the DELETE completes without raising an exception, the function returns `True` to indicate success.

**Reference Relationships**:  
- **Caller** – `_delete_measure_from_anthro` is invoked from `partition_measurement_table` (in `pedsnetdcc/partition_measurement.py`). Within that function, after a successful copy of site‑specific BMI‑related measurements to their own partition tables, `_delete_measure_from_anthro` is called to purge the same concept IDs from the central `measurement_anthro` table.  
- **Callee** – The function relies on two utility components:  
  * `Statement` (from `pedsnetdcc/db.py`) for constructing and executing the DELETE command.  
  * `check_stmt_err` (from `pedsnetdcc/utils.py`) for centralized error detection and exception raising.  
No other functions are directly called from within `_delete_measure_from_anthro`.

**Note**:  
- The `concepts` argument must be a correctly formatted comma‑separated list of integer IDs; the function does not perform validation or quoting. Supplying malformed input will cause a SQL syntax error that will be caught by `check_stmt_err`.  
- The function deletes rows only from `measurement_anthro`; other measurement tables (e.g., `measurement_labs`, `measurement_vitals`) are unaffected.  
- Because the `Statement.execute` method runs with autocommit, the deletion is permanent as soon as the command succeeds; there is no transaction rollback within this function.  
- The `table` parameter is used solely for logging; it does not affect the SQL logic.

**Output Example**:  
```python
# Example call
result = _delete_measure_from_anthro(conn_str,
                                     table='bmi',
                                     concepts='3038553')
# If the DELETE succeeds, result is True
print(result)   # -> True
```
## FunctionDef partition_measurement_table(conn_str, model_version, search_path, dcc, site3, no_vital)
**partition_measurement_table**: The function of partition_measurement_table is to reorganize the core `measurement` table by truncating it, adding check constraints and inheritance to partition tables, and installing a trigger that routes new rows to the appropriate partition based on measurement concept IDs.

**parameters**:  
· **conn_str** (str): PostgreSQL connection string used for all database operations.  
· **model_version**: PEDSnet data model version (e.g., `2.3.0`). It is only used for logging.  
· **search_path** (str): Schema search path supplied to the database; the first schema in the list is treated as the primary schema.  
· **dcc** (bool): When `True` the function creates DCC‑level partitions (`measurement_anthro`, `measurement_labs`, `measurement_vitals`).  
· **site3** (bool): When `True` the function creates site‑level partitions but uses the same DCC‑style table set.  
· **no_vital** (bool): When `True` vitals tables are omitted from the partition set.

**Code Description**:  

1. **Logging start and timing** – The function builds a log dictionary that merges the model version with connection‑info extracted by `get_conn_info_dict`. It logs the start of the partitioning process and records the current epoch time (`start_time`).  

2. **Determine primary schema** – `primary_schema(search_path)` extracts the first schema name from the supplied `search_path`. All subsequent DDL statements are formatted with this schema name.  

3. **Decide whether to move site‑specific BMI‑related measurements** –  
   * If `dcc` or `site3` is `True`, `move` is set to `False`.  
   * Otherwise (`move` is `True`) the function iterates over a hard‑coded mapping `move_measures` that lists concept IDs for `bmi`, `bmiz`, `ht_z`, and `wt_z`.  
   * For each measurement type it calls `_copy_to_measure_table` to copy rows from the temporary `measurement_anthro` table into the dedicated partition table (`measurement_{type}`). If the copy succeeds, `_delete_measure_from_anthro` removes the same rows from `measurement_anthro`. Log messages indicate success or failure.  

4. **Truncate the central measurement table** – A `Statement` object is created with the `TRUNCATE_MEASUREMENT_SQL` template (filled with the primary schema) and executed via `execute`. The helper `check_stmt_err` validates that the truncate succeeded; any error raises a `DatabaseError`.  

5. **Select partition tables and concept ID groups** –  
   * When `dcc` or `site3` is `True`, only three tables are used (`anthro`, `labs`, optionally `vitals`). The `measure_like_tables` dictionary records whether each table should receive rows whose concept IDs are *in* or *not in* the supplied list.  
   * When partitioning for a site (`dcc` and `site3` are `False`), additional tables for `bmi`, `bmiz`, `ht_z`, and `wt_z` are added.  
   * Corresponding `concept_id` dictionaries contain the exact concept ID tuples for each table. The selection of IDs differs between DCC and site modes and respects the `no_vital` flag.  

6. **Add check constraints** – For every entry in `measure_like_tables` a `Statement` is built using `ADD_CHECK_CONSTRAINT_SQL`. The constraint enforces that rows inserted into a partition table have measurement concept IDs that satisfy the “in” or “not in” condition. All constraint statements are collected in a `StatementSet`, executed in parallel with a pool size of five workers (`stmts.parallel_execute(conn_str, 5)`), and each statement is examined with `check_stmt_err`.  

7. **Add inheritance** – A second `StatementSet` is created, this time containing `ADD_INHERIT_SQL` statements that make each partition table inherit from the core `measurement` table. Execution and error checking follow the same parallel pattern as the constraints step.  

8. **Create the routing trigger** – Using a direct `psycopg2` connection, the function creates the PL/pgSQL function `trg_insert_measurement` (or its DCC/site3 variants) that decides, based on `measurement_concept_id`, which partition table should receive a new row. The appropriate SQL template is selected according to the `dcc`, `site3`, and `no_vital` flags. After the function definition, the corresponding BEFORE INSERT trigger (`measurement_insert`) is created on the core `measurement` table.  

9. **Logging completion** – The function logs that the trigger creation succeeded, records the total elapsed time (`secs_since(start_time)`), and returns `True` to indicate overall success.  

**Reference relationships**:  

*Callers* –  
- `pedsnetdcc.main.partition_measurement` invokes `partition_measurement_table` after constructing a connection string from command‑line arguments.  
- `pedsnetdcc.main.subset_by_cohort` calls the function after optionally splitting measurements; it passes a modified search path that points to the newly created target schema.  

*Callees* –  
- `_copy_to_measure_table` and `_delete_measure_from_anthro` handle the optional movement of BMI‑related rows.  
- `primary_schema` extracts the primary schema name from `search_path`.  
- `Statement` and `StatementSet` (from `pedsnetdcc.db`) are used to build, execute, and manage DDL statements in parallel.  
- `check_stmt_err` validates each executed statement and raises `DatabaseError` on failure.  
- `secs_since` provides elapsed‑time formatting for log entries.  

**Note**:  
- The function assumes that all partition tables (`measurement_anthro`, `measurement_labs`, `measurement_vitals`, `measurement_bmi`, `measurement_bmiz`, `measurement_ht_z`, `measurement_wt_z`) already exist with the same column layout as the core `measurement` table.  
- The `no_vital` flag removes vitals from both the constraint definition and the trigger function; vitals are completely omitted when it is `True`.  
- Parallel execution uses a fixed pool size of five workers for constraint and inheritance statements; this value can be changed by modifying the call to `parallel_execute`.  
- Errors in any step cause an immediate exception via `check_stmt_err`; the function will not return `True` if any DDL statement fails.  
- The function does not commit or roll back transactions explicitly; each `Statement` runs with autocommit, which is appropriate for DDL operations.  

**Output Example**:  

```python
# Example call
success = partition_measurement_table(
    conn_str="host=db.example.com port=5432 dbname=pedsnet user=loader",
    model_version="2.3.0",
    search_path="dcc_pedsnet, public, vocabulary",
    dcc=False,
    site3=False,
    no_vital=False,
)

print(success)   # -> True  (if all steps completed without error)
```
## FunctionDef unpartition_measurement_table(conn_str, model_version, search_path, dcc, site3, no_vital)
**unpartition_measurement_table**: The function of unpartition_measurement_table is to reverse the partitioning of the measurement table by dropping the before‑insert trigger, removing check constraints, and removing inheritance from the split measurement tables.

**parameters**:  
· **conn_str** (str): database connection string in libpq format.  
· **model_version**: PEDSnet model version identifier (e.g., “2.3.0”).  
· **search_path** (str): PostgreSQL schema search path; the first schema is used as the primary schema for SQL statements.  
· **dcc** (bool): flag indicating that the operation targets DCC‑level tables (measurement_anthro, measurement_labs, measurement_vitals).  
· **site3** (bool): flag indicating that the operation targets a site but should use the DCC‑level partitioning layout.  
· **no_vital** (bool): when True, vitals tables are excluded from the unpartitioning steps.

**Code Description**:  
1. **Logging start** – The function builds a log dictionary that merges the model version with connection‑info extracted by `get_conn_info_dict`. It logs a “starting partitioning the measurement table” message and records the start time (`time.time()`).  

2. **Determine primary schema** – `primary_schema(search_path)` extracts the first schema name from the supplied search path; this schema name is used in all subsequent SQL statements.  

3. **Select split‑table groups** – Based on the combination of `dcc`, `site3`, and `no_vital`, the function constructs a dictionary `measure_like_tables`. The keys are the suffixes of the split tables (e.g., “anthro”, “labs”, “vitals”, “bmi”, “bmiz”, “ht_z”, “wt_z”) and the values are the strings “in” or “not in”, which are later used to format the DROP CHECK CONSTRAINT and DROP INHERIT SQL templates.  

4. **Drop the before‑insert trigger** – A log entry “dropping measurement before insert trigger” is emitted. Using a short‑lived `psycopg2` connection, the function executes one of two SQL statements:  
   - `DELETE_TRIGGER_DCC_MEASUREMENT_SQL.format(schema)` if `dcc` is True,  
   - `DELETE_TRIGGER_SITE_MEASUREMENT_SQL.format(schema)` otherwise (covers both `site3` and regular site mode).  
   After execution the connection is closed and a confirmation log is written.  

5. **Drop check constraints** – A `StatementSet` (`stmts`) is created. For each table suffix in `measure_like_tables`, a `Statement` is instantiated with `DROP_CHECK_CONSTRAINT_SQL.format(schema, measure_like_table)`. All statements are added to the set, then `stmts.parallel_execute(conn_str, 5)` runs them in parallel using five worker processes. After execution the function iterates over each `Statement` and calls `check_stmt_err(stmt, 'unpartition measurement table')`. If any statement reports an error, the error is logged (including the offending SQL and exception) and re‑raised, aborting the function. Successful completion logs “drop constraints complete”.  

6. **Drop inheritance** – The same pattern is repeated for inheritance removal. A new `StatementSet` is populated with `DROP_INHERIT_SQL.format(schema, measure_like_table)` for each table suffix, executed in parallel, and each result is checked with `check_stmt_err`. Errors are logged with context “dropping inherit from measurement”. On success, a log entry “inherit from measurement dropped” is emitted.  

7. **Final logging and return** – The function logs “finished unpartitioning the measurement table” together with the total elapsed time (computed by `secs_since(start_time)`). If no exception was raised, the function returns `True` to indicate success.  

**Reference relationships**:  
- **Caller**: `pedsnetdcc/main.py/unpartition_measurement` invokes this function after constructing a connection string. The CLI wrapper passes the command‑line options (`dcc`, `site3`, `no_vital`, `model_version`, `searchpath`, `dburi`) to it.  
- **Callees / utilities**:  
  * `combine_dicts` – merges dictionaries for logging.  
  * `get_conn_info_dict` – extracts connection metadata for log enrichment.  
  * `primary_schema` – parses the search path to obtain the target schema name.  
  * `Statement` and `StatementSet` – encapsulate individual DDL commands and execute them in parallel.  
  * `check_stmt_err` – validates each executed statement and raises `DatabaseError` on failure.  
  * `secs_since` – formats elapsed time for log messages.  
  * SQL template constants (`DELETE_TRIGGER_DCC_MEASUREMENT_SQL`, `DELETE_TRIGGER_SITE_MEASUREMENT_SQL`, `DROP_CHECK_CONSTRAINT_SQL`, `DROP_INHERIT_SQL`) – provide the actual DDL strings that are formatted with the schema and table suffix.  
  * `logger` – central logging object used throughout the function.  

**Note**:  
- The function assumes that the required SQL template constants are defined elsewhere in the module and that they contain placeholders for the schema name and table suffix.  
- Parallel execution uses a fixed pool size of five workers; this value is hard‑coded and may be adjusted only by modifying the source.  
- If any statement fails, `check_stmt_err` will raise a `DatabaseError`, causing the function to exit early and return `False` to the caller (the caller then terminates the process with a non‑zero exit code).  
- The `no_vital` flag only influences the composition of `measure_like_tables`; it does not affect trigger deletion, which is performed unconditionally for the selected mode (`dcc` or site).  
- The function does not truncate the measurement table or recreate any triggers; it solely removes the partition‑related objects, leaving the base measurement table empty of constraints and inheritance.  

**Output Example**:  
When all DDL statements succeed, the function returns the boolean value `True`.  

```python
result = unpartition_measurement_table(conn_str, "2.3.0",
                                       "dcc_pedsnet, public", dcc=True,
                                       site3=False, no_vital=False)
print(result)   # -> True
```
