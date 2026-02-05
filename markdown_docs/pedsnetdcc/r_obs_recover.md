## FunctionDef _fill_concept_names(conn_str, schema)
**_fill_concept_names**: The function of _fill_concept_names is to populate the human‑readable concept name columns in the `observation_derivation_recover` table by joining the corresponding concept IDs to the `vocabulary.concept` table.

**parameters**:  
· **conn_str** – a libpq‑style PostgreSQL connection string used to open a database connection.  
· **schema** – the name of the target schema that contains the `observation_derivation_recover` table; it is interpolated into the SQL statement.

**Code Description**:  
1. **SQL construction** – The function defines a multi‑line string `fill_concept_names_sql`. The statement updates the table `{schema}.observation_derivation_recover` (aliased as `zs`) and sets five columns (`observation_concept_name`, `observation_type_concept_name`, `qualifier_concept_name`, `unit_concept_name`, `value_as_concept_name`). The new values are taken from a sub‑query (`v`) that selects the original `observation_id` together with the `concept_name` fields obtained by left‑joining each concept‑id column (`observation_concept_id`, `observation_type_concept_id`, `qualifier_concept_id`, `unit_concept_id`, `value_as_concept_id`) to `vocabulary.concept`. Because the joins are left joins, rows with missing concept IDs are left unchanged (NULL names).

2. **Message preparation** – A simple descriptive message `fill_concept_names_msg = "adding concept names"` is created for logging and for the `Statement` object.

3. **Statement creation** – An instance of `pedsnetdcc.db.Statement` is instantiated with the formatted SQL (`fill_concept_names_sql.format(schema)`) and the message. The `Statement` class encapsulates the SQL, a human‑readable description, and execution metadata (row count, error, etc.).

4. **Execution** – The `execute` method of the `Statement` object is called with `conn_str`. This opens a new PostgreSQL connection, runs the UPDATE, and stores any error in `stmt.err`. Autocommit mode is used, so the UPDATE is applied immediately.

5. **Error handling** – After execution, the helper `check_stmt_err` is invoked with the statement and a context string `'add concept names'`. `check_stmt_err` inspects `stmt.err`; if an error is present it logs the failure and raises a `DatabaseError`. If no error occurs, execution proceeds.

6. **Return value** – When the UPDATE completes without raising an exception, the function returns `True` to signal success to the caller.

**Reference relationships**:  
- **Caller** – The function is called from `run_r_obs_recover` (in `pedsnetdcc/r_obs_recover.py`). After adding observation IDs, `run_r_obs_recover` logs a message, invokes `_fill_concept_names(conn_str, schema)`, and checks the Boolean result. If the function returns `False` (which would happen only if an exception were caught upstream), the overall recover process aborts.  
- **Callees** – The function directly uses the `Statement` class (for SQL execution) and the utility `check_stmt_err` (for error detection). No other project functions are called.

**Note**:  
- The `schema` argument must be a valid schema name that contains both `observation_derivation_recover` and has access to the `vocabulary.concept` table; otherwise the UPDATE will fail with a permission or relation error.  
- Because the UPDATE uses left joins, concept name columns will be set to `NULL` when the corresponding concept ID does not exist in `vocabulary.concept`. This is intentional to preserve rows with missing concept definitions.  
- The function relies on the global `Statement` implementation to manage connections; therefore the caller must provide a correct `conn_str` and ensure the database is reachable.  
- Any exception raised by `check_stmt_err` propagates out of `_fill_concept_names`; the function itself never returns `False`. The `True` return is only reached when no error occurs.

**Output Example**:  
```python
result = _fill_concept_names(conn_str, 'dcc_observation')
print(result)          # -> True
# If an error occurs, a DatabaseError is raised and the function does not return.
```
## FunctionDef _fill_age_in_months(conn_str, schema)
**_fill_age_in_months**: The function of _fill_age_in_months is to create helper SQL functions for calculating month‑based intervals and then populate the column observation_age_in_months in the observation_derivation_recover table with the computed age (in months) of each observation relative to the person’s birth date.

**parameters**:  
· conn_str: a libpq‑style PostgreSQL connection string used to open a database connection.  
· schema: the name of the target schema (e.g., dcc) in which the helper functions and the update statement will be created.

**Code Description**:  
1. A multi‑line SQL string (fill_add_age_in_months_sql) is assembled. It contains four CREATE OR REPLACE FUNCTION statements that are placed in the supplied schema:  
   – last_month_of_interval(start_ts, end_ts) returns the timestamp of the last month that falls inside the interval between the two timestamps.  
   – month_after_last_month_of_interval(start_ts, end_ts) returns the timestamp of the month immediately after the last month of the interval.  
   – days_in_last_month_of_interval(start_ts, end_ts) returns the number of days in that last month, computed as the difference between the two functions above.  
   – months_in_interval(start_ts, end_ts) returns a double‑precision value representing the total number of months (including fractional months) between the two timestamps. The fractional part is calculated by dividing the extracted day count by the number of days in the last overlapping month. Each function is declared STRICT and IMMUTABLE and is accompanied by a COMMENT describing its purpose.  

2. After the helper functions are defined, an UPDATE statement is built that joins observation_derivation_recover (o) with person (p) on person_id. For each observation, the expression {schema}.months_in_interval(p.birth_datetime, o.observation_datetime) is evaluated and stored in the column observation_age_in_months. The UPDATE uses a sub‑query (aliased subquery) to compute the value once per observation and then applies it to the target table.

3. The assembled SQL string is formatted with the supplied schema value, producing a single executable command that creates the functions and performs the update in one transaction.

4. A human‑readable message "adding age in months" is stored in fill_add_age_in_months_msg.

5. A Statement object is instantiated with the formatted SQL and the message. The Statement class encapsulates the SQL, tracks execution metadata, and provides a convenient execute method.

6. The statement’s execute method is called with conn_str, which opens a new PostgreSQL connection (autocommit), runs the SQL, and records any error in stmt.err.  

7. Immediately after execution, the helper function check_stmt_err is invoked with the statement and a context string 'add age in months'. If the statement produced an error, check_stmt_err logs the failure and raises a DatabaseError; otherwise execution continues.

8. If no exception is raised, the function returns True to indicate successful creation of the helper functions and successful population of observation_age_in_months.

**Reference Relationships**:  
- **Caller**: The function is invoked from run_r_obs_recover in pedsnetdcc/r_obs_recover.py as part of the overall observation derivation recovery workflow. After adding concept names, run_r_obs_recover calls _fill_age_in_months to compute ages in months before creating indexes, foreign keys, and copying data to the final observation table.  
- **Callee**: The function relies on the Statement class (pedsnetdcc/db.py) to execute the generated SQL, and on check_stmt_err (pedsnetdcc/utils.py) to validate execution and raise a consistent DatabaseError if needed. No other project functions are called directly from within _fill_age_in_months.  

**Note**:  
- The helper SQL functions are created with OR REPLACE, so repeated calls will overwrite existing definitions without error.  
- The functions assume that the input timestamps are of type timestamp without time zone; the update casts observation_datetime accordingly.  
- Because the statement runs in autocommit mode, the function definitions and the UPDATE are committed immediately; there is no transaction rollback on failure.  
- Errors from the SQL execution are not raised automatically; they are captured in stmt.err and must be inspected via check_stmt_err, which will raise a DatabaseError if an error is present.  
- The function updates every row in observation_derivation_recover; ensure that the column observation_age_in_months exists and is of a numeric type compatible with double precision.  

**Output Example**:  
When called with a valid connection string and schema name, the function returns the Boolean value True, indicating that the helper functions were created and the age‑in‑months column was successfully populated. If an error occurs, a DatabaseError is raised and the function does not return.
## FunctionDef _copy_to_obs_table(conn_str, schema)
**_copy_to_obs_table**: The function of _copy_to_obs_table is to copy all rows from the temporary table observation_derivation_recover into the production observation table within the specified schema, ignoring rows that would violate a unique constraint.

**parameters**:  
· conn_str: a libpq‑style PostgreSQL connection string used to open a database connection.  
· schema: the name of the target schema (e.g., dcc) that contains both observation_derivation_recover and observation tables.

**Code Description**:  
1. **SQL statement construction** – A multi‑line INSERT‑SELECT statement is built in the variable copy_to_sql. The statement inserts every column listed (observation_concept_id, observation_date, …, person_id) from {schema}.observation_derivation_recover into {schema}.observation. The placeholder {0} is replaced with the supplied schema when the string is formatted. The clause ON CONFLICT DO NOTHING ensures that rows that would duplicate a primary‑key or unique‑key value are silently skipped rather than causing an error.

2. **Logging message** – copy_to_msg holds a human‑readable description (“copying {0} to observation”) that will be attached to the Statement object for debugging purposes.

3. **Statement creation** – A Statement instance is created with the formatted SQL and message:
   ```python
   copy_to_stmt = Statement(copy_to_sql.format(schema), copy_to_msg)
   ```
   The Statement class encapsulates the SQL, a descriptive message, and execution metadata (rowcount, error, etc.).

4. **Execution** – copy_to_stmt.execute(conn_str) opens a new PostgreSQL connection using conn_str, runs the INSERT‑SELECT, and stores any error in copy_to_stmt.err. The execution uses autocommit mode, which is appropriate for DML statements.

5. **Error handling** – Immediately after execution the helper check_stmt_err(copy_to_stmt, 'insert observations') is called. This function inspects copy_to_stmt.err; if an error is present it logs the failure and raises a DatabaseError. Because the caller ( run_r_obs_recover ) wraps the call in a conditional check, any exception will propagate and abort the overall recover process.

6. **Return value** – If the statement executes without raising an exception, the function reaches the final line and returns True to signal successful copying.

**Reference Relationships**:  
- **Caller** – The function is invoked only from run_r_obs_recover when the copy argument is True. In that context the caller first logs “copy to observation”, calls _copy_to_obs_table(conn_str, schema)_, checks the Boolean result, and logs “copied to observation” on success.  
- **Callee** – Internally the function uses the Statement class (pedsnetdcc/db.py) to run the SQL and the utility check_stmt_err (pedsnetdcc/utils.py) to validate the execution result. No other project functions are called directly.

**Note**:  
- The target observation table must already exist and have the same column layout as observation_derivation_recover; otherwise the INSERT will fail.  
- Because ON CONFLICT DO NOTHING is used, duplicate primary‑key values are ignored silently; this is intentional to avoid aborting the whole load when some rows already exist.  
- The function does not manage transactions; it relies on the autocommit behavior of Statement.execute.  
- Any database‑level error (e.g., permission denied, missing table, type mismatch) will be captured in copy_to_stmt.err and cause check_stmt_err to raise a DatabaseError, terminating the recover workflow.  
- The function returns a simple Boolean; callers should not expect any data payload.

**Output Example**:  
```python
result = _copy_to_obs_table(conn_str, 'dcc')
print(result)   # -> True  (indicates that rows were copied or already present)
```
## FunctionDef _create_argos_file(config_path, config_file, schema, password, conn_info_dict)
**_create_argos_file**: The function of _create_argos_file is to generate a temporary Argos JSON configuration file that supplies connection details for the R script used in observation derivation recovery.

**parameters**: The parameters of this Function.  
· **config_path**: The directory path where the Argos configuration file will be created. It is typically the site‑specific working directory prepared by `run_r_obs_recover`.  
· **config_file**: The name of the JSON file to be written (e.g., `site_observation_derivation_recover_argos_temp.json`).  
· **schema**: The PostgreSQL schema that should be placed in the `search_path` option of the Argos configuration.  
· **password**: The password for the database user; it is inserted directly into the JSON file.  
· **conn_info_dict**: A dictionary containing connection parameters extracted from the original connection string. Expected keys are `'host'`, `'dbname'`, and `'user'`.

**Code Description**:  
The function opens a new file at `os.path.join(config_path, config_file)` using binary write mode (`'wb'`). It then writes a JSON object line‑by‑line, inserting the appropriate line separator (`os.linesep`) after each line. The JSON structure is:

```
{
  "src_name": "Postgres",
  "src_args": {
    "host": "<host>",
    "port": 5432,
    "dbname": "<dbname>",
    "user": "<user>",
    "password": "<password>",
    "bigint": "numeric",
    "options": "-c search_path=<schema>,vocabulary"
  }
}
```

Values for `<host>`, `<dbname>`, and `<user>` are retrieved from `conn_info_dict` via the `get` method. The `<password>` value comes from the `password` argument. The `port` is hard‑coded to `5432`. The `options` field sets the PostgreSQL `search_path` to the supplied `schema` followed by the `vocabulary` schema, ensuring that the R script can resolve objects in both schemas.

After writing all lines, the file is closed automatically by the context manager.

**Relationship with Callers**:  
`_create_argos_file` is invoked by `run_r_obs_recover` (found in `pedsnetdcc/r_obs_recover.py`). In the recovery workflow, `run_r_obs_recover` prepares a site‑specific directory, copies the base R package files, and then calls `_create_argos_file` to place a temporary Argos configuration file in that directory. The subsequent step `_fix_site_info` and `_fix_run` modify R scripts to reference the newly created file. When the R script (`run.R`) is executed, it reads this Argos JSON to establish a connection to the PostgreSQL database, allowing the R code to query and manipulate observation data.

**Note**:  
- The file is opened in binary mode but strings are written directly; in Python 3 this raises a `TypeError`. The function should open the file in text mode (`'w'`) or encode strings to bytes before writing.  
- JSON is assembled manually; using `json.dump` would provide proper escaping and formatting, reducing the risk of malformed output.  
- The port number is fixed at `5432`; if the target database uses a different port, the function would need to be extended to accept it as a parameter.  
- No validation is performed on the values retrieved from `conn_info_dict`; missing keys will result in `None` being written to the JSON, which could cause connection failures in the R script.  
- The `options` field concatenates the schema name directly; if the schema name contains special characters, it may need quoting.
## FunctionDef _fix_site_info(file_path, site, schema)
**_fix_site_info**: The function of _fix_site_info is to substitute placeholder tokens in the R site‑information file with the actual site name and schema, then save the modified file.

**parameters**:  
· file_path: string – absolute path to the root directory of the R package copy (e.g., “…/observation_derivation_recover/site”).  
· site: string – the site identifier that should replace the `<SITE>` placeholder in the R file.  
· schema: string – the PostgreSQL schema name that should replace the `<SCHEMA>` placeholder in the R file.

**Code Description**:  
1. The function builds the full path to the file *site_info.R* located in the subdirectory *site* of *file_path* (`os.path.join(file_path, 'site', 'site_info.R')`).  
2. It opens this file in read mode, reads the entire content into the variable *file_data*, and closes the file.  
3. Two string replacements are performed on *file_data*:  
   * `<SITE>` is replaced with the value of the *site* argument.  
   * `<SCHEMA>` is replaced with the value of the *schema* argument.  
4. The function re‑opens the same *site_info.R* file in write mode and writes the updated *file_data* back, overwriting the original file.  
5. If any exception occurs during the read, replace, or write steps (for example, the file does not exist or the path is invalid), the function catches the exception, returns **False**, and silently ignores the error.  
6. If all operations succeed, the function returns **True**.

**Relationship with Callers**:  
The only current caller is `run_r_obs_recover` in *pedsnetdcc/r_obs_recover.py*. Within that workflow, a site‑specific copy of the R package is created, and `_fix_site_info` is invoked immediately after the Argos configuration file is generated. By inserting the concrete *site* and *schema* values into *site_info.R*, the subsequent R script (`run.R`) can correctly reference the target database schema and site identifier when it is executed. If `_fix_site_info` fails (returns **False**), the R script will still be run, but it may contain unresolved placeholders, potentially causing runtime errors in the R environment.

**Note**:  
- The function uses a bare `except:` clause, which suppresses all exceptions and may hide underlying I/O problems; callers should check the Boolean return value to detect failure.  
- The placeholders `<SITE>` and `<SCHEMA>` must exist in the original *site_info.R* file; otherwise the replacements will have no effect.  
- The function assumes the directory structure `file_path/site/site_info.R` is present; if the package version does not contain this file, the function will return **False** without raising an error.  
- No validation of the *site* or *schema* strings is performed; they are inserted verbatim.

**Output Example**:  
```python
result = _fix_site_info('/app/observation_derivation_recover', 'my_site', 'my_schema')
# result will be True if the file was found and updated, otherwise False
```
## FunctionDef _fix_run(file_path, site)
**_fix_run**: The function of _fix_run is to replace the placeholder `<SITE>` with the actual site name in the `run.R` script of a given observation derivation package.

**parameters**:  
· file_path: The root directory path where the observation derivation package has been copied (e.g., `/app/observation_derivation_recover`).  
· site: The site identifier string that should replace the `<SITE>` placeholder (e.g., `'dcc'` or `'onco'`).

**Code Description**:  
1. The function builds the full path to the `run.R` file located in the `site` sub‑directory of the supplied `file_path` (`os.path.join(file_path, 'site', 'run.R')`).  
2. It attempts to open this file in read mode. If the file does not exist or cannot be opened, the `except` block is triggered, and the function returns `False` to indicate that the expected script was not found.  
3. When the file is successfully read, its contents are stored in `file_data`. The function then performs a simple string replacement: every occurrence of the literal token `<SITE>` is replaced with the value of the `site` argument.  
4. The modified content is written back to the same `run.R` file, overwriting the original file.  
5. If the read‑modify‑write sequence completes without raising an exception, the function returns `True` to signal success.

**Relationship with Callers**:  
`_fix_run` is invoked by the `run_r_obs_recover` function in `pedsnetdcc/r_obs_recover.py`. After `run_r_obs_recover` copies the generic observation derivation package to a site‑specific directory and creates the Argos configuration file, it calls `_fix_site_info` and then `_fix_run`. The purpose of `_fix_run` in this workflow is to personalize the R script (`run.R`) for the target site before the script is executed via the `Rscript` command. By ensuring the placeholder is replaced, the subsequent R execution uses the correct site identifier, which is essential for generating site‑specific observation data.

**Note**:  
- The function uses a bare `except:` clause, which catches any exception type, including unrelated errors such as permission issues. It deliberately treats any failure as “file not present” and returns `False`.  
- No explicit logging is performed inside `_fix_run`; callers must handle the Boolean return value to decide whether to proceed or raise an error.  
- The replacement is case‑sensitive and will replace all literal occurrences of `<SITE>`; ensure that the placeholder appears exactly as expected in the original `run.R`.

**Output Example**:  
When the `run.R` file exists and the replacement succeeds:  
`True`  

When the `run.R` file is missing or an error occurs during processing:  
`False`
## FunctionDef _make_index_name(column_name)
**_make_index_name**: The function of _make_index_name is to generate a deterministic, length‑constrained index name for a given column in the *recover_derivation* observation table.

**parameters**:  
· column_name: *str* – the name of the column (in snake_case) for which the index name is to be created.

**Code Description**:  
1. The function works with a fixed logical table name, `'recover_derivation'`.  
2. An abbreviated table prefix is built as `"obs_"` plus the first three characters of the table name, resulting in the string `"obs_rec"`.  
3. The column abbreviation is created by taking the first character of each underscore‑separated token in *column_name*. For example, `'observation_concept_id'` becomes `'oci'`.  
4. A MD5 hash is calculated from the string `"{table_name}.{column_name}"` (e.g., `"recover_derivation.observation_concept_id"`). The hash is expressed as a hexadecimal string.  
5. The maximum length allowed for an Oracle identifier (`NAME_LIMIT`) is taken into account. The function computes how many characters of the MD5 hash can be used (`hashlen`) by subtracting the lengths of the table abbreviation, column abbreviation, three underscores, and the suffix `'ix'` from `NAME_LIMIT`.  
6. The final index name is assembled by joining the table abbreviation, column abbreviation, the truncated MD5 hash, and the literal `'ix'` with underscores:  
   `"{table_abbrev}_{column_abbrev}_{md5[:hashlen]}_ix"`.  
7. The resulting string is returned.

**Relationship with Callers**:  
`_make_index_name` is invoked inside `run_r_obs_recover` when the script adds indexes to the *observation_derivation_recover* table. For each column listed in `col_index`, the caller obtains a unique index identifier by passing the column name to this function. The generated name is then interpolated into the SQL template `IDX_OBS_LIKE_TABLE_SQL` to create the `CREATE INDEX` statement. By using a hash component, the function avoids name collisions that could arise from similarly named columns across different source tables (e.g., `provider.gender_source_concept_name` vs. `person.gender_source_concept_name`).

**Note**:  
- `NAME_LIMIT` must be defined elsewhere in the module (commonly 30 for Oracle). If it is not set, a `NameError` will be raised.  
- The function assumes *column_name* follows snake_case conventions; unusual characters may affect the column abbreviation.  
- Because the MD5 hash is deterministic, the same *column_name* will always produce the same index name, ensuring repeatable migrations.  
- The returned name always ends with the suffix `'ix'`, indicating an index object.

**Output Example**:  
```python
>>> _make_index_name('observation_concept_id')
'obs_rec_oci_ae1fd5b22b92397ca9_ix'
```  
(The exact hash segment will vary depending on the value of `NAME_LIMIT`.)
## FunctionDef run_r_obs_recover(conn_str, site, password, search_path, model_version, id_name, copy, noidx, nofk, skip_query, limit, owner)
**run_r_obs_recover**: The function of run_r_obs_recover is to orchestrate the full observation‑derivation recovery workflow for a given site, including preparation of an Argos configuration, execution of the R script, post‑processing of the derived table, and optional copying of the results into the production observation table.

**parameters**:  
· **conn_str** (str): PostgreSQL libpq connection string used for all database operations.  
· **site** (str): Identifier of the site for which the recovery is performed (e.g., “dcc” or “onco”).  
· **password** (str): Password for the database user; if None the password is extracted from *conn_str*.  
· **search_path** (str): PostgreSQL schema search_path; the first schema is treated as the primary schema.  
· **model_version** (str): PEDSnet data‑model version (e.g., “2.3.0”). Required for metadata lookup.  
· **id_name** (str): Base name of the last‑id tracking table (e.g., “dcc” or “onco”).  
· **copy** (bool): When True, rows from the temporary recovery table are copied into the production observation table.  
· **noidx** (bool): When True, index creation on the recovery table is skipped.  
· **nofk** (bool): When True, foreign‑key creation on the recovery table is skipped.  
· **skip_query** (bool, default False): When True, the R script is not executed; only the SQL post‑processing steps are performed.  
· **limit** (bool, default False): When True, permissions are granted only to the *owner* user; otherwise a default set of users receives permissions.  
· **owner** (str, default 'loading_user'): Name of the user that receives permissions when *limit* is True.

**Code Description**:  

1. **Initial setup** –  
   * The R package name is fixed as `observation_derivation_recover`.  
   * A temporary Argos JSON file name is built from the site and package (`<site>_observation_derivation_recover_argos_temp.json`).  
   * `get_conn_info_dict` parses *conn_str* into a dictionary of connection attributes.  
   * Logging context (`log_dict`) is created by merging the site identifier with the connection info. A start‑up log entry is emitted and the start time is recorded (`start_time`).  
   * The primary schema is derived from *search_path* via `primary_schema`.  

2. **Password handling** – If *password* is None, a regular‑expression search extracts the password from *conn_str*.

3. **Prepare site‑specific R package directory** –  
   * Source directory: `/app/observation_derivation_recover`.  
   * Destination directory: `/app/observation_derivation_recover/<site>`.  
   * Any existing destination directory is removed (`shutil.rmtree`).  
   * The entire source tree is copied to the destination (`shutil.copytree`).  

4. **Create Argos configuration** – `_create_argos_file` writes a JSON file containing host, port, dbname, user, password, and a `search_path` that includes the primary schema and the `vocabulary` schema. The file is placed in the site‑specific directory.

5. **Inject site and schema into R scripts** –  
   * `_fix_site_info` replaces the placeholders `<SITE>` and `<SCHEMA>` in `site/site_info.R` with the actual *site* and *schema* values.  
   * `_fix_run` replaces `<SITE>` in `site/run.R` with the actual *site*.  

6. **Execute the R script** – If *skip_query* is False, the script `run.R` is invoked with `Rscript`. The working directory is forced to `/app` and verbose output is enabled. Upon completion a log entry records the elapsed time.

7. **Drop primary key on the recovery table** – A `StatementSet` containing a single `DROP PK` statement (`DROP_PK_CONSTRAINT_SQL`) is executed. Errors are caught, logged, and re‑raised.

8. **Drop NOT NULL constraints** – A second `StatementSet` runs `DROP_NULL_SQL` to remove all NOT NULL constraints from the recovery table. Errors are handled similarly.

9. **Add site identifier column** –  
   * If *site* equals “dcc”, `ADD_DCC_SITE_SQL` is used; otherwise `ADD_SITE_SQL` is formatted with the *site* name.  
   * The statement is executed and any error causes a logged fatal error and exception.

10. **Assign observation IDs** – `_add_observation_ids` is called with the connection string, *site*, *search_path*, *model_version*, and *id_name*. This function counts rows lacking an ID, reserves a block of IDs from the last‑id tracking table, creates a sequence, populates `observation_id`, and makes it the primary key. If the function returns False (which occurs only on exception), `run_r_obs_recover` aborts and returns False.

11. **Populate concept name columns** – `_fill_concept_names` updates the five `*_concept_name` columns by joining the corresponding concept IDs to `vocabulary.concept`. Failure raises an exception; on success the function returns True.

12. **Calculate observation age in months** – `_fill_age_in_months` creates helper SQL functions (`last_month_of_interval`, `month_after_last_month_of_interval`, `days_in_last_month_of_interval`, `months_in_interval`) in the target schema and updates `observation_age_in_months` for every row. Errors raise an exception.

13. **Create indexes (optional)** – If *noidx* is False, an index is created for each column listed in `col_index`. Index names are generated by `_make_index_name`. All index statements are added to a `StatementSet` and executed in parallel (`parallel_execute`). Errors are logged and cause an exception.

14. **Create foreign keys (optional)** – If *nofk* is False, foreign‑key constraints are generated for each column in `col_fk`. The reference table and column are derived from the column name (concept‑id columns reference `vocabulary.concept`). Statements are added to a `StatementSet` and executed in parallel. Errors are logged and cause an exception.

15. **Copy to production observation table (optional)** – If *copy* is True, `_copy_to_obs_table` inserts all rows from `observation_derivation_recover` into the production `observation` table using `INSERT … SELECT … ON CONFLICT DO NOTHING`. Errors raise an exception.

16. **Grant permissions** – A `StatementSet` builds `GRANT` statements (`GRANT_OBS_LIKE_TABLE_SQL`).  
    * If *limit* is True, only the *owner* receives permissions; otherwise the default users `peds_staff` and `dcc_analytics` receive them.  
    * Each statement is executed; any error is logged and re‑raised.

17. **Vacuum and analyze** – `vacuum` is called for the `observation_derivation_recover` table with `analyze=True` to refresh statistics.

18. **Final logging and return** – A concluding log entry records the total elapsed time. If no exception was raised, the function returns **True** to indicate successful completion.

**Note**:  
- The function relies heavily on the `Statement` and `StatementSet` classes for all DDL/DML execution; each statement’s error must be inspected via `check_stmt_err`.  
- All SQL statements are executed in autocommit mode; therefore they cannot be rolled back by a surrounding transaction.  
- The Argos JSON file is written in binary mode; in Python 3 this requires the strings to be bytes. If the implementation is not adjusted, a `TypeError` may be raised.  
- Placeholders `<SITE>` and `<SCHEMA>` must exist in the original R package files; otherwise the corresponding fix functions will silently return False.  
- The function does not catch exceptions from the helper functions; any `DatabaseError` or other exception propagates to the caller, causing the overall recovery process to abort.  
- Index name generation uses a fixed table name (`recover_derivation`) and a global `NAME_LIMIT`; if `NAME_LIMIT` is not defined, a `NameError` will occur.  

**Output Example**:  
```python
result = run_r_obs_recover(
    conn_str="host=pghost port=5432 dbname=pedsnet user=loader password=secret options='-c search_path=dcc_pedsnet,public'",
    site="dcc",
    password=None,
    search_path="dcc_pedsnet,public",
    model_version="2.3.0",
    id_name="dcc",
    copy=True,
    noidx=False,
    nofk=False,
    skip_query=False,
    limit=False,
    owner="loading_user"
)
print(result)   # -> True (recovery completed successfully)
```
## FunctionDef _clear_fake_ids(conn_str, schema)
**_clear_fake_ids**: The function of _clear_fake_ids is to remove placeholder (“fake”) observation identifiers from the observation_derivation_covid table in a given schema.

**parameters**:  
· conn_str: a libpq‑style PostgreSQL connection string used to open a database connection.  
· schema: the name of the database schema that contains the observation_derivation_covid table; it is interpolated into the SQL statement.

**Code Description**:  
1. **SQL preparation** – The function builds a simple UPDATE statement that sets observation_id to NULL for all rows in {schema}.observation_derivation_covid. The placeholder {0} in the string is replaced by the supplied schema value via format().  
2. **Logging start** – An informational log entry is emitted with the message ‘begin clearing "fake" ids’ to indicate the operation is starting. The logger used is the module‑level logger imported elsewhere in the package.  
3. **Statement creation** – A Statement object is instantiated with the formatted SQL and a human‑readable message ‘clearing \'fake\' ids’. This object encapsulates the command and its metadata.  
4. **Execution** – The Statement.execute() method is called with the provided conn_str. This method opens a new PostgreSQL connection (autocommit mode), runs the UPDATE, captures the row count, and stores any exception in stmt.err.  
5. **Error checking** – After execution, the helper check_stmt_err(fake_id_stmt, 'clear "fake" ids') is invoked. It inspects fake_id_stmt.err; if an error is present, it logs a detailed error message and raises a DatabaseError exception, causing the function to abort.  
6. **Logging completion** – If no exception was raised, a second informational log entry records that the “fake ids” have been cleared.  
7. **Return value** – The function returns True to signal successful completion. No data is returned from the UPDATE; the success indicator is the absence of an exception.

**Relationship with callees**:  
- **Statement** (pedsnetdcc/db.py) is used to package and run the UPDATE command. Its execute method handles connection management, logging, and result capture.  
- **check_stmt_err** (pedsnetdcc/utils.py) validates the execution outcome, logs any failure, and raises a DatabaseError if the UPDATE did not succeed. Together these utilities provide a consistent pattern for database modifications across the project.

**Note**:  
- The function assumes that the target table observation_derivation_covid exists in the specified schema and that the user identified by conn_str has UPDATE privileges on it.  
- Because the UPDATE sets the column to NULL for every row, it is irreversible; ensure that “fake” IDs truly need to be removed before invoking this function.  
- The function does not return the number of rows affected; callers must rely on the log output or inspect the Statement.rowcount attribute (available on fake_id_stmt after execution) if that information is required.  
- Any exception raised by Statement.execute or check_stmt_err will propagate out of the function, so callers should be prepared to handle DatabaseError or other unexpected exceptions.

**Output Example**:  
```python
result = _clear_fake_ids(conn_str, 'dcc_observation')
print(result)   # -> True
# Log entries (simplified):
# {'msg': 'begin clearing "fake" ids'}
# {'msg': '"fake ids cleared'}
```
## FunctionDef _add_observation_ids(conn_str, site, search_path, model_version, id_name)
**_add_observation_ids**: The function of _add_observation_ids is to assign new, unique observation_id values to rows in the observation_derivation_recover table, update the corresponding last‑id tracking table, create and set a PostgreSQL sequence, and finally make observation_id the primary key.

**parameters**:  
· **conn_str** (str): PostgreSQL connection string used to open a database connection.  
· **site** (str): Site identifier (e.g., “dcc” or “onco”) that is incorporated into the name of the sequence that will be created.  
· **search_path** (str): PostgreSQL schema search_path; the first schema in the list is used as the primary schema for all SQL statements.  
· **model_version** (str): PEDSnet data‑model version (e.g., “2.3.0”) used to retrieve the stock metadata for the observation table.  
· **id_name** (str): Base name for the last‑id tracking table (e.g., “dcc” or “onco”).  

**Code Description**:  

1. **Preparation and logging** – The function extracts connection information with `get_conn_info_dict`, merges it with the site identifier via `combine_dicts`, and logs the start of the observation‑id assignment. The start time is recorded for elapsed‑time logging.

2. **Schema and table resolution** – `primary_schema(search_path)` returns the first schema in the supplied search_path; this schema is stored in `schema`. The target table name is fixed as `'observation'`. The name of the last‑id tracking table is built from `id_name` and the table name (`"{id_name}_observation_id"`).

3. **Metadata acquisition** – `stock_metadata(model_version)` obtains a SQLAlchemy `MetaData` object for the requested model version. The `observation` table object is retrieved from `metadata.tables` to ensure the table exists in the model.

4. **Count rows needing new IDs** – A `Statement` is created with SQL that counts rows in `{schema}.observation_derivation_recover` where `observation_id IS NULL`. The statement is executed, and `check_stmt_err` and `check_stmt_data` verify that the execution succeeded and returned a count. The count is incremented by one (to reserve the next free ID) and stored in `tpl_vars['new_id_count']`.

5. **Reserve a block of IDs atomically** – Two statements are placed in a `StatementList`:
   * `LOCK {last_id_table_name}` – acquires an exclusive lock on the last‑id tracking table.
   * `UPDATE {last_id_table_name} AS new SET last_id = new.last_id + '{new_id_count}'::bigint FROM {last_id_table_name} AS old RETURNING old.last_id, new.last_id` – increments the stored last_id by the number of IDs needed and returns the old and new values.
   The list is executed with `serial_execute(conn_str, transaction=True)`, guaranteeing that the lock and update occur in a single transaction. After execution, the old and new last‑id values are extracted from the second statement’s result and logged.

6. **Create the observation‑id sequence** – A `Statement` runs `CREATE SEQUENCE IF NOT EXISTS {schema}.{site}_obs_derivation_recover_seq`. Errors are captured with `check_stmt_err`.

7. **Set the sequence start value** – The sequence is altered to restart with the first unused ID (`old_last_id + 1`) using `ALTER SEQUENCE … RESTART WITH …`. Execution is checked for errors.

8. **Populate observation_id values** – An `UPDATE` statement assigns `observation_id = nextval('{schema}.{site}_obs_derivation_recover_seq')` to all rows where `observation_id IS NULL`. Errors are checked.

9. **Make observation_id the primary key** – A final `ALTER TABLE … ADD PRIMARY KEY (observation_id)` statement is executed, turning the newly populated column into the table’s primary key. Errors are checked.

10. **Final logging and return** – The function logs completion with the elapsed time and returns `True` to indicate success. Any failure in the steps above raises a `DatabaseError` (via `check_stmt_err`) or `MissingDataError` (via `check_stmt_data`), causing the caller to handle the exception.

**Reference relationships** –  
*Caller*: The function is invoked by `run_r_obs_recover` (in `pedsnetdcc/r_obs_recover.py`) after the R script has been executed and after preliminary schema modifications (dropping PK, adding site information, etc.). `run_r_obs_recover` expects a boolean return; if `False` is returned, the overall recovery process aborts.  
*Callees*: The function relies on several utility objects and functions:
  - `Statement`, `StatementList`, and their methods `execute`, `execute_on_conn`, `serial_execute` for SQL execution and transaction handling.
  - `check_stmt_err` and `check_stmt_data` for centralized error detection.
  - `get_conn_info_dict` and `combine_dicts` for enriched logging.
  - `primary_schema` to derive the working schema from the search_path.
  - `stock_metadata` to obtain the model’s table definitions.
  - Logging utilities (`logger`, `secs_since`) for structured log messages.

**Note**:  
- The function assumes that the last‑id tracking table `{id_name}_observation_id` already exists and contains a column `last_id`. If the table is missing or the column does not exist, the UPDATE statement will fail and a `DatabaseError` will be raised.  
- The sequence name is `{site}_obs_derivation_recover_seq`; it is created only if it does not already exist, so repeated calls will reuse the existing sequence after resetting its start value.  
- All SQL statements are formatted with `schema` and `site` values; callers must ensure that these identifiers are safe PostgreSQL identifiers (no injection risk).  
- The function runs several DDL commands (CREATE SEQUENCE, ALTER SEQUENCE, ALTER TABLE). These commands are executed in autocommit mode, so they cannot be rolled back by the surrounding transaction in `run_r_obs_recover`. Errors are caught and re‑raised as `DatabaseError`.  
- The function returns `True` only when every step completes without raising an exception; any exception propagates to the caller, which typically logs the failure and aborts the recovery workflow.

**Output Example**:  
When called with valid arguments and a database state that requires 42 new observation IDs, the function will log the counting, locking, sequence creation, ID assignment, and primary‑key creation steps, and finally return:

```python
result = _add_observation_ids(conn_str, 'dcc', 'dcc_pedsnet, public', '2.3.0', 'dcc')
# result == True
```
