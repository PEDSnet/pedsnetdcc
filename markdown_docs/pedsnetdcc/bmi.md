## FunctionDef _create_config_file(config_path, config_file, schema, table, out_table, password, max_time, conn_info_dict)
**_create_config_file**: The function of _create_config_file is to generate a configuration file required by the BMI calculation tool, populating it with measurement concept identifiers, database connection parameters, and processing options.

**parameters**: The parameters of this Function.  
· **config_path**: Directory path where the configuration file will be created.  
· **config_file**: Name of the configuration file to be written (e.g., “bmi.cfg”).  
· **schema**: Target database schema that contains the source measurement table.  
· **table**: Name of the input measurement table used by the BMI tool.  
· **out_table**: Name of the output table that will receive the BMI results.  
· **password**: Password for the database user; written into the config file for authentication.  
· **max_time**: Maximum allowed interval (in seconds) between a height and a weight measurement; written as the `meas_match_limit_sec` setting.  
· **conn_info_dict**: Dictionary containing database connection details; expected keys are `'host'`, `'dbname'`, and `'user'`.

**Code Description**:  
The function opens (or creates) a file at the location `os.path.join(config_path, config_file)` in binary write mode. It then sequentially writes a series of configuration directives, each terminated by the platform‑specific line separator (`os.linesep`). The written sections are:

1. **Static measurement concept IDs** – height (`ht_measurement_concept_ids`), weight (`wt_measurement_concept_ids`), the BMI measurement itself (`bmi_measurement_concept_id`), its type (`bmi_measurement_type_concept_id`), and the unit (`bmi_unit_concept_id`).  
2. **Input/Output table settings** – the source measurement table (`input_measurement_table`), chunk sizes for processing persons and measurements (`output_chunk_size`, `person_chunk_size`), a flag to clone BMI measurements (`clone_bmi_measurements`), verbosity (`verbose`), and the maximum time gap between height and weight (`meas_match_limit_sec`).  
3. **Source relational database block** (`<src_rdb> … </src_rdb>`) – driver set to PostgreSQL (`Pg`), host, database name, schema, username, password, a fixed domain (`stage`), type (`dcc`), and a post‑connection SQL command that sets the search path to the supplied schema and the vocabulary schema.  
4. **Output table definition** – the name of the table that will store the derived BMI measurements (`output_measurement_table`).  
5. **Person finder SQL** – a query that selects distinct `person_id`s from the input table where the measurement concept corresponds to height or weight concepts. This query is written across two `out_config.write` calls to keep the line length manageable.

The function does not return a value; its purpose is side‑effect driven (file creation). It is invoked by `run_bmi_calc` after the caller has prepared connection information and transformed the `max_time` argument into seconds. In `run_bmi_calc`, the configuration file is placed in the container’s `/app` directory, which is also the working directory used when the external BMI tool (`derive_bmi`) is executed. The generated file supplies all necessary parameters for the BMI tool to read source measurements, match height‑weight pairs within the allowed time window, compute BMI, and write results to the specified output table.

**Note**:  
- The file is opened in binary mode (`'wb'`) but strings are written directly; in Python 3 this will raise a `TypeError`. Ensure the file is opened in text mode (`'w'`) or encode the strings to bytes before writing.  
- All numeric IDs are hard‑coded; changes to concept identifiers in the underlying OMOP vocabulary require updating this function.  
- `max_time` must be supplied as a string representing seconds; callers convert days to seconds before passing it.  
- `conn_info_dict` must contain the keys `'host'`, `'dbname'`, and `'user'`; missing keys will result in `None` being written to the config file, causing connection failures.  
- The function overwrites any existing file with the same name; callers should ensure that the target path is writable and that overwriting is acceptable.  
- The generated configuration assumes the BMI tool expects the exact directive names and ordering shown; modifications to the tool’s expected format will require corresponding changes here.
## FunctionDef _make_index_name(table_name, column_name)
**_make_index_name**: The function of _make_index_name is to generate a deterministic, length‑constrained index name for a given table‑column pair, suitable for Oracle’s identifier length limit.  

**parameters**:  
· table_name (str): The name of the database table for which the index will be created.  
· column_name (str): The name of the column (or column list) that the index will cover.  

**Code Description**:  
The function builds an index identifier that never exceeds the constant `NAME_LIMIT` (the maximum identifier length imposed by Oracle). The steps are:

1. **Table abbreviation** – The first three characters of `table_name` are prefixed with the literal `'mea'` to form `table_abbrev`.  
   Example: for `table_name='bmi'` → `table_abbrev='meabmi'` (actually `'mea' + 'bmi'[:3]` = `'meabmi'`).  

2. **Column abbreviation** – The column name is split on underscores (`'_'`). The first character of each segment is taken and concatenated, producing `column_abbrev`.  
   Example: `column_name='measurement_age_in_months'` → `['measurement','age','in','months']` → `'mai'`.  

3. **Hash component** – A MD5 hash is calculated over the string `"{table_name}.{column_name}"` encoded as UTF‑8. The hexadecimal digest (`md5.hexdigest()`) yields a 32‑character string.  

4. **Length calculation** – The number of hash characters that can be safely included (`hashlen`) is computed as:  

   ```
   hashlen = NAME_LIMIT
             - (len(table_abbrev) + 4 + len(column_abbrev)
                + 3 * len('_') + len('ix'))
   ```

   The constant `4` accounts for the three underscores that will separate the parts and the trailing `'ix'` suffix. This ensures the final assembled name respects the length limit.  

5. **Final assembly** – The parts are joined with underscores:  

   ```
   return '_'.join([table_abbrev, column_abbrev, md5[:hashlen], 'ix'])
   ```

   The result looks like `mea<tbl>_<colabbr>_<hash>_ix`. Because the hash is derived from the full table‑column string, collisions are extremely unlikely even for similarly named columns in different tables.

**Relationship with Callers**:  
`_make_index_name` is invoked in `pedsnetdcc/bmi.py/run_bmi_calc`. When the BMI tool creates the output table (default `measurement_bmi`), `run_bmi_calc` iterates over a predefined list of columns that require indexes. For each column, it calls `_make_index_name('bmi', col)` to obtain a unique, Oracle‑compatible index name, which is then embedded in the SQL statement `IDX_MEASURE_LIKE_TABLE_SQL`. This ensures that the generated indexes can be created without exceeding identifier length restrictions, avoiding naming conflicts between tables such as `provider` and `person` that may have similarly structured column names.

**Note**:  
- The function assumes that `NAME_LIMIT` and the `hashlib` module are defined/imported elsewhere in the module.  
- The abbreviation scheme (`'mea'` prefix) is hard‑coded; changing it requires updating both the function and any dependent naming conventions.  
- Because the hash is truncated to fit the length limit, the exact number of hash characters varies with the lengths of the table and column abbreviations.  

**Output Example**:  
Calling `_make_index_name('bmi', 'measurement_age_in_months')` (with `NAME_LIMIT = 30`) could produce a string similar to:  

```
meabmi_mai_a1b2c3d4e5f6g7h8_ix
``` 

where `a1b2c3d4e5f6g7h8` represents the leading portion of the MD5 hash truncated to satisfy the length constraint.
## FunctionDef _fill_age_in_months(conn_str, schema, out_table)
**_fill_age_in_months**: The function of _fill_age_in_months is to create helper PostgreSQL functions for interval calculations and then populate the column measurement_age_in_months in a specified output table with the age of each measurement expressed in months.

**parameters**:  
· conn_str: a libpq‑style connection string used to connect to the target PostgreSQL database.  
· schema: the name of the schema in which the helper functions will be created (and where the output table resides).  
· out_table: the name of the table that will receive the computed measurement_age_in_months values.

**Code Description**:  
1. **SQL definition block** – A multi‑line string fill_add_age_in_months_sql is built. It contains four CREATE OR REPLACE FUNCTION statements that are placed in the supplied schema:  
   * last_month_of_interval(ts1, ts2) returns the timestamp of the last month that falls inside the interval between ts1 and ts2.  
   * month_after_last_month_of_interval(ts1, ts2) returns the timestamp of the month immediately after that last month.  
   * days_in_last_month_of_interval(ts1, ts2) returns the number of days in the month identified by the previous function (computed as the difference between the two timestamps).  
   * months_in_interval(ts1, ts2) returns a double‑precision value representing the total number of months (including fractional months) between the two timestamps. The fractional part is calculated by dividing the extracted day count by the number of days in the last month of the interval.  

   Each function is marked STRICT and IMMUTABLE and is accompanied by a COMMENT describing its purpose.

2. **UPDATE statement** – After the helper functions, an UPDATE command is generated that writes into {schema}.{out_table} (the alias b) the result of the expression {schema}.months_in_interval(p.birth_datetime, m.measurement_datetime) for every row m in the same table. The sub‑query joins the measurement row with the corresponding person row (via person_id) to obtain the birth date. The computed value is stored in the column measurement_age_in_months.

3. **Statement creation** – The formatted SQL (with schema and out_table substituted) is wrapped in a Statement object together with a descriptive message "adding age in months". This object encapsulates the SQL and provides execution metadata.

4. **Execution** – The execute method of the Statement instance is called with conn_str. The method opens a new database connection, runs the SQL, and captures any error in stmt.err. After execution, the helper function check_stmt_err is expected to be called by the caller to raise an exception if an error occurred.

5. **Return value** – If the SQL runs without raising an exception, the function returns True to signal successful completion.

**Reference relationships**:  
*Caller* – The function is invoked from run_bmi_calc (pedsnetdcc/bmi.py) when the age flag is true. run_bmi_calc logs the start of the operation, calls _fill_age_in_months, checks the returned Boolean, and logs success or failure.  
*Callees* – Within its body the function uses the Statement class (pedsnetdcc/db.py) to execute the generated SQL. It does not directly call check_stmt_err; that responsibility lies with the caller (run_bmi_calc) which inspects the statement after execution. No other project modules are called.

**Note**:  
- The helper functions are created with CREATE OR REPLACE so repeated calls will overwrite existing definitions without error.  
- The target table must already contain a column named measurement_age_in_months of a numeric type compatible with double precision; otherwise the UPDATE will fail.  
- The function assumes that the person table exists in the same schema and that each measurement row has a matching person_id.  
- Because the SQL is executed in a single statement, all helper functions and the UPDATE run in one transaction (autocommit mode). Any failure aborts the whole statement and stmt.err will be populated.  
- The function does not perform its own error handling; callers must invoke check_stmt_err or otherwise inspect stmt.err to detect problems.

**Output Example**:  
```python
result = _fill_age_in_months(conn_str, 'dcc_pedsnet', 'measurement_bmi')
print(result)   # -> True  (indicates that the age‑in‑months column was successfully populated)
```
## FunctionDef _fill_concept_names(conn_str, out_table)
**_fill_concept_names**: The function of _fill_concept_names is to populate the output BMI measurement table with human‑readable concept name columns derived from the OMOP vocabulary.

**parameters**:  
· **conn_str**: a libpq‑style PostgreSQL connection string used to open a database connection.  
· **out_table**: the name of the BMI output table (e.g., `measurement_bmi`) that will receive the concept name columns.

**Code Description**:  
1. **SQL statement construction** – The function defines a multi‑line SQL string (`fill_concept_names_sql`). This statement performs an `UPDATE` on the table identified by `out_table` (aliased as `bmi`). For each row, it sets nine new columns (`measurement_concept_name`, `measurement_source_concept_name`, `measurement_type_concept_name`, `operator_concept_name`, `priority_concept_name`, `range_high_operator_concept_name`, `range_low_operator_concept_name`, `unit_concept_name`, `value_as_concept_name`) to the corresponding `concept_name` values obtained from the `vocabulary.concept` table.  

   The `FROM` clause builds a derived table `v` that selects the `measurement_id` from `measurement_bmi` (aliased as `b`) and left‑joins `vocabulary.concept` nine times (aliases `v1` … `v9`). Each join matches a specific concept‑id column in `measurement_bmi` (e.g., `measurement_concept_id`, `operator_concept_id`, etc.) to `concept_id` in the vocabulary, retrieving the textual `concept_name`.  

   The outer `UPDATE` matches rows by `measurement_id` (`WHERE bmi.measurement_id = v.measurement_id`) and writes the retrieved names into the target columns.

2. **Statement creation** – A human‑readable message `"adding concept names"` is stored in `fill_concept_names_msg`. A `Statement` object (`add_measurement_ids_stmt`) is instantiated with the formatted SQL (`fill_concept_names_sql.format(out_table)`) and the message. The `Statement` class encapsulates the SQL, execution metadata, and provides methods for safe execution.

3. **Execution** – The statement’s `execute` method is called with `conn_str`. This opens a new database connection, runs the `UPDATE`, and stores the result metadata (`rowcount`, `err`, etc.) inside the `Statement` instance.

4. **Error handling** – After execution, the helper `check_stmt_err` is invoked with the statement and the context string `'add concept names'`. `check_stmt_err` inspects `stmt.err`; if an error is present it logs the failure and raises a `DatabaseError`. If no error occurs, execution proceeds.

5. **Return value** – When the update completes without raising an exception, the function returns `True` to signal success.

**Reference Relationships**:  
- **Caller** – The function is invoked from `run_bmi_calc` (pedsnetdcc/bmi.py) when the `concept` flag is true. `run_bmi_calc` logs the start of the operation, calls `_fill_concept_names(conn_str, out_table)`, checks the Boolean return, and logs completion.  
- **Callee** – Internally the function relies on the `Statement` class (pedsnetdcc/db.py) for SQL execution and on `check_stmt_err` (pedsnetdcc/utils.py) for standardized error detection and logging. No other project functions are called directly.

**Note**:  
- The function assumes that the target table (`out_table`) already contains the nine concept‑id columns referenced in the joins; otherwise the `UPDATE` will fail with a column‑not‑found error.  
- All joins are left joins; if a concept‑id is null or does not have a matching entry in `vocabulary.concept`, the corresponding concept‑name column will be set to `NULL`.  
- The function does not commit or roll back explicitly; the `Statement.execute` method runs the statement in autocommit mode, so changes are persisted immediately.  
- Any database error (e.g., permission issues, missing schema) will be captured in `stmt.err` and cause `check_stmt_err` to raise a `DatabaseError`, aborting the BMI workflow.  

**Output Example**:  
```python
result = _fill_concept_names(conn_str, 'measurement_bmi')
print(result)   # -> True  (indicates that concept name columns were successfully populated)
```
## FunctionDef _copy_to_measurement_table(conn_str, schema, table, out_table)
**_copy_to_measurement_table**: The function of _copy_to_measurement_table is to insert BMI measurement rows from a temporary output table into the canonical measurement table within the same schema, ignoring duplicate rows.

**parameters**:  
· conn_str: PostgreSQL connection string used to open a database connection.  
· schema: Name of the schema that contains both the target measurement table and the source temporary table.  
· table: Name of the target measurement table (e.g., `measurement`).  
· out_table: Name of the source table that holds the BMI results (e.g., `measurement_bmi`).  

**Code Description**:  
1. **SQL statement construction** – A multi‑line INSERT … SELECT statement is built in the variable `copy_to_sql`.  
   * The target columns are listed explicitly (measurement_concept_id, measurement_date, …, site_id).  
   * The SELECT clause mirrors the column list and pulls data from `{schema}.{out_table}` (the BMI output table).  
   * The statement ends with `ON CONFLICT DO NOTHING`, which tells PostgreSQL to silently skip rows that would violate a unique constraint on the target table.  
   * The placeholders `{0}`, `{1}`, and `{2}` are later replaced by `schema`, `table`, and `out_table` respectively via `format`.  

2. **Logging message** – `copy_to_msg` is set to the human‑readable string “copying bmi to measurement”. This message is attached to the `Statement` object for later debugging and log output.

3. **Statement creation** – An instance of `pedsnetdcc.db.Statement` is created with the formatted SQL string and the descriptive message. The `Statement` class encapsulates the SQL, tracks execution metadata, and provides a uniform interface for error handling.

4. **Execution** – The `execute` method of the `Statement` instance is called with `conn_str`. This opens a new database connection, runs the INSERT … SELECT command, and stores the result in the statement’s attributes (`rowcount`, `err`, etc.).  

5. **Error checking** – Immediately after execution, the helper `check_stmt_err` is invoked with the statement object and the context string `'insert BMI measurements'`. If the statement’s `err` attribute is not `None`, `check_stmt_err` logs the error and raises a `DatabaseError`, causing the calling function to abort.  

6. **Success return** – If no exception is raised, the function reaches the final line and returns `True`, signalling that the copy operation completed without conflict‑related errors.

**Reference relationships**:  
*Callers* – The function is called by two higher‑level routines:  
  • `run_bmi_calc` (in `pedsnetdcc/bmi.py`) when the user requests that BMI results be copied into the central `measurement` table (`copy=True`).  
  • `copy_bmi_measurement` (also in `pedsnetdcc/bmi.py`), which is a thin wrapper used by external scripts that only need to perform the copy step. Both callers supply the same four arguments and rely on the Boolean return value to decide whether to continue processing.  

*Callees* – Within its body the function uses two utility objects from the project:  
  • `Statement` (from `pedsnetdcc/db.py`) to encapsulate and run the SQL command.  
  • `check_stmt_err` (from `pedsnetdcc/utils.py`) to validate execution and raise a consistent `DatabaseError` on failure.  

No other database objects are directly accessed; the function assumes that the target and source tables already exist with compatible column definitions.

**Note**:  
- The INSERT uses `ON CONFLICT DO NOTHING`; therefore duplicate rows (as defined by the target table’s primary key or unique index) are silently ignored and do not cause an error.  
- All columns are listed explicitly; any schema change that adds, removes, or reorders columns in either the source or target table must be reflected in the SQL string, otherwise the statement will fail with a column‑mismatch error.  
- The function does not commit or roll back transactions explicitly; the `Statement.execute` method sets the connection to autocommit mode, so the insert is applied immediately.  
- Errors related to connection problems, syntax errors, or constraint violations (other than the handled conflict) are captured in `stmt.err` and cause `check_stmt_err` to raise a `DatabaseError`. Callers should be prepared to handle this exception.  
- The function returns a simple Boolean; it does not expose the number of rows inserted. If that information is needed, callers can inspect `stmt.rowcount` before the function returns, but the current implementation discards the statement object after the check.

**Output Example**:  
```python
# Successful copy
result = _copy_to_measurement_table(conn_str, 'dcc_pedsnet', 'measurement', 'measurement_bmi')
print(result)   # -> True

# If a database error occurs (e.g., missing column)
# check_stmt_err raises DatabaseError and the function never returns.
```
## FunctionDef run_bmi_calc(config_file, conn_str, site, copy, ids, indexes, concept, age, neg_ids, skip_calc, table, out_table, password, search_path, model_version, id_name, max_time)
**run_bmi_calc**: The function of run_bmi_calc is to orchestrate the complete BMI derivation workflow, including configuration generation, table creation, BMI calculation, optional indexing, ID assignment, concept‑name enrichment, age calculation, optional copying to the canonical measurement table, and final vacuuming.

**parameters**:  
· config_file (str): name of the temporary BMI configuration file to be written (e.g., “site_bmi_temp.conf”).  
· conn_str (str): libpq‑style PostgreSQL connection string used for all database operations.  
· site (str): identifier of the site for which the BMI derivation is being performed.  
· copy (bool): if True, the resulting BMI rows are inserted into the central dcc_pedsnet.measurement table after all other processing.  
· ids (bool): if True, unique measurement_id values are generated and assigned to the rows in the BMI output table.  
· indexes (bool): if True, a predefined set of indexes is created on the BMI output table (or on a user‑specified table).  
· concept (bool): if True, human‑readable concept_name columns are populated from the OMOP vocabulary.  
· age (bool): if True, the column measurement_age_in_months is calculated and filled.  
· neg_ids (bool): if True, the measurement_id sequence is created to generate negative identifiers; otherwise a normal positive sequence is used.  
· skip_calc (bool): if True, the external BMI calculation tool is not invoked; the function will only create the output table and run any post‑processing steps that follow.  
· table (str): name of the source measurement table (normally “measurement” or “measurement_anthro”).  
· out_table (str): name of the table that will receive the derived BMI rows (default “measurement_bmi”).  
· password (str): database password; if None the password is extracted from conn_str.  
· search_path (str): PostgreSQL schema search_path; the first schema is taken as the primary schema for DDL statements.  
· model_version (str): PEDSnet data model version (e.g., “2.3.0”). Used for vacuuming and for locating the appropriate metadata.  
· id_name (str): prefix used for the site‑specific last‑id tracking table (e.g., “dcc” or “onco”).  
· max_time (str): maximum allowed interval between a height and a weight measurement. The value may be supplied as days (“60”) or as a numeric string; it is converted to seconds before being written to the configuration file.

**Code Description**:  
1. **Connection information extraction** – The helper `get_conn_info_dict` parses `conn_str` into a dictionary of host, user, dbname, etc. This dictionary is merged with a minimal log dictionary containing the site name. A start‑time stamp is recorded for elapsed‑time logging.  
2. **Primary schema resolution** – `primary_schema(search_path)` returns the first schema listed in the search_path; this schema is used for all subsequent DDL statements.  
3. **Password handling** – If `password` is None, a regular‑expression search extracts the password from `conn_str`.  
4. **Maximum time conversion** – If `max_time` equals the string “60”, it is replaced with the hard‑coded 5 184 000 seconds (60 days). Otherwise, if the string is numeric, it is interpreted as a number of days and converted to seconds (`days * 24 * 60 * 60`). Non‑numeric values fall back to the default 5 184 000 seconds.  
5. **Statement preparation** – An empty `StatementSet` (`stmts`) is created to hold DDL statements that will be executed in sequence or in parallel.  
6. **Skip‑calculation branch** – When `skip_calc` is False the function proceeds with the full workflow:  
   a. **Configuration file creation** – `_create_config_file` writes a BMI configuration file into the container’s `/app` directory. The file contains static concept IDs, table names, chunk sizes, the computed `max_time`, and a `<src_rdb>` block populated from `conn_info_dict`.  
   b. **Output table creation** – A `Statement` containing `CREATE_MEASURE_LIKE_TABLE_SQL.format(out_table)` is added to `stmts`. The statement is executed immediately (single‑threaded) and any error is logged and re‑raised via `check_stmt_err`.  
   c. **Null‑row removal** – After the table exists, a second `Statement` with `DROP_NULL_BMI_TABLE_SQL.format(out_table)` is executed to delete rows where required fields are null. Errors are handled in the same way as step b.  
   d. **External BMI tool invocation** – `derive_bmi` is called with the configuration file (minus the “.cfg” suffix), verbose flag, working directory `/app`, and foreground execution. This step performs the actual height‑weight matching and BMI calculation inside the external binary.  
7. **Index creation (optional)** – If `indexes` is True, a list of column names (`col_index`) is iterated. For each column an index name is generated by `_make_index_name('bmi', col)` when the output table is the default `measurement_bmi`; otherwise a generic index name template is used. Corresponding `Statement` objects are added to `stmts`. The entire set is executed in parallel with a pool size of five workers (`stmts.parallel_execute(conn_str,5)`). After execution each statement is inspected with `check_stmt_err`; any failure aborts the function.  
8. **Measurement‑ID assignment (optional)** – If `ids` is True, `_add_measurement_ids` is called with the connection string, site, `neg_ids`, `out_table`, `search_path`, `model_version`, and `id_name`. This helper reserves a block of IDs from the site‑specific last‑id tracking table, creates or adjusts a sequence, populates the `measurement_id` column, and adds a primary‑key constraint. A Boolean return value is checked; a False result causes immediate termination.  
9. **Concept‑name enrichment (optional)** – If `concept` is True, `_fill_concept_names` updates the output table by joining each concept‑id column to `vocabulary.concept` and writing the corresponding `concept_name` into newly added text columns. Failure returns False and aborts the workflow.  
10. **Age‑in‑months calculation (optional)** – If `age` is True, `_fill_age_in_months` creates helper PostgreSQL functions for interval arithmetic and updates `measurement_age_in_months` for every row by comparing the person’s birth date to the measurement timestamp. Errors are propagated as in previous steps.  
11. **Copy to canonical measurement table (optional)** – If `copy` is True, `_copy_to_measurement_table` inserts all rows from `out_table` into the target `measurement` table using an `INSERT … SELECT … ON CONFLICT DO NOTHING` statement. This avoids duplicate primary‑key violations. The function returns a Boolean that is checked.  
12. **Vacuum and analyze** – Regardless of the optional flags, `vacuum` is called with `analyze=True` on the `out_table` to reclaim space and update planner statistics.  
13. **Final logging** – A completion log entry records the total elapsed time (`secs_since(start_time)`).  
14. **Return value** – If the function reaches the end without raising an exception, it returns `True` to indicate successful execution.

**Reference relationships**:  
*Callers* – `run_bmi` (pedsnetdcc/main.py), `run_bmi_bmiz` and `run_derivations` all import `run_bmi_calc` and pass command‑line arguments through to it. These entry points construct the appropriate flags (ids, indexes, concept, age, copy, etc.) based on user options and then exit with status 1 on failure.  

*Callees* – The function relies on a suite of internal helpers:  
- `_create_config_file` (writes the BMI configuration file).  
- `primary_schema` (determines the target schema).  
- `get_conn_info_dict` and `combine_dicts` (build log dictionaries).  
- `Statement` and `StatementSet` (encapsulate and execute DDL/DML statements).  
- `_make_index_name` (produces Oracle‑compatible index identifiers).  
- `_add_measurement_ids`, `_fill_concept_names`, `_fill_age_in_months`, `_copy_to_measurement_table` (post‑processing steps).  
- `derive_bmi` (external binary that performs the actual BMI derivation).  
- `vacuum` (maintenance of the output table).  
- `check_stmt_err` and `secs_since` (error handling and timing).  

These helpers together provide a modular pipeline where each step can be toggled on or off via the Boolean parameters.

**Note**:  
- The configuration file is opened in binary mode (`'wb'`) but strings are written directly; in Python 3 this will raise a `TypeError`. The file should be opened in text mode (`'w'`) or the strings encoded to bytes.  
- `max_time` must be supplied as a string representing days or seconds; non‑numeric values default to 5 184 000 seconds (60 days).  
- Index names generated by `_make_index_name` respect the Oracle identifier length limit; the function assumes the constant `NAME_LIMIT` is defined elsewhere.  
- When `neg_ids` is True, the created sequence is constrained to negative values only, which is useful for sites that reserve negative identifiers to avoid clashes.  
- The `copy` step uses `ON CONFLICT DO NOTHING`; duplicate rows are silently ignored but no warning is emitted.  
- All DDL statements are executed with autocommit; therefore they cannot be rolled back as part of a larger transaction.  
- Errors from any helper are propagated as `DatabaseError` via `check_stmt_err`; callers must be prepared to handle this exception.  
- The function logs extensively; the log dictionary always contains the site and connection details, making troubleshooting easier in multi‑site environments.

**Output Example**:  
When called with typical arguments (e.g., `run_bmi_calc('site_bmi.cfg', conn_str, 'site', True, True, True, True, True, False, False, 'measurement', 'measurement_bmi', None, 'dcc_pedsnet, public', '2.3.0', 'dcc', '60')`), the function will return:

```
True
```

indicating that the BMI derivation, optional post‑processing, and vacuuming completed without error. If any step fails, a `DatabaseError` is raised and the function does not return.
## FunctionDef _add_measurement_ids(conn_str, site, neg_ids, out_table, search_path, model_version, id_name)
**_add_measurement_ids**: The function of _add_measurement_ids is to assign unique measurement_id values to rows in a BMI output table, reserve a block of IDs from the site‑specific last‑id tracking table, create or adjust the measurement ID sequence, and make measurement_id the primary key of the table.

**parameters**:  
· conn_str (str): database connection string in libpq format.  
· site (str): name of the site for which the BMI calculation is being performed.  
· neg_ids (bool): if True, the function creates a sequence that generates negative IDs; otherwise it creates a normal (positive) sequence.  
· out_table (str): name of the BMI output table that will receive measurement_id values.  
· search_path (str): PostgreSQL schema search_path; the first schema is used as the primary schema.  
· model_version (str): PEDSnet data model version (e.g., “2.3.0”).  
· id_name (str): prefix used for the last‑id tracking table (e.g., “dcc” or “onco”).  

**Code Description**:  

1. **Preparation and logging** – The function extracts connection information with `get_conn_info_dict`, builds a log dictionary that includes the site and connection details, and logs the start of the measurement_id assignment. The start time is recorded for elapsed‑time logging.

2. **Schema and table metadata** – `primary_schema(search_path)` returns the first schema in the search_path; this schema is used for all subsequent SQL statements. The function builds the name of the last‑id tracking table using the pattern `{id_name}_measurement_id`. It obtains a SQLAlchemy `MetaData` object for the supplied `model_version` via `stock_metadata`, then retrieves the `measurement` table object (although the table object is not directly used beyond confirming existence).

3. **Count required new IDs** – A `Statement` is created with SQL that counts rows in `out_table` where `measurement_id IS NULL`. The statement is executed, and `check_stmt_err` and `check_stmt_data` verify that the execution succeeded and returned a count. The count is incremented by one and stored in `tpl_vars['new_id_count']`; this value represents the number of new IDs that must be reserved.

4. **Reserve a block of IDs** – Two statements are placed in a `StatementList`:
   * `LOCK {last_id_table_name}` – locks the last‑id tracking table to prevent concurrent updates.
   * `UPDATE {last_id_table_name} AS new SET last_id = new.last_id + '{new_id_count}'::bigint FROM {last_id_table_name} AS old RETURNING old.last_id, new.last_id` – increments the stored last_id by the number of IDs needed and returns the previous and new values.
   The list is executed with `transaction=True`, ensuring both statements run in a single transaction. After execution, the old and new last IDs are extracted from the returned data and logged.

5. **Create or adjust the measurement ID sequence** – Depending on `neg_ids`, the function builds either a normal sequence creation SQL (`CREATE SEQUENCE IF NOT EXISTS {schema}.{site}_bmi_measurement_id_seq`) or a negative‑only sequence (`CREATE SEQUENCE IF NOT EXISTS … INCREMENT 1 START -2147483647 MINVALUE -2147483647 MAXVALUE 0`). The statement is executed and errors are checked.

6. **Set the sequence start value** – The sequence is altered to restart with the first ID of the newly reserved block (`old_last_id + 1`). This guarantees that subsequent `nextval` calls will produce IDs that do not clash with existing ones.

7. **Assign measurement IDs to the BMI table** – An `UPDATE` statement sets `measurement_id = nextval('{schema}.{site}_bmi_measurement_id_seq')` for all rows where `measurement_id` is currently NULL. Execution is verified with `check_stmt_err`.

8. **Make measurement_id the primary key** – A final `ALTER TABLE` adds a primary‑key constraint on the `measurement_id` column of the output table. Execution is verified with `check_stmt_err`.

9. **Final logging and return** – The function logs completion with the elapsed time and returns `True` to indicate success. Any error encountered in the steps above raises a `DatabaseError` (via `check_stmt_err`) or `MissingDataError` (via `check_stmt_data`), causing the caller to handle the exception.

**Reference relationships**:  

*Caller*: The function is invoked from `run_bmi_calc` in `pedsnetdcc/bmi.py` when the `ids` flag is True. `run_bmi_calc` passes the same connection string, site, neg_ids flag, output table name, search_path, model_version, and id_name that were supplied to the overall BMI run.

*Callees*:  
- `get_conn_info_dict` (pedsnetdcc/utils.py) – parses the connection string for logging.  
- `combine_dicts` (pedsnetdcc/utils.py) – merges dictionaries for log messages.  
- `primary_schema` (pedsnetdcc/schema.py) – extracts the primary schema from the search_path.  
- `stock_metadata` (pedsnetdcc/utils.py) – obtains the SQLAlchemy metadata for the specified model version.  
- `Statement`, `StatementList` (pedsnetdcc/db.py) – encapsulate and execute the individual SQL commands.  
- `check_stmt_err` and `check_stmt_data` (pedsnetdcc/utils.py) – validate execution results and raise appropriate exceptions.  
- `secs_since` (pedsnetdcc/dict_logging.py) – formats elapsed time for log entries.

**Note**:  
- The function assumes that the last‑id tracking table `{id_name}_measurement_id` already exists and contains a numeric `last_id` column.  
- The sequence name is always `{schema}.{site}_bmi_measurement_id_seq`; if the sequence already exists, `CREATE SEQUENCE IF NOT EXISTS` leaves it unchanged.  
- When `neg_ids` is True, the sequence is constrained to negative values only; this is useful for sites that reserve negative IDs to avoid collisions with positive IDs from other sources.  
- All SQL statements are executed with autocommit mode; the block that updates the last‑id table runs inside an explicit transaction to guarantee atomicity.  
- Errors are not caught inside the function beyond the checks; they propagate as `DatabaseError` or `MissingDataError`, allowing the caller (`run_bmi_calc`) to log and handle them.  
- The function returns a boolean (`True`) on success; any failure results in an exception before the return statement is reached.

**Output Example**:  
When called with valid arguments and no errors, the function completes and returns:

```python
result = _add_measurement_ids(conn_str, site='stlouis', neg_ids=False,
                              out_table='measurement_bmi', search_path='dcc_pedsnet, public',
                              model_version='2.3.0', id_name='dcc')
# result == True
```
## FunctionDef copy_bmi_measurement(conn_str, site, table, out_table, search_path)
**copy_bmi_measurement**: The function of copy_bmi_measurement is to copy BMI measurement rows from a temporary output table into the canonical measurement table within the same schema.

**parameters**: The parameters of this Function.  
· conn_str: PostgreSQL connection string used to open a database connection.  
· site: Identifier of the site for which the BMI copy is being performed.  
· table: Name of the target measurement table (e.g., `measurement` or `measurement_anthro`).  
· out_table: Name of the source temporary table that holds the BMI results (e.g., `measurement_bmi`).  
· search_path: PostgreSQL schema search path; the first schema in this list is used as the primary schema for the operation.

**Code Description**:  
1. **Connection and schema resolution** – The function calls `get_conn_info_dict(conn_str)` to extract connection metadata for logging. It then determines the primary schema by invoking `primary_schema(search_path)`.  
2. **Logging start** – A dictionary combining the site identifier and connection info is built with `combine_dicts`. An informational log entry “starting BMI calculation” is emitted, and the current epoch time is stored in `start_time` for later elapsed‑time reporting.  
3. **Copy operation** – A log entry “copy bmi measurements to measurement” is written. The core copy is performed by calling the internal helper `_copy_to_measurement_table(conn_str, schema, table, out_table)`. This helper constructs and executes an `INSERT … SELECT … ON CONFLICT DO NOTHING` statement that moves rows from `{schema}.{out_table}` into `{schema}.{table}` while silently ignoring duplicate primary‑key conflicts. The helper returns a Boolean `True` on success; if it returns `False` the wrapper function aborts and returns `False`.  
4. **Logging success of copy** – Upon a successful copy, a log entry “bmi measurements copied to measurement” is emitted.  
5. **Logging completion** – The elapsed time since `start_time` is calculated with `secs_since(start_time)`. A final log entry “finished BMI copy” together with the elapsed seconds is recorded.  
6. **Return value** – If no exception was raised and the copy succeeded, the function returns `True` to indicate overall success.

**Reference relationships**:  
*Callers* –  
- `copy_bmi` in `pedsnetdcc/main.py` invokes `copy_bmi_measurement` after constructing a connection string from command‑line arguments.  
- `copy_to_measurement` in `pedsnetdcc/main.py` also calls `copy_bmi_measurement` as part of a larger workflow that copies BMI and various Z‑score tables into the core schema.  

*Callees* –  
- `_copy_to_measurement_table` (same module) performs the actual INSERT … SELECT operation.  
- Utility functions `get_conn_info_dict`, `primary_schema`, `combine_dicts`, and `secs_since` are used for connection parsing, schema extraction, dictionary merging, and elapsed‑time calculation, respectively.  
- Logging is performed through the module‑level `logger` object.  

**Note**:  
- The function assumes that both the target measurement table (`table`) and the source BMI output table (`out_table`) already exist in the identified schema and have compatible column definitions.  
- Duplicate rows are ignored because `_copy_to_measurement_table` uses `ON CONFLICT DO NOTHING`; this prevents the copy from failing due to primary‑key or unique‑index violations.  
- Any database error raised inside `_copy_to_measurement_table` (e.g., missing columns, connection failure) propagates as a `DatabaseError`; the wrapper does not catch it, so callers must be prepared to handle the exception.  
- The function does not manage transactions explicitly; the underlying `Statement.execute` runs in autocommit mode, so the insert is committed immediately.  
- Logging dictionaries are built with `combine_dicts`; later keys overwrite earlier ones, so callers should ensure the order of arguments reflects the desired precedence.

**Output Example**:  
```python
result = copy_bmi_measurement(
    conn_str="host=db.example.com port=5432 dbname=pedsnet user=loader options='-c search_path=dcc_pedsnet'",
    site="stlouis",
    table="measurement",
    out_table="measurement_bmi",
    search_path="dcc_pedsnet, public"
)
print(result)   # -> True   (indicates the BMI rows were successfully copied)
```
