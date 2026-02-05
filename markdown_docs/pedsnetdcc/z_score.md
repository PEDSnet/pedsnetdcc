## FunctionDef _create_bmiz_config_file(config_path, config_file, schema, out_table, password, conn_info_dict, person_table)
**_create_bmiz_config_file**: The function of _create_bmiz_config_file is to generate a BMI‑Z configuration file required by the external Z‑Score calculation tool.

**parameters**:  
· config_path: directory path where the configuration file will be created.  
· config_file: name of the configuration file to be written.  
· schema: target database schema name that will be referenced in the config.  
· out_table: name of the output measurement table that will receive the BMI‑Z results.  
· password: database password used for the connection section of the config.  
· conn_info_dict: dictionary containing connection details (host, dbname, user, etc.).  
· person_table: name of the person table to be supplied to the Z‑Score tool as input.

**Code Description**:  
The function opens a new file at the location `os.path.join(config_path, config_file)` in binary write mode. It then writes a series of XML‑like and key‑value lines that constitute the configuration expected by the Z‑Score executable.

1. **Concept mapping block** – Begins with `<concept_id_map>` and writes a fixed `measurement_concept_id = 3038553`. The block is closed later with `</concept_id_map >`.  
2. **Z‑score information block** – Starts with `<z_score_info>` and defines:
   - `z_class_system = NHANES_2000` (the reference growth chart system)  
   - `z_class_measure = BMI for Age` (the specific measure)  
   - `z_measurement_concept_id = 2000000043` (concept identifier for BMI)  
   The block is closed with `</z_score_info >`.  
3. **Additional Z‑score parameters** – After the block, the function repeats `z_measurement_concept_id = 2000000043` (ensuring the key is present), then writes:
   - `z_measurement_type_concept_id = 45754907`  
   - `z_unit_source_value = SD` (standard deviation unit)  
   - `clone_z_measurements = 1` (indicates cloning behavior)  
   - `input_person_table = <person_table>` (value supplied by the caller)  
   - `output_chunk_size = 1000` and `person_chunk_size = 1000` (batch sizes)  
   - `verbose = 0` (verbosity level).  

4. **Source relational database block** – Enclosed in `<src_rdb>` tags, it supplies connection parameters for the Z‑Score tool:
   - `driver = Pg` (PostgreSQL driver)  
   - `host`, `database`, `username` are taken from `conn_info_dict`.  
   - `schema` uses the `schema` argument.  
   - `password` uses the provided `password`.  
   - Fixed entries `domain = stage` and `type = dcc`.  
   - `post_connect_sql` sets the search_path to the target schema and the vocabulary schema.  

5. **Measurement table mapping** – After closing the `<src_rdb>` block, the function writes:
   - `input_measurement_table = measurement_bmi` (source table containing raw BMI measurements)  
   - `output_measurement_table = <out_table>` (destination table for the calculated Z‑scores).  

The file is closed automatically by the `with` statement. No return value is produced; the side effect is the creation of the configuration file.

**Relationship with callers**:  
`_create_bmiz_config_file` is invoked exclusively from `run_z_calc` when the requested Z‑score type is `'bmiz'`. In `run_z_calc`, after extracting connection information and determining the schema, the function is called with the appropriate arguments to produce the config file before the tool `derive_z` is executed. The generated file supplies all necessary parameters for the external BMI‑Z calculation, enabling `run_z_calc` to create the output measurement table, populate it, and continue with subsequent processing steps (indexing, adding IDs, etc.).

**Note**:  
- The function writes the file in binary mode (`'wb'`) but only writes text strings; this works because the strings are ASCII, but using text mode (`'w'`) would be more conventional.  
- The XML‑like tags are not well‑formed (e.g., extra spaces before closing `>`). They must match the exact format expected by the Z‑Score tool; altering them may cause the tool to reject the config.  
- The function overwrites any existing file with the same name without warning. Ensure that `config_path` points to a writeable temporary directory (e.g., `/app`) and that the filename is unique per run.  
- All values except `person_table`, `schema`, `out_table`, and connection details are hard‑coded; changes to measurement concept IDs or Z‑score system require modifying this function.  
- The password is written in plain text to the config file; handle the file securely and delete it after use if confidentiality is a concern.
## FunctionDef _create_height_z_config_file(config_path, config_file, schema, table, out_table, password, conn_info_dict, person_table)
**_create_height_z_config_file**: The function of _create_height_z_config_file is to generate a configuration file required by the Height‑Z score calculation tool.

**parameters**: The parameters of this function.  
· **config_path**: Directory path where the configuration file will be created.  
· **config_file**: Name of the configuration file to be written.  
· **schema**: Target database schema name; written into the `<src_rdb>` section of the config.  
· **table**: Name of the input measurement table (e.g., `measurement_anthro`).  
· **out_table**: Name of the output measurement table that will receive the Height‑Z results.  
· **password**: Database password; inserted into the `<src_rdb>` section.  
· **conn_info_dict**: Dictionary containing connection details (`host`, `dbname`, `user`, etc.) extracted from the PostgreSQL connection string.  
· **person_table**: Name of the person table; written as `input_person_table` in the config.

**Code Description**:  
The function opens a new file at `os.path.join(config_path, config_file)` in binary write mode. It then writes a series of lines that compose an XML‑like configuration required by the external Z‑score calculation utility. The content is written in the exact order expected by the tool:

1. Starts a `<concept_id_map>` block and defines `measurement_concept_id = 3023540`.  
2. Opens a `<z_score_info>` block, specifying the NHANES‑2000 reference system, the measurement class “Height for Age”, and the Z‑score measurement concept id `2000000042`. The block is closed with `</z_score_info >`.  
3. Closes the `<concept_id_map >` block.  
4. Writes additional global settings:  
   - `z_measurement_concept_id = 2000000042` (repeated for clarity).  
   - `z_measurement_type_concept_id = 45754907`.  
   - `z_unit_source_value = SD`.  
   - `clone_z_measurements = 1`.  
   - `input_person_table = <person_table>`.  
   - Chunk sizes for processing (`output_chunk_size` and `person_chunk_size` set to 1000).  
   - `verbose = 0`.  
5. Begins the `<src_rdb>` section that describes the source relational database:  
   - `driver = Pg`.  
   - `host`, `database`, `username` are taken from `conn_info_dict`.  
   - `schema` is the supplied schema argument.  
   - `password` is the supplied password argument.  
   - Fixed values `domain = stage` and `type = dcc`.  
   - `post_connect_sql` sets the search path to the target schema and the vocabulary schema.  
   - The block is closed with `</src_rdb>`.  
6. Finally, the input and output measurement table names are written as `input_measurement_table = <table>` and `output_measurement_table = <out_table>`.

The function does not return a value; its purpose is side‑effectual file creation. All line endings use the platform‑specific `os.linesep` to ensure proper formatting.

**Relationship with callers**:  
`_create_height_z_config_file` is invoked exclusively from `run_z_calc` when the requested Z‑score type is `'ht_z'`. In `run_z_calc`, the caller determines the configuration directory (`/app`), passes the user‑provided configuration filename, and supplies the current schema, measurement table names, password, connection dictionary, and person table name. The generated file is later used by the `derive_z` function (called later in `run_z_calc`) to perform the actual Height‑Z calculation. Thus, this function supplies the necessary static and dynamic parameters that bridge the Python orchestration layer and the external Z‑score calculation binary.

**Note**:  
- The function writes the file in binary mode (`'wb'`) but only writes text strings; this is acceptable because the strings are ASCII and `os.linesep` resolves to a byte‑compatible newline.  
- The XML‑like tags (`<concept_id_map>`, `<z_score_info>`, `<src_rdb>`) must match the exact syntax expected by the Z‑score tool; any deviation may cause the tool to reject the configuration.  
- The password is written in plain text; ensure that file permissions restrict access to authorized users only.  
- The function does not perform any validation of input arguments; callers must guarantee that `conn_info_dict` contains the required keys (`host`, `dbname`, `user`).  
- Duplicate lines (e.g., `z_measurement_concept_id`) appear intentionally to satisfy the external tool’s format; removing them may break compatibility.
## FunctionDef _create_weight_z_config_file(config_path, config_file, schema, table, out_table, password, conn_info_dict, person_table)
**_create_weight_z_config_file**: The function of _create_weight_z_config_file is to generate a configuration file required by the Weight‑for‑Age Z‑score calculation tool.

**parameters**: The parameters of this Function.  
· **config_path**: Directory path where the configuration file will be created.  
· **config_file**: Name of the configuration file to write (e.g., “weight_z.cfg”).  
· **schema**: Target database schema name; inserted into the `<src_rdb>` section of the file.  
· **table**: Name of the input measurement table; written as `input_measurement_table` near the end of the file.  
· **out_table**: Name of the output measurement table; written as `output_measurement_table`.  
· **password**: Database password; placed in the `<src_rdb>` block.  
· **conn_info_dict**: Dictionary containing connection details (`host`, `dbname`, `user`). Values are extracted to populate the `<src_rdb>` block.  
· **person_table**: Name of the person table used by the Z‑score tool; written as `input_person_table`.

**Code Description**:  
The function opens (or creates) a file at the path `os.path.join(config_path, config_file)` in binary write mode. It then writes a series of lines that together form a simple XML‑like configuration required by the external Z‑score calculation utility. The written sections are:

1. **Concept ID mapping** – Begins with `<concept_id_map>` and defines `measurement_concept_id = 3013762`.  
2. **Z‑score information** – Enclosed by `<z_score_info>` and specifies:  
   - `z_class_system = NHANES_2000` (reference growth‑chart system)  
   - `z_class_measure = Weight for Age` (the specific measure)  
   - `z_measurement_concept_id = 2000000041` (concept ID for weight‑for‑age)  
   The closing tag `</z_score_info >` (note the extra space before `>`) follows.  
3. **Closing of concept map** – `</concept_id_map >` (also contains an extra space).  
4. **Additional measurement settings** – Re‑writes `z_measurement_concept_id = 2000000041` (duplicate line), then sets:  
   - `z_measurement_type_concept_id = 45754907`  
   - `z_unit_source_value = SD` (standard deviation)  
   - `clone_z_measurements = 1` (indicates cloning behavior)  
   - `input_person_table` using the supplied `person_table` argument  
   - Chunk sizes (`output_chunk_size` and `person_chunk_size`) both set to 1000  
   - `verbose = 0` (quiet mode)  

5. **Source relational database block** – Enclosed by `<src_rdb>` and contains connection parameters:  
   - `driver = Pg` (PostgreSQL)  
   - `host`, `database`, `username` taken from `conn_info_dict`  
   - `schema` from the function argument  
   - `password` from the function argument  
   - Fixed values `domain = stage` and `type = dcc`  
   - `post_connect_sql` that sets the search_path to the supplied schema and the vocabulary schema.  

6. **Input/Output measurement tables** – Finally writes `input_measurement_table` and `output_measurement_table` using the `table` and `out_table` arguments.

The function does not return a value; its purpose is side‑effectual file creation. All lines are terminated with the platform‑appropriate newline (`os.linesep`).

**Relationship with callers**:  
`_create_weight_z_config_file` is invoked exclusively from `run_z_calc` in `pedsnetdcc/z_score.py` when the requested Z‑score type is `'wt_z'`. In that workflow, the function is called after extracting connection information and before any SQL statements are executed. The generated configuration file is subsequently passed to the external `derive_z` command (via `derive_z(config_file[:-5], '--verbose=1', ...)`) to perform the actual Weight‑for‑Age Z‑score computation. Thus, this function supplies the necessary static configuration that drives the Z‑score tool, linking the Python orchestration layer with the underlying calculation engine.

**Note**:  
- The directory specified by `config_path` must exist and be writable by the process; otherwise an `IOError` will be raised.  
- The function writes the file in binary mode (`'wb'`) but only writes text strings; this works in Python 3 because strings are automatically encoded to UTF‑8, but using text mode (`'w'`) would be more conventional.  
- Duplicate and mis‑spaced closing tags (`</z_score_info >`, `</concept_id_map >`) are present; they are retained to match the expected format of the external tool, but any change to the tool’s parser may require correction.  
- The `conn_info_dict` must contain the keys `'host'`, `'dbname'`, and `'user'`; missing keys will cause an `AttributeError` when `dict.get` returns `None`.  
- The password is written in plain text inside the configuration file; ensure the file’s permissions restrict access to authorized users only.  
- The function does not perform validation of input arguments; callers should guarantee that table and schema names are correctly quoted for the downstream SQL environment.
## FunctionDef _make_index_name(z_type, column_name)
**_make_index_name**: The function of _make_index_name is to generate a deterministic, length‑constrained index name for a specific Z‑score type and column, suitable for Oracle databases.

**parameters**:  
· z_type: a string indicating the Z‑score calculation type (e.g., “bmiz”, “ht_z”, “wt_z”).  
· column_name: a string containing the column name for which the index will be created (e.g., “measurement_age_in_months”).

**Code Description**:  
The function builds an index name that satisfies the Oracle identifier length limit defined by the constant `NAME_LIMIT`.  

1. **Table abbreviation** – It creates a prefix `mea_` followed by the first three characters of the Z‑score type after removing underscores.  
   ```python
   table_abbrev = "mea_" + z_type.replace("_","")[:3]
   ```
   For “bmiz” this yields “mea_bmi”.

2. **Column abbreviation** – It reduces the column name to a short token by taking the first character of each underscore‑separated segment.  
   ```python
   column_abbrev = ''.join([x[0] for x in column_name.split('_')])
   ```
   For “measurement_age_in_months” the result is “mai”.

3. **Hash component** – To guarantee uniqueness even when different tables share similar column names, the function computes an MD5 hash of the string “`z_type.column_name`”.  
   ```python
   md5 = hashlib.md5(
       '{}.{}'.format(z_type, column_name).encode('utf-8')).hexdigest()
   ```

4. **Length calculation** – It determines how many characters of the hash can be kept while staying within `NAME_LIMIT`. The calculation subtracts the lengths of the table abbreviation, column abbreviation, three underscores, and the suffix “ix” from the limit.  
   ```python
   hashlen = NAME_LIMIT - (len(table_abbrev) + len(column_abbrev) +
                           3 * len('_') + len('ix'))
   ```

5. **Final assembly** – The function concatenates the parts with underscores, using only the allowed portion of the hash, and appends the literal suffix “ix”.  
   ```python
   return '_'.join([table_abbrev, column_abbrev, md5[:hashlen], 'ix'])
   ```

The resulting string is deterministic (same inputs always give the same output) and highly unlikely to collide because the hash component encodes the full `z_type.column_name` pair.

**Relationship with Callers**:  
`_make_index_name` is invoked inside `run_z_calc` when the `indexes` flag is true. For each column listed in `col_index`, `run_z_calc` calls `_make_index_name(z_type, col)` to obtain a unique index identifier, which is then embedded in the SQL statement that creates the index on the output measurement table. By centralising the naming logic, the tool avoids name collisions across different Z‑score types (e.g., “provider.gender_source_concept_name” vs. “person.gender_source_concept_name”) and ensures all generated index names respect the database’s identifier length restriction.

**Note**:  
- The constant `NAME_LIMIT` must be defined elsewhere in the module (commonly 30 for Oracle).  
- The function assumes `z_type` and `column_name` are non‑empty strings; passing other types will raise an exception.  
- Because the hash is truncated, the maximum usable length of the hash (`hashlen`) may be zero if `NAME_LIMIT` is very small; in such a case the function will still return a syntactically valid name but with no hash portion.  
- The function relies on the `hashlib` module; ensure it is imported (`import hashlib`) before use.

**Output Example**:  
```python
>>> _make_index_name('bmiz', 'measurement_age_in_months')
'mea_bmi_m_ae1fd5b22b92397ca9_ix'
```
## FunctionDef _fill_age_in_months(conn_str, schema, out_table)
**_fill_age_in_months**: The function of _fill_age_in_months is to add a column containing the measurement age expressed in months to a Z‑score result table.

**parameters**:  
· conn_str (str): libpq‑style connection string that identifies the target PostgreSQL database.  
· schema (str): name of the schema that contains the tables referenced in the SQL statements.  
· out_table (str): name of the Z‑score result table that will receive the new `measurement_age_in_months` values.

**Code Description**:  
1. **SQL block construction** – The function builds a multi‑statement SQL string (`fill_add_age_in_months_sql`). The string contains:  
   * Three helper PostgreSQL functions created in the target schema:  
     - `last_month_of_interval(timestamp, timestamp)` returns the timestamp of the last month that falls inside the interval between two timestamps.  
     - `month_after_last_month_of_interval(timestamp, timestamp)` returns the timestamp of the month immediately after that last month.  
     - `days_in_last_month_of_interval(timestamp, timestamp)` returns the number of days in the last month of the interval, computed as the difference between the two functions above.  
   * A fourth helper function `months_in_interval(timestamp, timestamp)` that returns a **double precision** value representing the total number of months (including fractional months) between two timestamps. The fractional part is calculated by dividing the number of days in the interval by the number of days in the last overlapping month.  
   * A comment is attached to each helper function to document its purpose.  
   * An `UPDATE` statement that joins the output table (`{schema}.{out_table}`) with the `person` table to compute `measurement_age_in_months` for each row. The calculation uses `months_in_interval(p.birth_datetime, m.measurement_datetime)` where `p` is the person record and `m` is the measurement record.

2. **Message preparation** – A short descriptive message (`fill_add_age_in_months_msg = "adding age in months"`) is prepared for logging and for the `Statement` object.

3. **Statement creation** – An instance of `Statement` is created with the formatted SQL (schema and out_table are interpolated) and the message. This object encapsulates the SQL and provides execution facilities.

4. **Execution** – The statement’s `execute` method is called with the supplied `conn_str`. `Statement.execute` opens a new database connection, runs the SQL, and stores any error in `stmt.err`.

5. **Error handling** – Immediately after execution, `check_stmt_err` is invoked with the statement object and a caller identifier (`'add age in months'`). `check_stmt_err` logs the error and raises a `DatabaseError` if `stmt.err` is not `None`. This guarantees that any failure aborts the function.

6. **Return value** – If the SQL runs without error, the function returns `True` to indicate success.

**Reference relationships**:  
*Caller*: The function is invoked from `run_z_calc` (in `pedsnetdcc/z_score.py`) when the `age` flag is set. `run_z_calc` logs the start of the operation, calls `_fill_age_in_months`, checks the Boolean result, and logs completion.  
*Callees*: Internally it relies on two utility functions from the same package:  
- `Statement.execute` (from `pedsnetdcc/db.py`) to run the generated SQL against the database.  
- `check_stmt_err` (from `pedsnetdcc/utils.py`) to detect and raise any execution errors.  
The helper PostgreSQL functions created inside the SQL are used only by the `UPDATE` statement; they are defined in the target schema each time the function runs, overwriting any previous definitions.

**Note**:  
- The helper functions are created with `CREATE OR REPLACE`, so repeated calls will replace existing definitions without error.  
- The `UPDATE` statement updates **all rows** in the output table; ensure that the table already contains the column `measurement_age_in_months` with a compatible data type (double precision).  
- The function assumes that the `person` table exists in the same schema and contains a `birth_datetime` column, and that the output table contains `person_id`, `measurement_id`, and `measurement_datetime` columns.  
- Because the SQL is executed in autocommit mode, the changes are permanent as soon as the statement finishes.  
- Any exception raised by `check_stmt_err` propagates out of `_fill_age_in_months`; callers should be prepared to handle `DatabaseError`.

**Output Example**:  
```python
result = _fill_age_in_months(conn_str, 'dcc', 'measurement_bmiz')
print(result)   # => True  (indicates that the age‑in‑months column was successfully populated)
```
## FunctionDef _fill_concept_names(conn_str, schema, out_table)
**_fill_concept_names**: The function of _fill_concept_names is to populate the concept‑name columns of a Z‑score result table by joining the table’s concept‑id columns to the OMOP vocabulary.

**parameters**:  
· conn_str (str): PostgreSQL connection string used to open a database session.  
· schema (str): Name of the schema that contains the output table.  
· out_table (str): Name of the Z‑score result table that will receive the concept‑name values.

**Code Description**:  
1. **SQL statement construction** – A multi‑line string `fill_concept_names_sql` is defined. The statement performs an `UPDATE` on `{schema}.{out_table}` (aliased as `zs`). For each row it sets nine concept‑name columns (`measurement_concept_name`, `measurement_source_concept_name`, `measurement_type_concept_name`, `operator_concept_name`, `priority_concept_name`, `range_high_operator_concept_name`, `range_low_operator_concept_name`, `unit_concept_name`, `value_as_concept_name`).  
   The new values are taken from a sub‑query (`v`) that selects the original `measurement_id` together with the `concept_name` of each related concept id. The sub‑query joins the Z‑score table (`z`) to `vocabulary.concept` nine times (aliases v1‑v9), each join matching a different concept‑id column (`measurement_concept_id`, `measurement_source_concept_id`, …, `value_as_concept_id`). All joins are left joins, so missing concept rows result in NULL names.  
   The outer `UPDATE` matches rows by `measurement_id` (`WHERE zs.measurement_id = v.measurement_id`).

2. **Logging message** – `fill_concept_names_msg` is set to the human‑readable string “adding concept names”. This message is attached to the `Statement` object for later logging.

3. **Statement creation** – An instance of `Statement` is created with the formatted SQL (`fill_concept_names_sql.format(schema, out_table)`) and the message. This object encapsulates the SQL and provides the `execute` method.

4. **Execution** – `add_measurement_ids_stmt.execute(conn_str)` opens a new database connection (autocommit) and runs the UPDATE statement. The execution is performed by `Statement.execute`, which handles connection management, logging, and error capture.

5. **Error handling** – Immediately after execution, `check_stmt_err(add_measurement_ids_stmt, 'add concept names')` is called. `check_stmt_err` inspects the `err` attribute of the `Statement`. If an error is present, it logs a detailed error record and raises a `DatabaseError`. If no error is found, execution continues.

6. **Return value** – If the UPDATE completes without raising an exception, the function returns `True` to indicate success.

**Reference relationships**:  
- **Caller** – The function is invoked from `run_z_calc` (in `pedsnetdcc/z_score.py`). `run_z_calc` calls `_fill_concept_names` when the `concept` flag is true, after optionally adding measurement IDs and before adding age in months.  
- **Callees** – It directly uses the `Statement` class (specifically `Statement.execute`) to run the SQL, and the utility `check_stmt_err` to verify that the statement succeeded. No other project modules are called.

**Note**:  
- The UPDATE relies on the existence of the nine concept‑id columns in the output table; if any of these columns are missing, the SQL will fail.  
- All joins are left joins; therefore, if a concept id does not have a matching entry in `vocabulary.concept`, the corresponding name column will be set to NULL rather than causing an error.  
- The function does not commit or roll back explicitly; the `Statement.execute` method uses autocommit, so the changes are applied immediately.  
- Any database‑level error (e.g., permission issues, missing schema/table, or malformed SQL) will be captured by `Statement` and re‑raised by `check_stmt_err`, causing the caller to abort.  
- The function returns a simple boolean; callers should check the return value only after confirming that no exception was raised.

**Output Example**:  
```python
# Typical usage inside run_z_calc
success = _fill_concept_names(conn_str, 'dcc', 'measurement_bmiz')
if success:
    print("Concept names added successfully.")   # → Concept names added successfully.
```
## FunctionDef _copy_to_measurement_table(conn_str, schema, table, out_table)
**_copy_to_measurement_table**: The function of _copy_to_measurement_table is to copy rows from a temporary Z‑score output table into the canonical `measurement` table within the same schema, ignoring duplicate rows.

**parameters**:  
· **conn_str** (str): libpq‑style PostgreSQL connection string used to open a database connection.  
· **schema** (str): name of the target schema that contains both the destination `measurement` table and the source temporary table.  
· **table** (str): name of the destination table (normally the standard `measurement` table).  
· **out_table** (str): name of the source table that holds the Z‑score results to be copied.

**Code Description**:  
1. **SQL statement construction** – A multi‑line INSERT … SELECT statement is built in the variable `copy_to_sql`.  
   * The target column list enumerates every column of the `measurement` table, including derived columns such as `measurement_age_in_months`, `measurement_concept_name`, and site identifiers.  
   * The SELECT clause pulls the same columns from the source table `{0}.{2}` (the `out_table`), preserving the order of columns.  
   * The statement is wrapped with `ON CONFLICT DO NOTHING`, which causes PostgreSQL to silently skip rows that would violate a unique constraint (typically the primary key) instead of raising an error.  
   * The placeholders `{0}`, `{1}`, `{2}` are later replaced by `schema`, `table`, and `out_table` respectively via `format`.

2. **Logging message preparation** – `copy_to_msg` holds a template string `"copying {0} to measurement"` that will be used as the human‑readable message for the `Statement` object.

3. **Statement object creation** – An instance of `Statement` is created with two arguments: the formatted SQL string and the formatted log message.  
   * `Statement(sql, msg)` stores the SQL and a descriptive message; the object also tracks execution results and errors.

4. **Execution of the INSERT** – The `execute` method of the `Statement` instance is called with the supplied `conn_str`.  
   * `execute` opens a new PostgreSQL connection, sets autocommit, runs the SQL, captures any database‑level exception in `stmt.err`, and then closes the connection.  

5. **Error handling** – After execution, the helper `check_stmt_err` is invoked with the statement object and the caller name `'insert measurements'`.  
   * If `stmt.err` is not `None`, `check_stmt_err` logs the error and raises a `DatabaseError`. This propagates the failure to the caller of `_copy_to_measurement_table`.  

6. **Successful completion** – If no exception is raised, the function reaches the final line and returns `True`, indicating that the copy operation succeeded (or that any duplicate rows were silently ignored).

**Reference relationships**:  
* **Callers** – The function is invoked by two higher‑level routines in the same module:  
  * `run_z_calc` (the main driver for the Z‑score workflow) calls `_copy_to_measurement_table` when the `copy` flag is true, after all transformations and optional indexing have been performed.  
  * `copy_z_measurement` is a thin wrapper that prepares logging and then delegates to `_copy_to_measurement_table`. Both callers expect a boolean return value and will abort the overall process if `False` is returned.  
* **Callees** – Within `_copy_to_measurement_table` the following utilities are used:  
  * `Statement` (from `pedsnetdcc.db`) to encapsulate the SQL command.  
  * `Statement.execute` to run the command against the database.  
  * `check_stmt_err` (from `pedsnetdcc.utils`) to verify that the execution completed without error and to raise a `DatabaseError` if needed.  

**Note**:  
- The INSERT uses `ON CONFLICT DO NOTHING`; therefore, if a row with the same primary‑key already exists in the destination table, it will be skipped without raising an error. This behavior is intentional to make the copy idempotent.  
- All column names in the INSERT list must exactly match the columns present in both the destination and source tables; any schema change that adds, removes, or reorders columns will require an update to this SQL template.  
- The function does not perform any explicit transaction management; autocommit mode is used, so each INSERT statement is committed immediately.  
- Errors that occur during connection establishment or SQL execution are captured by `Statement.execute` and then re‑raised by `check_stmt_err`. Callers should be prepared to handle `DatabaseError`.  
- The function returns a simple boolean; it does not return the number of rows inserted. If callers need that information they must query the database separately.

**Output Example**:  
```python
result = _copy_to_measurement_table(
    conn_str="host=db.example.com dbname=pedsnet user=etl password=secret",
    schema="dcc_pedsnet",
    table="measurement",
    out_table="measurement_bmiz"
)
print(result)   # => True  (copy succeeded, duplicates ignored)
```
## FunctionDef run_z_calc(z_type, config_file, conn_str, site, copy, ids, indexes, concept, age, neg_ids, skip_calc, table, out_table, person_table, password, search_path, model_version, id_name)
**run_z_calc**: The function of run_z_calc is to orchestrate the complete Z‑Score derivation workflow for BMI‑Z, Height‑Z, or Weight‑Z measurements, including configuration file creation, output table preparation, execution of the external Z‑Score tool, optional indexing, enrichment of the result table, optional copying into the canonical measurement table, and final vacuuming.

**parameters**: The parameters of this Function.  
· z_type (str): type of Z‑Score calculation; accepted values are `'bmiz'`, `'ht_z'`, or `'wt_z'`.  
· config_file (str): name of the temporary configuration file that will be written for the external Z‑Score tool.  
· conn_str (str): PostgreSQL libpq connection string used for all database operations.  
· site (str): identifier of the site for which the derivation is being run; used in log messages.  
· copy (bool): if True, the resulting rows are copied from the temporary Z‑Score output table into the core `measurement` table.  
· ids (bool): if True, unique `measurement_id` values are generated and assigned to the rows in the output table.  
· indexes (bool): if True, indexes mirroring those on the core `measurement` table are created on the output table.  
· concept (bool): if True, concept‑name columns are populated by joining concept‑id fields to the OMOP vocabulary.  
· age (bool): if True, a column `measurement_age_in_months` is calculated and filled.  
· neg_ids (bool): if True, the sequence used for generating measurement IDs produces negative values; otherwise it produces positive values.  
· skip_calc (bool): if True, the actual Z‑Score calculation step (external tool) is skipped; the function will only create the output table and perform optional post‑processing.  
· table (str): name of the source measurement table that contains the raw BMI, height, or weight measurements (e.g., `measurement` or `measurement_anthro`).  
· out_table (str): name of the temporary output table that will receive the Z‑Score results.  
· person_table (str): name of the person table; required by the configuration file for the Z‑Score tool.  
· password (str): database password; if None the password is extracted from `conn_str`.  
· search_path (str): PostgreSQL schema search_path; the first schema is used as the primary schema for all DDL.  
· model_version (str): PEDSnet data model version (e.g., `'2.3.0'`).  
· id_name (str): prefix used for naming the last‑id tracking table (e.g., `'dcc'` or `'onco'`).  

**Code Description**:  
1. **Determine human‑readable Z‑Score name** – The function maps `z_type` to a display name (`'Height‑Z'`, `'Weight‑Z'`, or `'BMI‑Z'`).  
2. **Extract connection information** – `get_conn_info_dict` parses `conn_str` into a dictionary; this dictionary is merged with the site identifier to form a base log dictionary.  
3. **Log start of the derivation** – An INFO log entry records the start of the calculation together with the site and connection details; the start time is saved for elapsed‑time reporting.  
4. **Identify primary schema** – `primary_schema(search_path)` returns the first schema in the supplied search_path; this schema is used for all subsequent DDL statements.  
5. **Obtain password if missing** – If `password` is `None`, a regular‑expression search extracts the password from `conn_str`.  
6. **Prepare a StatementSet** – `stmts = StatementSet()` will hold DDL statements that need to be executed before the external tool runs.  
7. **Configuration file creation (unless `skip_calc`)** –  
   * The configuration directory is fixed to `"/app"`.  
   * Depending on `z_type`, one of the three helper functions is called to write the appropriate config file:  
     - `_create_height_z_config_file` for `'ht_z'`  
     - `_create_weight_z_config_file` for `'wt_z'`  
     - `_create_bmiz_config_file` for `'bmiz'`  
   * Each helper writes an XML‑like file containing concept mappings, Z‑Score parameters, and a `<src_rdb>` block with database connection details.  
8. **Create the output measurement table** – A `Statement` is built from the constant `CREATE_MEASURE_LIKE_TABLE_SQL` (template `CREATE TABLE {schema}.{out_table} (LIKE measurement INCLUDING ALL)`) and added to the StatementSet. The statement is executed sequentially; any error is logged and re‑raised via `check_stmt_err`.  
9. **Special handling for BMI‑Z** – If `z_type` is `'bmiz'`, an additional `ALTER TABLE` statement (`BMIZ_INCREASE_VALUE_AS_NUMBER`) is executed to enlarge the `value_as_number` column and avoid numeric overflow. Errors are handled in the same way.  
10. **Drop NULL measurement IDs** – A `DROP NULL` statement (`DROP_NULL_Z_TABLE_SQL`) removes rows where `measurement_id` is NULL; this prepares the table for later ID assignment.  
11. **Run the external Z‑Score tool** – `derive_z` is invoked with the configuration file (basename without the `.conf` suffix) and `--verbose=1`. The tool reads the config, performs the Z‑Score calculation, and populates the output table.  
12. **Post‑processing for BMI‑Z** –  
    * Rows where `value_as_number` overflowed the numeric limit (`10^15`) are deleted using `BMIZ_DELETE_OVERFLOW`.  
    * The column size is restored to its default definition with `BMIZ_DEFAULT_VALUE_AS_NUMBER`.  
13. **Remove NaN source values** – A `DELETE` statement (`Z_DELETE_NAN`) removes rows whose `value_source_value` is `'NaN'`.  
14. **Index creation (if `indexes` is True)** –  
    * A list of column names (`col_index`) mirrors the indexes on the core `measurement` table.  
    * For each column, an index name is generated by `_make_index_name(z_type, col)`.  
    * If the output table follows the naming convention `measurement_<z_type>`, the index name is used; otherwise a generic index name template (`IDX_NONAME_MEASURE_LIKE_TABLE_SQL`) is applied.  
    * All index statements are added to the StatementSet and executed in parallel with `stmts.parallel_execute(conn_str, 5)`. Errors are checked after execution.  
15. **Add measurement IDs (if `ids` is True)** – `_add_measurement_ids` is called with the appropriate arguments. This function reserves a block of IDs from the site‑specific last‑id tracking table, creates or resets a sequence, populates the `measurement_id` column, and adds a primary‑key constraint. Failure returns `False`.  
16. **Populate concept name columns (if `concept` is True)** – `_fill_concept_names` runs an UPDATE that joins each concept‑id column to `vocabulary.concept` to fill the corresponding `*_concept_name` columns. Failure returns `False`.  
17. **Calculate and store age in months (if `age` is True)** – `_fill_age_in_months` creates helper PostgreSQL functions to compute fractional months between birth and measurement dates, then updates the `measurement_age_in_months` column. Failure returns `False`.  
18. **Copy to core measurement table (if `copy` is True)** – `_copy_to_measurement_table` inserts rows from the temporary output table into the canonical `measurement` table using an `INSERT … SELECT … ON CONFLICT DO NOTHING` statement, then checks for errors. Failure returns `False`.  
19. **Vacuum the output table** – `vacuum(conn_str, model_version, analyze=True, tables=[out_table])` runs `VACUUM ANALYZE` on the newly created table to reclaim space and update planner statistics.  
20. **Log completion** – An INFO log entry records the end of the derivation and the total elapsed time.  
21. **Return value** – If no exception was raised and all optional steps succeeded, the function returns `True`.  

**Reference relationships**:  
*Callers* – `run_z_calc` is invoked by several command‑line entry points in `pedsnetdcc/main.py`: `run_derivations`, `run_bmiz`, `run_bmi_bmiz`, `run_height_z`, `run_weight_z`, and `run_ht_wt_z`. Each caller supplies the appropriate `z_type` and flags based on user options.  
*Callees* – The function relies on a number of internal helpers and utilities:  
- Configuration file creators: `_create_bmiz_config_file`, `_create_height_z_config_file`, `_create_weight_z_config_file`.  
- Database utilities: `get_conn_info_dict`, `primary_schema`, `Statement`, `StatementSet`, `StatementList`, `check_stmt_err`, `check_stmt_data`, `combine_dicts`, `secs_since`.  
- External tool wrapper: `derive_z`.  
- Post‑processing helpers: `_make_index_name`, `_add_measurement_ids`, `_fill_concept_names`, `_fill_age_in_months`, `_copy_to_measurement_table`.  
- Maintenance utility: `vacuum`.  

**Note**:  
- The function assumes that the schema identified by `search_path` exists and that the user has sufficient privileges to create tables, sequences, indexes, and to run `VACUUM`.  
- The configuration files are written to `/app` in binary mode; the directory must be writable by the process.  
- If `skip_calc` is True, the external Z‑Score tool is not executed, but the output table is still created and all optional post‑processing steps are performed on the empty table.  
- Index names generated by `_make_index_name` respect the Oracle identifier length limit (`NAME_LIMIT`). Changing the limit requires updating that constant.  
- The function uses `StatementSet.parallel_execute` with a fixed pool size of 5 for index creation; this value can be adjusted if the environment permits more parallel workers.  
- All SQL statements are executed with autocommit; there is no explicit transaction surrounding the whole workflow, so partial results may remain if an error occurs after some steps have completed.  

**Output Example**:  
When called with typical arguments and all steps succeed, the function returns a boolean indicating success:

```
result = run_z_calc(
    z_type='bmiz',
    config_file='stlouis_bmiz_temp.conf',
    conn_str='host=db.example.com port=5432 dbname=pedsnet user=loader password=secret',
    site='stlouis',
    copy=True,
    ids=True,
    indexes=True,
    concept=True,
    age=True,
    neg_ids=False,
    skip_calc=False,
    table='measurement',
    out_table='measurement_bmiz',
    person_table='person',
    password=None,
    search_path='dcc_pedsnet,public',
    model_version='2.3.0',
    id_name='dcc'
)
print(result)   # => True
```
## FunctionDef _add_measurement_ids(z_type, out_table, conn_str, site, search_path, model_version, neg_ids, id_name)
**_add_measurement_ids**: The function of _add_measurement_ids is to assign unique measurement_id values to rows in a BMI‑derived measurement table, updating the corresponding last‑id tracking table, creating (or re‑using) a PostgreSQL sequence, and finally making measurement_id the primary key of the table.

**parameters**  
· z_type (str): type of Z‑score calculation – one of “bmiz”, “htz”, or “wtz”.  
· out_table (str): name of the output measurement table that will receive the new IDs.  
· conn_str (str): libpq‑style PostgreSQL connection string used for all database operations.  
· site (str): identifier of the site for which the Z‑score is being calculated; included in log messages.  
· search_path (str): PostgreSQL schema search_path; the first schema is used as the primary schema.  
· model_version (str): PEDSnet data model version (e.g., “2.3.0”).  
· neg_ids (bool): if True, creates a sequence that generates negative IDs; otherwise creates a normal positive‑increment sequence.  
· id_name (str): prefix used for naming the last‑id tracking table (e.g., “dcc” or “onco”).

**Code Description**  

1. **Prepare logging context** – The function extracts connection information with `get_conn_info_dict(conn_str)` and merges it with the site identifier via `combine_dicts`. A logger for the module is obtained and a start‑up message is logged. The current time is stored in `start_time` for later elapsed‑time reporting.

2. **Determine schema and table names** – `primary_schema(search_path)` returns the first schema in the supplied search_path; this value is stored in `schema`. The target table name is fixed as `'measurement'`. The name of the last‑id tracking table is built from the `id_name` argument using the template `"{id_name}_{table_name}_id"`.

3. **Load model metadata** – `stock_metadata(model_version)` fetches a SQLAlchemy `MetaData` object for the requested PEDSnet model version. The `measurement` table object is retrieved from `metadata.tables` and stored for later use in SQL template formatting.

4. **Count rows needing new IDs** – A `Statement` is created with the SQL  
   `SELECT COUNT(*) FROM {schema}.{out_table} WHERE measurement_id IS NULL`  
   and executed via `Statement.execute(conn_str)`. After execution, `check_stmt_err` verifies that no database error occurred, and `check_stmt_data` confirms that a result row was returned. The count is read from `new_id_count_stmt.data[0][0]` and incremented by one (the sequence will start at the next free value). The count is logged.

5. **Reserve a block of IDs atomically** – Two statements are assembled in a `StatementList`:
   * `LOCK {last_id_table_name}` – obtains an exclusive lock on the tracking table.
   * `UPDATE {last_id_table_name} AS new SET last_id = new.last_id + '{new_id_count}'::bigint FROM {last_id_table_name} AS old RETURNING old.last_id, new.last_id` – increments the stored last_id by the number of IDs needed and returns the old and new values.
   The list is executed with `serial_execute(conn_str, transaction=True)`, ensuring both statements run in a single transaction. After execution, `check_stmt_err` validates each statement, and `check_stmt_data` validates the UPDATE result. The old and new last_id values are stored in `tpl_vars`.

6. **Create the measurement_id sequence** – Depending on `neg_ids`, either a normal sequence or a negative‑range sequence is created with `CREATE SEQUENCE IF NOT EXISTS {schema}.{site}_{z_type}_measurement_id_seq`. The appropriate `Statement` is executed and error‑checked.

7. **Set the sequence start value** – The sequence is reset to start at `old_last_id + 1` using  
   `ALTER SEQUENCE {schema}.{site}_{z_type}_measurement_id_seq RESTART WITH {start}`. Execution is checked for errors.

8. **Populate measurement_id column** – An UPDATE statement assigns a fresh ID to each row that currently has `measurement_id IS NULL` by calling `nextval` on the newly created sequence:  
   `UPDATE {schema}.{out_table} SET measurement_id = nextval('{schema}.{site}_{z_type}_measurement_id_seq') WHERE measurement_id IS NULL`. Execution is checked for errors.

9. **Make measurement_id the primary key** – The table is altered with  
   `ALTER TABLE {schema}.{out_table} ADD PRIMARY KEY (measurement_id)`. Execution is checked for errors.

10. **Log completion** – A final log entry records the elapsed time since `start_time`. If no exception was raised, the function returns `True`.

**Reference Relationships**  

* **Callers** – The primary caller is `run_z_calc` in `pedsnetdcc/z_score.py`. After creating the Z‑score output table and optionally adding indexes, `run_z_calc` invokes `_add_measurement_ids` when the `ids` flag is True. No other functions in the repository call `_add_measurement_ids` directly.

* **Callees / utilities** –  
  * `get_conn_info_dict` – parses the connection string for logging.  
  * `combine_dicts` – merges dictionaries to build structured log messages.  
  * `primary_schema` – extracts the primary schema from the search_path.  
  * `stock_metadata` – provides the SQLAlchemy metadata needed to reference the measurement table.  
  * `Statement` and `StatementList` – encapsulate individual SQL commands and batch execution.  
  * `check_stmt_err` – raises `DatabaseError` if any statement reports an error.  
  * `check_stmt_data` – raises `MissingDataError` if a statement returns no data.  
  * Logging utilities (`logger`, `secs_since`) – record progress and timing.

**Note**  
* The function assumes that the `out_table` already exists and contains a `measurement_id` column that is currently NULL for rows needing IDs.  
* The last‑id tracking table (`{id_name}_{measurement}_id`) must exist and contain a single column `last_id` that stores the highest ID assigned so far.  
* The sequence is created with `IF NOT EXISTS`; if a sequence already exists (e.g., from a previous run), it will be reused and its start value will be reset to the newly calculated `old_last_id + 1`.  
* When `neg_ids` is True, the sequence is defined to generate negative numbers down to –2 147 483 647, matching the convention used for certain ID spaces.  
* All database modifications are performed with autocommit mode (via `Statement.execute`) except the two‑statement ID reservation, which runs inside a transaction to avoid race conditions.  
* Errors are propagated as `DatabaseError` or `MissingDataError`; callers should be prepared to catch these exceptions if they need custom handling.  
* The function returns a simple boolean (`True`) on success; any failure raises an exception before the return statement is reached.

**Output Example**  
When all steps complete without error, the call  

```python
result = _add_measurement_ids('bmiz', 'measurement_bmiz', conn_str,
                              site='stlouis', search_path='dcc_pedsnet,public',
                              model_version='2.3.0', neg_ids=False, id_name='dcc')
```  

will log a series of informational messages and finally return  

```python
True
```
## FunctionDef copy_z_measurement(z_type, conn_str, site, table, out_table, search_path)
**copy_z_measurement**: The function of copy_z_measurement is to copy the rows produced by a Z‑score calculation (BMI‑Z, Height‑Z, or Weight‑Z) from a temporary output table into the canonical `measurement` table within the primary schema, while providing structured logging of the operation.

**parameters**:  
· **z_type** (str): Identifier of the Z‑score calculation type. Accepted values are `'bmiz'` (BMI‑Z), `'ht_z'` (Height‑Z), or `'wt_z'` (Weight‑Z).  
· **conn_str** (str): Libpq‑style PostgreSQL connection string used to open a database connection.  
· **site** (str): Site identifier that is included in log messages to distinguish multi‑site runs.  
· **table** (str): Name of the destination measurement table (normally `"measurement"`).  
· **out_table** (str): Name of the temporary source table that holds the Z‑score results to be copied.  
· **search_path** (str): PostgreSQL schema search path; the first schema in this list is treated as the primary schema for the operation.

**Code Description**:  
1. **Determine human‑readable Z‑score name** – The function maps the supplied `z_type` to a printable label (`"Height-Z"`, `"Weight-Z"`, or `"BMI-Z"`). This label is used only for logging.  
2. **Extract connection metadata** – `get_conn_info_dict(conn_str)` parses the connection string and returns a dictionary containing `user`, `host`, `port`, `dbname`, and `search_path`. The result is stored in `conn_info_dict`.  
3. **Prepare a generic log message template** – `logger_msg = '{0} {1} entries'` will later be formatted with start/finish verbs and the Z‑score name.  
4. **Log the start of the copy operation** –  
   * `log_dict` is built by merging `{'site': site}` with the connection‑info dictionary using `combine_dicts`.  
   * `logger.info` records a message such as “Starting copy of BMI‑Z entries” together with the combined context.  
   * `start_time` records the current epoch time for later elapsed‑time calculation.  
5. **Identify the primary schema** – `primary_schema(search_path)` returns the first schema name from the supplied `search_path`. The value is stored in `schema`.  
6. **Perform the actual copy** –  
   * A log entry “copy measurements to measurement” is emitted.  
   * `_copy_to_measurement_table(conn_str, schema, table, out_table)` is called. This helper builds an `INSERT … SELECT … ON CONFLICT DO NOTHING` statement that copies all columns from `out_table` into `table` within `schema`. It executes the statement, checks for errors via `check_stmt_err`, and raises `DatabaseError` on failure.  
   * The return value (`okay`) is expected to be `True` on success. If it is falsy, `copy_z_measurement` returns `False` immediately.  
   * On success, a log entry “measurements copied to measurement” is emitted.  
7. **Log the completion of the copy operation** – The elapsed time is calculated with `secs_since(start_time)` and logged together with a finish message such as “Finished copying BMI‑Z entries”.  
8. **Return status** – If no exception was raised and the copy succeeded, the function returns `True` to indicate overall success.

**Reference Relationships**  
*Callers* – The function is invoked by several command‑line entry points defined in `pedsnetdcc/main.py`:  
  • `copy_bmiz` (copies BMIZ results)  
  • `copy_height_z` (copies Height‑Z results)  
  • `copy_weight_z` (copies Weight‑Z results)  
  • `copy_to_measurement` (orchestrates copying of BMI, BMIZ, Height‑Z, and Weight‑Z tables in a single run)  

These callers construct the appropriate `z_type` value, build a connection string via `make_conn_str`, and forward the site, table, and output‑table names to `copy_z_measurement`. They exit the process with a non‑zero status if the function returns `False`.

*Callees* – `copy_z_measurement` relies on the following utilities:  
  • `get_conn_info_dict` – parses the connection string for logging.  
  • `combine_dicts` – merges dictionaries to create a unified log context.  
  • `primary_schema` – extracts the primary schema from the search path.  
  • `_copy_to_measurement_table` – performs the actual INSERT … SELECT operation and raises `DatabaseError` on failure.  
  • Logging facilities (`logger.info`) and timing helpers (`time.time`, `secs_since`).  

Through these relationships the function integrates logging, schema resolution, and database interaction in a single, reusable step.

**Note**:  
- The function does not perform any validation of `z_type` beyond the three recognized values; an unrecognized value will be treated as BMI‑Z for logging purposes.  
- Errors raised inside `_copy_to_measurement_table` (e.g., `DatabaseError`) propagate out of `copy_z_measurement`; the function only returns `False` when `_copy_to_measurement_table` explicitly returns a falsy value, which under normal circumstances does not occur.  
- The elapsed‑time log uses `secs_since`, which must be imported from the utilities module; if it is unavailable, a `NameError` will be raised.  
- The function assumes that the destination `table` and source `out_table` share identical column definitions; mismatched schemas will cause the underlying INSERT to fail.  
- All logging is performed at INFO level; callers should configure the logger appropriately to capture these messages.

**Output Example**:  
When called with valid arguments and a successful copy, the function returns the boolean value `True`. If an error occurs inside `_copy_to_measurement_table` and is not caught, a `DatabaseError` exception is raised instead of returning a value.
