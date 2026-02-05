## FunctionDef _populate_last_id(conn_str, schema, id_name)
**_populate_last_id**: The function of _populate_last_id is to create a PL/pgSQL helper function in the database that updates the “last_id” columns of a set of mapping tables and then invoke that helper for a specific schema and identifier prefix.

**parameters**:  
· conn_str (str): PostgreSQL connection string used to open a database connection.  
· schema (str): Name of the target schema (the primary schema derived from the search path) in which the mapping tables reside.  
· id_name (str): Prefix that identifies the set of mapping tables (e.g., “study1”) whose “last_id” values will be populated.

**Code Description**:  
1. **SQL Template Construction** – A multi‑line string `populate_last_id_sql` is defined. The string contains the definition of a PL/pgSQL function named `populate_last_id(schemanm text, mapmn text)`.  
   * The function declares local variables (`tbl_array`, `count_tbl`, `sqlstr`, `sel_stat`).  
   * It builds an array `tbl_array` of table names that exist in the source schema `dcc_pedsnet` (excluding `dose_era` and `hash_token`) and for which a corresponding mapping table `<mapmn>_<tablename>_id` exists in the target schema `schemanm`.  
   * It iterates over each table name in `tbl_array`. For each table it constructs an appropriate `UPDATE` (or `INSERT` for a few special tables) statement that sets the `last_id` column of the mapping table to one greater than the maximum primary‑key value in the source table. Special cases are handled for `death`, `measurement_organism`, and the core tables `care_site`, `person`, `provider`, `visit_occurrence`.  
   * Each generated statement is executed with `EXECUTE sqlstr`. The loop finishes after all tables have been processed, and the PL/pgSQL function ends.  

2. **Invocation of the PL/pgSQL Function** – After the function definition, the template adds a `SELECT count(*) FROM populate_last_id('{0}', '{1}');` line. The placeholders `{0}` and `{1}` are later replaced with the `schema` and `id_name` arguments, respectively. This SELECT call executes the newly created PL/pgSQL function and returns the number of rows processed (the count is not used further).

3. **Statement Object Creation** – A human‑readable message `"populating last_ids"` is stored in `populate_last_id_msg`. A `Statement` instance is created with the formatted SQL (`populate_last_id_sql.format(schema, id_name)`) and the message. The `Statement` class encapsulates the SQL command and provides execution metadata.

4. **Execution and Error Handling** – The statement’s `execute` method is called with `conn_str`. After execution, the helper function `check_stmt_err` is invoked to inspect `populate_last_id_stmt.err`. If an error is present, `check_stmt_err` logs the problem and raises a `DatabaseError`. If no error occurs, execution proceeds.

5. **Return Value** – When the function reaches the end without raising an exception, it returns `True` to indicate successful population of all `last_id` values.

**Reference Relationships**:  
*Caller*: The public function `populate_last_id` (in the same module) calls `_populate_last_id` after determining the primary schema from the supplied `search_path`. `populate_last_id` logs start and finish messages and measures elapsed time, but the actual database work is performed by `_populate_last_id`.  
*Callees*: `_populate_last_id` relies on the `Statement` class from `pedsnetdcc/db.py` to send the SQL to the database, and on `check_stmt_err` from `pedsnetdcc/utils.py` to verify that the execution succeeded. No other internal functions are called.

**Note**:  
- The PL/pgSQL function `populate_last_id` is created (or replaced) each time `_populate_last_id` runs; therefore concurrent executions may overwrite each other’s definition, but the subsequent SELECT call ensures the latest definition is used.  
- The function assumes that the target schema already contains mapping tables named `<id_name>_<source_table>_id`. If a required mapping table is missing, the SELECT that builds `tbl_array` will exclude that source table, and no `last_id` will be set for it.  
- Special‑case handling for `death`, `measurement_organism`, and the core tables is hard‑coded; changes to source table primary‑key column names require updates to this function.  
- Because the SQL is executed via `Statement.execute`, the connection is opened with autocommit mode; any DDL performed by the PL/pgSQL definition is committed immediately.  
- Errors occurring during the creation of the PL/pgSQL function or during its execution are captured in `populate_last_id_stmt.err` and will cause `check_stmt_err` to raise a `DatabaseError`. Callers should be prepared to handle this exception.

**Output Example**:  
```python
result = _populate_last_id(conn_str, 'study_schema', 'study1')
print(result)   # True
```
## FunctionDef _temp_dump_file(site)
**_temp_dump_file**: The function of _temp_dump_file is to generate a temporary file path for PostgreSQL dump operations based on a site identifier.  

**parameters**:  
· site: a string that identifies the site or ID group for which the temporary dump file is being created.  

**Code Description**:  
The function consists of a single return statement:  

```python
return _temp_dump_file_templ.format(site)
```  

* `_temp_dump_file_templ` is a module‑level string template defined elsewhere in the same file (for example, something like `"/tmp/{0}_dump.sql"`).  
* The `format(site)` call substitutes the provided `site` argument into the placeholder(s) of the template, producing a concrete file system path.  
* The resulting path is used as the destination for a `pg_dump` command and later as the source for a `pg_restore` command.  

**Relationship with Callers**:  
Both `_dump_and_restore_dcc_ids` and `_dump_and_restore_id_maps` invoke `_temp_dump_file` to obtain a unique temporary file name before performing a database dump. After the dump is restored into the target database, each caller removes the file with `os.remove(dump_file_path)`. Thus, `_temp_dump_file` is a utility that centralises the naming convention for these temporary dump files, ensuring consistency across different restoration workflows.  

**Note**:  
* The `site` argument must be a valid identifier that matches the placeholders expected by `_temp_dump_file_templ`; otherwise a `IndexError` or `KeyError` may be raised.  
* The template must be defined before this function is called; if it is missing or incorrectly formatted, a `NameError` or `AttributeError` will occur.  
* The generated path should be writable by the process executing the dump and restore commands, and the caller is responsible for deleting the file after use to avoid littering the filesystem.  

**Output Example**:  
If `_temp_dump_file_templ` is defined as `"/tmp/{0}_dump.sql"` and the function is called with `site = "pedsnet"` then the return value will be:  

`/tmp/pedsnet_dump.sql`
## FunctionDef _base_dump_args(conn_str, dump_path)
**_base_dump_args**: The function of _base_dump_args is to construct a basic tuple of command‑line arguments for invoking `pg_dump` with a specific database connection string and output file path.

**parameters**:  
· conn_str: A string containing the PostgreSQL connection URI (e.g., `postgres://user:pass@host/db`).  
· dump_path: A string specifying the filesystem path where the dump file should be written.

**Code Description**:  
The function returns a fixed sequence of arguments that are common to all dump operations performed by this module.  
1. `'--dbname=' + conn_str` – tells `pg_dump` which database to connect to.  
2. `'-Fc'` – selects the custom archive format, which is the default for `pg_dump` when a portable binary dump is desired.  
3. `'-Z'` followed by `'9'` – enables compression and sets the compression level to the maximum (9).  
4. `'--data-only'` – instructs `pg_dump` to dump only table data, omitting schema definitions.  
5. `'-f'` – indicates that the next argument will be the output file name.  
6. `dump_path` – the actual path where the dump file will be written.

The function returns these items as a tuple, preserving order for direct concatenation with additional arguments.  

Both `_dump_args` and `_dcc_dump_args` in the same module call `_base_dump_args` to obtain this core argument set, then extend the tuple with table‑specific `-t` options (and, in the case of `_dump_args`, an extra `-f` to repeat the output file argument). This design centralises the common dump configuration, ensuring consistency across different dump scenarios.

**Note**:  
- The returned tuple is immutable; callers typically concatenate additional tuples to build the final argument list.  
- The function does not perform any validation on `conn_str` or `dump_path`; callers must ensure these strings are correctly formatted.  
- Because `--data-only` is always included, schema objects (tables, indexes, constraints) are never dumped by the arguments generated here.

**Output Example**:  
```python
_base_dump_args('postgres://user:pass@localhost/pedsnet', '/tmp/pedsnet_dump.dump')
# Returns:
(
    '--dbname=postgres://user:pass@localhost/pedsnet',
    '-Fc',
    '-Z',
    '9',
    '--data-only',
    '-f',
    '/tmp/pedsnet_dump.dump'
)
```
## FunctionDef _dump_args(site, conn_str, dump_path, id_name)
**_dump_args**: The function of _dump_args is to build a complete tuple of command‑line arguments for invoking `pg_dump` to export ID‑map tables from a source database.

**parameters**:  
· site: The site identifier (e.g., a short string such as “omop” or “pedsnet”) used to format table‑specific SQL patterns.  
· conn_str: PostgreSQL connection URI for the source database (e.g., `postgres://user:pass@host/db`).  
· dump_path: Filesystem path where the dump file should be written.  
· id_name: Identifier that distinguishes which set of ID‑map tables to dump; the special value `'dcc'` indicates the default set.

**Code Description**:  
1. The function starts by obtaining a base argument tuple from `_base_dump_args(conn_str, dump_path)`. This base tuple already contains the connection string, format options, compression level, `--data-only`, and the initial `-f dump_path` flag.  
2. It then normalises the `id_name` argument:  
   - If `id_name` is not `'dcc'`, a leading underscore is prefixed (e.g., `'foo'` becomes `'_foo'`).  
   - If `id_name` equals `'dcc'`, it is replaced with an empty string, meaning no suffix will be added to the table names.  
3. For every table name listed in the module‑level constant `CONSISTENT_ID_MAP_TABLES`, the function appends a `-t` option to the argument tuple. The table specification is produced by formatting the `ID_MAP_TABLE_SQL` template with the current `site`, the table name, and the processed `id_name`. This results in a series of `-t <SQL>` arguments that tell `pg_dump` to dump only the selected ID‑map tables.  
4. After all table‑specific options have been added, the function appends a final `-f dump_path` pair. This repeats the output‑file flag so that the last `-f` on the command line is the one that `pg_dump` ultimately uses (the earlier `-f` from `_base_dump_args` is retained for consistency with other dump functions).  
5. The completed tuple, now containing the base arguments, a `-t` entry for each ID‑map table, and the final `-f` flag, is returned to the caller.

**Reference Relationship**:  
- **Callee**: `_dump_args` calls `_base_dump_args` to obtain the common dump configuration. It also relies on the module constants `CONSISTENT_ID_MAP_TABLES` and `ID_MAP_TABLE_SQL` to generate table‑specific `-t` arguments.  
- **Caller**: `_dump_and_restore_id_maps` invokes `_dump_args` to produce the argument list passed to the `pg_dump` wrapper function. The resulting dump file is later restored by `_restore_args` and `pg_restore`. This chain ensures that only the relevant ID‑map tables are exported and then re‑imported into the target database.

**Note**:  
- The function does not validate `site`, `conn_str`, `dump_path`, or `id_name`; callers must guarantee that these values are correct and safe.  
- The returned tuple is immutable; callers typically concatenate it with other tuples to form the final command‑line argument list.  
- Because the base arguments include `--data-only`, only table data (no schema) will be dumped.  
- The handling of `id_name` means that a value of `'dcc'` results in no suffix, while any other value adds an underscore prefix, which must match the naming convention used in the database.

**Output Example**:  
```python
_dump_args('omop', 'postgres://user:pass@localhost/pedsnet', '/tmp/omop_id_maps.dump', 'foo')
# Returns a tuple similar to:
(
    '--dbname=postgres://user:pass@localhost/pedsnet',
    '-Fc',
    '-Z',
    '9',
    '--data-only',
    '-f',
    '/tmp/omop_id_maps.dump',
    '-t', 'SELECT * FROM omop.id_map_table1_foo',
    '-t', 'SELECT * FROM omop.id_map_table2_foo',
    # ... one -t entry for each table in CONSISTENT_ID_MAP_TABLES ...
    '-f', '/tmp/omop_id_maps.dump'
)
```
## FunctionDef _dcc_dump_args(conn_str, dump_path, id_name)
**_dcc_dump_args**: The function of _dcc_dump_args is to build a complete tuple of command‑line arguments for `pg_dump` that includes the core dump options and a set of table‑specific `-t` selections for all DCC ID mapping tables.

**parameters**:  
· conn_str: PostgreSQL connection URI string that identifies the source database (e.g., `postgres://user:pass@host/db`).  
· dump_path: Filesystem path where the dump file will be written.  
· id_name: The identifier name (such as a cohort or study name) that is interpolated into the table‑specific SQL pattern.

**Code Description**:  
1. The function starts by calling `_base_dump_args(conn_str, dump_path)`. This helper returns a tuple containing the common `pg_dump` arguments: database connection (`--dbname=`), custom format (`-Fc`), maximum compression (`-Z 9`), data‑only mode (`--data-only`), and the output file option (`-f dump_path`).  
2. The returned tuple is stored in the local variable `dump_args`.  
3. The function then iterates over the global iterable `CONSISTENT_ID_MAP_TABLES`, which lists the names of all tables that hold DCC ID mappings. For each table name, it formats the SQL fragment `DCC_IDS_TABLE_SQL` with the current table name and the supplied `id_name`. The resulting string is a `-t` argument that tells `pg_dump` to dump only that specific table.  
4. Each generated `-t` argument is concatenated to `dump_args` using the `+=` operator, which creates a new tuple that includes the additional table selector.  
5. After processing all tables, the fully assembled tuple `dump_args` is returned to the caller.

**Reference Relationship**:  
- **Callee**: `_dcc_dump_args` relies on `_base_dump_args` to obtain the baseline argument set. It also uses the constants `CONSISTENT_ID_MAP_TABLES` and `DCC_IDS_TABLE_SQL` defined elsewhere in the module to construct the table‑specific options.  
- **Caller**: The function is invoked by `_dump_and_restore_dcc_ids` when dumping ID mapping tables from an old database. `_dump_and_restore_dcc_ids` passes the old connection string, a temporary dump file path, and the `id_name` to `_dcc_dump_args`, then forwards the resulting argument tuple to the `pg_dump` wrapper. This ensures that the dump operation includes all required ID tables and follows the standard dump configuration.

**Note**:  
- The returned value is a tuple; tuples are immutable, so callers typically concatenate additional tuples rather than modifying it in place.  
- No validation of `conn_str`, `dump_path`, or `id_name` is performed; callers must guarantee that these inputs are correctly formatted.  
- Because `_base_dump_args` includes `--data-only`, the generated arguments will never dump schema objects—only the data rows of the selected tables.  
- The order of arguments matters for `pg_dump`; the function preserves the order established by `_base_dump_args` and appends each `-t` option sequentially.

**Output Example**:  
```python
# Assume:
#   conn_str = 'postgres://user:pass@localhost/pedsnet'
#   dump_path = '/tmp/pedsnet_ids.dump'
#   id_name = 'study123'
#   CONSISTENT_ID_MAP_TABLES = ['person', 'visit_occurrence']
#   DCC_IDS_TABLE_SQL = '-t {}_{}'   # simplified example

_dcc_dump_args(conn_str, dump_path, id_name)
# Returns a tuple equivalent to:
(
    '--dbname=postgres://user:pass@localhost/pedsnet',
    '-Fc',
    '-Z',
    '9',
    '--data-only',
    '-f',
    '/tmp/pedsnet_ids.dump',
    '-t', 'person_study123',
    '-t', 'visit_occurrence_study123'
)
```
## FunctionDef _restore_args(conn_str, dump_path)
**_restore_args**: The function of _restore_args is to build a tuple of command‑line arguments for the `pg_restore` utility, targeting a specific PostgreSQL database and dump file.

**parameters**: The parameters of this Function.  
· conn_str: A PostgreSQL connection string (or database name) that identifies the target database where the dump will be restored.  
· dump_path: The filesystem path to the dump file that was created by `pg_dump` and will be supplied to `pg_restore`.

**Code Description**:  
The function returns a 5‑element tuple that represents the arguments passed to the external `pg_restore` command. The elements are constructed as follows:  

1. `'--dbname=' + conn_str` – Supplies the target database name or connection string to `pg_restore` using the `--dbname` option.  
2. `'-Fc'` – Informs `pg_restore` that the input file is in the custom format (`-Fc`). This matches the format produced by the corresponding `pg_dump` call elsewhere in the project.  
3. `'-j'` – Enables parallel restoration by specifying the number of worker jobs.  
4. `'8'` – Sets the number of parallel jobs to eight, allowing up to eight concurrent restore processes.  
5. `dump_path` – The path to the dump file that `pg_restore` will read.

The returned tuple is directly unpacked by the `pg_restore` wrapper function (not shown here) and used to execute the restore operation.  

In the project, `_restore_args` is invoked by two higher‑level helper functions:  

- `_dump_and_restore_dcc_ids` – After dumping DCC ID tables from an old database, it calls `pg_restore(_restore_args(new_conn_str, dump_file_path))` to load the dump into the new database.  
- `_dump_and_restore_id_maps` – After dumping site‑specific ID map tables, it similarly calls `pg_restore(_restore_args(new_conn_str, dump_file_path))` to restore the data.

Both callers rely on `_restore_args` to supply a consistent set of arguments (custom format, 8 parallel jobs) for restoring dumps, ensuring that the restoration step is performed uniformly across different types of ID data.

**Note**:  
- The function does not validate its inputs; callers must ensure that `conn_str` is a valid PostgreSQL connection identifier and that `dump_path` points to an existing dump file.  
- The parallel job count (`'8'`) is hard‑coded; if the target environment has fewer CPU cores or limited resources, callers may need to modify this value or replace the function with a configurable variant.  
- The tuple order must match the expectations of the `pg_restore` wrapper; altering the order could cause incorrect command‑line construction.

**Output Example**:  
```python
_restore_args('mydb', '/tmp/dcc_ids.dump')
# Returns:
('--dbname=mydb', '-Fc', '-j', '8', '/tmp/dcc_ids.dump')
```
## FunctionDef _dump_and_restore_dcc_ids(old_conn_str, new_conn_str, starttime, id_name)
**_dump_and_restore_dcc_ids**: The function of _dump_and_restore_dcc_ids is to dump all DCC ID mapping tables from an old PostgreSQL database and restore them into a new PostgreSQL database, while logging progress and cleaning up temporary files.

**parameters**: The parameters of this Function.  
· old_conn_str: PostgreSQL connection string that identifies the source (old) database.  
· new_conn_str: PostgreSQL connection string that identifies the target (new) database.  
· starttime: Epoch time (float or int) captured at the beginning of the overall copy operation; used for elapsed‑time logging.  
· id_name: Identifier string that distinguishes the set of ID tables (e.g., a cohort name) and is incorporated into the temporary dump file name.

**Code Description**:  
1. The function begins by writing an informational log entry that the dump of ID tables is starting. The log includes the elapsed time since *starttime* by calling **secs_since(starttime)**.  
2. A temporary file path for the PostgreSQL dump is generated with **_temp_dump_file(id_name)**. The *id_name* value is interpolated into a module‑level template, producing a unique file name such as “/tmp/<id_name>_dump.sql”.  
3. The dump operation is performed by invoking **pg_dump** with the argument tuple returned from **_dcc_dump_args(old_conn_str, dump_file_path, id_name)**.  
   - **_dcc_dump_args** builds a base argument list via **_base_dump_args** and appends a “‑t” selector for each table listed in **CONSISTENT_ID_MAP_TABLES**, formatting each selector with the pattern defined in **DCC_IDS_TABLE_SQL** and the supplied *id_name*.  
   - The resulting tuple tells pg_dump to connect to the old database, use custom format, compress maximally, dump data only, write to the temporary file, and include only the DCC ID tables relevant to *id_name*.  
4. After the dump file is created, the function restores its contents into the new database by calling **pg_restore** with the tuple from **_restore_args(new_conn_str, dump_file_path)**.  
   - **_restore_args** returns a fixed set of arguments: the target database identifier, the custom‑format flag, a parallel‑job flag “‑j” with a hard‑coded job count of 8, and the path to the dump file.  
5. Once the restore completes, the temporary dump file is removed from the filesystem with **os.remove(dump_file_path)** to avoid leaving stray files.  
6. A final log entry records that the restoration of ID tables has finished, again reporting elapsed time via **secs_since(starttime)**.

**Reference Relationships**:  
- **Caller**: The function is invoked by **copy_id_maps** (in the same module) as the first step of the overall ID‑map copying workflow. `copy_id_maps` supplies the old and new connection strings, the start time captured at the beginning of the copy process, and the *id_name* that identifies the ID set to be transferred.  
- **Callees**: Within its body, _dump_and_restore_dcc_ids calls the following helpers:  
  * **_temp_dump_file** – creates a temporary file name based on *id_name*.  
  * **_dcc_dump_args** – assembles the pg_dump command‑line arguments for the DCC ID tables.  
  * **pg_dump** – external wrapper that executes the PostgreSQL dump using the arguments from _dcc_dump_args.  
  * **_restore_args** – assembles the pg_restore command‑line arguments for loading the dump into the new database.  
  * **pg_restore** – external wrapper that runs the PostgreSQL restore.  
  * **os.remove** – deletes the temporary dump file after restoration.  
- The function therefore acts as a bridge between the low‑level command‑construction utilities and the higher‑level copy workflow, ensuring that DCC ID tables are transferred atomically and that temporary resources are cleaned up.

**Note**:  
- Both *old_conn_str* and *new_conn_str* must be valid PostgreSQL connection URIs; malformed strings will cause pg_dump or pg_restore to fail.  
- *id_name* is used directly in the temporary file name; it should contain only filesystem‑safe characters to avoid path errors.  
- The temporary dump file is created in a location defined by the module‑level template **_temp_dump_file_templ**; the process must have write permission for that directory.  
- The restore step uses a fixed parallel job count of eight; on systems with fewer CPU cores or limited I/O bandwidth, this may need adjustment by modifying **_restore_args**.  
- Errors raised by pg_dump, pg_restore, or os.remove are not caught inside this function; callers should handle exceptions if they need graceful degradation.  
- Logging relies on a configured **logger** instance; if the logger is not set up, the calls will have no effect but will not interrupt execution.
## FunctionDef _dump_and_restore_id_maps(site, old_conn_str, new_conn_str, starttime, id_name)
**_dump_and_restore_id_maps**: The function of _dump_and_restore_id_maps is to export the ID‑map tables for a specific site from an old PostgreSQL database and import them into a new PostgreSQL database.

**parameters**: The parameters of this Function.  
· **site**: a string identifier for the site whose ID‑map tables are being processed (e.g., “omop”, “pedsnet”).  
· **old_conn_str**: PostgreSQL connection string (or URI) that points to the source (old) database.  
· **new_conn_str**: PostgreSQL connection string (or URI) that points to the target (new) database.  
· **starttime**: a numeric epoch timestamp (typically obtained from `time.time()`) used to calculate elapsed time for logging.  
· **id_name**: a string that selects which set of ID‑map tables to dump; the special value `'dcc'` indicates the default set, any other value is treated as a suffix for the table names.

**Code Description**:  
1. The function begins by writing an informational log entry that the dump of ID‑map tables for the given **site** is starting. The elapsed time since **starttime** is included using `secs_since(starttime)`.  
2. A temporary file path for the dump is generated by calling `_temp_dump_file(site)`. This path follows the module‑level template and is unique per site.  
3. `pg_dump` is invoked with the argument tuple returned by `_dump_args(site, old_conn_str, dump_file_path, id_name)`. `_dump_args` builds a complete command‑line for `pg_dump`, selecting only the ID‑map tables relevant to **site** and **id_name**, writing the output in custom format to the temporary file.  
4. After the dump completes, a second log entry records that the restoration phase is beginning, again showing elapsed time.  
5. `pg_restore` is called with the argument tuple from `_restore_args(new_conn_str, dump_file_path)`. This restores the data from the temporary dump file into the target database using the custom format and eight parallel jobs.  
6. The temporary dump file is removed from the filesystem with `os.remove(dump_file_path)` to avoid leaving stray files.  
7. A final log entry marks the successful completion of the restore operation, including the total elapsed time.

**Reference Relationships**:  
- **Caller**: `copy_id_maps` iterates over a list of sites and calls `_dump_and_restore_id_maps` for each site after handling DCC ID tables. This makes `_dump_and_restore_id_maps` a core step in the overall ID‑map copying workflow.  
- **Callees / Dependencies**:  
  * `_temp_dump_file` – provides the temporary dump file path.  
  * `_dump_args` – constructs the argument list for `pg_dump`.  
  * `pg_dump` – external wrapper that executes the PostgreSQL dump command.  
  * `_restore_args` – constructs the argument list for `pg_restore`.  
  * `pg_restore` – external wrapper that executes the PostgreSQL restore command.  
  * `os.remove` – cleans up the temporary file.  
  * `logger` and `secs_since` – used for structured logging and elapsed‑time reporting.  

**Note**:  
- The function assumes that the PostgreSQL utilities `pg_dump` and `pg_restore` are available in the execution environment and that the generated temporary file path is writable.  
- No explicit error handling is present; if `pg_dump`, `pg_restore`, or `os.remove` raise an exception, the function will propagate the error to the caller.  
- The **id_name** handling follows the convention used by `_dump_args`: a value other than `'dcc'` is prefixed with an underscore before being interpolated into table names.  
- The **site** argument must correspond to a valid identifier used in the `ID_MAP_TABLE_SQL` template; otherwise the dump command may fail.  
- The elapsed‑time fields in the log messages are formatted as zero‑padded four‑character strings by `secs_since`.  
- After successful restoration, the temporary dump file is always deleted; callers should ensure that no other process needs the file after this function returns.
## FunctionDef create_dcc_ids_tables(conn_str, id_name, id_type)
**create_dcc_ids_tables**: The function of create_dcc_ids_tables is to create one “last‑generated‑id” table for each PEDSnet data table, using the supplied database connection, identifier name, and identifier type.

**parameters**:  
· conn_str (str): PostgreSQL connection string that identifies the target database.  
· id_name (str): Base name for the identifier set (e.g., “dcc” or “onco”). It is used to build the schema name and each table name.  
· id_type (str): SQL data type for the identifier column; accepted values are “INTEGER” or “BIGINT”.

**Code Description**:  
1. The function logs the start of the operation and records the current time (`starttime`).  
2. It derives the schema name by concatenating the `id_name` with the suffix “_ids”. This schema will contain all the generated‑id tables.  
3. An empty `StatementList` instance (`statements`) is created to collect the SQL commands that will be executed.  
4. The function iterates over the constant `ID_MAP_TABLES`, which enumerates all PEDSnet tables that require an ID‑tracking table. For each table:  
   a. It builds the target table name as `<id_name>_<table>_id`.  
   b. It adds a `Statement` that creates the table using the template `CREATE_DCC_ID_TABLE_SQL`, filling in the schema, table name, and the supplied `id_type`.  
   c. If the current table is **not** listed in `CONSISTENT_ID_MAP_TABLES`, an additional `Statement` is added to initialize the table with a starting value using the template `INITIALIZE_DCC_ID_TABLE_SQL`. Tables in `CONSISTENT_ID_MAP_TABLES` are assumed to already contain appropriate initial data, so they are not re‑initialized.  
5. After the loop, `statements.serial_execute(conn_str)` is called. Because the default `transaction` argument is `False`, each `Statement` opens its own connection, runs the DDL, and closes the connection. This is appropriate for DDL statements that cannot be executed inside a single transaction block.  
6. The function then iterates over the executed statements and invokes `check_stmt_err(statement, 'type_ids table creation')`. This utility inspects the `err` attribute of each `Statement`; if an error is present, it logs the failure and raises a `DatabaseError`, causing the whole operation to abort.  
7. Finally, a completion log entry is emitted, including the elapsed time calculated by `secs_since(starttime)`.

**Reference Relationships**:  
- **Caller**: `create_id_maps` in `pedsnetdcc/main.py` invokes `create_dcc_ids_tables` after constructing a connection string. This is part of the CLI command that prepares the database for a new ID‑mapping cycle.  
- **Callees / Dependencies**:  
  * `Statement` (from `pedsnetdcc/db.py`) – used to encapsulate each CREATE and INITIALIZE SQL command.  
  * `StatementList` (from `pedsnetdcc/db.py`) – aggregates the `Statement` objects and provides the `serial_execute` method that runs them.  
  * `check_stmt_err` (from `pedsnetdcc/utils.py`) – validates that each statement executed without error.  
  * `secs_since` (from `pedsnetdcc/dict_logging.py`) – formats the elapsed time for the final log entry.  
  * Constants `ID_MAP_TABLES`, `CONSISTENT_ID_MAP_TABLES`, `CREATE_DCC_ID_TABLE_SQL`, and `INITIALIZE_DCC_ID_TABLE_SQL` (defined elsewhere in the `id_maps` module) supply the list of tables and the SQL templates used to build the statements.  

**Note**:  
- The function creates a separate schema (`<id_name>_ids`) for the tracking tables; the schema must either already exist or be created elsewhere before this function runs.  
- Because DDL statements are executed with `serial_execute` in non‑transactional mode, a failure on any single statement will not roll back previously created tables. The caller should be prepared to handle partially created schemas.  
- Tables listed in `CONSISTENT_ID_MAP_TABLES` are intentionally omitted from the initialization step; ensure that these tables are pre‑populated with a valid starting ID to avoid later ID‑allocation errors.  
- All logging is performed through the module‑level `logger`; the log entries contain structured dictionaries, which can be consumed by external log aggregators.  
- The function does not return a value; its success is indicated by the absence of raised exceptions. Any error during table creation will be propagated as a `DatabaseError` by `check_stmt_err`.
## FunctionDef create_id_map_tables(conn_str, skipsites, addsites, id_name, id_type, site_type, site_len)
**create_id_map_tables**: The function of create_id_map_tables is to create, for each selected site, a schema‑qualified table that stores the mapping between that site’s local identifiers and the DCC (or other) identifiers.

**parameters**  
· conn_str (str): PostgreSQL connection string for the target database.  
· skipsites (str): Comma‑separated list of site identifiers that must be excluded from table creation.  
· addsites (str): Comma‑separated list of additional site identifiers that must be forced into the creation set, even if they appear in the skip list.  
· id_name (str): Base name of the identifier being mapped (e.g., “dcc” or “onco”).  
· id_type (str): PostgreSQL data type of the DCC identifier column (e.g., “INTEGER” or “BIGINT”).  
· site_type (str): PostgreSQL data type of the site identifier column; may be “INTEGER”, “BIGINT”, or “VARCHAR”.  
· site_len (str): Length specification used only when site_type is “VARCHAR” (e.g., “10”).

**Code Description**  
1. **Logging start and timing** – The function logs a start message (`'starting id_map table creation'`) and records the current epoch time in `starttime` for later elapsed‑time reporting.  

2. **Prepare site identifier column type** – If `site_type` equals the literal string `'VARCHAR'`, it is transformed into a sized type by concatenating the supplied `site_len` (resulting in `'VARCHAR(<site_len>)'`). Otherwise the original type string is retained.  

3. **Determine the set of sites to process**  
   - `skip_sites` is built by splitting `skipsites` on commas.  
   - `id_sites` is initialized as the set difference between the global constant `SITES_AND_EXTERNAL` and `skip_sites`. This yields all known sites except those explicitly skipped.  
   - `add_sites` is built by splitting `addsites` on commas.  
   - The two sets are united (`|`) so that any site listed in `addsites` is guaranteed to be present, even if it was previously omitted.  
   - Empty strings are removed with `filter(None, ...)` to avoid creating tables for a blank site name.  

4. **Build the list of SQL statements**  
   - An empty `StatementList` instance (`statements`) is created.  
   - For each site in `id_sites` the target schema name is assembled:  
     * Base schema = site name.  
     * If `id_name` is not `'dcc'`, the schema name is suffixed with `_<id_name>` (e.g., `site_onco`).  
     * Finally `'_id_maps'` is appended, yielding a schema such as `site_id_maps` or `site_onco_id_maps`.  
   - For each table name defined in the module‑level list `ID_MAP_TABLES`, a `Statement` object is instantiated with the formatted DDL string `CREATE_ID_MAP_TABLE_SQL.format(schema, table, id_name, id_type, site_type)`. This SQL creates the mapping table inside the computed schema.  
   - Each `Statement` is added to the `StatementList` via `extend`.  

5. **Execute the statements** – `statements.serial_execute(conn_str)` runs all DDL commands sequentially. Because the statements are DDL, the default non‑transactional mode (`transaction=False`) is used, allowing each `CREATE TABLE` to be executed independently.  

6. **Error handling** – After execution, the function iterates over every `Statement` in `statements` and calls `check_stmt_err(statement, 'id map table creation')`. `check_stmt_err` inspects the `err` attribute of each statement; if an error is present, it logs the failure and raises a `DatabaseError`, causing the whole function to abort.  

7. **Logging completion** – Upon successful creation of all tables, a final log entry is emitted containing the message `'finished creation of id_maps tables'` and the elapsed time calculated by `secs_since(starttime)`.  

**Reference Relationships**  
- **Callers**: The function is invoked by `pedsnetdcc.main.create_id_maps`, which first builds a connection string, creates the DCC‑specific ID tables via `create_dcc_ids_tables`, and then calls `create_id_map_tables` with the user‑supplied parameters. This makes `create_id_map_tables` part of the high‑level CLI workflow for initializing a new data‑cycle environment.  

- **Callees**:  
  * `Statement` (from `pedsnetdcc.db`) – encapsulates each individual `CREATE TABLE` command and stores execution metadata.  
  * `StatementList` (from `pedsnetdcc.db`) – aggregates the `Statement` objects and provides the `serial_execute` method that runs them in order.  
  * `check_stmt_err` (from `pedsnetdcc.utils`) – validates that each statement completed without error and raises a `DatabaseError` if necessary.  
  * `secs_since` (from `pedsnetdcc.dict_logging`) – computes the elapsed time for the final log entry.  
  * Global constants `SITES_AND_EXTERNAL`, `ID_MAP_TABLES`, and the template `CREATE_ID_MAP_TABLE_SQL` are used to determine which schemas and tables to create and to format the DDL.  

**Note**  
- The function assumes that `SITES_AND_EXTERNAL` and `ID_MAP_TABLES` are defined elsewhere in the module and contain valid site identifiers and table names, respectively.  
- When `site_type` is `'VARCHAR'`, `site_len` must be a numeric string; otherwise the resulting SQL will be malformed.  
- The skip/add logic treats empty entries as non‑existent sites; therefore an empty `skipsites` or `addsites` string results in no sites being removed or added.  
- Because DDL statements cannot be run inside a transaction block, `serial_execute` is deliberately called without the `transaction=True` flag. Attempting to enable a transaction would cause PostgreSQL to reject the `CREATE TABLE` commands and would be captured as errors by `check_stmt_err`.  
- All errors are propagated as `DatabaseError` exceptions after logging; callers should be prepared to handle these exceptions to avoid unhandled crashes.  
- The function logs both the start and end of the operation, including a zero‑padded four‑character elapsed‑seconds field, which aids in performance monitoring of large site sets.
## FunctionDef copy_id_maps(old_conn_str, new_conn_str, id_name, skipsites, addsites)
**copy_id_maps**: The function of copy_id_maps is to copy the `id_maps` and `dcc_ids` tables from an old PostgreSQL database to a new PostgreSQL database using `pg_dump` and `pg_restore`.

**parameters**: The parameters of this Function.  
· **old_conn_str**: Connection string for the source (old) PostgreSQL database.  
· **new_conn_str**: Connection string for the target (new) PostgreSQL database.  
· **id_name**: Identifier for the set of ID tables to be processed (e.g., “dcc” or “onco”).  
· **skipsites**: Comma‑separated string of site identifiers that should be excluded from the copy operation.  
· **addsites**: Comma‑separated string of site identifiers that should be forcibly included, even if they appear in the skip list.

**Code Description**:  
1. The function logs the start of the ID‑map copying process and records the current epoch time in `starttime`.  
2. It splits the `skipsites` string on commas to obtain a list `skip_sites`.  
3. `id_sites` is initially built as the set difference between the module‑level constant `SITES_AND_EXTERNAL` and `skip_sites`. This yields all sites that are not explicitly skipped.  
4. The `addsites` string is split on commas to produce `add_sites`. These sites are added to `id_sites` by taking the union of the two sets, ensuring that any site listed in `addsites` will be processed regardless of the skip list.  
5. `filter(None, id_sites)` removes empty strings that may result from trailing commas or empty input, producing a clean list of site identifiers to process.  
6. The helper `_dump_and_restore_dcc_ids` is called once with the original connection strings, the captured `starttime`, and `id_name`. This step copies the global DCC ID tables that are not site‑specific.  
7. The function then iterates over each site in the final `id_sites` list and calls `_dump_and_restore_id_maps` for that site. Each call dumps the site‑specific `id_map` tables from the old database and restores them into the new database, using the same `starttime` and `id_name` for consistent elapsed‑time logging.  
8. After all sites have been processed, a final log entry records the completion of the copy operation, including the total elapsed time calculated by `secs_since(starttime)`.

**Note**:  
- `skipsites` and `addsites` must be comma‑separated strings; an empty string results in an empty list after `split(",")`.  
- The function relies on the module‑level constant `SITES_AND_EXTERNAL`; if this constant does not contain the expected site identifiers, the resulting `id_sites` list may be incomplete.  
- No explicit error handling is performed; any exception raised by `_dump_and_restore_dcc_ids`, `_dump_and_restore_id_maps`, or the underlying `pg_dump`/`pg_restore` commands will propagate to the caller.  
- The logging calls assume a configured `logger` instance; if logging is not set up, the calls will be no‑ops but will not affect execution.  
- The elapsed‑time fields in log messages are formatted by `secs_since`, which returns a zero‑padded four‑character string.  
- Ensure that the PostgreSQL utilities `pg_dump` and `pg_restore` are available in the execution environment and that the process has write permission to the temporary directory used by the helper functions.
## FunctionDef populate_last_id(conn_str, search_path, id_name)
**populate_last_id**: The function of populate_last_id is to coordinate the population of “last_id” values in study‑specific ID‑mapping tables by invoking the internal helper that performs the database work.

**parameters**: The parameters of this Function.  
· **conn_str** (str): PostgreSQL connection string used to open a database connection.  
· **search_path** (str): PostgreSQL schema search path; the first schema in this list is treated as the primary target schema for the operation.  
· **id_name** (str): Prefix that identifies the set of mapping tables (for example, “study1”) whose “last_id” columns will be populated.

**Code Description**:  
1. The function logs a start message (`'starting populating last_id'`) using the module‑level logger.  
2. It records the current epoch time (`starttime = time.time()`) to later calculate elapsed execution time.  
3. It calls **primary_schema(search_path)** to extract the first schema name from the supplied search_path. The returned value is stored in the local variable **schema**.  
4. With the derived **schema**, the function delegates the actual database work to the private helper **_populate_last_id(conn_str, schema, id_name)**. This helper creates a PL/pgSQL function in the database that updates the `last_id` columns of all relevant mapping tables and then executes that PL/pgSQL function.  
5. After the helper returns, the function logs a completion message that includes the elapsed time, calculated by **secs_since(starttime)**, which formats the number of whole seconds as a zero‑padded four‑character string.  
6. Finally, the function returns **True** to indicate successful completion.

**Note**:  
- The function does not perform any error handling itself; any exception raised by **primary_schema** or **_populate_last_id** (including database errors) will propagate to the caller.  
- The returned value is always the boolean **True** when the function reaches its final line; failure is signaled by raising an exception rather than returning False.  
- Logging relies on a configured logger instance; if the logger is not set up, the log calls will be no‑ops.  
- The elapsed‑time field in the finish log is a string padded to at least four characters (e.g., `'0012'` for 12 seconds).  

**Output Example**:  
```python
result = populate_last_id(conn_str, "study_schema, public", "study1")
print(result)   # True
```
