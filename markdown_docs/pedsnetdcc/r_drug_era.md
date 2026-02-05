## FunctionDef _create_argos_file(config_path, config_file, schema, password, conn_info_dict)
**_create_argos_file**: The function of _create_argos_file is to generate a temporary Argos JSON configuration file that supplies connection details for the R drug‑era derivation script.

**parameters**: The parameters of this Function.  
· **config_path**: The directory path where the temporary Argos JSON file will be written.  
· **config_file**: The name of the JSON file to create (e.g., “site_drug_era_argos_temp.json”).  
· **schema**: The primary PostgreSQL schema that should be placed in the search_path option of the Argos configuration.  
· **password**: The database password that will be embedded in the JSON file.  
· **conn_info_dict**: A dictionary containing connection components (host, dbname, user) extracted from the original connection string.

**Code Description**:  
The function opens a new file in binary write mode at the location constructed from `config_path` and `config_file`. It then writes a JSON object line‑by‑line, using the operating‑system line separator (`os.linesep`) to ensure proper formatting on any platform.

1. Writes the opening brace `{`.  
2. Adds the fixed key `"src_name"` with the value `"Postgres"`.  
3. Begins the nested `"src_args"` object.  
4. Inserts the `"host"` value taken from `conn_info_dict.get('host')`.  
5. Inserts a hard‑coded `"port"` value of `5432`.  
6. Inserts the `"dbname"` value from `conn_info_dict.get('dbname')`.  
7. Inserts the `"user"` value from `conn_info_dict.get('user')`.  
8. Inserts the `"password"` value supplied directly via the `password` argument.  
9. Adds the `"bigint"` mapping set to `"numeric"` (required by Argos to treat PostgreSQL BIGINT as numeric).  
10. Constructs the `"options"` string that sets the PostgreSQL `search_path` to the supplied `schema` followed by the `vocabulary` schema, ensuring the R script can resolve tables correctly.  
11. Closes the `"src_args"` object and then the outer JSON object.

The file is written in a straightforward, line‑oriented fashion without using a JSON library; this approach guarantees the exact ordering and formatting expected by the downstream R script.

**Relationship with Callers**:  
`_create_argos_file` is invoked exclusively by `run_r_drug_era` (found in `pedsnetdcc/r_drug_era.py`). Within `run_r_drug_era`, after preparing a site‑specific copy of the R script package, the function is called to produce the Argos configuration file inside the temporary site directory. The generated JSON file is subsequently read by the R script (`run.R`) to establish a PostgreSQL connection using the supplied credentials and schema information. Thus, `_create_argos_file` acts as a bridge between the Python orchestration layer and the R derivation layer, encapsulating all connection parameters required for the R process.

**Note**:  
- The function writes the file in binary mode (`'wb'`) but only writes text strings; this works because Python automatically encodes the strings to bytes using the default encoding. If a different encoding is required, explicit encoding should be added.  
- No validation is performed on the input values; callers must ensure that `conn_info_dict` contains the keys `'host'`, `'dbname'`, and `'user'`, and that `password` is not `None`.  
- The JSON is constructed manually; any change to the Argos configuration schema should be reflected by updating the corresponding write statements.  
- The temporary file name ends with `_argos_temp.json`; callers should delete or overwrite this file as needed to avoid leaking credentials.
## FunctionDef _fix_site_info(file_path, site, schema)
**_fix_site_info**: The function of _fix_site_info is to substitute placeholder tokens in a site‑specific R configuration file with the actual site name and schema, persisting the changes for later use.

**parameters**:  
· file_path: absolute or relative path to the root directory that contains the site‑specific sub‑directory.  
· site: the site identifier string that will replace the `<SITE>` placeholder.  
· schema: the database schema name string that will replace the `<SCHEMA>` placeholder.

**Code Description**:  
The function begins by constructing the full path to the file *site_info.R* located in the *site* sub‑directory of *file_path* (`os.path.join(file_path,'site','site_info.R')`). It opens this file in read mode, reads the entire content into the variable *file_data*, and then performs two literal string replacements: every occurrence of the token `<SITE>` is replaced with the value of the *site* argument, and every occurrence of `<SCHEMA>` is replaced with the value of the *schema* argument. After the substitutions, the function re‑opens the same file in write mode and writes the modified content back, thereby updating the file in place.

All file‑handling operations are wrapped in a `try` block. If any exception occurs—such as the file not existing, lacking read/write permissions, or an unexpected I/O error—the `except` clause is executed, and the function returns `False` to signal that the update could not be performed. If the process completes without raising an exception, the function returns `True`, indicating a successful modification.

**Relationship with Callers**:  
Within the project, `_fix_site_info` is invoked by the `run_r_drug_era` function (found in *pedsnetdcc/r_drug_era.py*). After copying the generic R script package to a site‑specific directory, `run_r_drug_era` calls `_fix_site_info` to inject the concrete site name and schema into *site_info.R*. This step ensures that the subsequent R script execution (`run.R`) operates with the correct database context for the targeted site. If `_fix_site_info` returns `False`, the calling routine proceeds without raising an error, but the R script may later fail due to missing or incorrect configuration.

**Note**:  
- The function catches all exceptions indiscriminately; therefore, debugging file‑related issues may require inspecting logs or adding more granular error handling.  
- The placeholder tokens must match exactly `<SITE>` and `<SCHEMA>`; any deviation in the template file will leave the tokens unchanged.  
- The caller must guarantee that *file_path* points to a directory containing the expected *site/site_info.R* file and that the process has sufficient permissions to read and write this file.

**Output Example**:  
When the file exists and is successfully updated: `True`  
When the file is missing or an I/O error occurs: `False`
## FunctionDef _fix_size(file_path, size)
**_fix_size**: The function of _fix_size is to update the placeholder values for the cohort size in the R driver script used by the drug‑era derivation.  

**parameters**:  
· file_path: Path to the site‑specific package directory that contains a sub‑directory named **code** with the file **driver.R**.  
· size: Integer representing the number of persons that should be processed in each group; the function also computes **size‑1** for a secondary placeholder.  

**Code Description**:  
The function attempts to open the file **driver.R** located at `os.path.join(file_path, 'code', 'driver.R')`. It reads the entire content into memory (`file_data`). Two placeholder strings are then replaced:  

1. `<SIZE>` is replaced with the string representation of the supplied **size**.  
2. `<SIZEMINUS1>` is replaced with the string representation of **size‑1**.  

After performing the replacements, the modified content is written back to the same **driver.R** file, overwriting the original.  

If any step in this process raises an exception (for example, the file does not exist, the path is incorrect, or there are permission issues), the generic `except` block catches the error, the function returns **False**, and no further action is taken. When the file is successfully updated, the function returns **True**.  

**Relationship with Callers**:  
`_fix_size` is invoked from the `run_r_drug_era` function in **pedsnetdcc/r_drug_era.py**. Within `run_r_drug_era`, after copying the base R package to a site‑specific directory and creating the Argos configuration file, the script customises the R code for the current site. `_fix_size` is called with the destination path (`dest_path`) and the integer value supplied by the user (or default) for the **size** argument. By updating `<SIZE>` and `<SIZEMINUS1>` in **driver.R**, the R script that will later be executed (`run.R`) receives the correct cohort size parameters, ensuring that the derivation runs with the intended batch size.  

**Note**:  
- The function uses a bare `except:` clause, which suppresses all exceptions and makes debugging harder; callers should verify the return value to detect failures.  
- The placeholders `<SIZE>` and `<SIZEMINUS1>` must exist in the original **driver.R** file; otherwise, the replacements will have no effect but the function will still return **True**.  
- The function expects **size** to be an integer; passing a non‑numeric value will raise a `TypeError` when `size-1` is evaluated.  

**Output Example**:  
When the file is present and writable, the function returns `True`.  
If the file is missing or cannot be opened, the function returns `False`.
## FunctionDef _fix_start(file_path, start)
**_fix_start**: The function of _fix_start is to replace the placeholder `<START>` in the R driver script with a concrete start value and indicate success or failure.

**parameters**: The parameters of this Function.  
· **file_path**: The root directory of the generated R package for the drug‑era derivation. It is expected to contain a sub‑directory `code` with a file named `driver.R`.  
· **start**: An integer (or string convertible to integer) representing the person identifier from which the derivation should begin. This value will be inserted into the R script.

**Code Description**:  
1. The function builds the full path to `driver.R` by joining `file_path`, the literal folder name `code`, and the file name `driver.R`.  
2. It attempts to open this file in read mode. If the file cannot be opened (for example, because it does not exist in the given package), the `except` block is triggered and the function returns `False`.  
3. When the file is successfully read, its entire contents are stored in `file_data`.  
4. The string `<START>` within `file_data` is replaced by the string representation of the `start` argument (`str(start)`). This substitution injects the actual start identifier into the R script.  
5. The modified content is written back to the same `driver.R` file, overwriting the original.  
6. If any exception occurs during the read‑modify‑write sequence, the function silently catches it and returns `False`.  
7. If no exception occurs, the function reaches the final statement and returns `True`, signalling that the placeholder was successfully replaced.

**Relationship with callers**:  
`_fix_start` is invoked by `run_r_drug_era` (located in `pedsnetdcc/r_drug_era.py`). After `run_r_drug_era` copies the generic R package template to a site‑specific directory, it calls a series of helper functions to customize the R scripts: `_create_argos_file`, `_fix_site_info`, `_fix_run`, `_fix_size`, and finally `_fix_start`. By updating the `<START>` token, `_fix_start` ensures that the R derivation script processes records beginning with the user‑specified person identifier. The success flag returned by `_fix_start` is not explicitly checked in `run_r_drug_era`; the calling code proceeds assuming the modification succeeded, and any failure would be caught later when the R script is executed.

**Note**:  
- The function catches all exceptions without re‑raising them, which means any I/O error, permission issue, or unexpected file content will simply result in a `False` return value.  
- The placeholder `<START>` must exist exactly as written in `driver.R`; otherwise the replacement will have no effect but will still return `True`.  
- The function does not validate that `start` is a valid person identifier; it only converts the argument to a string.  

**Output Example**:  
```python
result = _fix_start('/app/drug_era/site1', 12345)
# result will be True if driver.R was found and updated,
# or False if the file was missing or an error occurred.
```
## FunctionDef _fix_run(file_path, site)
**_fix_run**: The function of _fix_run is to substitute the placeholder `<SITE>` with the actual site name inside the R script `run.R` for a given query package.

**parameters**:  
· **file_path**: The root directory of the query package on the filesystem (string).  
· **site**: The site identifier that should replace the `<SITE>` placeholder (string).

**Code Description**:  
The function attempts to locate the file `run.R` inside the sub‑directory `site` of the supplied `file_path`. It opens the file in read mode, loads its entire contents into memory, and performs a simple string replacement: every occurrence of the literal token `<SITE>` is replaced with the value of the `site` argument. After the substitution, the modified content is written back to the same file, overwriting the original.  

If any step fails—such as the file not existing, lacking read/write permissions, or an unexpected I/O error—the generic `except` block catches the exception and the function returns `False`, indicating that the modification could not be performed. When the operation succeeds, the function returns `True`.  

In the broader project, `_fix_run` is invoked by `run_r_drug_era` (found in `pedsnetdcc/r_drug_era.py`). After `run_r_drug_era` copies a template query package to a site‑specific directory, it calls `_fix_site_info` and then `_fix_run` to inject the actual site name into the R script that will later be executed. This ensures that the R derivation script runs with the correct site context.

**Note**:  
- The function uses a bare `except:` clause, which suppresses all exceptions and may hide useful debugging information.  
- The placeholder must be exactly `<SITE>`; any variation will not be replaced.  
- The caller must guarantee that `file_path/site/run.R` exists and is writable; otherwise the function will return `False`.  
- The function returns a boolean, not the modified file content.

**Output Example**:  
```python
result = _fix_run('/app/drug_era', 'umich')
# result will be True if the file was found and updated, False otherwise
```
## FunctionDef _copy_to_dcc_table(conn_str, era_type, schema)
**_copy_to_dcc_table**: The function of _copy_to_dcc_table is to copy drug‑era rows from a site‑specific schema into the central `dcc_pedsnet.drug_era` table, handling errors and returning a success flag.

**parameters**:  
· `conn_str` (str): PostgreSQL connection string used to open a database connection.  
· `era_type` (str): Identifier of the era type being copied (always `"drug"` for this module).  
· `schema` (str): Name of the source schema that contains the temporary `drug_era` table to be copied.

**Code Description**:  
1. **SQL statement preparation** – A multi‑line string `copy_to_drug_sql` is defined. It inserts all columns from `{schema}.drug_era` into `dcc_pedsnet.drug_era`. The `ON CONFLICT DO NOTHING` clause prevents duplicate primary‑key violations. The placeholder `{0}` is later replaced with the actual `schema` argument.  
2. **Logging message preparation** – `copy_to_msg` holds a human‑readable log template that will be interpolated with the `era_type`.  
3. **Statement object creation** – An instance of `pedsnetdcc.db.Statement` is constructed with the formatted SQL (`copy_to_drug_sql.format(schema)`) and the formatted message (`copy_to_msg.format(era_type)`). This object encapsulates the SQL, a description, and a unique immutable identifier.  
4. **Execution** – The statement’s `execute` method is called with `conn_str`. Internally, `Statement.execute` opens a new connection, sets autocommit, runs the SQL via `execute_on_conn`, captures `rowcount`, and stores any exception in `stmt.err`.  
5. **Error handling** – After execution, the helper `pedsnetdcc.utils.check_stmt_err` is invoked. It inspects `stmt.err`; if an error is present, it logs a detailed error message and raises a `DatabaseError`. If no error occurred, execution proceeds.  
6. **Return value** – When the insert completes without raising an exception, the function returns `True` to indicate success.

**Reference relationships**:  
- **Callers** – `_copy_to_dcc_table` is invoked by two higher‑level functions:  
  * `run_r_drug_era` (in the same module) when the `copy` flag is true, after the drug‑era derivation has finished.  
  * `copy_era_dcc` (also in this module) which is a thin wrapper used when only the copy step is required. Both callers supply the same three arguments (`conn_str`, `era_type`, `schema`) and rely on the boolean return to decide whether to continue processing.  
- **Callees** – The function directly uses two utilities from the codebase:  
  * `Statement` (from `pedsnetdcc.db`) to encapsulate and run the INSERT statement.  
  * `check_stmt_err` (from `pedsnetdcc.utils`) to validate execution and raise a consistent `DatabaseError` on failure. No other functions are called.

**Note**:  
- The INSERT uses `ON CONFLICT DO NOTHING`; therefore rows that would violate a unique constraint are silently ignored, which is intentional for idempotent copying.  
- The function does not perform any schema validation; it assumes that the source schema contains a table named `drug_era` with the exact column list required by the target table.  
- All logging is performed through the `Statement` object’s `msg` attribute; callers should ensure that their logging configuration captures DEBUG/INFO messages if they need detailed trace output.  
- If a database error occurs, `check_stmt_err` raises `DatabaseError`; callers must be prepared to handle this exception or allow it to propagate.  
- The function always returns `True` on success; it never returns `False` because any failure results in an exception rather than a false return.

**Output Example**:  
```python
# Example usage
success = _copy_to_dcc_table(conn_str, "drug", "site123")
print(success)   # -> True
```
## FunctionDef run_r_drug_era(conn_str, site, copy, neg_ids, search_path, password, model_version, id_name, notable, noids, nopk, novac, size, start, notrunc, test)
**run_r_drug_era**: The function of run_r_drug_era is to orchestrate the full drug‑era derivation workflow, including SQL preparation, execution of an R script, ID assignment, optional copying to the central dcc_pedsnet schema, primary‑key creation, and table vacuuming.

**parameters**:  
· conn_str (str): PostgreSQL libpq connection string used for all database operations.  
· site (str): Identifier of the site for which the derivation is being run; inserted into R scripts and log messages.  
· copy (bool): If True, the resulting drug_era rows are copied into the central dcc_pedsnet.drug_era table.  
· neg_ids (bool): If True, a sequence that generates negative identifiers is created for the era IDs; otherwise a normal positive sequence is used.  
· search_path (str): PostgreSQL schema search_path; the first schema is used as the primary schema for DDL statements.  
· password (str): Database password; if None the password is extracted from conn_str.  
· model_version (str): PEDSnet data‑model version (e.g., “2.3.0”) used to load model metadata.  
· id_name (str): Prefix for the last‑id tracking table (e.g., “dcc” or “onco”).  
· notable (bool, default False): When False the function drops existing primary‑key and null constraints, truncates the target table, and re‑runs the R derivation; when True those steps are skipped.  
· noids (bool, default False): When True the ID‑assignment step is omitted.  
· nopk (bool, default False): When True the primary‑key addition step is omitted.  
· novac (bool, default False): When True the VACUUM ANALYZE step is omitted.  
· size (str, default '5000'): Number of persons to process per batch; converted to int before being written into the R driver script.  
· start (str, default '0'): Person identifier from which processing should begin; converted to int before being written into the R driver script.  
· notrunc (bool, default False): When True the target drug_era table is not truncated before the derivation.  
· test (bool, default False): When True the function uses the “drug_era_test” R package instead of the production “drug_era” package.

**Code Description**:  
1. **Derivation type and package selection** – The function fixes `era_type` to “drug”. If `test` is True the R package directory is set to *drug_era_test*; otherwise it is *drug_era*. A temporary Argos JSON configuration filename is built from the site and package names.  

2. **Password handling** – If `password` is None the function extracts the password from `conn_str` using a regular‑expression search for “password=…”.  

3. **Connection info and logging setup** – `get_conn_info_dict` parses `conn_str` into a dictionary of connection components. A log dictionary combining the site identifier and connection info is created with `combine_dicts`. The start of the derivation is logged and the current epoch time is stored in `start_time`. The primary schema is obtained from `search_path` via `primary_schema`.  

4. **StatementSet preparation** – An empty `StatementSet` (`stmts`) is created to hold DDL statements that will be executed sequentially (not in parallel).  

5. **Conditional preparation when `notable` is False**  
   * **Drop primary‑key constraint** – A `Statement` containing `DROP_PK_CONSTRAINT_ERA_SQL` (formatted with `era_type`) is added to `stmts` and executed. Errors are caught, logged, and re‑raised.  
   * **Drop NOT NULL constraint** – A similar `Statement` with `DROP_NULL_ERA_SQL` is executed.  
   * **Truncate target table** – Unless `notrunc` is True, a `TRUNCATE_ERA_SQL` statement (formatted with the primary schema) is executed.  
   * **Run the R derivation** –  
     - The base R package directory (`/app/<package>`) is copied to a site‑specific directory (`/app/<package>/<site>`); any previous copy is removed first.  
     - `_create_argos_file` writes a temporary Argos JSON file containing the connection parameters, schema, and password.  
     - `_fix_site_info` replaces `<SITE>` and `<SCHEMA>` placeholders in `site_info.R`.  
     - `_fix_run` replaces `<SITE>` in `run.R`.  
     - `_fix_size` and `_fix_start` replace `<SIZE>`, `<SIZEMINUS1>`, and `<START>` placeholders in `driver.R` with the integer values derived from the `size` and `start` arguments.  
     - The fully prepared R script `run.R` is executed with `Rscript`, passing `--verbose=1` and running in the `/app` working directory. Completion of the R script is logged with elapsed time.  

6. **ID assignment** – If `noids` is False, `_add_era_ids` is called with the parameters (`era_type='drug'`, `conn_str`, `site`, `neg_ids`, `search_path`, `model_version`, `id_name`). This helper counts rows lacking IDs, reserves a block of IDs in the appropriate last‑id tracking table, creates (or re‑creates) the sequence, sets its start value, and updates the drug_era table with new IDs. Failure in any sub‑step raises an exception; on success the function proceeds.  

7. **Copy to central schema** – If `copy` is True, the function logs the copy action and invokes `_copy_to_dcc_table(conn_str, era_type, schema)`. The helper builds an INSERT … SELECT statement that copies all rows from `<schema>.drug_era` into `dcc_pedsnet.drug_era` using `ON CONFLICT DO NOTHING`. Errors are checked with `check_stmt_err`.  

8. **Primary‑key creation** – If `nopk` is False, `_add_primary_key(era_type, conn_str, schema)` is called. This builds an `ALTER TABLE … ADD PRIMARY KEY` statement for the `<schema>.drug_era` table and executes it, checking for errors.  

9. **Vacuum / analyze** – If `novac` is False, the function logs the start of vacuuming and calls `vacuum(conn_str, model_version, analyze=True, tables=['drug_era'])`. This runs `VACUUM ANALYZE drug_era` on the target schema (or the default schema derived from `search_path`). Completion is logged with elapsed time.  

10. **Final logging and return** – The function logs the overall completion message with total elapsed time and returns `True`. Any exception raised by the helper utilities (`check_stmt_err`, `DatabaseError`, or unexpected errors) propagates out of the function, causing the caller to treat the run as a failure.

**Reference relationships**  
*Callers*: The primary entry point is `run_r_drug_era_test` in `pedsnetdcc/main.py`, which builds the connection string, obtains the password (optionally via a prompt), and forwards all command‑line arguments to `run_r_drug_era`.  

*Callees*:  
- `primary_schema` (pedsnetdcc/schema.py) – determines the primary schema from `search_path`.  
- `get_conn_info_dict` and `combine_dicts` (pedsnetdcc/utils.py) – provide connection metadata for logging.  
- `Statement` and `StatementSet` (pedsnetdcc/db.py) – encapsulate and execute the DDL statements.  
- `_create_argos_file`, `_fix_site_info`, `_fix_run`, `_fix_size`, `_fix_start` (same module) – prepare the site‑specific R package and configuration.  
- `Rscript` (external command wrapper) – runs the generated R script.  
- `_add_era_ids` (same module) – assigns unique era identifiers.  
- `_copy_to_dcc_table` (same module) – copies rows into the central dcc_pedsnet table.  
- `_add_primary_key` (same module) – adds the primary‑key constraint.  
- `vacuum` (pedsnetdcc/utils.py) – performs VACUUM ANALYZE on the output table.  
- `check_stmt_err` (pedsnetdcc/utils.py) – validates execution of each `Statement`.  

**Note**:  
- The function assumes that the R package directory structure (`site_info.R`, `run.R`, `code/driver.R`) contains the exact placeholder tokens `<SITE>`, `<SCHEMA>`, `<SIZE>`, `<SIZEMINUS1>`, and `<START>`. Missing placeholders will result in unchanged files and potential R‑script failures.  
- All DDL statements are executed with autocommit; therefore they cannot be rolled back automatically. Errors are logged and raised immediately.  
- The `size` and `start` arguments are accepted as strings for CLI compatibility but are converted to integers before being written into the R driver script. Supplying non‑numeric values will raise a `ValueError` during conversion.  
- The `notable` flag bypasses the entire DDL preparation and R‑script execution; it is intended for cases where the derivation has already been run and only post‑processing steps (IDs, copy, PK, vacuum) are required.  
- The `copy` step uses `ON CONFLICT DO NOTHING`; duplicate primary‑key values are silently ignored, making the copy operation idempotent.  
- The function returns a simple boolean (`True` on success, `False` only when `_add_era_ids` explicitly returns `False`). Any database‑level error raises `DatabaseError` and prevents a return value.  

**Output Example**:  
```python
# Example call from a CLI wrapper
success = run_r_drug_era(
    conn_str="host=pg host=localhost dbname=pedsnet user=loader password=secret",
    site="umich",
    copy=True,
    neg_ids=False,
    search_path="dcc_pedsnet, public, vocabulary",
    password=None,
    model_version="2.3.0",
    id_name="dcc",
    notable=False,
    noids=False,
    nopk=False,
    novac=False,
    size='5000',
    start='0',
    notrunc=False,
    test=False
)

print(success)   # -> True (if all steps complete without error)
```
## FunctionDef _add_primary_key(era_type, conn_str, schema)
**_add_primary_key**: The function of _add_primary_key is to add a primary‑key constraint on the `<era_type>_era_id` column of the `<era_type>_era` table in the specified schema.

**parameters**:  
· era_type (str): the name of the era derivation (e.g., `"drug"` or `"condition"`); it is used to build the target table name.  
· conn_str (str): a libpq‑style PostgreSQL connection string that identifies the database to which the ALTER TABLE statement will be sent.  
· schema (str): the PostgreSQL schema that contains the `<era_type>_era` table; it is interpolated into the ALTER TABLE command.

**Code Description**:  
1. **SQL template preparation** – Two format strings are defined:  
   * `pk_era_id_sql` = `"alter table {0}.{1}_era add primary key ({2}_era_id)"` – placeholders correspond to `schema`, `era_type`, and a temporary copy of `era_type`.  
   * `pk_era_id_msg` = `"making {0}_era_id the priomary key"` – a human‑readable log message (note the typo “priomary” is retained from the source).  
   The variable `temp_era_type` is assigned the value of `era_type` solely to keep the original string unchanged for formatting.

2. **Logging start of operation** – A log entry with the message `'begin add primary key'` is emitted via the module‑level `logger`. This marks the beginning of the DDL operation.

3. **Statement construction** – An instance of `pedsnetdcc.db.Statement` is created:  
   * `sql` argument is the formatted `pk_era_id_sql` where `{0}` → `schema`, `{1}` → `era_type`, `{2}` → `temp_era_type`.  
   * `msg` argument is the formatted `pk_era_id_msg` where `{0}` → `temp_era_type`.  
   The resulting `Statement` object encapsulates the ALTER TABLE command and a descriptive message.

4. **Execution of the ALTER TABLE** – The statement’s `execute` method is called with `conn_str`. This method opens a new PostgreSQL connection, runs the SQL, and stores any exception in `stmt.err`. Because the command is DDL, no result rows are expected; only the success of the execution matters.

5. **Error checking** – Immediately after execution, the helper `pedsnetdcc.utils.check_stmt_err` is invoked with the statement object and a context string `'make {era_type}_era_id the primary key'`.  
   * If `stmt.err` is not `None`, `check_stmt_err` logs the error and raises a `DatabaseError`, causing the calling function to abort.  
   * If no error is present, execution continues.

6. **Logging completion** – Upon successful execution, a log entry with the message `'primary key created'` is emitted.

7. **Return value** – The function returns the boolean `True` to indicate that the primary‑key addition succeeded without raising an exception.

**Reference relationships**:  
* **Caller** – The only direct caller in the codebase is `run_r_drug_era` (in the same module). After optional steps such as copying data and adding IDs, `run_r_drug_era` invokes `_add_primary_key` when the `nopk` flag is not set. The caller relies on the boolean return value to confirm success; any failure propagates as an exception from `check_stmt_err`.  
* **Callee** – `_add_primary_key` creates and executes a `Statement` object and then calls `check_stmt_err`. Both of these utilities are defined in `pedsnetdcc/db.py` and `pedsnetdcc/utils.py` respectively. No other functions are called from within `_add_primary_key`.

**Note**:  
- The function assumes that the target table `<era_type>_era` already exists in the given schema; otherwise the ALTER TABLE will fail and `check_stmt_err` will raise an exception.  
- The primary‑key column name is constructed as `<era_type>_era_id`. If the column does not exist or already has a primary‑key constraint, the statement will error. Callers typically guard against this by using the `nopk` flag or by dropping existing constraints earlier in the workflow.  
- Logging uses the module‑level `logger`; the message for the primary‑key creation contains a typo (“priomary”). This does not affect functionality but appears in log output.  
- The function does not commit or rollback explicitly; the `Statement.execute` method sets the connection to autocommit, so the ALTER TABLE is applied immediately.  
- Because the function returns only `True`, callers should not expect any additional data (e.g., row counts). All error handling is performed via exceptions.

**Output Example**:  
```python
result = _add_primary_key('drug', conn_str, 'dcc_pedsnet')
print(result)   # -> True
```
## FunctionDef _add_era_ids(era_type, conn_str, site, neg_ids, search_path, model_version, id_name)
**_add_era_ids**: The function of _add_era_ids is to assign unique identifiers to rows in a condition or drug era table, updating the corresponding last‑id tracking table, creating or resetting the appropriate PostgreSQL sequence, and finally making the era‑id column the primary key.

**parameters**:  
· era_type (str): type of era derivation; accepted values are “condition” or “drug”.  
· conn_str (str): libpq‑style PostgreSQL connection string used for all database operations.  
· site (str): site identifier for which the derivation is being run; incorporated into log messages.  
· neg_ids (bool): when True, creates a sequence that generates negative identifiers; otherwise creates a standard positive sequence.  
· search_path (str): PostgreSQL schema search_path; the first schema in the list is used as the primary schema for SQL statements.  
· model_version (str): PEDSnet data‑model version (e.g., “2.3.0”) used to retrieve the stock SQLAlchemy metadata.  
· id_name (str): prefix used for the last‑id tracking table (e.g., “dcc” or “onco”).

**Code Description**:  

1. **Preparation and logging** – The function extracts connection information with `get_conn_info_dict`, merges it with the site identifier via `combine_dicts`, and logs the start of the ID assignment step. The start time is recorded for elapsed‑time reporting.

2. **Schema and table identification** – `primary_schema(search_path)` returns the first schema in the search_path; this schema is stored in `schema`. The target table name is built as `{era_type}_era`. Temporary variables `temp_table_name` and `temp_era_type` keep the original values for later formatting.

3. **Metadata loading** – `stock_metadata(model_version)` obtains a SQLAlchemy `MetaData` object that contains table definitions for the requested model version. The table object for the era table is retrieved (`metadata.tables[temp_table_name]`) but is not directly used later; it ensures the table exists in the model.

4. **Template variable map** – A dictionary `tpl_vars` is created to hold values needed for SQL string formatting. It initially contains `table_name`. The name of the last‑id tracking table is derived from the `id_name` pattern `{id_name}_{table_name}_id` and added to `tpl_vars`.

5. **Count new IDs needed** – A `Statement` (`new_id_count_stmt`) is built with SQL that counts rows where the era‑id column is NULL. The statement is executed, and helper functions `check_stmt_err` and `check_stmt_data` verify that execution succeeded and that a result row was returned. The count is stored in `tpl_vars['new_id_count']` and logged.

6. **Reserve a block of IDs** – Two statements are placed in a `StatementList` (`update_last_id_stmts`):
   - `LOCK {last_id_table_name}` to obtain an exclusive lock on the tracking table.
   - `UPDATE {last_id_table_name} SET last_id = last_id + {new_id_count} RETURNING old.last_id, new.last_id` to increment the stored last‑id by the number of IDs required.
   The list is executed with `serial_execute(conn_str, transaction=True)`, guaranteeing that the lock and update occur atomically. After execution, the old and new last‑id values are extracted from the second statement’s result and stored in `tpl_vars['old_last_id']` and `tpl_vars['new_last_id']`. These values are logged.

7. **Create the era‑id sequence** – Depending on `neg_ids`, either a normal sequence (`create_seq_sql`) or a negative‑only sequence (`create_neg_seq_sql`) is created with `CREATE SEQUENCE IF NOT EXISTS`. The statement is executed and any error is reported via `check_stmt_err`.

8. **Set the sequence start value** – The sequence is altered to restart at the previously saved `old_last_id` using `ALTER SEQUENCE … RESTART WITH {old_last_id}`. Execution is checked for errors.

9. **Assign era IDs** – An `UPDATE` statement assigns a new identifier to each row that still has a NULL era‑id by calling `nextval` on the newly created sequence. The statement is executed and errors are checked.

10. **Final logging and return** – The function logs the completion of the ID‑assignment step together with the elapsed time and returns `True` to indicate success.

**Reference relationships**:  

*Callers* – The only direct caller in the project is `run_r_drug_era` (in `pedsnetdcc/r_drug_era.py`). After the R‑script derivation finishes, `run_r_drug_era` invokes `_add_era_ids` (with `era_type='drug'`) unless the `noids` flag is set. The caller supplies all required arguments, including the site, connection string, and model version, and expects a boolean return indicating success.

*Callees* – Within `_add_era_ids` the following helper objects are used:
- `get_conn_info_dict` and `combine_dicts` for building log dictionaries.
- `primary_schema` to obtain the primary schema name.
- `stock_metadata` to load the model metadata.
- `Statement`, `StatementList`, and their methods `execute`, `execute_on_conn`, `serial_execute` for all SQL execution.
- `check_stmt_err` and `check_stmt_data` for error and data validation.
- `secs_since` for elapsed‑time calculation.
- Logging is performed through the module‑level `logger`.

All database interactions are performed via the `Statement` abstraction, ensuring consistent error handling and logging across the code base.

**Note**:  
- The function assumes that the era table and the corresponding last‑id tracking table already exist in the target schema. If either table is missing, the SQL statements will raise a `DatabaseError` caught by `check_stmt_err`.  
- The sequence name follows the pattern `{schema}.{site}_{era_type}_era_id_seq`. When `neg_ids` is True the sequence is created with `INCREMENT 1 START -2147483647 MINVALUE -2147483647 MAXVALUE 0`; otherwise it uses default PostgreSQL settings.  
- The function does not modify the primary‑key constraint; that step is performed later by `_add_primary_key`.  
- All statements are executed with autocommit; the block that updates the last‑id tracking table runs inside a single transaction (`transaction=True`) to avoid race conditions when multiple processes allocate IDs concurrently.  
- If any of the helper checks (`check_stmt_err` or `check_stmt_data`) raise an exception, the function will not reach the final return statement; the exception propagates to the caller.

**Output Example**:  
When called successfully, the function returns a boolean value:

```python
result = _add_era_ids('drug', conn_str, 'stlouis', False,
                      'dcc_pedsnet, public', '2.3.0', 'dcc')
# result == True
```
## FunctionDef copy_era_dcc(era_type, conn_str, site, search_path)
**copy_era_dcc**: The function of copy_era_dcc is to copy condition‑ or drug‑era rows from a site‑specific schema into the central `dcc_pedsnet` era table and report success.

**parameters**: The parameters of this Function.  
· **era_type** (str): type of derivation to copy; accepted values are `"condition"` or `"drug"`.  
· **conn_str** (str): PostgreSQL libpq connection string used to open a database connection.  
· **site** (str): identifier of the site whose era data are being copied; used only for logging.  
· **search_path** (str): PostgreSQL schema search_path string; the first schema in the list is treated as the source schema for the copy.

**Code Description**:  
1. **Parse connection information** – The function calls `get_conn_info_dict(conn_str)` to extract user, host, port, dbname and search_path from the connection string. The resulting dictionary is later merged into log messages.  
2. **Log start of the copy operation** – A log dictionary is built with `combine_dicts({'site': site}, conn_info_dict)`. The message “start copying {era_type} era entries” together with the log dictionary is emitted via `logger.info`. The current epoch time is stored in `start_time` for later elapsed‑time reporting.  
3. **Determine source schema** – `primary_schema(search_path)` extracts the first schema name from the supplied `search_path`. This schema is the one that contains the temporary `{era_type}_era` table to be copied.  
4. **Perform the copy** – An informational log entry “copy {era_type}_era to dcc_pedsnet” is written. The helper `_copy_to_dcc_table(conn_str, era_type, schema)` is invoked.  
   - `_copy_to_dcc_table` builds an INSERT … SELECT statement that copies all rows from `{schema}.{era_type}_era` into `dcc_pedsnet.{era_type}_era`, using `ON CONFLICT DO NOTHING` to make the operation idempotent.  
   - It executes the statement with a `Statement` object, checks for errors via `check_stmt_err`, and returns `True` on success.  
5. **Handle copy result** – If `_copy_to_dcc_table` returns a falsy value (which can only happen if the helper is modified to return `False`), `copy_era_dcc` returns `False` immediately, aborting further logging. In the current implementation the helper always returns `True` or raises an exception, so execution normally proceeds.  
6. **Log successful copy** – After the helper returns, a log entry “{era_type}_era copied to dcc_pedsnet” is emitted.  
7. **Log completion with elapsed time** – The elapsed seconds are calculated with `secs_since(start_time)`, which returns a zero‑padded four‑character string. A final log message “finished copying {era_type} era entries” together with the elapsed time and the previously built log dictionary is written.  
8. **Return success flag** – If no exception was raised, the function returns `True` to indicate that the copy completed successfully.

**Note**:  
- The function does not perform any validation of `era_type`; passing an unsupported value will cause the underlying SQL in `_copy_to_dcc_table` to reference a non‑existent table and raise a `DatabaseError`.  
- All database errors are propagated as `DatabaseError` exceptions from `_copy_to_dcc_table`; callers must be prepared to catch them if they need graceful degradation.  
- Logging relies on a globally configured `logger`; if the logger’s level is higher than INFO, the start, progress, and finish messages will not appear.  
- The elapsed‑time string from `secs_since` is always at least four characters; for durations longer than 9999 seconds the full number is returned without truncation.  
- The function assumes that the source schema (the first entry in `search_path`) already contains a table named `{era_type}_era` with a column layout matching the target `dcc_pedsnet` table.

**Output Example**:  
```python
result = copy_era_dcc('drug', conn_str, 'stlouis', 'dcc_pedsnet, public')
print(result)   # -> True
```
