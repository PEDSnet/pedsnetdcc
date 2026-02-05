## FunctionDef _update_drug_exposure(conn_str, schema)
**_update_drug_exposure**: The function of _update_drug_exposure is to update the `drug_exposure` table with derived dose information (dose unit concept id, effective drug dose, and dose unit concept name) that were previously calculated and stored in the `drug_exposures_mgkg_derivations` table.

**parameters**:  
· `conn_str` (str): PostgreSQL connection string used to create a database connection for executing the update statement.  
· `schema` (str): Name of the target schema that contains the `drug_exposure` and `drug_exposures_mgkg_derivations` tables; it is interpolated into the SQL command.

**Code Description**:  
1. **SQL command construction** – A multi‑line SQL string (`update_drug_exposure_sql`) is defined. The statement performs an `UPDATE` on `{schema}.drug_exposure` (aliased as `de`). It sets three columns (`dose_unit_concept_id`, `effective_drug_dose`, `dose_unit_concept_name`) to the corresponding values taken from a derived sub‑query (`de_dev`). The sub‑query selects `drug_exposure_id`, `person_id`, `dose_unit_concept_id`, `effective_drug_dose`, and `dose_unit_concept_name` from `{schema}.drug_exposures_mgkg_derivations`. The `WHERE` clause matches rows on both `person_id` and `drug_exposure_id` to ensure the correct exposure record is updated.

2. **Message definition** – `update_drug_exposure_msg` holds a short human‑readable description (“updating drug_exposure”) that will be attached to the `Statement` object for logging purposes.

3. **Statement creation** – An instance of `Statement` (from `pedsnetdcc.db`) is created with the formatted SQL (`update_drug_exposure_sql.format(schema)`) and the message. The `Statement` object encapsulates the SQL, the description, and later stores execution results, row count, and any error.

4. **Execution** – The `execute` method of the `Statement` instance is called with `conn_str`. This method opens a new database connection, runs the SQL, captures the number of rows affected, and records any exception in the `err` attribute.

5. **Error handling** – Immediately after execution, the helper function `check_stmt_err` is invoked with the statement object and the context string `'update drug_exposure'`. `check_stmt_err` inspects `stmt.err`; if an error is present, it logs the failure and raises a `DatabaseError`. This ensures that any problem during the update aborts the calling workflow.

6. **Return value** – If execution completes without raising an exception, the function returns `True` to signal successful completion.

**Reference relationships**:  
- **Caller** – The function is invoked by `run_r_dose` (in `pedsnetdcc/r_dose.py`) when the `copy` flag is set. After the R script finishes, `run_r_dose` calls `_update_drug_exposure` to propagate the derived dose values back into the core `drug_exposure` table.  
- **Callee** – Internally the function relies on the `Statement` class for SQL execution and on `check_stmt_err` for standardized error detection and logging. No other project modules are directly called.

**Note**:  
- The function assumes that the `drug_exposures_mgkg_derivations` table already contains the derived columns for every `drug_exposure_id` that needs updating. Missing rows will simply leave the corresponding `drug_exposure` records unchanged.  
- Because the update runs in autocommit mode (handled by `Statement.execute`), the changes are permanent as soon as the statement succeeds; there is no transaction rollback within this function.  
- The function does not return the number of rows updated; callers must rely on the boolean success indicator or inspect the `Statement` object manually if needed.  
- Any exception raised by `check_stmt_err` propagates as a `DatabaseError`; callers should be prepared to catch this exception if they need custom recovery logic.

**Output Example**:  
```python
# Example usage
success = _update_drug_exposure(conn_str="dbname=ccd user=loader", schema="dcc_pedsnet")
print(success)   # -> True  (indicates the UPDATE completed without error)
```
## FunctionDef _create_argos_file(config_path, config_file, schema, password, conn_info_dict)
**_create_argos_file**: The function of _create_argos_file is to generate a temporary Argos JSON configuration file that describes how the R script should connect to the PostgreSQL database.

**parameters**: The parameters of this Function.  
· **config_path**: The directory path where the Argos configuration file will be created. It is typically the site‑specific folder prepared by `run_r_dose`.  
· **config_file**: The name of the JSON file to be written (e.g., “site_dose_derivations_argos_temp.json”).  
· **schema**: The primary PostgreSQL schema that should be placed in the `search_path` option of the Argos configuration.  
· **password**: The password for the database user; it is inserted verbatim into the JSON file.  
· **conn_info_dict**: A dictionary containing connection details extracted from the original connection string. Expected keys are `'host'`, `'dbname'`, and `'user'`.

**Code Description**:  
The function opens a new file at `os.path.join(config_path, config_file)` in binary write mode. It then writes a well‑formed JSON object line‑by‑line, using `os.linesep` to ensure platform‑appropriate line endings. The JSON structure is:

```
{
  "src_name": "Postgres",
  "src_args": {
    "host": "<host from conn_info_dict>",
    "port": 5432,
    "dbname": "<dbname from conn_info_dict>",
    "user": "<user from conn_info_dict>",
    "password": "<password argument>",
    "bigint": "numeric",
    "options": "-c search_path=<schema>,vocabulary"
  }
}
```

Key points of the implementation:  

1. **Hard‑coded port** – The PostgreSQL port is fixed at `5432`; it is not taken from `conn_info_dict`.  
2. **Data type mapping** – The `"bigint"` field is set to `"numeric"` to instruct Argos how to treat PostgreSQL `bigint` columns.  
3. **Search path option** – The `"options"` field builds a PostgreSQL command‑line option that sets the `search_path` to the supplied `schema` followed by the `vocabulary` schema, ensuring the R script sees the correct tables.  
4. **Binary mode** – The file is opened with `'wb'`. Although the content is textual, writing in binary mode works because Python automatically encodes the string literals to bytes.  
5. **Overwrite behavior** – Each call creates a fresh file, overwriting any existing file with the same name.

**Relationship with callers**:  
`_create_argos_file` is invoked exclusively by `run_r_dose` in `pedsnetdcc/r_dose.py`. Within `run_r_dose`, a site‑specific directory is prepared, the Argos configuration file is generated by this function, and then the R script (`run.R`) is executed. The generated JSON file is consumed by the R script (via the Argos library) to establish a database connection using the supplied credentials and schema. Consequently, the correctness of the JSON format and the values written by `_create_argos_file` directly affect the ability of the R dose derivation script to run successfully.

**Note**:  
- Ensure that `config_path` exists and is writable before calling the function; otherwise a `FileNotFoundError` or permission error will be raised.  
- The function does not perform validation on the contents of `conn_info_dict`; missing keys will cause a `TypeError` when `None` is concatenated to the JSON strings.  
- Because the port is hard‑coded, the function will fail for PostgreSQL instances running on non‑standard ports unless the code is modified.  
- The generated file is temporary; callers should delete or overwrite it as needed to avoid stale credentials.
## FunctionDef _fix_site_info(file_path, site, schema)
**_fix_site_info**: The function of _fix_site_info is to replace placeholder tokens in the site‑specific R script file *site_info.R* with the actual site name and schema, and to write the updated content back to the file.

**parameters**:  
· **file_path**: The absolute path to the directory that contains the *site* sub‑directory where *site_info.R* resides.  
· **site**: The site identifier string that should replace the `<SITE>` placeholder in the file.  
· **schema**: The PostgreSQL schema name string that should replace the `<SCHEMA>` placeholder in the file.

**Code Description**:  
1. The function builds the full path to *site_info.R* by joining `file_path`, the literal folder name `'site'`, and the filename `'site_info.R'`.  
2. It attempts to open this file in read mode. If the file cannot be opened (for example, because the file does not exist in the current query package), the `except` block is triggered, the function returns `False`, and no further processing occurs.  
3. When the file is successfully read, its entire contents are stored in the variable `file_data`.  
4. Two string replacements are performed on `file_data`:  
   * All occurrences of the token `<SITE>` are replaced with the value of the `site` argument.  
   * All occurrences of the token `<SCHEMA>` are replaced with the value of the `schema` argument.  
5. The modified content is then written back to the same *site_info.R* file, overwriting the original file.  
6. If the read‑modify‑write sequence completes without raising an exception, the function returns `True` to indicate success.

**Relationship with callers**:  
`_fix_site_info` is invoked by the `run_r_dose` function in *pedsnetdcc/r_dose.py*. Within `run_r_dose`, a site‑specific copy of the R dose derivation package is created under a temporary directory. After the Argos configuration file is generated, `run_r_dose` calls `_fix_site_info` (followed by `_fix_run`) to inject the concrete site name and schema into the R scripts before executing them with `Rscript`. Thus, `_fix_site_info` prepares the R environment so that the subsequent R script can reference the correct database schema and site identifier.

**Note**:  
- The function uses a bare `except:` clause, which suppresses all exceptions and treats any failure (including I/O errors, permission issues, or unexpected file content) as a missing file scenario, returning `False`.  
- The function assumes that the placeholders `<SITE>` and `<SCHEMA>` appear exactly as written in the file; any deviation will not be replaced.  
- Because the file is overwritten in place, any prior custom modifications to *site_info.R* will be lost after this function runs.

**Output Example**:  
```python
result = _fix_site_info('/app/dose_derivations', 'umich', 'public')
# result will be True if the file was found and updated,
# or False if the file does not exist or an error occurred.
```
## FunctionDef _fix_run(file_path, site)
**_fix_run**: The function of _fix_run is to insert the actual site identifier into the R script template (run.R) used for dose derivations.

**parameters**:  
· file_path: a string representing the root directory of the dose derivations package on the filesystem.  
· site: a string containing the site name that should replace the placeholder `<SITE>` in the R script.

**Code Description**:  
The function builds the path to the file *run.R* located in the sub‑directory `site` under the supplied `file_path`. It opens this file in read mode, reads its entire contents into memory, and then performs a simple string substitution: every occurrence of the literal token `<SITE>` is replaced with the value of the `site` argument. After the replacement, the function re‑opens the same file in write mode and writes the modified content back, overwriting the original template.  

If any step raises an exception—most commonly because the expected file does not exist—the function catches the exception silently and returns `False`, indicating that the modification could not be performed. When the operation succeeds without error, the function returns `True`.

In the broader project, `_fix_run` is invoked by `run_r_dose` (found in *pedsnetdcc/r_dose.py*). After `run_r_dose` copies the generic dose derivations package to a site‑specific directory and creates the Argos configuration file, it calls `_fix_site_info` and then `_fix_run`. The purpose of this call chain is to prepare a fully customized R environment for the given site before the R script is executed via the `Rscript` command. By ensuring that `run.R` contains the correct site name, `_fix_run` enables the downstream R code to locate site‑specific resources and to embed the site identifier in any generated output.

**Note**:  
- The function uses a bare `except:` clause, which suppresses all exceptions, including unrelated I/O errors; callers should interpret a `False` return as “file not found or could not be processed.”  
- The placeholder `<SITE>` must exist in the original `run.R`; otherwise the file will be rewritten unchanged but still return `True`.  
- The caller must have write permission for the target file; otherwise the function will return `False`.  
- No validation of the `site` string is performed; it is inserted verbatim.

**Output Example**:  
When the file exists and is successfully updated: `True`  
When the file is missing or an error occurs: `False`
## FunctionDef run_r_dose(conn_str, site, password, search_path, model_version, copy, limit, owner)
**run_r_dose**: The function of run_r_dose is to prepare a site‑specific R dose‑derivation package, execute the associated R script, optionally copy the derived results back into the core `drug_exposure` table, grant appropriate permissions, and vacuum the affected tables.

**parameters**:  
· **conn_str** (str): PostgreSQL connection string used for all database operations.  
· **site** (str): Identifier of the site for which the dose derivations are being run.  
· **password** (str): Password for the database user; if `None` the password is extracted from `conn_str`.  
· **search_path** (str): PostgreSQL schema search path; the first schema is used as the primary schema.  
· **model_version** (str): PEDSnet data‑model version (e.g., “2.3.0”) used for vacuuming tables later.  
· **copy** (bool): When `True` the function updates the core `drug_exposure` table with the derived dose values.  
· **limit** (bool, default False): When `True` restricts permission grants to a single user (`owner`).  
· **owner** (str, default 'loading_user'): Database user that receives permissions when `limit` is `True`.

**Code Description**:  

1. **Initial setup**  
   * The R package name is fixed to `'dose_derivations'`.  
   * A temporary Argos JSON configuration file name is built as `<site>_dose_derivations_argos_temp.json`.  
   * `get_conn_info_dict` parses `conn_str` into a dictionary (`conn_info_dict`) for logging.  
   * Logging of the start of the function is performed with a combined dictionary containing the site and connection info; the start time is recorded.

2. **Determine primary schema**  
   * `primary_schema(search_path)` extracts the first schema from the supplied `search_path`. This schema is used for all subsequent SQL statements and for the Argos configuration.

3. **Password handling**  
   * If `password` is `None`, a regular‑expression search extracts the password from `conn_str`.

4. **Prepare site‑specific R package directory**  
   * `source_path` points to `/app/dose_derivations`.  
   * `dest_path` is `/app/dose_derivations/<site>`.  
   * Any existing directory at `dest_path` is removed (`shutil.rmtree`).  
   * The whole package directory is copied to the site‑specific location (`shutil.copytree`).  

5. **Create Argos configuration**  
   * `_create_argos_file` writes a JSON file (`config_file`) inside `dest_path` that contains the host, dbname, user, password, and a `search_path` option pointing to the primary schema plus the `vocabulary` schema. This file is consumed by the R script to connect to the database.

6. **Inject site‑specific values into R scripts**  
   * `_fix_site_info` replaces the placeholders `<SITE>` and `<SCHEMA>` in `site_info.R` with the actual `site` and `schema`.  
   * `_fix_run` replaces `<SITE>` in `run.R` with the actual `site`. Both functions return `True` on success; failures are ignored because the script will later fail if the files are missing.

7. **Execute the R script**  
   * The full path to the R entry point is constructed as `/app/dose_derivations/<site>/site/run.R`.  
   * `Rscript` is invoked with `--verbose=1`, working directory `/app`, and foreground execution (`_fg=True`). The R script performs the dose derivations and writes results into the temporary table `drug_exposures_mgkg_derivations`.

8. **Optional copy of results**  
   * If `copy` is `True`, `_update_drug_exposure` is called. This function builds an `UPDATE` statement that copies the derived columns (`dose_unit_concept_id`, `effective_drug_dose`, `dose_unit_concept_name`) from `drug_exposures_mgkg_derivations` into the core `drug_exposure` table, executes it via a `Statement`, and checks for errors with `check_stmt_err`. Failure aborts the workflow; success returns `True`.

9. **Grant permissions**  
   * A `StatementSet` (`stmts`) is created to hold GRANT statements.  
   * If `limit` is `True`, only the user specified by `owner` receives permissions; otherwise both `peds_staff` and `dcc_analytics` are granted.  
   * Two GRANT SQL templates (`GRANT_OBS_MGKG_DERIVATION_SQL` and `GRANT_OBS_MGKG_METADATA_SQL`) are formatted with the schema and each target user, wrapped in `Statement` objects, and added to the set.  
   * Each statement is executed sequentially (no parallel execution is used here). Errors are caught, logged, and re‑raised, ensuring that permission failures abort the function.

10. **Vacuum the derivation tables**  
    * `vacuum` is called with `model_version`, `analyze=True`, and an explicit list of the two tables created by the R script: `drug_exposures_mgkg_derivations` and `drug_exposures_mgkg_metadata`. This refreshes statistics and reclaims space.

11. **Final logging and return**  
    * Completion of the entire process is logged with elapsed time.  
    * The function returns `True` to indicate successful execution; any exception raised earlier prevents reaching this point.

**Note**:  
- The function assumes that the R package `dose_derivations` exists under `/app` and contains the expected template files (`site_info.R`, `run.R`). Missing files cause the corresponding fix functions to return `False`, but the function does not abort; the subsequent R execution will fail if the placeholders are not replaced.  
- Password extraction via regex works only when the password appears as `password=...` in the connection string.  
- Permission granting uses two hard‑coded GRANT SQL templates; they must be defined elsewhere in the module.  
- The vacuum step operates with `analyze=True`; this may increase runtime but improves query planning for the newly populated tables.  
- All database interactions are performed with autocommit mode via the `Statement` class, so each statement is committed immediately.  
- Errors from the R script itself are not captured by Python; they will appear in the R process’s stdout/stderr and cause `Rscript` to raise an exception, which propagates out of `run_r_dose`.  

**Output Example**:  
```python
result = run_r_dose(
    conn_str="host=db.example.com port=5432 dbname=ccd user=loader password=secret options='-c search_path=dcc_pedsnet'",
    site="umich",
    password=None,
    search_path="dcc_pedsnet, public, vocabulary",
    model_version="2.3.0",
    copy=True,
    limit=False,
    owner="loading_user"
)
print(result)   # -> True (indicates the R script ran, results were copied, permissions set, and tables vacuumed)
```
