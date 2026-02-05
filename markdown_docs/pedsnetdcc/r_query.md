## FunctionDef _create_argos_file(config_path, config_file, schema, password, conn_info_dict)
**_create_argos_file**: The function of _create_argos_file is to generate a JSON‑formatted Argos configuration file that supplies connection details for an R script to access a PostgreSQL database.

**parameters**: The parameters of this Function.  
· **config_path**: The directory path where the configuration file will be created.  
· **config_file**: The name of the configuration file to be written (e.g., “argos.json”).  
· **schema**: The primary PostgreSQL schema that should be placed in the search_path option of the Argos configuration.  
· **password**: The database password to be embedded in the configuration file.  
· **conn_info_dict**: A dictionary containing connection information extracted from a PostgreSQL connection string; expected keys are “host”, “dbname”, and “user”.

**Code Description**:  
The function opens (or creates) a file at the location `os.path.join(config_path, config_file)` using binary write mode (`'wb'`). It then writes a series of lines that together form a JSON object. The structure of the JSON is:

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

Each line is terminated with the platform‑specific line separator (`os.linesep`). The `host`, `dbname`, and `user` values are obtained from `conn_info_dict` via the `get` method. The `port` is hard‑coded to `5432`, which is the default PostgreSQL port. The `bigint` mapping is set to `"numeric"` to ensure large integer values are handled correctly by Argos. The `options` field injects a PostgreSQL `search_path` setting that includes the supplied `schema` and the `vocabulary` schema, ensuring that subsequent queries resolve objects in the correct namespace.

From a functional perspective, `_create_argos_file` is invoked by `run_r_query` after the R script’s working directory has been prepared. `run_r_query` supplies the destination directory (`dest_path`), the name of the configuration file, the primary schema derived from the user‑provided `search_path`, the database password (extracted from the connection string if not explicitly passed), and the connection information dictionary. By writing the Argos configuration file, `_create_argos_file` enables the subsequent R script (`run.R`) to connect to the correct database instance with the appropriate credentials and schema context.

**Note**:  
- The file is opened in binary mode (`'wb'`) but strings are written directly; this works in Python 2 but raises a `TypeError` in Python 3 unless the strings are encoded to bytes. If the project runs under Python 3, consider opening the file in text mode (`'w'`) or encoding each line (`line.encode()`).  
- The function does not perform any validation on the presence of required keys in `conn_info_dict`. Callers must ensure that `host`, `dbname`, and `user` are present to avoid writing `None` values into the JSON.  
- The port number is fixed at 5432; if the target database uses a non‑standard port, the function would need to be extended to accept a port argument.  
- The generated JSON does not include a trailing newline after the final closing brace; this is acceptable for most parsers but may be adjusted for readability.  
- Because the password is written in plain text, the configuration file should be protected with appropriate filesystem permissions to prevent unauthorized access.
## FunctionDef _fix_site_info(file_path, site, schema)
**_fix_site_info**: The function of _fix_site_info is to substitute placeholder tokens in a site‑specific R configuration file with the actual site name and database schema, then save the updated file.

**parameters**:  
· file_path: The root directory of the copied query package (string).  
· site: The site identifier that should replace the `<SITE>` placeholder (string).  
· schema: The database schema name that should replace the `<SCHEMA>` placeholder (string).

**Code Description**:  
1. The function builds the path to the file *site_info.R* located in the subdirectory `site` under the supplied `file_path`.  
2. It attempts to open this file in read mode, reads its entire contents into `file_data`, and then closes the file.  
3. Two string replacements are performed on `file_data`: every occurrence of the literal token `<SITE>` is replaced with the value of the `site` argument, and every occurrence of `<SCHEMA>` is replaced with the value of the `schema` argument.  
4. The modified content is written back to the same *site_info.R* file, overwriting the original.  
5. If any step raises an exception (for example, the file does not exist, the directory structure is missing, or a permission error occurs), the `except` block catches it, and the function returns `False`. This behavior acknowledges that some query packages may not contain a *site_info.R* file, and the caller can treat the absence as a non‑fatal condition.  
6. When the file is successfully updated, the function returns `True`.

**Relationship with Callers**:  
`_fix_site_info` is invoked by `run_r_query` after the query package has been duplicated into a site‑specific directory. `run_r_query` prepares the execution environment for an R script, creates an Argos configuration file, and then calls `_fix_site_info` to embed the concrete site and schema values into the R script’s configuration. By ensuring that *site_info.R* reflects the current execution context, the subsequent call to `_fix_run` and the execution of `run.R` operate with the correct metadata. If `_fix_site_info` returns `False`, `run_r_query` proceeds without raising an error, because the presence of *site_info.R* is optional for some packages.

**Note**:  
- The function uses a bare `except:` clause, which suppresses all exceptions, including unrelated programming errors. This design choice is intentional to allow packages without a *site_info.R* file to continue, but it may also hide genuine I/O problems.  
- The placeholders `<SITE>` and `<SCHEMA>` must exist in the original file; otherwise, the file will be rewritten unchanged.  
- The caller must ensure that `file_path` points to a writable location and that the directory structure `site/site_info.R` exists if the file is expected.  

**Output Example**:  
```python
result = _fix_site_info('/app/my_package', 'umich', 'pedsnet')
# result will be True if the file was found and updated,
# or False if the file does not exist or an error occurred.
```
## FunctionDef _fix_run(file_path, site)
**_fix_run**: The function of _fix_run is to replace the `<SITE>` placeholder in the `run.R` script with the actual site name and report whether the operation succeeded.  

**parameters**:  
· **file_path**: String representing the root directory of the query package (e.g., `/app/<package>`).  
· **site**: String containing the site identifier that should replace the `<SITE>` placeholder in the script.  

**Code Description**:  
The function attempts to locate the R script `run.R` inside the sub‑directory `site` of the supplied `file_path`.  
1. It builds the full path using `os.path.join(file_path, 'site', 'run.R')` and opens the file in read mode.  
2. The entire file content is read into `file_data`.  
3. The placeholder string `'<SITE>'` within `file_data` is replaced by the value of the `site` argument via `str.replace`.  
4. The modified content is written back to the same file, overwriting the original.  
If any exception occurs during these steps (for example, the file does not exist, permission is denied, or an I/O error), the `except` block catches it and the function returns `False`, indicating that the expected file was not present or could not be processed.  
When the try block completes without error, the function returns `True`, signalling that the placeholder substitution was performed successfully.  

In the broader project, `_fix_run` is invoked by `run_r_query` after the query package has been copied to a site‑specific directory. `run_r_query` prepares the environment, creates an Argos configuration file, adjusts other site‑specific files, and then calls `_fix_run` to ensure that the R script knows which site it should operate on. The return value of `_fix_run` is not explicitly checked in `run_r_query`; the calling function proceeds regardless, assuming that the absence of the file simply means the package does not require site substitution.  

**Note**:  
- The function catches all exceptions indiscriminately; therefore, unrelated errors (e.g., encoding issues) will also result in a `False` return without detailed diagnostics.  
- The placeholder must be exactly `<SITE>`; any variation will not be replaced.  
- The caller must guarantee that `file_path` points to a writable location and that the process has permission to read and write `run.R`.  

**Output Example**:  
```python
result = _fix_run('/app/my_query_package', 'siteA')
# result will be True if the file was found and updated,
# or False if the file was missing or an error occurred.
```
## FunctionDef run_r_query(config_file, conn_str, site, package, password, search_path, model_version, copy)
**run_r_query**: The function of run_r_query is to prepare a site‑specific execution environment, generate an Argos configuration file, adjust R script placeholders, execute the R script, and optionally copy the results to the output directory.

**parameters**: The parameters of this Function.  
· **config_file** (str): Name of the Argos configuration file to be created (e.g., “site_package_argos_temp.json”).  
· **conn_str** (str): PostgreSQL libpq connection string that contains host, port, dbname, user, password and search_path.  
· **site** (str): Identifier of the site for which the R query is being run.  
· **package** (str): Name of the R query package located under ``/app`` (e.g., “lab_loinc”).  
· **password** (str or None): Database password; if ``None`` the function extracts it from ``conn_str``.  
· **search_path** (str): PostgreSQL schema search path supplied by the user; used to determine the primary schema.  
· **model_version** (str): Version string of the PEDSnet data model (e.g., “2.3.0”). *(The argument is currently not used inside the function but is kept for API compatibility.)*  
· **copy** (bool): When ``True`` the function copies the ``results`` directory produced by the R script to ``/output/<package>/<site>``.

**Code Description**:  

1. **Parse connection information** – Calls ``get_conn_info_dict(conn_str)`` to obtain a dictionary with keys ``host``, ``dbname``, ``user`` and ``search_path``.  

2. **Logging start** – Builds a log dictionary by merging ``{'site': site}`` with the connection‑info dictionary using ``combine_dicts``. Emits an INFO log entry “starting R Script” together with the combined context. Records the start time with ``time.time()``.  

3. **Determine primary schema** – Calls ``primary_schema(search_path)`` to extract the first schema name from the supplied search path; this value is stored in ``schema`` and later written into the Argos file.  

4. **Obtain password if missing** – If the ``password`` argument is ``None``, a regular‑expression search extracts the password from ``conn_str`` (pattern ``password=(\S*)``).  

5. **Prepare package directory** – Constructs ``source_path`` as ``/app/<package>`` and ``dest_path`` as ``/app/<package>/<site>``.  
   * If ``dest_path`` already exists, it is removed with ``shutil.rmtree`` to ensure a clean environment.  
   * The entire package directory is copied to the site‑specific location using ``shutil.copytree``.  

6. **Create Argos configuration file** – Calls the internal helper ``_create_argos_file`` with ``dest_path``, ``config_file``, the primary ``schema``, the resolved ``password`` and the connection‑info dictionary. The helper writes a JSON file (named ``config_file``) that R will use to connect to the database.  

7. **Insert site and schema into R helper files** –  
   * ``_fix_site_info(dest_path, site, schema)`` replaces the placeholders ``<SITE>`` and ``<SCHEMA>`` in ``site/site_info.R`` (if the file exists).  
   * ``_fix_run(dest_path, site)`` replaces the placeholder ``<SITE>`` in ``site/run.R`` (if the file exists).  

8. **Execute the R script** – Builds the absolute path to ``run.R`` (``/app/<package>/<site>/site/run.R``) and invokes the ``Rscript`` command with ``--verbose=1``. The call runs in the ``/app`` working directory and blocks until completion (``_fg=True``).  

9. **Optional result copying** – If ``copy`` is ``True`` and a ``results`` sub‑directory exists under ``dest_path``, the function copies that directory to ``/output/<package>/<site>``. Existing output directories are removed first. Log messages indicate whether results were copied or not found.  

10. **Logging end** – Emits an INFO log entry “finished R Script” together with the elapsed time, formatted by ``secs_since(start_time)``.  

11. **Return value** – If the function reaches the end without raising an exception, it returns ``True`` to signal successful execution.

**Reference relationships**:  

*Callers* – ``run_r_query`` is invoked by high‑level command‑line wrappers such as ``run_r_lab_loinc`` in ``pedsnetdcc/main.py``. Those wrappers construct the configuration file name, connection string and other arguments, then rely on ``run_r_query`` to perform the full R execution workflow.  

*Callees* – Within its body, ``run_r_query`` calls several utility functions:  
- ``get_conn_info_dict`` (parses the connection string).  
- ``combine_dicts`` (builds log dictionaries).  
- ``primary_schema`` (extracts the primary schema).  
- ``_create_argos_file`` (writes the Argos JSON file).  
- ``_fix_site_info`` and ``_fix_run`` (replace placeholders in R helper files).  
- ``Rscript`` (external command wrapper that actually runs the R script).  
- ``secs_since`` (formats elapsed time for logging).  

These relationships show that ``run_r_query`` is the central orchestrator for running any site‑specific R query package in the project.

**Note**:  
- The function assumes the directory structure ``/app/<package>/site`` exists in the source package and contains the files ``site_info.R`` and ``run.R`` when placeholder substitution is required. Missing files are silently ignored (the helper functions return ``False``).  
- ``_create_argos_file`` writes the JSON file in binary mode; this works under Python 2 but requires text mode or explicit encoding under Python 3.  
- Password handling is minimal: the password is either passed explicitly or extracted via a regex; it is stored in plain text inside the Argos file, so filesystem permissions should restrict access to the generated directory.  
- The ``model_version`` argument is not used inside the function; callers should ensure future extensions handle it if needed.  
- Any exception raised by the R script (e.g., non‑zero exit status) will propagate as a ``DatabaseError`` according to the docstring, causing the caller to exit with a non‑zero status.

**Output Example**:  

```python
success = run_r_query(
    config_file='umich_lab_loinc_argos_temp.json',
    conn_str='host=db.example.com port=5432 dbname=pedsnet user=loader password=secret options=\'-c search_path=dcc_pedsnet\'',
    site='umich',
    package='lab_loinc',
    password=None,
    search_path='dcc_pedsnet,public',
    model_version='2.3.0',
    copy=True
)
# success will be True if the R script ran without error and the function completed.
```
