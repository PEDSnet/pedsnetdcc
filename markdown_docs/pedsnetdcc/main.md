## FunctionDef pedsnetdcc(logfmt, loglvl)
**pedsnetdcc**: The function of pedsnetdcc is to configure and return a logger for the PEDSnet Data Coordinating Center command‑line interface, applying a chosen log format filter.

**parameters**:  
· logfmt: a string that selects the output format for log messages; accepted values are `'tty'`, `'text'`, or any other value which defaults to JSON.  
· loglvl: a string or integer representing the desired logging level (e.g., `'INFO'`, `'DEBUG'`, `'WARNING'`).

**Code Description**:  
The function begins by obtaining a logger instance named `'pedsnetdcc'` via `logging.getLogger`. It then creates a `logging.StreamHandler` that will write log records to `stderr`.  

1. **Automatic TTY format selection** – If the caller does not supply a `logfmt` value and `sys.stderr` is attached to an interactive terminal (`isatty()` returns True), the function sets `logfmt` to `'tty'`. This provides a human‑readable, colour‑enhanced format when the tool is run directly in a console.  

2. **Filter attachment based on format** –  
   * When `logfmt == 'tty'`, a `DictLogFilter('tty')` instance is added to the stream handler.  
   * When `logfmt == 'text'`, a `DictLogFilter('text')` instance is added.  
   * For any other value (including `None`), a `DictLogFilter('json')` instance is added, which is the default JSON representation.  

   The `DictLogFilter` class (located in `pedsnetdcc/dict_logging.py`) implements the standard logging filter interface. Its `filter` method inspects each `LogRecord`; if the record’s `msg` attribute is a dictionary, the filter transforms the dictionary into the selected textual representation (JSON, plain‑text logfmt, or colourised TTY) and replaces `record.msg` with the formatted string. By attaching the filter to the handler before any formatter, the logger ensures that all dictionary‑based log messages emitted by the application are automatically converted to the chosen format.  

3. **Handler registration** – The configured `StreamHandler` (now equipped with the appropriate `DictLogFilter`) is added to the `'pedsnetdcc'` logger via `logger.addHandler(sh)`.  

4. **Log level configuration** – The logger’s severity threshold is set with `logger.setLevel(logging.getLevelName(loglvl))`. `logging.getLevelName` translates the supplied string (or integer) into the corresponding numeric logging level, ensuring that only messages at or above that level are emitted.  

The function does not return a value; its purpose is side‑effect driven: it prepares the global logger for subsequent use throughout the CLI application.

**Note**:  
- The function must be called early in the program (typically at CLI entry point) before any log messages are generated, otherwise messages may be emitted with the default logger configuration.  
- `logfmt` is case‑sensitive; only the exact strings `'tty'` and `'text'` trigger the corresponding filters. Any other value, including `'json'` or an empty string, results in JSON formatting.  
- The `DictLogFilter` only processes records whose `msg` attribute is a dictionary; standard string messages bypass the filter unchanged.  
- Because the filter mutates `record.msg` in place, it should be added to the handler before any formatter that expects a pre‑formatted string. Adding it after a formatter could lead to double formatting or loss of the intended output.  
- The logger name `'pedsnetdcc'` is fixed; other modules that obtain a logger with the same name will share this configuration, enabling consistent logging across the entire package.
## FunctionDef post_load(searchpath, pwprompt, dburi, site, name, force, limit, owner, stringid, logged, post_only, pool1, nopk, nonull, noidx, nodrop, nofk, model_version)
**post_load**: The function of **post_load** is to orchestrate the complete post‑load workflow for a PEDSnet database, including integrity checks, observation‑period synchronization, and the full transformation pipeline.

**parameters**:  
· **searchpath** (str): Schema search path that will be applied to the database connection.  
· **pwprompt** (bool): If True, the user is prompted on the command line for the database password (input hidden).  
· **dburi** (str): PostgreSQL URI that identifies the target database (e.g., `postgresql://user@host/dbname`).  
· **site** (str): Site identifier used by the transformation step (e.g., “stlouis”).  
· **name** (str): Name of the identifier column to be created in the transformed tables (e.g., “dcc”).  
· **force** (bool): When True, benign errors such as duplicate objects are ignored during transformation.  
· **limit** (bool): If True, schema permissions are restricted to the supplied **owner** role after the transformation.  
· **owner** (str): Database role that receives limited permissions when **limit** is True.  
· **stringid** (bool): Determines the data type of the identifier column; True selects a `String` type, otherwise `BigInteger`.  
· **logged** (bool): If True, transformed tables are created as LOGGED; otherwise they are UNLOGGED.  
· **post_only** (bool): If True, only the post‑transform steps are executed (the core transformation and temporary schema creation are skipped).  
· **pool1** (bool): Passed to the transformation runner as the *pool_limit* flag; when True the parallel worker pool is limited to a single process.  
· **nopk** (bool): When True, primary‑key creation is skipped during the transformation.  
· **nonull** (bool): When True, NOT NULL constraints are not added during the transformation.  
· **noidx** (bool): When True, index creation is skipped during the transformation.  
· **nodrop** (bool): When True, dropping of unneeded indexes is skipped during the transformation.  
· **nofk** (bool): When True, foreign‑key creation is skipped during the transformation.  
· **model_version** (str): PEDSnet data‑model version (e.g., “2.3.0”) passed to the transformation runner.

**Code Description**:  
1. **Password handling** – If **pwprompt** is True, the function uses `click.prompt` to request the database password from the user, storing the result in the local variable `password`.  
2. **Connection string construction** – The helper `make_conn_str` is called with **dburi**, **searchpath**, and the optional **password** to produce a libpq‑compatible connection string (`conn_str`). This string is used for all subsequent database operations.  
3. **Fact‑relationship integrity check** – The function imports `check_fact_relationship` from `pedsnetdcc.check_fact_relationship` and executes it with `conn_str`. The returned boolean (`success`) indicates whether the `fact_relationship` table passed its referential‑integrity validation. If the check fails (`success` is False), the process terminates immediately with `sys.exit(1)`.  
4. **Observation‑period synchronization** – After a successful integrity check, `sync_observation_period` from `pedsnetdcc.sync_observation_period` is imported and run with the same `conn_str`. This step rebuilds the `observation_period` table from the fact data. Failure also results in an immediate `sys.exit(1)`.  
5. **Identifier type selection** – The flag **stringid** determines the identifier column type for the transformation: `'String'` if True, otherwise `'BigInteger'`. The chosen value is stored in the local variable `idtype`.  
6. **Transformation execution** – The function imports `run_transformation` from `pedsnetdcc.transform_runner` and calls it with a full set of arguments:  
   - `conn_str` – the prepared connection string,  
   - `model_version`, `site`, `searchpath`, `name` – contextual parameters,  
   - `idtype` – the identifier data type selected in step 5,  
   - `logged`, `post_only`, `pool1`, `nopk`, `nonull`, `noidx`, `nodrop`, `nofk`, `limit`, `owner`, `force` – all flags controlling the transformation behavior.  
   The function captures the boolean result (`success`). If the transformation reports failure, the script exits with `sys.exit(1)`.  
7. **Successful completion** – If all three stages (fact‑relationship check, observation‑period sync, transformation) succeed, the function terminates the process with `sys.exit(0)`, indicating a clean exit.

**Note**:  
- The function terminates the entire Python process using `sys.exit`; it does not raise exceptions to the caller. This makes it suitable only as a top‑level CLI entry point.  
- All database interactions rely on a correctly formed DBURI; malformed URIs will cause `make_conn_str` to raise `ValueError`.  
- Password prompting occurs only when **pwprompt** is True; otherwise any password embedded in the URI is used.  
- The three major steps are executed sequentially; any failure aborts the remaining steps, ensuring that partially transformed data is not left in the database.  
- The **pool1** argument is passed directly to the transformation runner as the *pool_limit* flag; naming may be confusing but the effect is to restrict parallelism to a single worker when True.  
- The function imports its callees locally (inside the function body) to avoid unnecessary import overhead when the script is invoked with different sub‑commands.  
- No explicit logging is performed inside **post_load** itself; all status messages are emitted by the called modules (`check_fact_relationship`, `sync_observation_period`, `run_transformation`).  
- The **model_version** parameter is forwarded unchanged to the transformation runner; it is not used elsewhere in this function.  
## FunctionDef sync_observation_period(searchpath, pwprompt, dburi)
**sync_observation_period**: The function of sync_observation_period is to synchronize the observation period table in the database by deleting any existing records and rebuilding them from the fact data, reporting success or failure via the process exit code.

**parameters**:  
· **searchpath** (str): The schema search_path that should be applied to the database connection. It is passed to `make_conn_str` to override or set the `search_path` option in the libpq connection string.  
· **pwprompt** (bool): When True, the function will interactively prompt the user for the database password using `click.prompt`. The entered password is then supplied to `make_conn_str`.  
· **dburi** (str): A PostgreSQL URI that identifies the target database (e.g., `postgresql://user:pass@host:port/dbname`). It is the base for constructing the libpq connection string.

**Code Description**:  
1. The function begins by importing the concrete implementation `sync_observation_period` from the module `pedsnetdcc.sync_observation_period`. This imported function performs the actual database work.  
2. It initializes a local variable `password` to `None`.  
3. If the `pwprompt` flag is True, the function calls `click.prompt('Database password', hide_input=True)` to read a password from the terminal without echoing it, and stores the result in `password`.  
4. The helper `make_conn_str` (from `pedsnetdcc.utils`) is invoked with three arguments: the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. `make_conn_str` returns a libpq‑compatible connection string that incorporates the search_path and password as needed.  
5. The returned connection string (`conn_str`) is passed to the imported `sync_observation_period` function. This call returns a Boolean `success` indicating whether the synchronization completed without error.  
6. If `success` is False, the script terminates with exit status 1 (`sys.exit(1)`), signalling an error to the operating system or calling process.  
7. If `success` is True, the script terminates with exit status 0 (`sys.exit(0)`), indicating normal completion.

**Note**:  
- The function does not perform any logging itself; any informational or error messages are produced by the called `sync_observation_period` implementation.  
- The exit codes are deliberately explicit: 0 for success, 1 for failure. This behavior is important for integration with shell scripts or CI pipelines that rely on process return codes.  
- The password prompt is only shown when `pwprompt` is True; otherwise the password must be present in the URI or omitted entirely.  
- The `searchpath` argument overrides any `search_path` option already encoded in the URI, because `make_conn_str` gives precedence to the explicit argument.  
- Because the function ends with `sys.exit`, it is intended to be used as a command‑line entry point rather than as a reusable library function. Calling it from other Python code will cause the interpreter to exit unless the call is wrapped in a try/except that catches `SystemExit`.
## FunctionDef check_fact_relationship(searchpath, pwprompt, output, poolsize, dburi)
**check_fact_relationship**: The function of check_fact_relationship is to validate the referential integrity of the fact relationship table by executing the appropriate SQL checks in parallel and reporting the results.

**parameters**:  
· **searchpath** (str): Schema search_path to be applied when constructing the database connection string.  
· **pwprompt** (bool): If True, the user is prompted on the command line for the database password; the entered password is then used for the connection.  
· **output** (str): Desired output format for the integrity‑check results (e.g., “json”, “csv”, or a custom format defined by the underlying check implementation).  
· **poolsize** (int): Number of parallel worker processes/threads to use when running the SQL checks.  
· **dburi** (str): PostgreSQL connection URI in libpq‑compatible form (e.g., `postgresql://user:pass@host:port/dbname?...`).  

**Code Description**:  
1. The function imports the concrete implementation `check_fact_relationship` from the module `pedsnetdcc.check_fact_relationship`. This imported callable performs the actual SQL validation logic.  
2. It initializes a local variable `password` to `None`.  
3. If the `pwprompt` flag is true, the function uses `click.prompt` to request the database password from the user, hiding the input. The entered value is stored in `password`.  
4. It calls the utility `make_conn_str` (from `pedsnetdcc.utils`) with three arguments: the supplied `dburi`, the optional `searchpath`, and the possibly‑prompted `password`. `make_conn_str` returns a libpq‑style connection string that incorporates the search_path and password overrides.  
5. The returned connection string (`conn_str`) and the user‑provided `output` and `poolsize` arguments are passed to the imported `check_fact_relationship` implementation. The implementation runs the required SQL statements in parallel, logs warnings for any integrity violations, logs informational messages for successful checks and timing, and returns a Boolean `success` flag.  
6. If `success` is `False`, the function terminates the process with exit code 1 (`sys.exit(1)`), indicating failure to the operating system or calling script.  
7. If `success` is `True`, the function exits cleanly with code 0 (`sys.exit(0)`).  

**Relationship with Callees**:  
- **make_conn_str** (pedsnetdcc.utils): Generates a libpq‑compatible connection string from the supplied URI, optionally overriding the search_path and password. This ensures that the downstream integrity‑check logic connects to the correct database schema with the correct credentials.  
- **pedsnetdcc.check_fact_relationship.check_fact_relationship**: Performs the core validation work. It receives the prepared connection string, the output format, and the parallel pool size, executes the necessary SQL queries, logs results, and returns a success indicator.  

**Note**:  
- The function does not perform any validation of the `output` or `poolsize` arguments; such validation is expected to be handled by the underlying check implementation.  
- When `pwprompt` is True, the password prompt blocks execution until the user provides input; this is suitable for interactive use but may be undesirable in automated scripts.  
- The function relies on `sys.exit` to communicate success or failure, which will terminate the entire Python interpreter. Calls from within another Python process should be aware of this side effect.  
- Any exception raised inside the imported `check_fact_relationship` implementation (e.g., connection errors, SQL errors) will propagate out of this wrapper unless caught internally, causing the script to exit with a non‑zero status.  
- The function assumes that `click` and `sys` have been imported elsewhere in the module; missing imports would raise a `NameError`.
## FunctionDef create_id_maps(dburi, pwprompt, skipsites, addsites, name, type, site_type, site_len)
**create_id_maps**: The function of create_id_maps is to create the necessary ID‑map and DCC‑ID tracking tables in a target PostgreSQL database for a new data‑cycle, without inserting any data.

**parameters**  
· dburi: PostgreSQL URI that identifies the target database (e.g., `postgresql://user[:password]@host:port/dbname`).  
· pwprompt: Boolean flag; when True the function prompts the user on the command line for the database password (input is hidden).  
· skipsites: Comma‑separated string of site identifiers that should be excluded from ID‑map table creation.  
· addsites: Comma‑separated string of site identifiers that must be forced into the creation set, even if they appear in `skipsites`.  
· name: Base name for the identifier set (e.g., `"dcc"` or `"onco"`); used when creating the DCC‑ID tables and the ID‑map tables.  
· type: PostgreSQL data type for the DCC identifier column (`"INTEGER"` or `"BIGINT"`).  
· site_type: PostgreSQL data type for the site identifier column (`"INTEGER"`, `"BIGINT"` or `"VARCHAR"`).  
· site_len: Length specification used only when `site_type` is `"VARCHAR"` (e.g., `"10"`).

**Code Description**  
1. The function imports the two table‑creation helpers from `pedsnetdcc.id_maps`: `create_id_map_tables` and `create_dcc_ids_tables`.  
2. It initializes a local variable `password` to `None`. If `pwprompt` is true, it uses `click.prompt` to request the database password from the user, hiding the input; the entered value is stored in `password`.  
3. It calls `make_conn_str` (from `pedsnetdcc.utils`) with the supplied `dburi` and the possibly‑provided `password`. `make_conn_str` converts the URI into a libpq‑compatible connection string, inserting the password if given. The resulting string is assigned to `conn_str`.  
4. `create_dcc_ids_tables` is invoked with `conn_str`, `name`, and `type`. This creates the “last‑generated‑id” tables (one per PEDSnet data table) inside a schema named `<name>_ids`. No data are inserted; only the DDL is executed.  
5. `create_id_map_tables` is invoked with `conn_str`, `skipsites`, `addsites`, `name`, `type`, `site_type`, and `site_len`. This creates, for each selected site, a schema‑qualified mapping table that links the site’s local identifiers to the DCC identifiers. The function handles the skip/add logic, determines the appropriate site identifier column type (including sizing for `VARCHAR`), and builds the required `CREATE TABLE` statements.  
6. The function does not return a value. Any exception raised by `make_conn_str`, `create_dcc_ids_tables`, or `create_id_map_tables` (e.g., connection errors, SQL execution errors) propagates to the caller, signalling failure.

**Note**  
- The function only creates the structural tables; populating them with actual ID mappings must be performed by subsequent steps in the workflow.  
- The `dburi` must follow the PostgreSQL URI syntax; malformed URIs will cause `make_conn_str` to raise a `ValueError`.  
- When `pwprompt` is true, the password entered via the prompt overrides any password embedded in the URI.  
- The `skipsites` and `addsites` strings may be empty; in that case all known sites (as defined by `SITES_AND_EXTERNAL` in `id_maps.py`) are processed.  
- The DCC‑ID tables are always created under the schema `<name>_ids`. If that schema does not already exist, the DDL in `create_dcc_ids_tables` will fail.  
- Because the underlying table‑creation functions execute DDL statements individually (non‑transactionally), a failure on one statement will not roll back previously created tables; callers should be prepared to handle partially created schemas.  
- All logging is performed inside the called functions; `create_id_maps` itself does not emit log messages.  
- No return value is provided; success is indicated by the absence of raised exceptions.
## FunctionDef populate_last_id(dburi, pwprompt, searchpath, name)
**populate_last_id**: The function of populate_last_id is to update the `last_id` column in the study‑id‑map tables that were created with the `create_id_maps` command.

**parameters**  
· dburi (str): A PostgreSQL DBURI that identifies the target database (e.g., `postgresql://user@host/dbname`).  
· pwprompt (bool): If True, the function will interactively ask the user for the database password.  
· searchpath (str or None): An optional schema search_path that will be injected into the connection string.  
· name (str): The name of the id‑map table (or identifier) whose `last_id` value should be populated.

**Code Description**  
1. The function imports `populate_last_id` from the internal module `pedsnetdcc.id_maps`. This imported routine performs the actual SQL work of setting the `last_id` value.  
2. It initializes a local variable `password` to `None`.  
3. If `pwprompt` is True, the function uses `click.prompt` to request the database password from the user, hiding the input. The entered password is stored in `password`.  
4. It calls the utility `make_conn_str` (from `pedsnetdcc.utils`) with the supplied `dburi`, the optional `searchpath`, and the possibly‑provided `password`. `make_conn_str` returns a libpq‑compatible connection string that incorporates the search_path and password overrides.  
5. The resulting connection string (`conn_str`) is passed, together with `searchpath` and `name`, to the imported `populate_last_id` routine. The routine returns a Boolean `success` indicating whether the operation completed without error.  
6. If `success` is False, the function terminates the process with exit status 1 (`sys.exit(1)`).  
7. If `success` is True, the function terminates the process with exit status 0 (`sys.exit(0)`).

**Relationship with other components**  
- **Callee**: `pedsnetdcc.id_maps.populate_last_id` – executes the database‑level update of the `last_id` column.  
- **Callee**: `pedsnetdcc.utils.make_conn_str` – builds the connection string that includes any overridden password or search_path.  
- **Callee**: `click.prompt` – obtains a password from the terminal when interactive prompting is requested.  
- **Callee**: `sys.exit` – ends the CLI process with an appropriate status code based on success or failure.

**Note**  
- The function does not perform any validation of `dburi`, `searchpath`, or `name`; it relies on the downstream `make_conn_str` and `populate_last_id` implementations to raise errors if inputs are malformed.  
- When `pwprompt` is False, no password is supplied; any password embedded in the DBURI will be used by `make_conn_str`.  
- The exit codes follow the conventional CLI pattern: `0` for success, `1` for failure, allowing calling scripts or shell pipelines to detect errors reliably.  
- Because the function ends with `sys.exit`, it should be used only as a top‑level command entry point (e.g., from a Click command) and not imported for reuse in long‑running Python processes.
## FunctionDef copy_id_maps(dburi, old_db, new_db, pwprompt, name, skipsites, addsites)
**copy_id_maps**: The function of copy_id_maps is to copy ID‑map tables from the most recent data‑cycle database into a newly created data‑cycle database.

**parameters**  
· dburi: Base PostgreSQL URI (e.g., `postgresql://user@host/`). The URI is concatenated with the specific database names supplied in `old_db` and `new_db`.  
· old_db: Name of the source database that contains the existing ID‑map tables.  
· new_db: Name of the target database that will receive the copied ID‑map tables.  
· pwprompt: Boolean flag. When True the function prompts the user for a database password on the command line; the entered password is then applied to both connection strings.  
· name: Identifier (string) passed through to the underlying `pedsnetdcc.id_maps.copy_id_maps` implementation; typically the name of the data‑cycle or a descriptive label.  
· skipsites: Collection (list, tuple, or set) of site identifiers that should be excluded from the copy operation.  
· addsites: Collection of site identifiers that should be added to the target database even if they are not present in the source.

**Code Description**  
1. The function imports the concrete implementation `copy_id_maps` from the module `pedsnetdcc.id_maps`. This imported function performs the actual table‑level copying; the wrapper in `main.py` only prepares connection strings and forwards arguments.  
2. A local variable `password` is initialised to `None`. If `pwprompt` is truthy, the function uses `click.prompt` to request a password from the user, hiding the input. The entered password is stored in `password`.  
3. The full connection URI for the source database is assembled by concatenating `dburi` and `old_db`. The helper `make_conn_str` (from `pedsnetdcc.utils`) is called with this URI and the optional `password`. `make_conn_str` parses the URI, applies the explicit password if supplied, and returns a libpq‑compatible connection string (`old_conn_str`).  
4. The same process is repeated for the target database, producing `new_conn_str`.  
5. Finally, the imported `copy_id_maps` function is invoked with the two connection strings and the remaining arguments (`name`, `skipsites`, `addsites`). No further processing occurs in this wrapper; all error handling and database operations are delegated to the callee.

**Note**  
- The function expects `dburi` to be a valid PostgreSQL URI without a trailing slash; the database names are simply appended.  
- Password handling is optional; if `pwprompt` is False, any password embedded in the URI will be used, otherwise the user‑supplied password overrides it.  
- The wrapper does not perform any validation of `skipsites` or `addsites`; they are passed unchanged to the underlying implementation, which must handle type checking.  
- Because the function relies on `make_conn_str`, any limitations or validation rules of that helper (e.g., single occurrence of query parameters, proper quoting of values) apply here as well.  
- Errors raised by `make_conn_str` (such as malformed URIs) or by the underlying `pedsnetdcc.id_maps.copy_id_maps` will propagate to the caller; the wrapper does not catch exceptions.
## FunctionDef transform(pwprompt, searchpath, site, name, limit, owner, stringid, logged, post_only, pool1, nopk, nonull, noidx, nodrop, nofk, force, model_version, undo, dburi)
**transform**: The function of transform is to orchestrate the conversion of PEDSnet data from a specified model version into the DCC format, optionally undoing a previous transformation.

**parameters**:  
· pwprompt: bool – If True, the function will prompt the user for the database password on the command line.  
· searchpath: str – The PostgreSQL schema search_path that identifies the primary schema containing the core tables to be transformed.  
· site: str – The site identifier (e.g., “stlouis”) that is used for site‑specific transformations such as adding a site column and generating DCC IDs.  
· name: str – The name of the identifier column to be created (e.g., “dcc” or “onco”).  
· limit: bool – When True, schema permissions are granted only to the specified owner role after the transformation.  
· owner: str – The database role that receives limited permissions if *limit* is True (default “loading_user”).  
· stringid: bool – If True, the identifier column is created as a PostgreSQL `String`; otherwise it is created as `BigInteger`.  
· logged: bool – Determines whether the transformed tables are created as LOGGED (True) or UNLOGGED (False).  
· post_only: bool – If True, only the post‑transform steps (DDL, permission grants, schema swap) are performed; the creation of the temporary transformed schema and the core transformation are skipped.  
· pool1: bool – When True, the parallel worker pool used for table creation is limited to a single process; otherwise up to 25 workers may be used.  
· nopk: bool – Skip creation of primary‑key constraints when True.  
· nonull: bool – Skip addition of NOT NULL constraints when True.  
· noidx: bool – Skip creation of indexes when True.  
· nodrop: bool – Skip dropping of unneeded indexes when True.  
· nofk: bool – Skip creation of foreign‑key constraints when True.  
· force: bool – When True, benign errors such as attempts to create duplicate objects are ignored.  
· model_version: str – The PEDSnet data‑model version to be transformed (e.g., “2.3.0”).  
· undo: bool – If True, the function will revert a previous transformation instead of performing a new one.  
· dburi: str – A PostgreSQL URI that identifies the target database (e.g., `postgresql://user:pass@host:port/dbname`).  

**Code Description**:  
1. **Password handling** – If *pwprompt* is True, the function uses `click.prompt` to read the database password from the terminal without echoing it. The entered password is stored in the local variable `password`.  
2. **Connection string construction** – `make_conn_str` is called with the supplied *dburi*, the *searchpath*, and the optional *password*. This builds a libpq‑compatible connection string (`conn_str`) that incorporates the search_path and password overrides.  
3. **Identifier type selection** – The identifier column type is set to `'String'` when *stringid* is True; otherwise it defaults to `'BigInteger'`. The result is stored in `idtype`.  
4. **Branch on undo flag** –  
   * If *undo* is False, the function imports `run_transformation` from `pedsnetdcc.transform_runner` and invokes it with all relevant arguments: the connection string, model version, site label, search path, identifier name, identifier type, logging flag, post‑only flag, pool‑limit flag, and all DDL‑skip/force options. The call returns a boolean `success`.  
   * If *undo* is True, the function imports `undo_transformation` from the same module and calls it with the connection string, model version, and search path. The call also returns a boolean `success`.  
5. **Exit handling** – If `success` is falsy, the process terminates with `sys.exit(1)` indicating failure. Otherwise the function exits cleanly with `sys.exit(0)`.  

**Relationship with callees**:  
- `make_conn_str` (pedsnetdcc.utils) is used to translate the user‑supplied URI into a libpq connection string, handling optional password and search_path overrides.  
- `run_transformation` (pedsnetdcc.transform_runner) performs the full transformation pipeline: creation of a temporary schema, generation of transformed tables, application of post‑transform DDL (primary keys, indexes, foreign keys, NOT NULL constraints, logging), and atomic schema swap with backup. All flags passed to `transform` are forwarded to `run_transformation` unchanged.  
- `undo_transformation` (pedsnetdcc.transform_runner) restores the original core tables from the backup schema, drops the transformed schema, and cleans up constraints and indexes. It is invoked only when the *undo* flag is set.  

**Note**:  
- The function does not perform any validation of the individual arguments; it relies on the downstream runner functions to raise appropriate errors.  
- When *post_only* is True, the temporary transformed schema must already exist; otherwise `run_transformation` will fail because the core transformation step is skipped.  
- The *force* flag is propagated to the runner functions and only affects benign duplicate‑object errors; serious errors still cause the process to exit with status 1.  
- The function terminates the Python process with `sys.exit`, making it suitable as a command‑line entry point but unsuitable for reuse as a library call that expects to continue execution after transformation.  
- All boolean flags are passed through unchanged; the default values in the runner functions are respected when the corresponding arguments are omitted or set to their defaults.
## FunctionDef age_transform(pwprompt, searchpath, site, force, model_version, table, undo, dburi)
**age_transform**: The function of age_transform is to coordinate the execution of the age‑in‑months transformation on a PEDSnet database, handling password prompting, connection‑string creation, and CLI exit status.

**parameters**:  
· **pwprompt**: Boolean flag indicating whether the user should be prompted for the database password on the command line.  
· **searchpath**: String containing the PostgreSQL schema search_path that will be applied to the connection (e.g., “dcc_pedsnet, public”).  
· **site**: Identifier of the site (e.g., “stlouis”) that is recorded in the transformation metadata.  
· **force**: Boolean flag passed through to the transformation runner; when True, benign errors such as duplicate‑schema creation are ignored.  
· **model_version**: String specifying the PEDSnet model version (e.g., “2.3.0”) that determines source metadata for the transformation.  
· **table**: Comma‑separated list of table names that should be transformed (e.g., “person,measurement”).  
· **undo**: Boolean flag; when True the transformation step is skipped (useful for dry‑run or rollback scenarios).  
· **dburi**: PostgreSQL URI that identifies the target database (e.g., “postgresql://user@host/dbname”).  

**Code Description**:  
1. **Password handling** – The variable `password` is initialised to `None`. If `pwprompt` is true, the function uses `click.prompt` to request the database password from the user, hiding the input. The entered password is stored in `password`.  
2. **Connection‑string construction** – `make_conn_str` (from `pedsnetdcc.utils`) is called with the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. This helper parses the URI, merges or overrides the `options` component to include the desired `search_path`, and returns a libpq‑compatible connection string suitable for `psycopg2`. The resulting string is assigned to `conn_str`.  
3. **Conditional execution** – If the `undo` flag is false, the function proceeds to import `run_age_transformation` from `pedsnetdcc.transform_runner`. Import is performed locally to avoid unnecessary loading when the transformation is not required.  
4. **Running the transformation** – `run_age_transformation` is invoked with the prepared `conn_str` and the CLI arguments (`model_version`, `site`, `searchpath`, `table`, `force`). This called function performs the full schema‑swap age transformation, returning a Boolean `success` flag.  
5. **Exit handling** – If `success` is false, the script terminates with exit code 1 (`sys.exit(1)`), signalling an error to the operating system. If the transformation succeeded (or was skipped because `undo` is true), the script exits cleanly with code 0 (`sys.exit(0)`).  

**Relationship with callees**:  
- **make_conn_str** (in `pedsnetdcc.utils`) builds the libpq connection string from the supplied URI, optional search_path, and optional password. It is essential for establishing a database connection that respects the desired schema context.  
- **run_age_transformation** (in `pedsnetdcc.transform_runner`) carries out the actual DCC‑format transformation, including creation of a temporary transformed schema, generation of `_age_in_months` columns, and atomic schema swapping. `age_transform` acts as a thin CLI wrapper around this core routine.  
- **click.prompt** is used for interactive password entry when `pwprompt` is set.  
- **sys.exit** is used to propagate the success or failure status back to the command‑line environment.  

**Note**:  
- The function does not perform any validation of the input arguments; it relies on downstream components (`make_conn_str` and `run_age_transformation`) to raise appropriate exceptions for malformed values.  
- The `undo` flag only bypasses the call to `run_age_transformation`; it does not roll back any prior changes.  
- Because the function calls `sys.exit` directly, it terminates the entire Python process; it is intended to be used as a top‑level CLI command, not as a library function called from other Python code.  
- The database password is never logged; it is only passed to `make_conn_str` for inclusion in the connection string.  
- The expected format for `dburi` follows the PostgreSQL URI scheme (`postgresql://[user[:password]@][host][:port]/dbname[?params]`).  
- All schema‑level operations performed by `run_age_transformation` are executed within a single transaction, ensuring atomicity; any failure results in a non‑zero exit code from this wrapper.
## FunctionDef concept_transform(pwprompt, searchpath, site, force, model_version, table, undo, dburi)
**concept_transform**: The function of concept_transform is to orchestrate the transformation of PEDSnet data from a specified model version into the DCC format by building a database connection string, optionally prompting for a password, and invoking the core transformation routine.

**parameters**:  
· **pwprompt**: Boolean flag indicating whether the user should be prompted for the database password on the command line.  
· **searchpath**: String containing the PostgreSQL schema search_path that identifies the source schema(s) to be transformed.  
· **site**: String label of the site (e.g., “stlouis”) required by certain transformation steps.  
· **force**: Boolean flag that, when True, tells the underlying transformation to ignore benign errors such as duplicate‑schema or duplicate‑table conditions.  
· **model_version**: String identifier of the PEDSnet model version to be transformed (e.g., “2.3.0”).  
· **table**: Comma‑separated string of table names that should be processed by the transformation.  
· **undo**: Boolean flag intended to reverse a previous transformation; in the current implementation the flag disables the call to the transformation routine.  
· **dburi**: PostgreSQL URI that specifies the database host, port, name, user, and optional query parameters (e.g., “postgresql://user:pass@host/dbname”).

**Code Description**:  
1. **Password handling** – The function initializes a local variable `password` to `None`. If `pwprompt` is true, it uses `click.prompt` to request the database password from the user, hiding the input. The entered password is stored in `password`.  
2. **Connection string construction** – The helper `make_conn_str` (defined in `pedsnetdcc/utils.py`) is called with the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. This function parses the URI, merges or overrides the `search_path` and `password` options, and returns a libpq‑compatible connection string (`conn_str`).  
3. **Conditional execution** – If the `undo` flag is false, the function imports `run_concept_transformation` from `pedsnetdcc.transform_runner`. It then calls this routine with the arguments `conn_str`, `model_version`, `site`, `searchpath`, `table`, and `force`. The return value (`success`) is expected to be a boolean indicating whether the transformation completed without raising an exception.  
4. **Exit handling** – After the optional call, the function checks the `success` flag. If `success` is falsy, the process terminates with exit status 1 (`sys.exit(1)`). If `success` is truthy (or if the transformation was not invoked because `undo` is true), the process exits with status 0 (`sys.exit(0)`).  

**Relationship with callees**:  
- **make_conn_str**: Generates the PostgreSQL connection string used for all subsequent database operations. It incorporates the `searchpath` and optional password into the libpq format.  
- **run_concept_transformation**: Performs the actual concept‑name transformation workflow, including schema preparation, creation of transformed tables, logging, and atomic schema swapping. `concept_transform` passes through all user‑supplied parameters to this function and relies on its boolean return value to decide the CLI exit status.  
- **click.prompt**: Provides interactive password input when `pwprompt` is true.  
- **sys.exit**: Terminates the command‑line tool with an appropriate exit code based on the transformation outcome.

**Note**:  
- The `undo` flag currently only suppresses the call to `run_concept_transformation`; no undo logic is implemented, and the variable `success` will be undefined if `undo` is true, which would raise a `NameError`. This flag should be used only when the undo functionality is fully implemented.  
- The function expects `table` to be a clean, comma‑separated list without surrounding whitespace; callers should ensure proper formatting.  
- All error handling is delegated to `run_concept_transformation`. If that routine raises an exception, the CLI will terminate with a non‑zero exit code because the exception propagates out of `concept_transform`.  
- The function does not perform any validation of `searchpath`, `site`, or `model_version`; validation is assumed to be handled upstream (e.g., by the Click command‑line interface).  
- Because the function ends with `sys.exit`, it is intended to be used as a top‑level entry point in a command‑line script and not as a reusable library function within other Python code.
## FunctionDef site_transform(pwprompt, searchpath, site, force, model_version, table, undo, dburi)
**site_transform**: The function of site_transform is to orchestrate the command‑line execution of a site‑specific transformation of PEDSnet data into the DCC format.

**parameters**  
· pwprompt: Boolean flag indicating whether the user should be prompted for the database password on the command line.  
· searchpath: String containing the PostgreSQL schema search_path that will be applied to the connection (e.g., “dcc_pedsnet,public”).  
· site: Identifier of the site whose name will be written into the new *site* column (e.g., “stlouis”).  
· force: Boolean flag that, when True, tells the transformation runner to ignore benign errors such as duplicate‑schema or duplicate‑table.  
· model_version: String specifying the PEDSnet data‑model version to be transformed (e.g., “2.3.0”).  
· table: Comma‑separated list of table names that should be transformed (e.g., “person,measurement”).  
· undo: Boolean flag; if True the function skips the transformation step and exits successfully.  
· dburi: PostgreSQL URI that identifies the target database (e.g., “postgresql://user:pass@host/dbname”).

**Code Description**  
1. Initialise a local variable `password` to `None`.  
2. If `pwprompt` is true, invoke `click.prompt` to request the database password from the user, hiding the input; the entered value is stored in `password`.  
3. Build a libpq‑compatible connection string by calling `make_conn_str(dburi, searchpath, password)`. This function parses the supplied URI, injects the optional `search_path` and the password (if provided), and returns a string suitable for `psycopg2`. The resulting string is assigned to `conn_str`.  
4. If the `undo` flag is false, import `run_site_transformation` from `pedsnetdcc.transform_runner` and invoke it with the arguments `(conn_str, model_version, site, searchpath, table, force)`. The call returns a Boolean `success` indicating whether the transformation completed without raising an exception.  
5. If `success` is false, terminate the process with exit status 1 (`sys.exit(1)`).  
6. If the transformation succeeded (or was skipped because `undo` is true), terminate the process with exit status 0 (`sys.exit(0)`).  

The function therefore acts as a thin CLI wrapper: it gathers user input, prepares the database connection string, delegates the heavy lifting to `run_site_transformation`, and translates the outcome into appropriate process exit codes.

**Note**  
- The `undo` flag bypasses the transformation entirely; the command will still exit with status 0.  
- Password handling is optional; if `pwprompt` is false and no password is embedded in `dburi`, the connection string will not contain a password, relying on other authentication methods (e.g., `.pgpass`).  
- `table` must be a clean, comma‑separated list without surrounding whitespace; the downstream runner splits the string on commas.  
- The function relies on `make_conn_str` to correctly embed the `search_path` into the connection string’s `options` parameter; an incorrect `searchpath` value may cause the transformation to run against the wrong schema.  
- Exit codes follow the conventional pattern: `0` for success, `1` for failure, allowing the CLI to be used in scripts and automation pipelines.  
- No explicit error handling is performed inside `site_transform`; any exception raised by `run_site_transformation` propagates up and results in a non‑zero exit status via the `if not success` check.  
## FunctionDef index_transform(pwprompt, searchpath, site, force, model_version, table, undo, dburi)
**index_transform**: The function of index_transform is to serve as the command‑line entry point that prepares a database connection string and invokes the index‑only transformation workflow for a specified PEDSnet model version.

**parameters**  
· pwprompt (bool): If True, the function will prompt the user to enter the database password interactively (input is hidden).  
· searchpath (str): The PostgreSQL schema search_path that should be used for the transformation; it is also incorporated into the backup schema name.  
· site (str): Identifier of the site (e.g., “stlouis”) that is passed through to the transformation runner.  
· force (bool): When True, benign errors such as “schema already exists” or “table already exists” are ignored during schema creation and table materialisation.  
· model_version (str): The PEDSnet model version string (e.g., “2.3.0”) that determines which transformation rules are applied.  
· table (str): A comma‑separated list of target table names to be transformed (e.g., “person,measurement”).  
· undo (bool): Currently unused in the implementation; present for future extension.  
· dburi (str): A PostgreSQL URI that identifies the target database (e.g., `postgresql://user:pass@host:port/dbname`).  

**Code Description**  
1. **Password handling** – The variable `password` is initialised to `None`. If `pwprompt` is true, the function uses `click.prompt` to request the database password from the user, hiding the input. The retrieved password is stored in `password`.  

2. **Connection string construction** – The helper `make_conn_str` (from `pedsnetdcc.utils`) is called with the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. This function parses the URI, merges or overrides the `options` parameter to include the desired `search_path`, and returns a libpq‑compatible connection string (`conn_str`).  

3. **Transformation execution** – If the `undo` flag is false (the only current path), the function imports `run_index_transformation` from `pedsnetdcc.transform_runner`. It then calls `run_index_transformation` with the following arguments:  
   - `conn_str` – the connection string built in step 2,  
   - `model_version` – the model version supplied by the caller,  
   - `site` – the site identifier,  
   - `searchpath` – the schema search_path,  
   - `table` – the comma‑separated list of target tables,  
   - `force` – the boolean flag controlling error suppression.  

   The called function performs the full index‑only transformation workflow, including creation of a temporary transformed schema, materialisation of index tables, conversion of those tables to logged state, and atomic swapping of the original tables with the transformed ones while backing up the originals. It returns a boolean `success` indicating whether the workflow completed without raising an exception.  

4. **Exit handling** – After the call, the code checks the `success` flag. If `success` is falsy, the process terminates with `sys.exit(1)` to indicate failure. If the transformation succeeded, the process exits cleanly with `sys.exit(0)`.  

5. **Imports and side effects** – The function relies on the following external modules:  
   - `click` for interactive password prompting,  
   - `sys` for process termination,  
   - `make_conn_str` for building the connection string,  
   - `run_index_transformation` for the actual transformation logic.  

   No other side effects are performed within `index_transform`.  

**Note**  
- The `undo` parameter is presently ignored; callers should set it to `False` unless future versions add undo functionality.  
- The `table` argument must be a clean, comma‑separated string without surrounding whitespace; malformed input may cause the downstream transformation runner to raise errors.  
- When `pwprompt` is False, any password embedded in the `dburi` is used; if both the URI and the `password` argument are present, the explicit `password` argument overrides the URI value.  
- The function exits the entire Python process after the transformation, making it suitable only as a top‑level CLI command and not for use as a library function within a longer‑running application.  
- Errors raised inside `run_index_transformation` (e.g., database connection failures, schema‑swap errors) propagate back as a false `success` value, causing the CLI to exit with status 1.  
- Because the function calls `make_conn_str` with the provided `searchpath`, the resulting connection string includes an `options` parameter that sets `search_path` for the session; this ensures that all subsequent SQL executed by the transformation runner operates within the intended schema context.
## FunctionDef id_transform(pwprompt, searchpath, site, name, force, model_version, table, undo, dburi)
**id_transform**: The function of id_transform is to invoke the PEDSnet ID‑mapping transformation workflow, converting data from a specified PEDSnet model version into the DCC format and handling command‑line interaction and database connection preparation.

**parameters**:  
· **pwprompt**: Boolean flag indicating whether the user should be prompted for the database password on the command line.  
· **searchpath**: String containing the PostgreSQL schema search_path that will be applied to the connection (e.g., “dcc_pedsnet, public”).  
· **site**: Identifier of the site (e.g., “stlouis”) required by the ID‑mapping logic.  
· **name**: Base name of the identifier set to be used in the transformation (e.g., “dcc” or “onco”).  
· **force**: Boolean flag passed to the transformation runner; when True, benign errors such as duplicate‑table creation are ignored.  
· **model_version**: String specifying the PEDSnet model version (e.g., “2.3.0”) that determines which metadata files are consulted.  
· **table**: Comma‑separated list of table names that should be transformed.  
· **undo**: Boolean flag; when True the function skips the transformation step (intended for future undo functionality).  
· **dburi**: PostgreSQL URI that identifies the target database (e.g., “postgresql://user@host/dbname”).

**Code Description**:  
1. **Password handling** – The variable `password` is initialized to `None`. If `pwprompt` is true, the function uses `click.prompt` to request the database password from the user, hiding the input.  
2. **Connection string construction** – `make_conn_str` (from *pedsnetdcc/utils.py*) is called with the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. This helper parses the URI, merges or overrides the `search_path` and password, and returns a libpq‑compatible connection string (`conn_str`).  
3. **Conditional execution of the transformation** – If `undo` is false, the function imports `run_id_transformation` from *pedsnetdcc/transform_runner.py* and calls it with the prepared arguments:  
   - `conn_str` – the libpq connection string,  
   - `model_version` – the PEDSnet model version,  
   - `site` – the site label,  
   - `searchpath` – the original schema search_path,  
   - `table` – the target tables,  
   - `name` – the identifier name,  
   - `force` – the force flag.  
   The called function performs the full ID‑mapping workflow (schema creation, table transformation, backup, and cleanup) and returns a Boolean `success`.  
4. **Process termination** – After the call, the function checks the `success` flag. If the transformation reported failure (`success` is false), the process exits with status 1 (`sys.exit(1)`). If the transformation succeeded or was skipped (when `undo` is true), the process exits cleanly with status 0 (`sys.exit(0)`).  

**Relationship with callees**:  
- **run_id_transformation**: Performs the heavy‑lifting of the ID‑mapping transformation, including creation of a temporary transformed schema, execution of the `_transform_id` helper, moving original tables to a backup schema, and swapping in the transformed tables. All logging, error handling, and transactional guarantees are implemented there.  
- **make_conn_str**: Generates a proper libpq connection string from the supplied URI, optionally overriding the search_path and password. This ensures that the downstream transformation runner receives a correctly formatted connection descriptor.  

**Note**:  
- The function exits the Python interpreter via `sys.exit`; therefore any code placed after the call will not be executed.  
- When `undo` is true, the transformation step is skipped, but the current implementation still reaches the `if not success:` check, which would raise a `NameError` because `success` is undefined. In practice `undo` should be used only when the corresponding undo logic is implemented.  
- The `force` flag is propagated unchanged to `run_id_transformation`; it only influences how duplicate‑table or duplicate‑schema errors are treated during the transformation.  
- The function relies on the external `click` library for password prompting and on the standard `sys` module for process termination.  
- Proper privileges are required for the database user: ability to create schemas, create tables, alter tables, and drop schemas.  
- All input parameters are passed verbatim to the transformation runner; validation of values (e.g., ensuring `table` contains valid table names) is performed downstream.
## FunctionDef run_target_transform(pwprompt, searchpath, site, name, force, model_version, table, undo, dburi)
**run_target_transform**: The function of run_target_transform is to serve as the command‑line entry point that prepares a database connection string and invokes the target‑table transformation workflow.

**parameters**  
· **pwprompt** (bool): If True, the user is prompted on the console for the database password; the entered password is hidden as it is typed.  
· **searchpath** (str): The PostgreSQL schema search_path that identifies the primary schema containing the tables to be transformed.  
· **site** (str): Identifier of the PEDSnet site (e.g., “stlouis”) that is required by the transformation logic.  
· **name** (str): The name of the identifier column to be added or used by the transformation (e.g., “dcc”).  
· **force** (bool): When True, benign errors such as duplicate‑schema or duplicate‑table are ignored during the transformation.  
· **model_version** (str): The PEDSnet data‑model version (e.g., “2.3.0”) that determines which transformation rules are applied.  
· **table** (str): Comma‑separated list of target tables that should be transformed.  
· **undo** (bool): If True, the transformation step is skipped; the function currently does not implement an undo operation.  
· **dburi** (str): PostgreSQL URI that specifies the database to connect to (e.g., `postgresql://user@host/dbname`).

**Code Description**  
1. **Password handling** – The variable `password` is initialised to `None`. If `pwprompt` is true, the function uses `click.prompt` to request the database password from the user, hiding the input. The entered value is stored in `password`.  
2. **Connection string construction** – `make_conn_str` (from `pedsnetdcc.utils`) is called with the supplied `dburi`, the `searchpath`, and the possibly‑provided `password`. This helper parses the URI and assembles a libpq‑compatible connection string that includes the desired `search_path` option. The resulting string is assigned to `conn_str`.  
3. **Conditional execution of the transformation** – If `undo` is false, the function imports `run_target_transformation` from `pedsnetdcc.transform_runner` and calls it with the arguments:  
   - `conn_str` – the connection string built in step 2,  
   - `model_version` – the model version supplied by the caller,  
   - `site` – the site identifier,  
   - `searchpath` – the original search_path,  
   - `table` – the comma‑separated target‑table list,  
   - `name` – the identifier column name,  
   - `force` – the force flag.  
   The return value of `run_target_transformation` (a boolean indicating success) is stored in `success`.  
4. **Result handling** – After the conditional block, the function checks `if not success:`. If the transformation reported failure (`success` is `False`), the process terminates with `sys.exit(1)`. If the transformation succeeded (or if `undo` was true and `success` was never set), the function exits cleanly with `sys.exit(0)`.  

**Relationship with other objects**  
- **Callees**:  
  * `make_conn_str` (pedsnetdcc.utils) – builds the libpq connection string from the URI, optional search_path, and optional password.  
  * `run_target_transformation` (pedsnetdcc.transform_runner) – performs the full transformation workflow, including schema creation, table generation, logging, primary‑key addition, and schema swap.  
  * `click.prompt` – used for interactive password entry when `pwprompt` is true.  
  * `sys.exit` – terminates the CLI process with the appropriate exit code.  
- **Callers**: This function is invoked by the Click‑based command‑line interface defined in `pedsnetdcc/main.py`. The CLI parses command‑line options, maps them to the parameters listed above, and then calls `run_target_transform` to carry out the operation.

**Note**  
- The `undo` flag currently only suppresses the call to `run_target_transformation`; no actual undo logic is implemented. Because `success` is defined only inside the `if not undo:` block, invoking the function with `undo=True` will raise an `UnboundLocalError` when the later `if not success:` test is reached. callers should therefore set `undo` to `False` or avoid using this flag until an undo implementation is added.  
- Password prompting occurs only when `pwprompt` is true; otherwise any password embedded in `dburi` is used, or no password is sent if none is present.  
- The function terminates the Python process via `sys.exit`; it does not return a value to the caller. This behavior is appropriate for a CLI entry point but makes the function unsuitable for reuse as a library call without modification.  
- All transformation logic, error handling, and logging are delegated to `run_target_transformation`; `run_target_transform` itself performs only orchestration and exit‑code handling.  
- The `searchpath` argument is passed unchanged to both `make_conn_str` and `run_target_transformation`; it must be a valid PostgreSQL search_path string (comma‑separated schema names) for the target database.  
- The function relies on the `click` library for prompting; ensure that the runtime environment includes this dependency.
## FunctionDef merge(pwprompt, addsites, force, notable, nolog, nopk, nonull, noidx, nodrop, norep, nofk, novac, model_version, undo, dburi)
**merge**: The function of merge is to coordinate the merging of site‑specific PEDSnet data into the central DCC schema, or to undo a previous merge by clearing the DCC data.

**parameters**  
· pwprompt (bool): If True, the function will prompt the user for the database password on the command line.  
· addsites (str): Comma‑separated list of site identifiers to be added to the merge operation.  
· force (bool): When True, benign errors (e.g., “object already exists” or “object does not exist”) are ignored during DDL execution.  
· notable (bool): When True, the creation of merged tables is skipped; only post‑merge steps are performed.  
· nolog (bool): When True, tables are left in their default UNLOGGED state after the merge.  
· nopk (bool): When True, primary‑key constraints are not added after the merge.  
· nonull (bool): When True, NOT‑NULL constraints are not applied after the merge.  
· noidx (bool): When True, index creation is skipped after the merge.  
· nodrop (bool): When True, unused indexes are not dropped after the merge.  
· norep (bool): When True, index‑replacement tables are not created after the merge.  
· nofk (bool): When True, foreign‑key constraints are not added after the merge.  
· novac (bool): When True, VACUUM (ANALYZE) is not run after the merge.  
· model_version (str): PEDSnet model version (e.g., “2.2.0”) that determines the schema metadata to use.  
· undo (bool): When True, the function performs an undo operation by clearing the DCC schema instead of merging.  
· dburi (str): PostgreSQL URI used to build a libpq‑style connection string for the target database.

**Code Description**  
1. **Password handling** – If `pwprompt` is True, the function prompts the user for a password using `click.prompt` with hidden input. The entered password is stored in the local variable `password`; otherwise `password` remains `None`.  

2. **Connection string construction** – The helper `make_conn_str` (defined in `pedsnetdcc/utils.py`) is called with the supplied `dburi`, an empty search_path (the function does not set a search_path here), and the optional `password`. The result, `conn_str`, is a libpq‑compatible connection string that will be passed to downstream functions.  

3. **Operation selection** – The boolean `undo` determines which sub‑routine is invoked:  
   * If `undo` is **False**, the function imports `merge_site_data` from `pedsnetdcc.merge_site_data` and calls it with all merge‑related flags (`model_version`, `conn_str`, `addsites`, `force`, `notable`, `nolog`, `nopk`, `nonull`, `noidx`, `nodrop`, `norep`, `nofk`, `novac`).  
   * If `undo` is **True**, the function imports `clear_dcc_data` from the same module and calls it with `model_version`, `conn_str`, and `force`.  

4. **Result handling** – The called sub‑routine returns a boolean `success`. If `success` is falsy, the function terminates the process with `sys.exit(1)`. If the call succeeds, the function exits cleanly with `sys.exit(0)`. No additional processing is performed after the sub‑routine returns.  

5. **Callee relationships** –  
   * `make_conn_str` builds the PostgreSQL connection string from the URI and optional password.  
   * `merge_site_data` performs the full merge workflow: creating DCC tables from site schemas, applying logging, primary‑key, NOT‑NULL, index, foreign‑key, and vacuum steps, all controlled by the same flag set passed through `merge`.  
   * `clear_dcc_data` drops all non‑vocabulary tables from the DCC schema, effectively undoing a previous merge. Both callees log their progress, handle errors, and return a boolean indicating success.  

**Note**  
- The function does not perform any validation of the flag combinations; it simply forwards them to the chosen sub‑routine.  
- When `undo` is True, all flags except `force` are ignored because `clear_dcc_data` only accepts `model_version`, `conn_str`, and `force`.  
- The connection string is built without a search_path; the callee (`merge_site_data` or `clear_dcc_data`) sets the appropriate search_path internally before executing any SQL.  
- The function terminates the entire Python process via `sys.exit`; it is intended to be used as a CLI entry point rather than as a library call.  
- Errors raised inside the callee are propagated to this function; if a callee returns `False` (which currently never occurs because errors raise exceptions), the process exits with status 1.  
- Prompting for a password (`pwprompt`) is optional; if the URI already contains a password, providing `pwprompt=True` will override it with the user‑entered value.
## FunctionDef merge_schema(pwprompt, schema, altname, skipsites, addsites, force, notable, nolog, nopk, nonull, noidx, nodrop, norep, nofk, novac, model_version, undo, dburi)
**merge_schema**: The function of merge_schema is to coordinate the merging of site‑level PEDSnet data into a single DCC schema, or to undo a previous merge by clearing the DCC schema.

**parameters**  
· pwprompt (bool): If True, the function prompts the user for a database password on the command line.  
· schema (str): Name of the target DCC schema that will receive the merged data (e.g., `dcc_pedsnet`).  
· altname (str): Alternate naming pattern for site schemas; an empty string uses the default pattern `{site}_pedsnet`.  
· skipsites (str): Comma‑separated list of site identifiers that must be excluded from the merge.  
· addsites (str): Comma‑separated list of additional site identifiers to include even if they are not in the default site list.  
· force (bool): When True, benign errors such as “object already exists” (during merge) or “object does not exist” (during undo) are ignored.  
· notable (bool): When True, the data‑merge step is skipped; only post‑merge actions are performed.  
· nolog (bool): When True, the step that converts tables to LOGGED is omitted.  
· nopk (bool): When True, primary‑key creation is omitted.  
· nonull (bool): When True, NOT NULL constraints are omitted.  
· noidx (bool): When True, index creation is omitted.  
· nodrop (bool): When True, dropping of unneeded indexes is omitted.  
· norep (bool): When True, creation of index‑replacement tables is omitted.  
· nofk (bool): When True, foreign‑key creation is omitted.  
· novac (bool): When True, the VACUUM ANALYZE step is omitted.  
· model_version (str): PEDSnet model version (e.g., `2.2.0`).  
· undo (bool): When True, the function performs an undo operation (clears the DCC schema) instead of a forward merge.  
· dburi (str): PostgreSQL URI used to build a libpq connection string (e.g., `postgresql://user@host/dbname`).  

**Code Description**  
1. **Password handling** – If `pwprompt` is True, the function uses `click.prompt` to read a password from the terminal without echoing it. The entered password is stored in the local variable `password`; otherwise `password` remains `None`.  
2. **Connection string construction** – The helper `make_conn_str` from `pedsnetdcc.utils` is called with `dburi`, an empty search_path (the function does not override the schema here), and the possibly‑prompted `password`. The result, `conn_str`, is a libpq‑style connection string suitable for `psycopg2`.  
3. **Branch on undo flag**  
   * **Forward merge (`undo` is False)** – The function imports `merge_data_to_schema` from `pedsnetdcc.merge_site_data`. It then calls `merge_data_to_schema` with all command‑line options (`model_version`, `conn_str`, `schema`, `altname`, `skipsites`, `addsites`, `force`, `notable`, `nolog`, `nopk`, `nonull`, `noidx`, `nodrop`, `norep`, `nofk`, `novac`). The return value (`success`) indicates whether the merge completed without fatal errors.  
   * **Undo (`undo` is True)** – The function imports `clear_schema_data` from the same module and calls it with `model_version`, `conn_str`, `schema`, and `force`. This drops all non‑vocabulary tables from the target DCC schema. The return value (`success`) reflects the outcome of the clear operation.  
4. **Exit handling** – If `success` is falsy, the function terminates the process with exit code 1 (`sys.exit(1)`), signalling an error to the operating system. If `success` is truthy, the function exits with code 0 (`sys.exit(0)`), indicating successful completion. No further Python code is executed after the exit call.  

**Relationship with callees**  
* `make_conn_str` (pedsnetdcc.utils) – builds the PostgreSQL connection string from the supplied URI and optional password.  
* `merge_data_to_schema` (pedsnetdcc.merge_site_data) – performs the actual data‑merge, including creation of tables, constraints, indexes, foreign keys, and vacuuming, based on the many boolean flags passed through `merge_schema`.  
* `clear_schema_data` (pedsnetdcc.merge_site_data) – drops all non‑vocabulary tables from the target schema when an undo operation is requested.  

Both `merge_data_to_schema` and `clear_schema_data` rely on additional utilities (e.g., `conn_str_with_search_path`, `stock_metadata`, `StatementSet`) to execute SQL statements in parallel and to log progress. `merge_schema` itself does not contain any SQL logic; it merely prepares the connection string, forwards the user‑specified options to the appropriate helper, and returns an appropriate process exit status.

**Note**  
* The function terminates the Python interpreter with `sys.exit`; therefore any code placed after the call to `merge_schema` will never run.  
* When `pwprompt` is True, the password prompt occurs before any database connection is attempted; if the user aborts the prompt, a `KeyboardInterrupt` will propagate.  
* The `undo` flag is mutually exclusive with all merge‑related flags; providing both a forward‑merge flag (e.g., `force`) and `undo=True` will still result in a clear operation, but the extra flags are ignored.  
* All boolean flags are passed through unchanged to the underlying merge or clear functions; their semantics are defined in the documentation of `merge_data_to_schema` and `clear_schema_data`.  
* The function assumes that the user encoded in `dburi` (or entered via the password prompt) has sufficient privileges to create, alter, and drop tables in the target schema.  
* No transaction is used; each DDL statement executed by the callee runs in its own autocommit context. Consequently, a failure in one statement does not automatically roll back previously executed statements.
## FunctionDef split_measurement(pwprompt, searchpath, truncate, view, model_version, limit, owner, skip_split, skip_index, skip_fk, skip_not_null, dburi)
**split_measurement**: The function of split_measurement is to orchestrate the splitting of the core *measurement* table into three specialized tables (anthro, labs, vitals) by building a database connection string, optionally prompting for a password, invoking the core split logic, and exiting with an appropriate status code.

**parameters**:  
· **pwprompt** (bool): When True, the function prompts the user on the command line for the database password; the entered password is hidden as it is typed.  
· **searchpath** (str): The PostgreSQL schema search_path that will be embedded in the connection string and used as the target schema for all DDL operations.  
· **truncate** (bool): Passed through to the split routine; if True the original *measurement* table will be truncated after the split tables are created.  
· **view** (bool): Passed through to the split routine; if True a unified view named *measurements* will be created after the split.  
· **model_version** (str): The PEDSnet data‑model version (e.g., “2.3.0”). It is forwarded to the split routine for logging and vacuuming.  
· **limit** (bool): Forwarded to the split routine; when True permissions are granted only to the role supplied in *owner*.  
· **owner** (str): The database role that receives permissions when *limit* is True.  
· **skip_split** (bool): Forwarded to the split routine; if True the creation of the three split tables is skipped.  
· **skip_index** (bool): Forwarded to the split routine; if True index creation on the split tables is omitted.  
· **skip_fk** (bool): Forwarded to the split routine; if True foreign‑key constraints are omitted.  
· **skip_not_null** (bool): Forwarded to the split routine; if True NOT‑NULL constraints are omitted.  
· **dburi** (str): A PostgreSQL URI that identifies the target database (e.g., `postgresql://user[:password]@host:port/dbname?...`). It is converted to a libpq connection string.

**Code Description**:  
1. **Initialize password variable** – `password` is set to `None`. This placeholder will hold a password only if the user is prompted.  
2. **Conditional password prompt** – If *pwprompt* is True, the function uses `click.prompt` with `hide_input=True` to request the database password from the terminal. The entered value is stored in `password`.  
3. **Build connection string** – The helper `make_conn_str` (defined in `pedsnetdcc/utils.py`) is called with three arguments: the raw *dburi*, the *searchpath*, and the possibly‑prompted *password*. It returns a libpq‑compatible connection string (`conn_str`) that incorporates the search_path via the `options` parameter and overrides any password present in the URI when a password was supplied.  
4. **Import split routine** – The function imports `split_measurement_table` from `pedsnetdcc.split_measurement`. Import is performed locally to avoid unnecessary import overhead when the command is not executed.  
5. **Execute split logic** – `split_measurement_table` is invoked with the constructed `conn_str` and all flags/parameters that control the split operation (`truncate`, `view`, `model_version`, `searchpath`, `limit`, `owner`, `skip_split`, `skip_index`, `skip_fk`, `skip_not_null`). The routine performs the actual DDL work (table creation, constraints, indexes, permissions, optional truncation, view creation, and vacuum). It returns a boolean `success`.  
6. **Handle return status** – If `success` is falsy, the function terminates the process with exit code 1 (`sys.exit(1)`), indicating an error occurred during the split.  
7. **Normal termination** – If `success` is truthy, the function exits cleanly with code 0 (`sys.exit(0)`).

**Relationship with callees**:  
- **make_conn_str** (utility) converts the user‑supplied URI into a libpq connection string, handling search_path injection and password overriding.  
- **click.prompt** provides interactive password entry when *pwprompt* is enabled.  
- **split_measurement_table** (core implementation) carries out the detailed split steps; it is the primary workhorse called by this wrapper.  
- **sys.exit** is used to propagate the success or failure of the operation to the operating system, making the command‑line interface behave like a typical Unix utility.

**Note**:  
- The function does not perform any validation of the flag combinations; all validation is delegated to `split_measurement_table`.  
- Because the function terminates the process with `sys.exit`, any code placed after the call will never be executed.  
- The password prompt is only triggered when *pwprompt* is True; otherwise the password must be present in the URI or supplied via other authentication mechanisms (e.g., `.pgpass`).  
- The `searchpath` argument is mandatory for correct schema targeting; omitting it may cause DDL statements to be executed in an unintended schema.  
- Errors raised inside `split_measurement_table` are not caught here; they propagate up and cause the function to receive a falsy return value, which then leads to a non‑zero exit status.  
- This wrapper is intended for use as a Click command entry point; it assumes the surrounding CLI infrastructure handles argument parsing and help generation.
## FunctionDef run_derivations(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, add_age, neg_ids, skip_calc, no_ids, no_concept, table, out_table_bmi, out_table_bmiz, out_table_ht_z, out_table_wt_z, person, model_version, idname, max_time, dburi)
**run_derivations**: The function of run_derivations is to execute the full set of PEDSnet derivation steps (BMI, BMI‑Z, Height‑Z, Weight‑Z, Drug Era, and Condition Era) for a given site and database connection.

**parameters**: The parameters of this Function.  
· pwprompt: bool – if True the user is prompted for the database password on the command line.  
· searchpath: str – PostgreSQL schema search_path that will be used for all DDL statements.  
· site: str – identifier of the site for which the derivations are being run (e.g., “stlouis”).  
· copy: bool – if True the derived rows are copied into the canonical `measurement` table after each derivation.  
· noids: bool – if True the measurement_id column is not populated (IDs are omitted).  
· noindexes: bool – if True no indexes are created on the derived tables.  
· noconcept: bool – if True concept_name columns are not populated from the OMOP vocabulary.  
· add_age: bool – if True the column `measurement_age_in_months` is calculated and filled.  
· neg_ids: bool – if True the ID sequences generated for the derived rows produce negative identifiers.  
· skip_calc: bool – if True the external calculation tools (BMI, Z‑Score) are not invoked; only table creation and post‑processing are performed.  
· no_ids: bool – passed to the era derivations; if True the era tables are not assigned identifiers.  
· no_concept: bool – passed to the era derivations; if True concept_name columns are not populated for era tables.  
· table: str – name of the source measurement table (normally “measurement” or “measurement_anthro”).  
· out_table_bmi: str – name of the table that will receive the BMI rows.  
· out_table_bmz: str – name of the table that will receive the BMI‑Z rows.  
· out_table_ht_z: str – name of the table that will receive the Height‑Z rows.  
· out_table_wt_z: str – name of the table that will receive the Weight‑Z rows.  
· person: str – name of the person table (required for Z‑Score configuration files).  
· model_version: str – PEDSnet data‑model version (e.g., “2.3.0”). Used for vacuuming and metadata lookup.  
· idname: str – prefix for the site‑specific last‑id tracking table (e.g., “dcc”).  
· max_time: str – maximum allowed interval between a height and a weight measurement, expressed as days (or the literal “60”).  
· dburi: str – PostgreSQL URI that identifies the target database (e.g., `postgresql://user:pass@host/dbname`).

**Code Description**:  

1. **Password handling** – If `pwprompt` is True the function prompts the user for the database password using `click.prompt`. The entered password is stored in the local variable `password`; otherwise `password` remains `None`.  

2. **Connection string construction** – The helper `make_conn_str` is called with `dburi`, the supplied `searchpath`, and the possibly‑prompted `password`. It returns a libpq‑style connection string (`conn_str`) that will be used for every subsequent database operation.  

3. **Flag normalisation** – Boolean flags controlling optional steps are converted to explicit True/False values:  
   * `ids` is True unless `noids` is True.  
   * `indexes` is True unless `noindexes` is True.  
   * `concept` is True unless `noconcept` is True.  
   * `age` is True only when `add_age` is True.  

4. **BMI derivation** –  
   * A temporary configuration file name is built as `{site}_bmi_temp.conf`.  
   * `run_bmi_calc` (from `pedsnetdcc.bmi`) is invoked with the configuration file, the connection string, the site identifier, and all flags (`copy`, `ids`, `indexes`, `concept`, `age`, `neg_ids`, `skip_calc`, `table`, `out_table_bmi`, `password`, `searchpath`, `model_version`, `idname`, `max_time`).  
   * If `run_bmi_calc` returns False, the process terminates with `sys.exit(1)`.  

5. **BMI‑Z derivation** –  
   * A temporary configuration file name is built as `{site}_bmiz_temp.conf`.  
   * `run_z_calc` (from `pedsnetdcc.z_score`) is called with `z_type='bmiz'` and the appropriate arguments (`out_table_bmz`, `person`, etc.).  
   * Failure causes an immediate exit (`sys.exit(1)`).  

6. **Height‑Z derivation** –  
   * Configuration file `{site}_htz_temp.conf` is created.  
   * `run_z_calc` is called with `z_type='ht_z'` and `out_table_ht_z`.  
   * Failure leads to `sys.exit(1)`.  

7. **Weight‑Z derivation** –  
   * Configuration file `{site}_wtz_temp.conf` is created.  
   * `run_z_calc` is called with `z_type='wt_z'` and `out_table_wt_z`.  
   * Failure leads to `sys.exit(1)`.  

8. **Era derivations** – The module `pedsnetdcc.era` is imported and `run_era` is executed three times:  
   * First with `era_type='drug'`.  
   * Second with `era_type='drug_scdf'`.  
   * Third with `era_type='condition'`.  
   * Each call receives the connection string, site, copy flag, `neg_ids`, `no_ids`, `no_concept`, `searchpath`, `model_version`, and `idname`.  
   * If any call returns False, the script exits with status 1.  

9. **Successful completion** – After all derivations have run without error, the function terminates the process with `sys.exit(0)`.  

**Relationship with callees**:  
* `run_bmi_calc` performs the complete BMI workflow (configuration generation, table creation, BMI calculation, optional indexing, ID assignment, concept enrichment, age calculation, optional copy, and vacuum).  
* `run_z_calc` performs the analogous workflow for the three Z‑Score derivations, handling configuration, table creation, external Z‑Score tool execution, overflow handling for BMI‑Z, optional indexing, ID assignment, concept enrichment, age calculation, optional copy, and vacuum.  
* `run_era` orchestrates the derivation of drug, drug_scdf, and condition eras, including dropping constraints, running the era SQL, optional ID assignment, concept enrichment, copying to the central schema, renumbering, primary‑key addition, and vacuum.  
* `make_conn_str` builds the libpq connection string from the supplied URI, optional search_path, and optional password.  

All three derivation functions share a common pattern: they create a temporary configuration file (unless `skip_calc`), create a LIKE‑table based on the core `measurement` table, optionally modify the table (e.g., enlarge `value_as_number` for BMI‑Z), run an external binary (`derive_bmi` or `derive_z`), perform post‑processing steps controlled by boolean flags, and finally vacuum the result table. `run_derivations` simply strings these steps together in the required order for a complete site‑wide run.

**Note**:  
* The function uses `sys.exit` to signal success (0) or failure (1); callers should be aware that the Python interpreter will terminate immediately after the call.  
* Password prompting occurs only when `pwprompt` is True; otherwise the password must be present in the URI or supplied via the `password` argument of `make_conn_str`.  
* All boolean flags are inverted from the “no‑” command‑line options (`noids`, `noindexes`, `noconcept`) to the positive flags expected by the underlying derivation functions.  
* The `max_time` argument is passed unchanged to `run_bmi_calc`; that function converts the value to seconds internally.  
* If any of the underlying derivation functions raise an exception instead of returning False, the exception propagates and the script will exit with a non‑zero status due to the uncaught exception.  
* The function assumes that the temporary configuration files can be written to the container’s `/app` directory and that the external BMI and Z‑Score binaries are available in the execution environment.  
* No transaction is used to wrap the whole sequence; each derivation runs in its own autocommit context. Partial results may remain in the database if a later step fails.
## FunctionDef run_bmi(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, add_age, neg_ids, skip_calc, table, out_table, model_version, idname, max_time, dburi)
**run_bmi**: The function of run_bmi is to orchestrate the execution of the BMI derivation workflow by preparing connection information, configuring optional processing flags, invoking the core BMI calculation routine, and terminating the process with an appropriate exit status.

**parameters**:  
· pwprompt (bool): If True, the user is prompted on the command line for the database password; the input is hidden.  
· searchpath (str): PostgreSQL schema search_path that will be incorporated into the connection string.  
· site (str): Identifier of the site for which the BMI derivation is being performed; used for logging and for naming the temporary configuration file.  
· copy (bool): When True, the derived BMI rows will be copied into the canonical `dcc_pedsnet.measurement_anthro` table after all other processing steps.  
· noids (bool): If True, the step that generates and assigns measurement_id values is skipped.  
· noindexes (bool): If True, creation of indexes on the output BMI table is skipped.  
· noconcept (bool): If True, the step that populates human‑readable concept_name columns is omitted.  
· add_age (bool): If True, the column `measurement_age_in_months` is calculated and filled for each BMI row.  
· neg_ids (bool): If True, measurement_id values are generated as negative identifiers; otherwise positive identifiers are used.  
· skip_calc (bool): If True, the external BMI calculation tool is not invoked; only table creation and post‑processing steps are performed.  
· table (str): Name of the source measurement table (e.g., `measurement` or `measurement_anthro`).  
· out_table (str): Name of the table that will receive the derived BMI rows (default is `measurement_bmi`).  
· model_version (str): PEDSnet data model version (e.g., `2.3.0`); passed through to downstream functions for vacuuming and metadata lookup.  
· idname (str): Prefix used for the site‑specific last‑id tracking table (e.g., `dcc` or `onco`).  
· max_time (str): Maximum allowed interval between a height and a weight measurement, expressed as a number of days; forwarded to the BMI calculation configuration.  
· dburi (str): PostgreSQL URI that identifies the target database (e.g., `postgresql://user:pass@host/dbname?...`).  

**Code Description**:  
1. **Password handling** – The variable `password` is initialised to `None`. If `pwprompt` is True, the function uses `click.prompt` to request the database password from the user, hiding the input. The obtained password (or `None` if not prompted) will later be supplied to the connection‑string builder.  
2. **Connection string construction** – `make_conn_str` (from `pedsnetdcc.utils`) is called with the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. This produces a libpq‑compatible connection string (`conn_str`) that encodes host, port, database name, user, password, and any `options` such as the search_path.  
3. **Temporary configuration file name** – The function builds a temporary BMI configuration filename by concatenating the `site` argument with the suffix `_bmi_temp.conf`. This file will be created by the downstream BMI routine.  
4. **Flag preparation** – Boolean flags that control optional steps are derived from the negated command‑line options:  
   - `ids` defaults to True; set to False when `noids` is True.  
   - `indexes` defaults to True; set to False when `noindexes` is True.  
   - `concept` defaults to True; set to False when `noconcept` is True.  
   - `age` defaults to False; set to True when `add_age` is True.  
   These flags are passed directly to the core BMI function.  
5. **Import of core routine** – The function imports `run_bmi_calc` from `pedsnetdcc.bmi`. This import is performed locally to avoid unnecessary loading when the command is not invoked.  
6. **Invocation of the core BMI workflow** – `run_bmi_calc` is called with the following arguments, in order:  
   - `config_file` (the temporary config filename)  
   - `conn_str` (the constructed connection string)  
   - `site`  
   - `copy`  
   - `ids`  
   - `indexes`  
   - `concept`  
   - `age`  
   - `neg_ids`  
   - `skip_calc`  
   - `table`  
   - `out_table`  
   - `password` (may be `None`; the core routine will extract it from `conn_str` if needed)  
   - `searchpath`  
   - `model_version`  
   - `idname`  
   - `max_time`  
   The core routine performs the full BMI derivation pipeline (configuration file creation, output table creation, optional external calculation, indexing, ID assignment, concept enrichment, age calculation, optional copy, and vacuum).  
7. **Result handling** – The return value of `run_bmi_calc` is stored in `success`. If `success` is falsy, the function terminates the process with exit status 1 (`sys.exit(1)`), indicating failure. If the call succeeds, the function exits with status 0 (`sys.exit(0)`). No further processing occurs after the exit call.  

**Relationship with other project components**:  
- **Callees**: `make_conn_str` (utility for building a libpq connection string) and `run_bmi_calc` (the comprehensive BMI derivation engine located in `pedsnetdcc/bmi.py`). Both are essential; the former prepares database connectivity, while the latter executes the actual derivation steps.  
- **Callers**: `run_bmi` is the entry point for the `pedsnetdcc` command‑line interface when the user selects the “run‑bmi” sub‑command. It is invoked indirectly by the CLI parser defined in `pedsnetdcc/main.py`. No other internal modules call `run_bmi` directly.  

**Note**:  
- The function relies on `click` for password prompting; ensure the `click` library is available in the execution environment.  
- All boolean flags default to enabling the corresponding step; the command‑line options are designed as “no‑” switches (e.g., `--noids`) that invert the default behaviour.  
- The temporary configuration file is named `<site>_bmi_temp.conf` and is created in the working directory of the BMI tool; the file is not explicitly removed by `run_bmi`, so downstream cleanup may be required.  
- The exit calls (`sys.exit`) terminate the entire Python process; this function should be used only as a top‑level command‑line entry point, not as a library call from other Python code.  
- If `pwprompt` is False and no password is supplied in the URI, the password component of the connection string will be omitted, and authentication will rely on other mechanisms (e.g., `.pgpass` or peer authentication).  
- Errors raised inside `run_bmi_calc` propagate as exceptions; they are not caught here, so the process will exit with status 1 only when `run_bmi_calc` returns a falsy value, not when it raises an exception. Proper exception handling is therefore delegated to the core routine.
## FunctionDef copy_bmi(pwprompt, searchpath, site, table, out_table, dburi)
**copy_bmi**: The function of copy_bmi is to orchestrate the copying of a BMI measurement table from a temporary source into the canonical measurement table within the DCC schema.

**parameters**: The parameters of this Function.  
· pwprompt: Boolean flag indicating whether the user should be prompted for the database password on the command line.  
· searchpath: String containing a comma‑separated list of PostgreSQL schemas that defines the search_path for the connection.  
· site: Identifier of the site (e.g., “stlouis”) for which the BMI copy is being performed.  
· table: Name of the target measurement table that will receive the BMI rows (e.g., `measurement` or `measurement_anthro`).  
· out_table: Name of the temporary source table that holds the BMI results (e.g., `measurement_bmi`).  
· dburi: PostgreSQL URI that specifies the database host, port, name, user and optional parameters.

**Code Description**:  
1. **Password handling** – The function initializes a local variable `password` to `None`. If the `pwprompt` flag is true, it uses `click.prompt` to request the database password from the user, hiding the input. The entered password is stored in `password`.  
2. **Connection string construction** – It calls the utility `make_conn_str` (defined in `pedsnetdcc.utils`) with three arguments: the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. `make_conn_str` parses the URI, merges or overrides the `options` and `password` components, and returns a libpq‑compatible connection string (`conn_str`).  
3. **Import of the BMI copy routine** – The function performs a local import: `from pedsnetdcc.bmi import copy_bmi_measurement`. This import is deferred until runtime so that the heavy BMI module is only loaded when the command is actually invoked.  
4. **Invocation of the core copy logic** – `copy_bmi_measurement` is called with the constructed `conn_str`, the `site`, `table`, `out_table`, and `searchpath`. This helper performs the actual INSERT … SELECT operation, logs progress, and returns a Boolean `success` flag.  
5. **Exit handling** – If `success` is `False`, the function terminates the process with `sys.exit(1)`, indicating an error condition to the operating system. If the copy succeeded, it exits cleanly with `sys.exit(0)`. No further cleanup or exception handling is performed inside `copy_bmi`; any uncaught exception will cause the Python interpreter to exit with a non‑zero status automatically.  

**Relationship with other project components**:  
- **Callees**:  
  * `make_conn_str` (pedsnetdcc.utils) – builds the database connection string from the URI, optional search_path, and optional password.  
  * `copy_bmi_measurement` (pedsnetdcc.bmi) – executes the actual BMI rows copy and logs detailed progress.  
- **Callers**: This function is exposed as a Click command in the project's command‑line interface (`pedsnetdcc/main.py`). It is invoked when a user runs the `copy-bmi` sub‑command, passing the required arguments.  

**Note**:  
- The function does not perform any validation of the `site`, `table`, or `out_table` arguments; it relies on `copy_bmi_measurement` to raise errors if the tables are missing or incompatible.  
- Password prompting occurs only when `pwprompt` is true; otherwise the password must be present in the URI or omitted entirely for password‑less connections.  
- Because the function ends with `sys.exit`, it should be used only as a top‑level command entry point; importing and calling it from other Python code will cause the interpreter to terminate.  
- All logging and error handling is delegated to `copy_bmi_measurement`; this wrapper merely propagates the Boolean result to the process exit code.  
- The `searchpath` argument is passed unchanged to both `make_conn_str` and `copy_bmi_measurement`; the first schema in the list determines the primary schema for the copy operation.  
## FunctionDef run_bmiz(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, add_age, neg_ids, skip_calc, table, out_table, person, model_version, idname, dburi)
**run_bmiz**: The function of run_bmiz is to orchestrate the complete BMI‑Z derivation workflow by preparing configuration, invoking the generic Z‑Score calculation routine, and handling post‑processing steps such as indexing, ID assignment, concept enrichment, optional copying, and table vacuuming.

**parameters**: The parameters of this Function.  
· pwprompt (bool): If True, the user is prompted on the command line for the database password; the input is hidden.  
· searchpath (str): PostgreSQL schema search_path that will be incorporated into the connection string (and ultimately used for DDL).  
· site (str): Identifier of the site for which the BMI‑Z derivation is being run; used for log messages and temporary file naming.  
· copy (bool): When True, the derived BMI‑Z rows are copied from the temporary output table into the canonical `measurement` table after all processing.  
· noids (bool): If True, the step that generates and assigns unique `measurement_id` values is skipped.  
· noindexes (bool): If True, creation of indexes on the output table is omitted.  
· noconcept (bool): If True, the step that populates concept‑name columns from the OMOP vocabulary is omitted.  
· add_age (bool): If True, an additional column `measurement_age_in_months` is calculated and filled.  
· neg_ids (bool): If True, the sequence used for generating measurement IDs produces negative values; otherwise it produces positive values.  
· skip_calc (bool): If True, the actual external Z‑Score calculation is bypassed; only table creation and optional post‑processing are performed.  
· table (str): Name of the source measurement table that contains the raw BMI values (e.g., `measurement` or `measurement_anthro`).  
· out_table (str): Name of the temporary output table that will receive the BMI‑Z results.  
· person (str): Name of the person table; required for the Z‑Score configuration file.  
· model_version (str): PEDSnet data model version string (e.g., `'2.3.0'`).  
· idname (str): Prefix used for naming the last‑id tracking table (e.g., `'dcc'` or `'onco'`).  
· dburi (str): PostgreSQL URI that identifies the target database (e.g., `postgresql://user:pass@host/dbname`).  

**Code Description**:  
1. **Password handling** – The variable `password` is initialised to `None`. If `pwprompt` is True, the function uses `click.prompt` to request the database password from the user, hiding the input. The obtained password (or `None` if not prompted) will later be passed to the connection‑string builder.  
2. **Connection string creation** – `make_conn_str` is called with the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. This utility builds a libpq‑compatible connection string that incorporates the search_path option and overrides any password present in the URI when an explicit password is supplied. The resulting string is stored in `conn_str`.  
3. **Temporary configuration file name** – A temporary configuration file name is constructed by concatenating the `site` identifier with the suffix `"_bmiz_temp.conf"`. This file will be written by the Z‑Score routine.  
4. **Flag normalisation** – The boolean command‑line flags are converted into the internal flags expected by `run_z_calc`:  
   * `ids` is set to `True` unless `noids` is True.  
   * `indexes` is set to `True` unless `noindexes` is True.  
   * `concept` is set to `True` unless `noconcept` is True.  
   * `age` is set to `True` only when `add_age` is True.  
   These derived flags are passed directly to the downstream routine.  
5. **Delegation to the generic Z‑Score driver** – The function imports `run_z_calc` from `pedsnetdcc.z_score` and invokes it with the following arguments:  
   * `z_type='bmiz'` – selects the BMI‑Z calculation path.  
   * `config_file` – the temporary configuration file name created in step 3.  
   * `conn_str` – the connection string from step 2.  
   * `site`, `copy`, `ids`, `indexes`, `concept`, `age`, `neg_ids`, `skip_calc` – the flags prepared in steps 1‑4.  
   * `table`, `out_table`, `person` – table names supplied by the caller.  
   * `password` – the password obtained in step 1 (may be `None`).  
   * `searchpath`, `model_version`, `idname` – additional context required by the Z‑Score workflow.  
   The call returns a boolean `success` indicating whether all steps completed without error.  
6. **Process termination** – If `success` is `False`, the function terminates the process with exit code 1 (`sys.exit(1)`), signalling failure to the operating system. If the call succeeded, the function exits with code 0 (`sys.exit(0)`), indicating normal completion.  

**Relationship with callees**:  
* `make_conn_str` (defined in `pedsnetdcc/utils.py`) is used to translate the user‑provided PostgreSQL URI, optional search_path, and optional password into a libpq connection string that `run_z_calc` can consume.  
* `run_z_calc` (defined in `pedsnetdcc/z_score.py`) implements the full BMI‑Z derivation pipeline: it creates the configuration file, builds the output table, runs the external Z‑Score tool, optionally adds indexes, measurement IDs, concept names, age columns, copies results to the core measurement table, and finally vacuums the output table. `run_bmiz` is essentially a thin wrapper that supplies the appropriate arguments for the BMI‑Z case and interprets the boolean result.  

**Note**:  
- The function terminates the entire Python process via `sys.exit`; it is intended to be used as a command‑line entry point rather than as a library call.  
- When `pwprompt` is False and the URI already contains a password, that password is used; otherwise the connection will be attempted without authentication, which will fail if the database requires a password.  
- All flag inversions (`noids`, `noindexes`, `noconcept`) follow the convention that the presence of the “no‑” prefix disables the corresponding feature.  
- The temporary configuration file is named `<site>_bmiz_temp.conf` and is written to the working directory of the process; the caller must have write permission in that location.  
- The function does not perform any explicit error handling beyond the boolean check returned by `run_z_calc`; any exception raised inside `run_z_calc` will propagate up and cause the process to exit with code 1.  
- The `searchpath` argument influences both the connection string and the schema used for DDL operations inside `run_z_calc`; ensure that the first schema in the path exists and is writable by the database user.  
## FunctionDef copy_bmiz(pwprompt, searchpath, site, table, out_table, dburi)
**copy_bmiz**: The function of copy_bmiz is to copy the BMIZ (BMI‑Z) measurement data from a temporary output table into the canonical measurement table in the DCC PEDSnet database.

**parameters**:  
· pwprompt (bool): If True, the function will prompt the user for the database password on the command line (input is hidden).  
· searchpath (str or None): The PostgreSQL schema search_path to be used when building the connection string; may be None to leave the search_path unchanged.  
· site (str): Identifier of the site being processed; passed through to logging performed by the downstream copy operation.  
· table (str): Name of the destination measurement table (normally “measurement”).  
· out_table (str): Name of the temporary source table that contains the BMIZ results to be copied.  
· dburi (str): PostgreSQL URI that identifies the target database (e.g., `postgresql://user@host/dbname`).  

**Code Description**:  
1. Initialise a local variable `password` to `None`.  
2. If `pwprompt` is True, invoke `click.prompt` to request the database password from the user, hiding the input; store the result in `password`.  
3. Call `make_conn_str` (defined in `pedsnetdcc/utils.py`) with the supplied `dburi`, `searchpath`, and the possibly‑prompted `password`. This returns a libpq‑compatible connection string (`conn_str`) that incorporates the search_path and password overrides.  
4. Import the function `copy_z_measurement` from `pedsnetdcc.z_score`.  
5. Invoke `copy_z_measurement` with the following arguments:  
   – `z_type` set to the literal string `'bmiz'` (indicating a BMI‑Z calculation),  
   – the connection string `conn_str` produced in step 3,  
   – the `site`, `table`, `out_table`, and `searchpath` values received by `copy_bmiz`.  
   The call returns a boolean `success` indicating whether the copy operation completed without error.  
6. If `success` is False, terminate the process with exit status 1 (`sys.exit(1)`).  
7. If `success` is True, terminate the process with exit status 0 (`sys.exit(0)`).  

The function therefore acts as a thin command‑line wrapper that prepares authentication information, builds a proper connection string, and delegates the actual database copy work to `copy_z_measurement`.

**Note**:  
- The function does not perform any validation of the input parameters beyond what is required for password prompting; callers must ensure that `searchpath`, `site`, `table`, and `out_table` are valid identifiers in the target database.  
- Password prompting occurs only when `pwprompt` is True; otherwise the password must be present in the `dburi` or omitted entirely if the database allows password‑less connections.  
- The exit codes are deterministic: 0 on success, 1 on failure. This behavior is intended for use in command‑line scripts where the return code signals overall job status.  
- All heavy lifting (logging, schema resolution, INSERT … SELECT operation) is performed inside `copy_z_measurement`; any exceptions raised there will propagate out of `copy_bmiz` before the explicit `sys.exit` calls.  
- Because `copy_bmiz` imports `copy_z_measurement` locally, the import occurs at runtime each time the function is called, ensuring that the latest version of the helper is used without requiring a top‑level import.
## FunctionDef run_bmi_bmiz(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, add_age, neg_ids, skip_calc, table, out_table_bmi, out_table_bmiz, person, model_version, idname, max_time, dburi)
**run_bmi_bmiz**: The function of run_bmi_bmiz is to orchestrate the complete derivation of Body‑Mass‑Index (BMI) and BMI‑Z (BMIZ) measurements for a given site, handling configuration, database connection, optional processing flags, and termination status.

**parameters**: The parameters of this Function.  
· pwprompt: bool – if True, the user is prompted on the command line for the database password (input hidden).  
· searchpath: str – PostgreSQL schema search_path that will be injected into the connection string.  
· site: str – identifier of the site for which the derivations are being performed; used to build temporary configuration file names.  
· copy: bool – if True, the derived rows are copied into the canonical `measurement` table after calculation.  
· noids: bool – if True, suppress generation of unique `measurement_id` values for the output tables.  
· noindexes: bool – if True, suppress creation of indexes on the output tables.  
· noconcept: bool – if True, suppress enrichment of concept‑name columns from the OMOP vocabulary.  
· add_age: bool – if True, calculate and populate the `measurement_age_in_months` column.  
· neg_ids: bool – if True, generate negative identifiers for `measurement_id` (used when a site reserves negative IDs).  
· skip_calc: bool – if True, skip the external BMI/BMIZ calculation tool; only table creation and post‑processing steps are performed.  
· table: str – name of the source measurement table (normally `measurement` or `measurement_anthro`).  
· out_table_bmi: str – name of the temporary table that will receive the derived BMI rows.  
· out_table_bmiz: str – name of the temporary table that will receive the derived BMI‑Z rows.  
· person: str – name of the person table; required by the BMIZ configuration.  
· model_version: str – PEDSnet data model version (e.g., “2.3.0”).  
· idname: str – prefix used for the site‑specific last‑id tracking table (e.g., “dcc” or “onco”).  
· max_time: str – maximum allowed interval between a height and a weight measurement; supplied as days or seconds and passed to the BMI tool.  
· dburi: str – PostgreSQL URI that identifies the target database (e.g., `postgresql://user@host/dbname`).  

**Code Description**:  
1. **Password handling** – If `pwprompt` is True, the function prompts the user for the database password using `click.prompt` with hidden input. The entered password is stored in the local variable `password`; otherwise `password` remains `None`.  
2. **Connection string construction** – The helper `make_conn_str` (from `pedsnetdcc.utils`) is called with `dburi`, the supplied `searchpath`, and the possibly‑prompted `password`. It returns a libpq‑compatible connection string (`conn_str`) that will be used for all subsequent database operations.  
3. **Flag translation** – The Boolean command‑line switches that disable optional steps are inverted to produce the flags expected by the downstream derivation functions:  
   * `ids` is set to `True` unless `noids` is True.  
   * `indexes` is set to `True` unless `noindexes` is True.  
   * `concept` is set to `True` unless `noconcept` is True.  
   * `age` is set to `True` only when `add_age` is True.  
4. **BMI derivation** –  
   * A temporary configuration file name is built as `{site}_bmi_temp.conf`.  
   * `run_bmi_calc` (from `pedsnetdcc.bmi`) is imported locally and invoked with the constructed configuration file name, the connection string, the site identifier, and all flags (`copy`, `ids`, `indexes`, `concept`, `age`, `neg_ids`, `skip_calc`). The source table, output table (`out_table_bmi`), password, search path, model version, id name, and `max_time` are also passed.  
   * The return value `success` is examined; if the call returns `False`, the program terminates with `sys.exit(1)`.  
5. **BMIZ derivation** –  
   * A second temporary configuration file name is built as `{site}_bmiz_temp.conf`.  
   * `run_z_calc` (from `pedsnetdcc.z_score`) is imported locally and invoked with the Z‑Score type `'bmiz'`, the new configuration file name, the same connection string, the site identifier, and the same set of Boolean flags. The source table, BMIZ output table (`out_table_bmiz`), the person table name, password, search path, model version, and id name are also supplied.  
   * Again, the Boolean result `success` is checked; a `False` result causes termination with `sys.exit(1)`.  
6. **Successful completion** – If both derivations succeed, the function ends with `sys.exit(0)`, signalling a successful command‑line execution.  

The function therefore acts as a thin command‑line wrapper that prepares environment‑specific parameters, delegates the heavy lifting to `run_bmi_calc` and `run_z_calc`, and enforces a strict exit‑code policy based on success or failure.

**Note**:  
- The function exits the entire Python process via `sys.exit`; it is intended to be used as a top‑level CLI entry point, not as a library call.  
- Password prompting occurs only when `pwprompt` is True; otherwise the password must be present in the URI or supplied via the `password` argument of `make_conn_str`.  
- All optional processing steps (ID generation, indexing, concept enrichment, age calculation, copying) are controlled by the inverse of the `no*` flags; ensure the flags are set consistently with the desired workflow.  
- The temporary configuration files are written in the current working directory of the container (`/app`) by the called functions; the caller must have write permission to that location.  
- `run_bmi_calc` and `run_z_calc` each perform their own logging, error handling, and database commits; `run_bmi_bmiz` only checks the Boolean return value.  
- Because the function terminates the interpreter on failure, any resources allocated before the call (e.g., open file handles) will be released only by the interpreter shutdown process.  
- The `max_time` argument is passed unchanged to `run_bmi_calc`; that function is responsible for converting it to seconds if necessary.  
- The `person` argument is required only for the BMIZ step; it is ignored by the BMI step.
## FunctionDef run_height_z(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, add_age, neg_ids, skip_calc, table, out_table, person, model_version, idname, dburi)
**run_height_z**: The function of run_height_z is to orchestrate the complete Height‑Z derivation workflow, including configuration creation, output table preparation, execution of the Z‑Score calculation, optional indexing, enrichment, copying, and final vacuuming.

**parameters**: The parameters of this Function.  
· pwprompt: bool – if True, the function will prompt the user for the database password on the command line.  
· searchpath: str – the PostgreSQL schema search_path to be used when building the connection string.  
· site: str – identifier of the site for which the derivation is being run; used for log messages and for naming temporary objects.  
· copy: bool – when True, the resulting Height‑Z rows are copied from the temporary output table into the canonical `measurement` table.  
· noids: bool – when True, measurement identifiers are **not** generated for the output rows.  
· noindexes: bool – when True, indexes are **not** created on the output table.  
· noconcept: bool – when True, concept‑name columns are **not** populated in the output table.  
· add_age: bool – when True, a column `measurement_age_in_months` is calculated and filled for each row.  
· neg_ids: bool – when True, the sequence used for generating measurement IDs produces negative values; otherwise it produces positive values.  
· skip_calc: bool – when True, the actual Height‑Z calculation step (external Z‑Score tool) is skipped; only table creation and optional post‑processing are performed.  
· table: str – name of the source measurement table that contains the raw height measurements (e.g., `measurement` or `measurement_anthro`).  
· out_table: str – name of the temporary output table that will receive the Height‑Z results.  
· person: str – name of the person table; required for the Z‑Score configuration file.  
· model_version: str – PEDSnet data model version (e.g., `'2.3.0'`).  
· idname: str – prefix used for naming the last‑id tracking table (e.g., `'dcc'` or `'onco'`).  
· dburi: str – PostgreSQL URI that identifies the target database; must follow the libpq URI format.

**Code Description**:  
1. **Password handling** – If `pwprompt` is True, the function prompts the user for a password using `click.prompt` with hidden input; otherwise `password` remains `None`.  
2. **Connection string construction** – Calls `make_conn_str` (defined in `pedsnetdcc/utils.py`) with the supplied `dburi`, `searchpath`, and the possibly‑prompted `password`. The helper builds a libpq‑compatible connection string, overriding any password or search_path present in the URI. The resulting string is stored in `conn_str`.  
3. **Temporary configuration file name** – Constructs a temporary configuration file name by concatenating the `site` identifier with the suffix `"_htz_temp.conf"`; this file will be written by the Z‑Score tool.  
4. **Flag translation** – Translates the command‑line negation flags into boolean variables expected by the downstream Z‑Score driver:  
   - `ids` is set to `True` unless `noids` is True.  
   - `indexes` is set to `True` unless `noindexes` is True.  
   - `concept` is set to `True` unless `noconcept` is True.  
   - `age` is set to `True` only when `add_age` is True.  
5. **Delegation to the Z‑Score engine** – Imports `run_z_calc` from `pedsnetdcc.z_score` and invokes it with the following arguments:  
   - `z_type='ht_z'` (identifies Height‑Z).  
   - `config_file` (the temporary config file name).  
   - `conn_str` (the connection string built in step 2).  
   - `site`, `copy`, `ids`, `indexes`, `concept`, `age`, `neg_ids`, `skip_calc`, `table`, `out_table`, `person`, `password`, `searchpath`, `model_version`, `idname`.  
   The `run_z_calc` function performs the full derivation workflow (configuration file creation, table creation, external tool execution, optional indexing, ID generation, concept name filling, age calculation, copying, and vacuuming).  
6. **Result handling** – `run_z_calc` returns a boolean indicating success. If the return value is `False`, `run_height_z` terminates the process with `sys.exit(1)`. If the call succeeds, the function exits cleanly with `sys.exit(0)`.  

**Relationship with callees**:  
- **make_conn_str** (pedsnetdcc/utils.py) is used to translate the user‑provided `dburi` into a libpq connection string, handling optional password prompting and schema search_path overrides.  
- **run_z_calc** (pedsnetdcc/z_score.py) is the core driver that implements the Height‑Z derivation steps. All heavy‑lifting, including configuration file generation, table manipulation, external Z‑Score tool invocation, and post‑processing, is performed inside this function. `run_height_z` acts primarily as a thin wrapper that prepares arguments, resolves flags, and interprets the success flag returned by `run_z_calc`.  

**Note**:  
- The function terminates the entire Python process using `sys.exit`; it is intended to be used as a command‑line entry point rather than as a library call.  
- If `pwprompt` is False and the URI does not contain a password, the connection string will be built without a password, which may cause authentication failures depending on the database configuration.  
- All boolean flags (`noids`, `noindexes`, `noconcept`, `add_age`) are inverted to match the expectations of `run_z_calc`; ensure that the command‑line interface supplies the correct negated options.  
- The temporary configuration file is created in the current working directory with a name derived from the `site`; the file is overwritten each time the function runs.  
- Errors occurring inside `run_z_calc` are propagated as a `False` return value; `run_height_z` responds by exiting with status 1, which signals failure to the invoking shell.  
- No explicit transaction is started around the whole workflow; each step inside `run_z_calc` commits independently. Partial results may remain if a later step fails after earlier steps have succeeded.
## FunctionDef copy_height_z(pwprompt, searchpath, site, table, out_table, dburi)
**copy_height_z**: The function of copy_height_z is to copy the Height‑Z measurement data from a temporary output table into the canonical measurement table in the DCC PEDSnet database.

**parameters**:  
· **pwprompt** (bool): If True, the function will prompt the user on the command line for the database password (input hidden).  
· **searchpath** (str): The PostgreSQL schema search_path to be used when building the connection string; it may also be incorporated into the libpq options.  
· **site** (str): Identifier of the site being processed; passed through to the logging performed by the downstream copy routine.  
· **table** (str): Name of the destination measurement table (normally `"measurement"`).  
· **out_table** (str): Name of the temporary source table that contains the Height‑Z results to be copied.  
· **dburi** (str): PostgreSQL URI that identifies the target database (e.g., `postgresql://user@host/dbname`).  

**Code Description**:  
1. Initialise a local variable `password` to `None`.  
2. If `pwprompt` is True, invoke `click.prompt` to request the database password from the user, hiding the input; store the result in `password`.  
3. Call `make_conn_str(dburi, searchpath, password)` (defined in *pedsnetdcc/utils.py*) to construct a libpq‑compatible connection string. The function merges the URI components with the optional `search_path` and the password obtained in step 2. The resulting string is stored in `conn_str`.  
4. Import the helper `copy_z_measurement` from *pedsnetdcc/z_score.py*.  
5. Execute `copy_z_measurement('ht_z', conn_str, site, table, out_table, searchpath)`. The first argument `'ht_z'` tells the helper that the Z‑score type is Height‑Z. `copy_z_measurement` performs logging, determines the primary schema from `searchpath`, and runs an `INSERT … SELECT … ON CONFLICT DO NOTHING` statement that copies all rows from `out_table` into `table`. It returns a Boolean indicating success.  
6. If the returned `success` flag is falsy, terminate the process with exit code 1 (`sys.exit(1)`).  
7. If the copy succeeded, terminate the process with exit code 0 (`sys.exit(0)`).  

**Relationship with Callees**:  
- **make_conn_str**: Generates the connection string required for any subsequent database operation. It handles parsing of the supplied URI, optional overriding of the password, and insertion of the `search_path` into the libpq `options` parameter.  
- **copy_z_measurement**: Performs the actual data movement for Height‑Z. It logs start and finish messages, calculates elapsed time, resolves the primary schema, and calls the internal `_copy_to_measurement_table` to execute the SQL copy.  

**Note**:  
- The function exits the entire Python process; it is intended to be used as a command‑line entry point, not as a library call that returns a value.  
- When `pwprompt` is False, no password is supplied; the password must already be present in `dburi` or be omitted if the database allows password‑less connections.  
- The `searchpath` argument overrides any `search_path` option embedded in `dburi`.  
- The exit status (0 for success, 1 for failure) follows the conventional Unix convention and can be used by shell scripts to detect errors.  
- No validation is performed on the values of `site`, `table`, or `out_table`; they are passed directly to `copy_z_measurement`, which assumes they are valid identifiers in the target database.
## FunctionDef run_weight_z(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, add_age, neg_ids, skip_calc, table, out_table, person, model_version, idname, dburi)
**run_weight_z**: The function of run_weight_z is to orchestrate the full Weight‑Z derivation workflow by preparing configuration, invoking the generic Z‑Score calculation routine, and handling post‑processing steps such as indexing, ID assignment, concept enrichment, optional copying, and table vacuuming.

**parameters**: The parameters of this Function.  
· pwprompt (bool): If True, the function will prompt the user on the command line for the database password (input hidden).  
· searchpath (str): The PostgreSQL schema search_path that should be applied to the connection; it is passed to `make_conn_str`.  
· site (str): Identifier of the site for which the derivation is being run; used for logging and for naming temporary files.  
· copy (bool): When True, the resulting Weight‑Z rows are copied from the temporary output table into the canonical `measurement` table.  
· noids (bool): When True, the generation of unique `measurement_id` values is suppressed (ids flag becomes False).  
· noindexes (bool): When True, creation of indexes on the output table is suppressed (indexes flag becomes False).  
· noconcept (bool): When True, the enrichment of concept‑name columns is suppressed (concept flag becomes False).  
· add_age (bool): When True, the column `measurement_age_in_months` is calculated and populated (age flag becomes True).  
· neg_ids (bool): Passed through to `run_z_calc`; determines whether the ID sequence generates negative values.  
· skip_calc (bool): When True, the actual external Z‑Score calculation step is skipped; only table creation and optional post‑processing are performed.  
· table (str): Name of the source measurement table that contains raw weight measurements (e.g., `measurement` or `measurement_anthro`).  
· out_table (str): Name of the temporary output table that will receive the Weight‑Z results.  
· person (str): Name of the person table; required by the Z‑Score configuration file.  
· model_version (str): PEDSnet data model version string (e.g., `'2.3.0'`).  
· idname (str): Prefix used for naming the last‑id tracking table (e.g., `'dcc'` or `'onco'`).  
· dburi (str): PostgreSQL URI that encodes host, port, database, user, and optional password; it is converted to a libpq connection string.

**Code Description**:  
1. **Password handling** – The variable `password` is initialised to `None`. If `pwprompt` is True, the function uses `click.prompt` to request the database password from the user, hiding the input. The obtained password (or `None`) will later be supplied to `make_conn_str`.  
2. **Connection string construction** – `make_conn_str` from `pedsnetdcc.utils` is called with the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. This helper parses the URI, merges any explicit password or search_path, and returns a libpq‑compatible connection string (`conn_str`).  
3. **Temporary configuration file name** – A temporary configuration file name is built by concatenating the `site` identifier with the suffix `"_wtz_temp.conf"`. This file will be written by the Z‑Score routine.  
4. **Flag translation** – The Boolean command‑line switches (`noids`, `noindexes`, `noconcept`, `add_age`) are inverted to the internal flags expected by `run_z_calc`:  
   * `ids` defaults to True; set to False if `noids` is True.  
   * `indexes` defaults to True; set to False if `noindexes` is True.  
   * `concept` defaults to True; set to False if `noconcept` is True.  
   * `age` defaults to False; set to True if `add_age` is True.  
5. **Import of the core Z‑Score driver** – The function imports `run_z_calc` from `pedsnetdcc.z_score`. This import is local to keep the command‑line entry point lightweight until the derivation is actually requested.  
6. **Invocation of the generic Z‑Score workflow** – `run_z_calc` is called with the following arguments:  
   * `z_type='wt_z'` to indicate a Weight‑Z calculation.  
   * `config_file` (the temporary file name).  
   * `conn_str` (the libpq connection string).  
   * `site`, `copy`, `ids`, `indexes`, `concept`, `age`, `neg_ids`, `skip_calc`, `table`, `out_table`, `person`, `password`, `searchpath`, `model_version`, `idname`.  
   The call returns a Boolean `success` indicating whether all steps completed without error.  
7. **Exit handling** – If `success` is False, the script terminates with `sys.exit(1)` to signal failure to the operating system. If the call succeeded, the script exits cleanly with `sys.exit(0)`. No further processing occurs after the exit call.  

**Relationship with callees**:  
* `make_conn_str` (in `pedsnetdcc.utils`) is responsible for turning the user‑supplied URI, optional search_path, and optional password into a libpq connection string. This string is required by every downstream database operation performed by `run_z_calc`.  
* `run_z_calc` (in `pedsnetdcc.z_score`) implements the complete Z‑Score derivation pipeline: it creates the configuration file, builds the output table, runs the external Z‑Score tool, optionally creates indexes, assigns measurement IDs, fills concept names, calculates age in months, copies results to the core measurement table, and finally vacuums the table. `run_weight_z` is essentially a thin wrapper that supplies the appropriate arguments for a Weight‑Z run.  

**Note**:  
- The function does not perform any validation of the input parameters beyond what is enforced by the downstream `run_z_calc`; callers must ensure that table names, schema, and site identifiers are correct.  
- If `pwprompt` is False and the URI does not contain a password, the connection string will be built without a password, which may cause authentication failures.  
- The temporary configuration file is created in the current working directory of the process (the name includes the site identifier); the file is overwritten each time the command is run.  
- All exit paths use `sys.exit`, which terminates the Python interpreter; this behavior is appropriate for command‑line entry points but should be considered when reusing the function programmatically.  
- The Boolean flags (`noids`, `noindexes`, `noconcept`, `add_age`) follow the conventional “no‑” pattern used by many CLI tools: the presence of the flag disables the default behaviour.  
- Because `run_z_calc` may raise exceptions on database errors, `run_weight_z` relies on the exit code to propagate failure; any uncaught exception will cause the process to exit with a non‑zero status.  
## FunctionDef copy_weight_z(pwprompt, searchpath, site, table, out_table, dburi)
**copy_weight_z**: The function of copy_weight_z is to copy the Weight‑Z measurement data from a temporary output table into the canonical measurement table in the DCC PEDSnet database.

**parameters**:  
· **pwprompt** (bool): If True, the function will prompt the user on the command line for the database password (input hidden).  
· **searchpath** (str): The PostgreSQL schema search_path to be used when building the connection string; it may also be incorporated into the libpq options.  
· **site** (str): Identifier of the site being processed; passed through to the logging performed by the copy routine.  
· **table** (str): Name of the destination measurement table (normally `"measurement"`).  
· **out_table** (str): Name of the temporary source table that contains the Weight‑Z results to be copied.  
· **dburi** (str): PostgreSQL URI that identifies the target database (e.g., `postgresql://user@host/dbname`).  

**Code Description**:  
1. Initialise a local variable `password` to `None`.  
2. If `pwprompt` is True, invoke `click.prompt` to request the database password from the user; the entered value is stored in `password` and the input is hidden.  
3. Call `make_conn_str` (defined in *pedsnetdcc/utils.py*) with the supplied `dburi`, `searchpath`, and the possibly‑provided `password`. This returns a libpq‑compatible connection string (`conn_str`).  
4. Import the helper `copy_z_measurement` from *pedsnetdcc/z_score.py*.  
5. Execute `copy_z_measurement('wt_z', conn_str, site, table, out_table, searchpath)`. The first argument `'wt_z'` tells the helper that the Z‑score type is Weight‑Z. The helper performs logging, determines the primary schema from `searchpath`, and copies all rows from `out_table` into `table` using an `INSERT … SELECT … ON CONFLICT DO NOTHING` statement. It returns a boolean indicating success.  
6. If the returned `success` flag is falsy, terminate the process with a non‑zero exit status (`sys.exit(1)`).  
7. If the copy succeeded, terminate the process with a zero exit status (`sys.exit(0)`).  

**Note**:  
- The function does not perform any validation of the input arguments beyond what is done inside `make_conn_str` and `copy_z_measurement`. Supplying an invalid `dburi` or a malformed `searchpath` will cause those utilities to raise exceptions before the copy is attempted.  
- When `pwprompt` is False, no password is added to the connection string; the password must already be present in the `dburi` or omitted if the database allows password‑less connections.  
- The exit calls (`sys.exit`) mean that `copy_weight_z` is intended to be used as a command‑line entry point; any caller should be prepared for the process to terminate immediately after the function returns.  
- The actual database work is delegated to `copy_z_measurement`; therefore, all logging, schema resolution, and error handling are performed inside that helper. This function’s responsibility is limited to preparing the connection string and invoking the helper with the correct Z‑type identifier.  
- Because the function imports `copy_z_measurement` locally, the import occurs at runtime each time the function is called, which isolates the dependency and avoids circular import issues.
## FunctionDef run_ht_wt_z(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, add_age, neg_ids, skip_calc, table, out_table_ht_z, out_table_wt_z, person, model_version, idname, dburi)
**run_ht_wt_z**: The function of run_ht_wt_z is to orchestrate the execution of both Height‑Z and Weight‑Z derivations for a given site by invoking the generic Z‑Score calculation workflow.

**parameters**: The parameters of this Function.  
· pwprompt: bool – if True, the user is prompted on the command line for the database password (input hidden).  
· searchpath: str – PostgreSQL schema search_path that will be incorporated into the connection string.  
· site: str – identifier of the site for which the derivations are being run; used to build configuration file names and for logging.  
· copy: bool – passed to run_z_calc; when True the resulting rows are copied into the core measurement table.  
· noids: bool – when True disables generation of measurement_id values (ids flag set to False).  
· noindexes: bool – when True disables creation of indexes on the temporary output tables (indexes flag set to False).  
· noconcept: bool – when True disables population of concept‑name columns (concept flag set to False).  
· add_age: bool – when True enables calculation of measurement_age_in_months (age flag set to True).  
· neg_ids: bool – forwarded to run_z_calc; determines whether generated measurement IDs are negative.  
· skip_calc: bool – forwarded to run_z_calc; when True the external Z‑Score calculation step is skipped.  
· table: str – name of the source measurement table containing raw height or weight values.  
· out_table_ht_z: str – name of the temporary table that will receive Height‑Z results.  
· out_table_wt_z: str – name of the temporary table that will receive Weight‑Z results.  
· person: str – name of the person table; required by the Z‑Score tool configuration.  
· model_version: str – PEDSnet data model version (e.g., “2.3.0”).  
· idname: str – prefix used for naming the last‑id tracking table (e.g., “dcc”).  
· dburi: str – PostgreSQL URI that identifies the target database (e.g., “postgresql://user@host/dbname”).  

**Code Description**:  
1. **Password handling** – The variable `password` is initialised to `None`. If `pwprompt` is True, the function uses `click.prompt` to request the database password from the user, hiding the input.  
2. **Connection string construction** – `make_conn_str` (from *pedsnetdcc.utils*) is called with `dburi`, the supplied `searchpath`, and the possibly‑prompted `password`. The helper builds a libpq‑compatible connection string that incorporates the search_path and overrides the password if one was provided. The resulting string is stored in `conn_str`.  
3. **Flag preparation** – Boolean flags required by `run_z_calc` are derived from the command‑line negation options:  
   * `ids` defaults to True; set to False when `noids` is True.  
   * `indexes` defaults to True; set to False when `noindexes` is True.  
   * `concept` defaults to True; set to False when `noconcept` is True.  
   * `age` defaults to False; set to True when `add_age` is True.  
4. **Import of the Z‑Score driver** – The function imports `run_z_calc` from *pedsnetdcc.z_score*. This import is local to avoid unnecessary loading when the command is not used.  
5. **Height‑Z execution** – A temporary configuration file name is built as `<site>_htz_temp.conf`. `run_z_calc` is invoked with the following arguments:  
   * `z_type='ht_z'`  
   * the configuration file name, the connection string, site identifier, copy flag, the four derived flags (`ids`, `indexes`, `concept`, `age`), `neg_ids`, `skip_calc`, the source table, the Height‑Z output table (`out_table_ht_z`), the person table, the password (may be `None` because it is already embedded in `conn_str`), the search_path, the model version, and the id name.  
   The return value (`success`) indicates whether the entire Height‑Z workflow completed without error. If `success` is False, the process terminates with `sys.exit(1)`.  
6. **Weight‑Z execution** – The same steps are repeated for Weight‑Z, using a configuration file named `<site>_wtz_temp.conf` and the Weight‑Z output table (`out_table_wt_z`). Again, failure results in `sys.exit(1)`.  
7. **Normal termination** – If both derivations succeed, the function ends with `sys.exit(0)`, signalling successful completion to the operating system.  

**Relationship with callees**:  
* `make_conn_str` (pedsnetdcc.utils) is responsible for translating the supplied PostgreSQL URI, optional search_path, and optional password into a libpq connection string that `run_z_calc` can use for all database operations.  
* `run_z_calc` (pedsnetdcc.z_score) implements the full Z‑Score derivation pipeline: creation of a temporary configuration file, creation of the output table, optional execution of the external Z‑Score tool, post‑processing (index creation, ID assignment, concept name enrichment, age calculation, copying to the canonical measurement table), and final vacuum. `run_ht_wt_z` simply calls this routine twice, once for each Z‑type, passing the appropriate flags and table names.  

**Note**:  
- The function terminates the entire process with `sys.exit`; it does not raise exceptions to the caller. This behaviour is appropriate for command‑line entry points but makes the function unsuitable for reuse as a library call.  
- Password prompting occurs only when `pwprompt` is True; otherwise the password must be present in the URI or supplied via the `searchpath` argument to `make_conn_str`.  
- All boolean flags are inverted from the “no‑” command‑line options (`noids`, `noindexes`, `noconcept`) to match the expectations of `run_z_calc`.  
- The temporary configuration files are written to the current working directory with a site‑specific prefix; they are not removed by this function.  
- If `skip_calc` is True, the external Z‑Score calculation step is omitted, but the output tables are still created and any post‑processing steps (indexes, IDs, concept names, age, copy) are performed on empty tables.  
- The function assumes that the caller supplies valid table names and that the database user has sufficient privileges to create tables, indexes, and perform inserts.  
- Because `run_z_calc` may raise `DatabaseError` on failure, `run_ht_wt_z` relies on its boolean return value to decide whether to exit with status 1. Any unexpected exception that propagates out of `run_z_calc` will cause the Python interpreter to terminate with a traceback.
## FunctionDef run_drug_era(pwprompt, searchpath, site, copy, neg_ids, no_ids, no_concept, model_version, idname, dburi)
**run_drug_era**: The function of run_drug_era is to execute the full drug‑era derivation workflow for a single site, using a PostgreSQL connection string built from a DBURI.

**parameters**:  
· pwprompt (bool): If True, the function prompts the user for the database password on the command line (input hidden).  
· searchpath (str): The PostgreSQL schema search_path that will be used for the derivation; the first schema in the list is the target schema.  
· site (str): Identifier of the site for which the derivation is being performed; written into the `site` column of the drug_era table.  
· copy (bool): When True, the resulting drug_era table is copied into the central `dcc_pedsnet` schema after derivation.  
· neg_ids (bool): If True, the ID sequence generated for the era rows will contain negative identifiers.  
· no_ids (bool): If True, the step that assigns identifiers to rows is skipped.  
· no_concept (bool): If True, the step that populates concept‑name columns is skipped.  
· model_version (str): PEDSnet data‑model version (e.g., `"2.3.0"`); required for metadata lookup during ID assignment and vacuuming.  
· idname (str): Prefix for the last‑ID tracking table (e.g., `"dcc"` or `"onco"`).  
· dburi (str): PostgreSQL URI that specifies host, port, database name, user, and optional query parameters. The URI is transformed into a libpq connection string.

**Code Description**:  

1. **Password handling** – The variable `password` is initialised to `None`. If `pwprompt` is True, the function uses `click.prompt` to request the database password from the user, hiding the input. The entered password is stored in `password`.

2. **Connection string construction** – The helper `make_conn_str` (defined in `pedsnetdcc/utils.py`) is called with three arguments: the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. `make_conn_str` parses the URI, merges or overrides the `options` parameter with the provided `search_path`, inserts the explicit password if given, and returns a libpq‑compatible connection string (`conn_str`). This string is used for every subsequent database operation.

3. **Import of the core derivation routine** – The function imports `run_era` from `pedsnetdcc.era`. This import is performed locally to avoid unnecessary imports when the CLI command is not executed.

4. **Invocation of the generic era engine** – `run_era` is called with the following arguments:  
   - era_type = `"drug"` (hard‑coded for this wrapper)  
   - conn_str = the connection string built in step 2  
   - site, copy, neg_ids, no_ids, no_concept – passed through unchanged  
   - searchpath – passed as the `search_path` argument for `run_era`  
   - model_version, idname – forwarded unchanged  

   `run_era` performs the complete derivation workflow: dropping constraints, executing the drug‑era SQL, assigning IDs, populating concept names, optional copying to `dcc_pedsnet`, renumbering, adding primary keys, and vacuuming. It returns a Boolean `success` indicating whether all steps completed without error.

5. **Exit handling** – If `run_era` returns `False`, the wrapper calls `sys.exit(1)` to terminate the process with a non‑zero status, signalling failure to the operating system or calling script. If `run_era` returns `True`, the wrapper exits cleanly with `sys.exit(0)`.

**Note**:  
- The function assumes that the target drug_era table already exists in the primary schema indicated by `searchpath`.  
- All database operations are performed using the connection string generated by `make_conn_str`; therefore any mis‑configuration in the DBURI or an incorrect `searchpath` will cause connection failures before `run_era` is invoked.  
- When `pwprompt` is False, no password prompt occurs; the password must be present in the DBURI or supplied via other authentication mechanisms (e.g., `.pgpass`).  
- The wrapper does not expose the optional `notable`, `nopk`, or `novac` flags of `run_era`; those defaults are used (derivation query is executed, primary key is added, and vacuum is performed).  
- Because the function calls `sys.exit`, it is intended to be used as a command‑line entry point rather than as a library function called from other Python code.  
- Any exception raised inside `run_era` propagates up to this wrapper; the wrapper only checks the Boolean return value and does not perform additional error handling.
## FunctionDef run_drug_scdf_era(pwprompt, searchpath, site, copy, neg_ids, no_ids, no_concept, model_version, idname, dburi)
**run_drug_scdf_era**: The function of run_drug_scdf_era is to execute the full drug‑SCDF era derivation workflow from the command‑line interface.

**parameters**:  
· pwprompt (bool): If True, the user is prompted on the console for the database password; the entered password is hidden.  
· searchpath (str): PostgreSQL schema search_path that will be embedded in the connection string.  
· site (str): Identifier of the site for which the derivation is being performed; passed through to the era table.  
· copy (bool): When True, the resulting era table is copied into the central `dcc_pedsnet` schema after derivation.  
· neg_ids (bool): If True, identifiers are generated as negative numbers; otherwise normal positive identifiers are used.  
· no_ids (bool): If True, the step that assigns identifiers to rows is skipped.  
· no_concept (bool): If True, the step that populates concept‑name columns is skipped.  
· model_version (str): PEDSnet data‑model version (e.g., `"2.3.0"`); required for metadata lookup during the derivation.  
· idname (str): Prefix for the last‑ID tracking table (e.g., `"dcc"` or `"onco"`).  
· dburi (str): PostgreSQL URI that identifies the target database (e.g., `postgresql://user@host/dbname`).  

**Code Description**:  
1. **Password handling** – The function initializes a local variable `password` to `None`. If the `pwprompt` flag is set, it uses `click.prompt` to request the database password from the user, hiding the input. The entered password is stored in `password`.  

2. **Connection string construction** – The helper `make_conn_str` (defined in `pedsnetdcc/utils.py`) is called with three arguments: the raw `dburi`, the supplied `searchpath`, and the possibly‑prompted `password`. `make_conn_str` parses the URI, merges or overrides the `search_path` and `password` options, and returns a libpq‑compatible connection string (`conn_str`).  

3. **Import of the core derivation routine** – The function imports `run_era` from `pedsnetdcc.era`. This import is performed locally to avoid unnecessary loading when the CLI entry point is not used.  

4. **Invocation of the era derivation** – `run_era` is called with the following arguments:  
   - era_type = `"drug_scdf"` (hard‑coded for this wrapper)  
   - conn_str = the connection string built in step 2  
   - site, copy, neg_ids, no_ids, no_concept, searchpath, model_version, idname – passed through unchanged.  

   `run_era` carries out the complete derivation workflow: execution of the derivation SQL, optional ID assignment, optional concept‑name population, optional copying to `dcc_pedsnet`, and final vacuum/analysis. It returns a Boolean `success` flag.  

5. **Exit handling** – If `run_era` returns `False`, the wrapper terminates the process with a non‑zero exit status (`sys.exit(1)`). If the call succeeds, the wrapper exits cleanly with status 0 (`sys.exit(0)`). No additional cleanup is performed in this wrapper.  

**Relationship with other project components**:  
- **Callees**:  
  * `make_conn_str` (pedsnetdcc/utils.py) – builds the PostgreSQL connection string from the supplied URI, search_path, and password.  
  * `run_era` (pedsnetdcc/era.py) – performs the actual drug‑SCDF era derivation; all heavy‑lifting logic resides here.  

- **Callers**: This function is exposed as a Click command in `pedsnetdcc/main.py`. It is invoked when a user runs the CLI sub‑command `run_drug_scdf_era` (or when the generic `run_derivations` command selects the drug‑SCDF era step).  

**Note**:  
- The function assumes that the target database is reachable and that the necessary schema objects (e.g., `drug_scdf_era` table) already exist in the schema indicated by `searchpath`.  
- Password prompting occurs only when `pwprompt` is True; otherwise any password embedded in `dburi` is used, or no password is sent if none is present.  
- The wrapper does not perform any validation of the other flags; it forwards them directly to `run_era`, which implements the corresponding conditional logic.  
- Because the function ends with `sys.exit`, it should be used only as a top‑level CLI entry point; importing and calling it from other Python code will cause the interpreter to terminate.  
- Any exception raised inside `make_conn_str` or `run_era` propagates out of this function and will also result in a non‑zero exit status, as the wrapper does not catch them.  
## FunctionDef copy_drug_era(pwprompt, searchpath, site, dburi)
**copy_drug_era**: The function of copy_drug_era is to copy the Drug Era table from a source schema into the central `dcc_pedsnet` schema for a specified site.

**parameters**:  
· **pwprompt** (bool): If True, the function will prompt the user on the command line for the database password (input is hidden).  
· **searchpath** (str): The PostgreSQL schema search path that identifies the source schema; it is passed through to the connection‑string builder and later used by the copy routine.  
· **site** (str): Identifier of the site whose drug‑era data are being copied; this value is forwarded to the logging performed by the underlying copy routine.  
· **dburi** (str): A PostgreSQL URI that specifies the target database (e.g., `postgresql://user:pass@host:port/dbname`). The URI is transformed into a libpq connection string.

**Code Description**:  
1. **Password handling** – The variable `password` is initialised to `None`. If `pwprompt` is True, the function calls `click.prompt('Database password', hide_input=True)` to read a password from the terminal without echoing it, and stores the result in `password`.  
2. **Build connection string** – `make_conn_str(dburi, searchpath, password)` is invoked. This utility parses the supplied URI, incorporates the optional `searchpath` (as a PostgreSQL `options` parameter) and the optional `password`, and returns a libpq‑compatible connection string (`conn_str`).  
3. **Import copy routine** – The function imports `copy_era_dcc` from `pedsnetdcc.era`. This import is performed locally to avoid circular imports and to keep the import cost limited to the execution path of this command.  
4. **Execute copy** – `copy_era_dcc("drug", conn_str, site, searchpath)` is called. The first argument `"drug"` tells the helper that a drug‑era table is to be copied. The helper performs logging, determines the primary source schema from `searchpath`, builds and runs the appropriate `INSERT … SELECT` statement, and returns a Boolean indicating success.  
5. **Exit handling** – If `copy_era_dcc` returns a falsy value, the function terminates the process with `sys.exit(1)` to signal failure. Otherwise it terminates with `sys.exit(0)` to signal success. No further cleanup or exception handling is performed inside `copy_drug_era`; any uncaught exception will cause the Python interpreter to exit with a non‑zero status automatically.

**Note**:  
- The function relies on the correctness of the supplied `dburi` and `searchpath`; malformed values will cause `make_conn_str` or the downstream copy routine to raise exceptions.  
- Password prompting occurs only when `pwprompt` is True; otherwise the password must be present in the URI or omitted entirely (in which case the database may use other authentication methods).  
- The exit codes are fixed: `0` for success, `1` for failure. Scripts that invoke this command should interpret the process return code accordingly.  
- All logging is performed inside `copy_era_dcc`; `copy_drug_era` itself does not emit log messages. Ensure that the logger configuration in the `pedsnetdcc` package is set up before calling this function.  
- Because the function calls `sys.exit`, it should be used as a top‑level command‑line entry point rather than as a library function called from other Python code that expects to continue execution after the call.
## FunctionDef run_r_drug_era(pwprompt, searchpath, site, copy, neg_ids, model_version, idname, notable, noids, nopk, novac, size, start, notrunc, dburi)
**run_r_drug_era**: The function of run_r_drug_era is to orchestrate the execution of the Drug Era derivation workflow from the command‑line interface.

**parameters**:  
· pwprompt: Boolean flag indicating whether the user should be prompted for the database password.  
· searchpath: Schema search_path to be applied when constructing the database connection string.  
· site: Identifier of the site (e.g., a study site) that is passed to the derivation routine.  
· copy: Boolean flag that determines whether the resulting table should be copied to the `dcc_pedsnet` schema.  
· neg_ids: Boolean flag controlling whether negative IDs are included in the output.  
· model_version: String specifying the model version to be used by the derivation.  
· idname: Name of the identifier column to be added to the output table.  
· notable: Boolean flag indicating whether the output table should be marked as “notable”.  
· noids: Boolean flag that suppresses the addition of IDs to the output.  
· nopk: Boolean flag that suppresses the creation of a primary key on the output table.  
· novac: Boolean flag that suppresses vacuuming of the output table after creation.  
· size: Integer or string controlling the batch size for the derivation process.  
· start: Integer indicating the starting offset for processing (used for incremental runs).  
· notrunc: Boolean flag that prevents truncation of the target table before loading new data.  
· dburi: PostgreSQL URI that identifies the target database (e.g., `postgresql://user:pass@host/dbname`).

**Code Description**:  
1. The function begins by initializing a local variable `password` to `None`.  
2. If `pwprompt` is true, the function uses `click.prompt` to request the database password from the user, hiding the input. The entered value is stored in `password`.  
3. It calls the utility `make_conn_str` (defined in `pedsnetdcc/utils.py`) with three arguments: the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. `make_conn_str` returns a libpq‑compatible connection string that incorporates the search_path and password overrides. The result is stored in `conn_str`.  
4. The function imports the actual derivation implementation `run_r_drug_era` from the module `pedsnetdcc.r_drug_era`. This import is performed locally to avoid circular imports and to keep the CLI entry point lightweight.  
5. It invokes the imported `run_r_drug_era` with the constructed `conn_str` followed by all command‑line parameters (`site, copy, neg_ids, searchpath, password, model_version, idname, notable, noids, nopk, novac, size, start, notrunc`). The underlying function performs the four logical steps described in the docstring (run derivation, add ids, add concept names, copy to `dcc_pedsnet`, vacuum). The return value `success` is a Boolean indicating whether the derivation completed without error.  
6. If `success` is false, the CLI exits with status code 1 (`sys.exit(1)`), signalling failure to the operating system.  
7. If `success` is true, the CLI exits cleanly with status code 0 (`sys.exit(0)`).

**Note**:  
- The password is only prompted when `pwprompt` is true; otherwise the password must be supplied either in the `dburi` or via the `searchpath` argument.  
- The connection string construction is delegated to `make_conn_str`, which ensures that any explicit `password` argument overrides a password embedded in the URI, and that the provided `searchpath` overrides any existing `search_path` option.  
- The function does not perform any validation of the other parameters; they are passed directly to the underlying derivation routine, which is responsible for interpreting them.  
- The use of `sys.exit` means that this function is intended to be the final step of a command‑line entry point; any code after the call will not be executed.  
- Importing `run_r_drug_era` from `pedsnetdcc.r_drug_era` inside the function avoids unnecessary imports when the CLI command is not invoked, reducing start‑up overhead.
## FunctionDef run_r_drug_era_test(pwprompt, searchpath, site, copy, neg_ids, model_version, idname, notable, noids, nopk, novac, size, dburi)
**run_r_drug_era_test**: The function of run_r_drug_era_test is to serve as the command‑line entry point that prepares a database connection string, optionally prompts for a password, and invokes the full drug‑era derivation workflow implemented in pedsnetdcc.r_drug_era.run_r_drug_era.

**parameters**:  
· pwprompt (bool): If True, the function will prompt the user on the console for the database password (input hidden).  
· searchpath (str): PostgreSQL schema search_path to be embedded in the connection string.  
· site (str): Identifier of the site for which the drug‑era derivation is executed; passed through to the derivation routine.  
· copy (bool): When True, the derived rows are copied into the central dcc_pedsnet.drug_era table after the derivation.  
· neg_ids (bool): If True, a sequence that generates negative identifiers is used for the era IDs; otherwise a normal positive sequence is created.  
· model_version (str): PEDSnet data‑model version (e.g., “2.3.0”) required for ID‑assignment and vacuum steps.  
· idname (str): Prefix for the last‑id tracking table (e.g., “dcc” or “onco”).  
· notable (bool): When True the derivation skips table recreation steps (drop PK, drop NOT NULL, truncate) and runs only post‑processing.  
· noids (bool): When True the ID‑assignment step is omitted.  
· nopk (bool): When True the primary‑key creation step is omitted.  
· novac (bool): When True the VACUUM ANALYZE step is omitted.  
· size (str): Number of persons to process per batch; passed as a string for CLI compatibility and later converted to integer inside the derivation.  
· dburi (str): PostgreSQL URI that identifies the target database (e.g., postgresql://user:pass@host:port/dbname[?params]).  

**Code Description**:  
1. **Password handling** – The variable `password` is initialised to None. If the `pwprompt` flag is True, the function uses `click.prompt` to request the database password from the user, hiding the input.  
2. **Connection string construction** – The helper `make_conn_str` (from pedsnetdcc.utils) is called with the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. It returns a libpq‑compatible connection string that incorporates the search_path and password as needed.  
3. **Import of derivation routine** – The function imports `run_r_drug_era` from pedsnetdcc.r_drug_era. This import is performed locally to avoid unnecessary loading when the entry point is not used.  
4. **Invocation of the derivation** – `run_r_drug_era` is called with the prepared connection string and all command‑line arguments, plus an explicit `test=True` flag. The argument order matches the signature of `run_r_drug_era`, ensuring that each option (copy, neg_ids, etc.) is forwarded unchanged.  
5. **Result handling** – The return value of `run_r_drug_era` is stored in `success`. If `success` is falsy, the process terminates with exit code 1 (`sys.exit(1)`), indicating failure. If the call succeeds, the process exits with code 0 (`sys.exit(0)`), indicating normal completion.  

**Relationship with other objects**:  
- **Callee make_conn_str** (pedsnetdcc.utils): builds the libpq connection string from the supplied DBURI, search_path, and password.  
- **Callee run_r_drug_era** (pedsnetdcc.r_drug_era): performs the complete drug‑era derivation, including SQL preparation, execution of the R script, ID assignment, optional copying to the central schema, primary‑key creation, and vacuuming. All flags passed to run_r_drug_era are directly derived from the parameters of run_r_drug_era_test.  
- **External dependency click**: used only for interactive password prompting when pwprompt is True.  

**Note**:  
- The function expects a valid PostgreSQL URI; malformed URIs will cause make_conn_str to raise ValueError.  
- When pwprompt is False and no password is embedded in the URI, the derived connection string will contain no password, which may lead to authentication failures.  
- The test=True argument forces run_r_drug_era to use the “drug_era_test” R package instead of the production package; this is intended for development or CI environments.  
- The function terminates the Python interpreter via sys.exit; therefore it should be used only as a top‑level CLI command, not as a library call from other Python code.  
- All boolean flags are passed through unchanged; callers must ensure they reflect the desired post‑derivation actions (e.g., copy, nopk, novac).
## FunctionDef run_condition_era(pwprompt, searchpath, site, copy, neg_ids, no_ids, no_concept, model_version, idname, notable, nopk, novac, dburi)
**run_condition_era**: The function of **run_condition_era** is to execute the full Condition‑Era derivation workflow for a specified site, using command‑line options to control password prompting, schema search path, and various derivation flags.

**parameters**  
· **pwprompt** (bool): If True, the function prompts the user on the console for the database password (input is hidden).  
· **searchpath** (str): PostgreSQL schema search_path that will be embedded in the connection string; the first schema in the list is used as the target schema for all operations.  
· **site** (str): Identifier of the site for which the derivation is being performed; this value is written into the `site` column of the condition_era table.  
· **copy** (bool): When True, the resulting condition_era table is copied into the central `dcc_pedsnet` schema after derivation.  
· **neg_ids** (bool): If True, identifiers assigned to rows are generated as negative numbers (the “negative‑ID” convention used by PEDSnet).  
· **no_ids** (bool): If True, the step that assigns identifiers to rows is skipped.  
· **no_concept** (bool): If True, the step that populates concept‑name columns is skipped.  
· **model_version** (str): PEDSnet data‑model version (e.g., `"2.3.0"`); required for metadata lookup during ID assignment and vacuuming.  
· **idname** (str): Prefix for the last‑ID tracking table (e.g., `"dcc"` or `"onco"`).  
· **notable** (bool): If True, the derivation SQL query itself is not executed; subsequent steps (ID assignment, concept‑name addition, copying, vacuum) are still performed unless their own flags suppress them.  
· **nopk** (bool): If True, the primary‑key constraint is not added after IDs are assigned.  
· **novac** (bool): If True, the final `VACUUM ANALYZE` step is omitted.  
· **dburi** (str): PostgreSQL URI that specifies host, port, database name, user, and optional query parameters. The URI is converted to a libpq connection string by `make_conn_str`.

**Code Description**  
1. **Password handling** – The variable `password` is initialised to `None`. If `pwprompt` is True, the function uses `click.prompt` to request the database password from the user, hiding the input. The obtained password (or `None`) is later passed to `make_conn_str`.  
2. **Connection‑string construction** – `make_conn_str` is called with three arguments: the supplied `dburi`, the `searchpath`, and the (optional) `password`. This helper parses the URI, merges or overrides the `options` component to include the desired `search_path`, and returns a libpq‑compatible connection string (`conn_str`).  
3. **Import of the core derivation routine** – The function imports `run_era` from `pedsnetdcc.era`. This import is performed locally to avoid unnecessary loading when the module is imported but the command is not executed.  
4. **Invocation of the generic era runner** – `run_era` is called with the following arguments:  
   - `"condition"` – fixed era type indicating a Condition‑Era derivation.  
   - `conn_str` – the connection string built in step 2.  
   - `site`, `copy`, `neg_ids`, `no_ids`, `no_concept`, `searchpath`, `model_version`, `idname`, `notable`, `nopk`, `novac` – all forwarded directly from the CLI options.  
   The call returns a Boolean `success` indicating whether the entire derivation pipeline completed without error.  
5. **Process termination** – If `success` is `False`, the function terminates the Python process with exit status 1 (`sys.exit(1)`), signalling failure to the calling shell. If `success` is `True`, the function exits with status 0 (`sys.exit(0)`), indicating successful completion. No further Python code is executed after the exit call.

**Relationship with other project components**  
- **Callee `make_conn_str` (pedsnetdcc/utils.py)** – Builds the libpq connection string from the supplied URI, optional search_path, and optional password. This ensures that all subsequent database operations use a consistent connection configuration.  
- **Callee `run_era` (pedsnetdcc/era.py)** – Implements the complete derivation workflow for Condition, Drug, or Drug‑SCDF eras. `run_condition_era` is a thin wrapper that fixes the `era_type` argument to `"condition"` and forwards all other options unchanged. `run_era` performs the actual SQL execution, ID assignment, concept‑name population, optional copying to `dcc_pedsnet`, and final vacuuming.  
- **CLI integration** – `run_condition_era` is exposed as a Click command in `pedsnetdcc/main.py`. It is invoked from the command line as part of the `run_derivations` suite or directly by users who need only the Condition‑Era derivation.

**Note**  
- The function terminates the entire Python process via `sys.exit`; therefore it should be used only as a top‑level command‑line entry point, not as a library function called from other Python code.  
- If `pwprompt` is False and the URI already contains a password, that password is used; otherwise authentication must be handled by other means (e.g., `.pgpass` file or environment variables).  
- All flags (`copy`, `neg_ids`, `no_ids`, `no_concept`, `notable`, `nopk`, `novac`) are passed directly to `run_era`; their semantics are defined in the documentation of `run_era`.  
- The `searchpath` argument determines the target schema for the derivation; it must be a comma‑separated list of schemas, with the first schema being the one that receives the new `condition_era` table.  
- Errors raised inside `run_era` (e.g., database execution failures) cause `run_era` to return `False`, which leads to an exit status of 1. No exception handling is performed beyond this return‑value check.
## FunctionDef copy_condition_era(pwprompt, searchpath, site, dburi)
**copy_condition_era**: The function of copy_condition_era is to copy the Condition Era table from a source schema into the central `dcc_pedsnet` schema for a specified site.

**parameters**:  
· **pwprompt** (bool): If True, the function will prompt the user on the command line for the database password (input is hidden).  
· **searchpath** (str): The PostgreSQL schema search path that identifies the source schema; it is passed through to the connection string and later to the era‑copy routine.  
· **site** (str): Identifier of the site whose condition era data are being copied; this value is forwarded to the logging performed by the underlying copy routine.  
· **dburi** (str): A PostgreSQL URI that describes the target database (e.g., `postgresql://user@host/dbname?...`). The URI is converted to a libpq connection string.

**Code Description**:  
1. **Initialize password variable** – `password` is set to `None`.  
2. **Optional password prompt** – If `pwprompt` is true, the function uses `click.prompt` with `hide_input=True` to read the password from the terminal and stores it in `password`.  
3. **Build connection string** – Calls `make_conn_str(dburi, searchpath, password)` (from `pedsnetdcc.utils`). This utility parses the URI, inserts or overrides the `search_path` and `password` options, and returns a libpq‑compatible connection string (`conn_str`).  
4. **Import copy routine** – Dynamically imports `copy_era_dcc` from `pedsnetdcc.era`.  
5. **Execute era copy** – Invokes `copy_era_dcc("condition", conn_str, site, searchpath)`. The callee performs the actual INSERT … SELECT operation that moves rows from the source `condition_era` table into `dcc_pedsnet.condition_era`, while logging start, end, and elapsed time. The call returns a boolean `success`.  
6. **Exit handling** – If `success` is falsy, the process terminates with exit code 1 (`sys.exit(1)`). Otherwise it terminates with exit code 0 (`sys.exit(0)`).

**Note**:  
- The function does not perform any validation of the input parameters beyond what is done by the called utilities; an invalid `searchpath` or malformed `dburi` will cause `make_conn_str` or `copy_era_dcc` to raise an exception.  
- Password prompting occurs only when `pwprompt` is true; otherwise the password must be present in the URI or supplied via other authentication mechanisms supported by libpq.  
- The exit calls (`sys.exit`) end the entire Python process; this function is intended to be used as a command‑line entry point, not as a library routine that returns a value.  
- All logging is handled inside `copy_era_dcc`; callers should ensure that the global logger is configured before invoking this function.  
- The function imports `copy_era_dcc` locally to avoid circular imports and to keep the import cost limited to the execution path of this command.
## FunctionDef run_drug_condition_era(pwprompt, searchpath, site, copy, neg_ids, no_ids, no_concept, model_version, idname, dburi)
**run_drug_condition_era**: The function of **run_drug_condition_era** is to execute the full derivation workflow for both Drug Era and Condition Era tables in a single run.

**parameters**:  
· **pwprompt** (bool): If True, the function will interactively prompt the user for the database password.  
· **searchpath** (str): PostgreSQL schema search_path that determines the target schema for all operations.  
· **site** (str): Identifier of the site (e.g., “dcc”, “onco”) that will be recorded in the derived era tables.  
· **copy** (bool): When True, the derived era tables are copied into the central `dcc_pedsnet` schema after derivation.  
· **neg_ids** (bool): If True, identifiers assigned to rows will be negative values (historical PEDSnet convention).  
· **no_ids** (bool): If True, the step that assigns identifiers to rows is skipped.  
· **no_concept** (bool): If True, the step that populates concept‑name columns is skipped.  
· **model_version** (str): PEDSnet data‑model version (e.g., “2.3.0”) required for metadata lookup.  
· **idname** (str): Prefix for the last‑ID tracking table (e.g., “dcc” or “onco”).  
· **dburi** (str): PostgreSQL URI that specifies host, port, database name, user and optional parameters.  

**Code Description**:  
1. **Password handling** – The variable `password` is initialized to `None`. If `pwprompt` is True, the function uses `click.prompt` to request the database password from the user without echoing the input.  

2. **Connection string construction** – The helper `make_conn_str` (from `pedsnetdcc.utils`) is called with the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. The result, `conn_str`, is a libpq‑compatible connection string that will be used for every subsequent database operation.  

3. **Derivation of Drug Era** –  
   * The function imports `run_era` from `pedsnetdcc.era`.  
   * It calls `run_era("drug", conn_str, site, copy, neg_ids, no_ids, no_concept, searchpath, model_version, idname)`.  
   * `run_era` performs the complete drug‑era workflow (SQL execution, ID assignment, concept‑name population, optional copy, vacuum, etc.).  
   * The return value `success` is examined; if it is falsy, the process terminates with `sys.exit(1)`.  

4. **Derivation of Condition Era** – The same steps are repeated for the condition era:  
   * `run_era("condition", conn_str, site, copy, neg_ids, no_ids, no_concept, searchpath, model_version, idname)` is invoked.  
   * Failure again results in `sys.exit(1)`.  

5. **Successful completion** – If both calls to `run_era` return True, the function ends with `sys.exit(0)`, signalling a successful execution to the operating system.  

**Relationship with other objects**:  
* **run_era** (in `pedsnetdcc/era.py`) is the core worker that carries out the actual derivation logic for each era type. `run_drug_condition_era` is essentially a thin wrapper that prepares the connection string and invokes `run_era` twice, once for “drug” and once for “condition”.  
* **make_conn_str** (in `pedsnetdcc/utils.py`) builds the libpq connection string from the supplied URI, optional search_path, and optional password. This ensures that all subsequent database calls use a consistent connection configuration.  
* **click.prompt** is used only for interactive password entry; no other external libraries are imported inside the function.  

**Note**:  
- The function exits the entire Python process using `sys.exit`; it is intended to be used as a command‑line entry point rather than as a library call.  
- Because imports of `run_era` occur inside the function body, any import‑time side effects in `pedsnetdcc.era` are delayed until the function is executed.  
- The DBURI must follow the PostgreSQL URI syntax (`postgresql://[user[:password]@][netloc][:port][/dbname][?param=value...]`). Invalid URIs will cause `make_conn_str` to raise a `ValueError`.  
- All parameters are passed unchanged to `run_era`; therefore the same flags (`copy`, `neg_ids`, `no_ids`, `no_concept`, etc.) affect both drug and condition derivations.  
- If `pwprompt` is False and the URI does not contain a password, the connection will be attempted without authentication, which may fail depending on the server configuration.  
- The function does not perform any logging itself; any informational or error messages are produced by `run_era` and its helper functions.
## FunctionDef copy_to_measurement(pwprompt, searchpath, site, table, out_table_bmi, out_table_bmiz, out_table_ht_z, out_table_wt_z, dburi)
**copy_to_measurement**: The function of copy_to_measurement is to orchestrate the copying of BMI, BMIZ, height‑Z, and weight‑Z measurement rows from temporary output tables into the canonical measurement tables in the target schema.

**parameters**: The parameters of this Function.  
· pwprompt: Boolean flag indicating whether the database password should be requested interactively from the user.  
· searchpath: String containing the PostgreSQL schema search path (e.g., `"dcc_pedsnet, public"`).  
· site: Identifier of the site for which the copy operation is being performed.  
· table: Name of the destination measurement table (typically `"measurement"` or `"measurement_anthro"`).  
· out_table_bmi: Name of the temporary table that holds BMI results.  
· out_table_bmiz: Name of the temporary table that holds BMI‑Z results.  
· out_table_ht_z: Name of the temporary table that holds height‑Z results.  
· out_table_wt_z: Name of the temporary table that holds weight‑Z results.  
· dburi: PostgreSQL URI that identifies the target database (e.g., `"postgresql://user@host/dbname"`).

**Code Description**:  
1. **Password handling** – If `pwprompt` is true, the function prompts the user for the database password using `click.prompt` with hidden input. The entered password is stored in the local variable `password`; otherwise `password` remains `None`.  
2. **Connection string construction** – The helper `make_conn_str` (from `pedsnetdcc.utils`) is called with the supplied `dburi`, the `searchpath`, and the optional `password`. This produces a libpq‑compatible connection string (`conn_str`) that incorporates the search path via the `options` parameter and overrides the password if one was entered.  
3. **Import of sub‑routines** – The function imports `copy_z_measurement` from `pedsnetdcc.z_score` and `copy_bmi_measurement` from `pedsnetdcc.bmi`. These imports are local to the function to avoid circular import issues.  
4. **Copy BMI measurements** – `copy_bmi_measurement` is invoked with `conn_str`, `site`, `table`, `out_table_bmi`, and `searchpath`. The function returns a Boolean `success`. If the return value is falsy, `copy_to_measurement` terminates the process with `sys.exit(1)`.  
5. **Copy BMI‑Z measurements** – `copy_z_measurement` is called with the arguments `('bmiz', conn_str, site, table, out_table_bmiz, searchpath)`. The same success‑check and early exit pattern is applied.  
6. **Copy height‑Z measurements** – `copy_z_measurement` is called with `('ht_z', ...)` and the corresponding temporary table name. Failure again triggers `sys.exit(1)`.  
7. **Copy weight‑Z measurements** – `copy_z_measurement` is called with `('wt_z', ...)` and the corresponding temporary table name. Failure triggers `sys.exit(1)`.  
8. **Successful completion** – If all four copy operations succeed, the function ends the program with `sys.exit(0)`, indicating a clean exit status.

**Relationship with callees**:  
- **copy_bmi_measurement** (in `pedsnetdcc/bmi.py`) performs the actual INSERT … SELECT from the BMI temporary table into the target measurement table, handling logging, schema resolution, and timing.  
- **copy_z_measurement** (in `pedsnetdcc/z_score.py`) performs analogous work for the three Z‑score tables (BMI‑Z, Height‑Z, Weight‑Z). Both helpers rely on the internal utility `_copy_to_measurement_table` to execute the database INSERT and on shared logging utilities (`logger`, `combine_dicts`, `primary_schema`, `secs_since`).  
- **make_conn_str** (in `pedsnetdcc/utils.py`) builds the connection string that is passed to the two copy helpers. It parses the supplied URI, injects the search path, and optionally overrides the password.

**Note**:  
- The function terminates the entire Python process on any failure (`sys.exit(1)`). Callers must be prepared for the process to exit rather than receiving an exception.  
- Password prompting occurs only when `pwprompt` is true; otherwise the password must already be encoded in `dburi` or supplied via other authentication mechanisms (e.g., `.pgpass`).  
- All copy helpers assume that the destination measurement table and the source temporary tables already exist in the primary schema identified by the first entry of `searchpath`. Missing tables will cause the underlying `_copy_to_measurement_table` to raise a `DatabaseError`, which propagates up to this function and results in a non‑zero exit status.  
- No explicit transaction management is performed; each helper runs in autocommit mode, so each INSERT is committed immediately.  
- The function does not return a value; its success is communicated solely through the process exit code.  
- Because imports are performed inside the function body, the module can be imported without triggering heavy database‑related imports unless `copy_to_measurement` is actually executed.
## FunctionDef partition_measurement(pwprompt, searchpath, dcc, site3, no_vital, model_version, dburi)
**partition_measurement**: The function of partition_measurement is to orchestrate the partitioning of the core `measurement` table by building a database connection string, invoking the partitioning routine, and handling exit status.

**parameters**:  
· **pwprompt** (bool): If true, the function prompts the user for the database password on the command line.  
· **searchpath** (str): The PostgreSQL schema search path that will be embedded in the connection string.  
· **dcc** (bool): Flag indicating whether DCC‑level partitions (`measurement_anthro`, `measurement_labs`, `measurement_vitals`) should be created.  
· **site3** (bool): Flag indicating whether site‑level partitions should be created using the DCC‑style table set.  
· **no_vital** (bool): When true, vitals tables are omitted from the partition set.  
· **model_version** (str): The PEDSnet data model version (e.g., `"2.3.0"`); passed through to the underlying partitioning function for logging purposes.  
· **dburi** (str): PostgreSQL URI that identifies the target database (e.g., `postgresql://user:pass@host:port/dbname`).  

**Code Description**:  

1. **Password handling** – If `pwprompt` is true, the function uses `click.prompt` to request the database password from the user without echoing the input. The entered password is stored in the local variable `password`; otherwise `password` remains `None`.  

2. **Connection string construction** – The helper `make_conn_str` (from `pedsnetdcc.utils`) is called with the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. This returns a libpq‑compatible connection string (`conn_str`) that incorporates the search path and password overrides as needed.  

3. **Import of the core partition routine** – The function imports `partition_measurement_table` from `pedsnetdcc.partition_measurement`. This import is performed locally so that the heavy module is only loaded when the command is actually executed.  

4. **Invocation of the partitioning logic** – `partition_measurement_table` is called with the constructed `conn_str`, the `model_version`, the original `searchpath`, and the three boolean flags (`dcc`, `site3`, `no_vital`). The routine performs all DDL operations required to:  
   * truncate the central `measurement` table,  
   * add check constraints to each partition table,  
   * set up inheritance from the core table,  
   * create the routing trigger function and its BEFORE INSERT trigger.  
   The call returns a boolean `success` indicating whether the operation completed without error.  

5. **Exit handling** – If `success` is `False`, the function terminates the process with exit code 1 (`sys.exit(1)`), signalling failure to the calling environment. If the call succeeded, the function exits with code 0 (`sys.exit(0)`), indicating normal completion.  

**Relationship with other objects**:  

* **Callees** –  
  * `make_conn_str` (in `pedsnetdcc.utils`) builds the libpq connection string from the URI, optional search path, and optional password.  
  * `partition_measurement_table` (in `pedsnetdcc.partition_measurement`) carries out the actual database modifications; it is the core implementation of the partitioning algorithm.  

* **Callers** – This function is exposed as a Click command in the `pedsnetdcc` command‑line interface. It is invoked when a user runs the `partition_measurement` sub‑command, passing the appropriate CLI options that map directly to the parameters described above.  

**Note**:  
- The function does not perform any logging itself; all informational and error messages are produced inside `partition_measurement_table`.  
- Because the function exits the interpreter via `sys.exit`, it should be used only as a top‑level command entry point, not as a library function called from other Python code.  
- The password prompt is only triggered when `pwprompt` is true; otherwise the password must be present in the URI or supplied via other means (e.g., environment variables).  
- The `searchpath` argument is passed unchanged to `make_conn_str`; ensure that it is a comma‑separated list of schemas with the primary schema first, as required by the downstream partitioning logic.  
- Any exception raised inside `partition_measurement_table` will propagate up and cause the process to exit with code 1, because the function checks the returned boolean and calls `sys.exit(1)` on failure.
## FunctionDef unpartition_measurement(pwprompt, searchpath, dcc, site3, no_vital, model_version, dburi)
**unpartition_measurement**: The function of **unpartition_measurement** is to serve as a command‑line entry point that removes the partitioning objects (trigger, check constraints, inheritance) from the measurement tables by delegating to the core implementation **unpartition_measurement_table**.

**parameters**:  
· **pwprompt** – Boolean flag; when true the function prompts the user for the database password on the console (input is hidden).  
· **searchpath** – String containing the PostgreSQL schema search_path that will be used when building the connection string.  
· **dcc** – Boolean flag indicating that the operation should target the DCC‑level split tables (measurement_anthro, measurement_labs, measurement_vitals).  
· **site3** – Boolean flag indicating that the operation should target a site but use the DCC‑style partition layout.  
· **no_vital** – Boolean flag; when true the vitals tables are excluded from the unpartitioning steps.  
· **model_version** – String identifying the PEDSnet model version (e.g., “2.3.0”). It is passed through to the underlying unpartition function for logging purposes.  
· **dburi** – String containing a PostgreSQL URI in libpq format (e.g., `postgresql://user:pass@host:port/dbname?...`). This URI is the basis for the database connection.

**Code Description**:  
1. **Password handling** – The variable `password` is initialised to `None`. If `pwprompt` is true, the function uses `click.prompt` to request the database password from the user, hiding the input. The obtained password (or `None`) is later supplied to the connection‑string builder.  

2. **Connection string construction** – `make_conn_str` (from `pedsnetdcc.utils`) is called with three arguments: the raw `dburi`, the supplied `searchpath`, and the possibly‑prompted `password`. This utility parses the URI, merges or overrides the `search_path` and password, and returns a libpq‑compatible connection string (`conn_str`).  

3. **Import of core unpartition routine** – The function imports `unpartition_measurement_table` from `pedsnetdcc.partition_measurement`. Import is performed locally to avoid unnecessary loading when the CLI command is not used.  

4. **Invocation of core logic** – `unpartition_measurement_table` is called with the prepared arguments:  
   - `conn_str` – the full connection string,  
   - `model_version` – for logging,  
   - `searchpath` – the original search_path (used again inside the core function to determine the primary schema),  
   - `dcc`, `site3`, `no_vital` – the same flags received by the CLI wrapper.  
   The function returns a Boolean `success` indicating whether all DDL operations completed without error.  

5. **Process termination** – If `success` is `False`, the wrapper exits the process with status code 1 (`sys.exit(1)`), signalling an error to the shell. If `success` is `True`, the wrapper exits with status code 0 (`sys.exit(0)`), indicating normal completion. No further code is executed after the exit call.  

**Relationship with other objects**:  
- **make_conn_str** (in `pedsnetdcc.utils`) is the utility that translates the user‑provided URI, optional search_path, and optional password into a libpq connection string suitable for `psycopg2`.  
- **unpartition_measurement_table** (in `pedsnetdcc.partition_measurement`) contains the actual DDL logic: dropping the before‑insert trigger, removing check constraints, and removing inheritance from the split measurement tables. The CLI wrapper passes all necessary context to this function and interprets its Boolean result.  
- **click** is used only for interactive password prompting; it does not affect the core database logic.  
- **sys** is used solely for terminating the process with the appropriate exit code.

**Note**:  
- This function is intended to be invoked from the command line; it does not return a value to Python callers because it terminates the interpreter with `sys.exit`.  
- The password prompt is optional; if `pwprompt` is false, any password embedded in the `dburi` will be used, or none if the URI does not contain one.  
- All validation of flags and construction of the DDL statements occurs inside `unpartition_measurement_table`; this wrapper performs no additional checks.  
- Because the function calls `sys.exit` directly, it should be used only as a top‑level entry point (e.g., via a Click command) and not imported for reuse in other Python code.  
- Errors raised inside `unpartition_measurement_table` are propagated as a `False` return value; the wrapper then exits with a non‑zero status, allowing external scripts or CI pipelines to detect failure.
## FunctionDef create_oracle_views_sql(model_version, source_schema, target_schema, file_name)
**create_oracle_views_sql**: The function of create_oracle_views_sql is to generate Oracle‑compatible view and grant SQL statements for a given PEDSnet model version and terminate the script with an appropriate exit status.  

**parameters**:  
· model_version: string indicating the PEDSnet model version (e.g., “2.3.0”).  
· source_schema: string name of the schema that contains the original tables.  
· target_schema: string name of the schema where the Oracle views will be created.  
· file_name: string path of the file that will receive the generated SQL statements.  

**Code Description**:  
1. The function imports the helper **create_oracle_views** from the module *pedsnetdcc.views*.  
2. It forwards the four received arguments (model_version, source_schema, target_schema, file_name) to **create_oracle_views**. This helper performs the heavy lifting: it reads the model metadata, builds a CREATE VIEW statement and a corresponding GRANT SELECT statement for every table, writes both groups of statements to the supplied file, logs progress, and finally returns a Boolean indicating success.  
3. The Boolean result from **create_oracle_views** is stored in the local variable *success*.  
4. If *success* is **False**, the function calls **sys.exit(1)**, causing the process to terminate with a non‑zero exit code that signals failure to the operating system or calling script.  
5. If *success* is **True**, the function calls **sys.exit(0)**, terminating the process with a zero exit code that signals successful completion.  
6. No further processing occurs after the exit call; the function does not return a value because the script ends at this point.  

**Relationship with other project components**:  
- **Callee**: *pedsnetdcc.views.create_oracle_views* – the only external function invoked. It encapsulates all logic for enumerating tables, constructing SQL strings, writing the output file, and logging.  
- **Caller**: This function is typically invoked from the command‑line entry point defined in *pedsnetdcc/main.py* when the user runs the “create_oracle_views_sql” command. The caller supplies the command‑line arguments and relies on the exit status to determine success or failure.  

**Note**:  
- The function assumes that **create_oracle_views** will either return **True** or raise an exception; any exception propagates out of this wrapper and will cause the script to exit with an uncaught error.  
- Because the function ends with **sys.exit**, it should be used only as a top‑level script entry point; calling it from other Python code will terminate the entire interpreter.  
- The module must import **sys** (or rely on a prior import) for the exit calls to work; otherwise a NameError will be raised.  
- The *file_name* argument is passed unchanged to the helper, so the caller must ensure the path is writable and that any existing file can be overwritten.  
- No validation of the input parameters is performed in this wrapper; all validation, if any, is handled inside **create_oracle_views**.
## FunctionDef prepdb(model_version, dcc_only, pwprompt, dburi)
**prepdb**: The function of prepdb is to create a new PEDSnet database and its associated schemas based on a specified model version and database URI.

**parameters**:  
· model_version: String indicating the PEDSnet model version (e.g., “2.1” or “2.1.3”).  
· dcc_only: Boolean flag; when True only the central DCC schema is created, otherwise site schemas are also created.  
· pwprompt: Boolean flag; when True the function prompts the user for the database password on the command line.  
· dburi: PostgreSQL URI that identifies the target server and optional database (e.g., `postgresql://user@host:port/dbname?param=value`).

**Code Description**:  
1. The function begins with a docstring that explains its purpose and the expected format of the `dburi` argument.  
2. It imports the `prepare_database` function from `pedsnetdcc.prepdb`. This imported function performs the actual creation of the database, schema generation, and permission grants.  
3. A local variable `password` is initialized to `None`.  
4. If `pwprompt` is `True`, the function uses `click.prompt` to request the database password from the user, hiding the input for security. The entered password is stored in `password`.  
5. The helper `make_conn_str` (from `pedsnetdcc.utils`) is called with `dburi` and the possibly‑provided `password`. `make_conn_str` converts the URI into a libpq‑compatible connection string, inserting the password if supplied. The resulting string is assigned to `conn_str`.  
6. `prepare_database` is invoked with the following arguments:  
   – `model_version` (the model identifier),  
   – `conn_str` (the connection string built in step 5),  
   – `update=False` (indicating that a new database should be created rather than updating an existing one),  
   – `dcc_only=dcc_only` (propagating the caller’s flag).  
   The function returns a Boolean `success` indicating whether the database preparation completed without error.  
7. If `success` is `False`, the script terminates with exit code 1 by calling `sys.exit(1)`. This signals an error to the operating system or calling process.  
8. If `success` is `True`, the script terminates normally with exit code 0 via `sys.exit(0)`. No further processing occurs after this point.

**Note**:  
- The function does not perform any logging itself; all informational output is handled inside `prepare_database`.  
- Password handling is optional; if `pwprompt` is `False` the password must already be embedded in `dburi` or omitted if the target server allows password‑less authentication.  
- The exit calls (`sys.exit`) make `prepdb` suitable as a command‑line entry point; it will end the Python process immediately after the database preparation step.  
- Errors raised inside `prepare_database` (e.g., connection failures, SQL errors) will propagate unless caught inside that function; `prepdb` only checks the Boolean return value.  
- The function relies on the external `click` library for interactive prompting and on the standard `sys` module for process termination.  
- Because `prepare_database` is called with `update=False`, the function always attempts to create a fresh database; callers must ensure that the target database name does not already exist unless they intend to handle the resulting error.
## FunctionDef prepdb_altname(model_version, name, addsites, skipsites, idname, alt_id_only, owner, pedsnet_only, inc_ext, new, limit, dcc_only, pwprompt, dburi)
**prepdb_altname**: The function of prepdb_altname is to orchestrate the creation of a new PostgreSQL database (using an alternate name supplied by the user) and to populate it with the required PEDSnet vocabularies and site schemas by delegating to the core preparation routine.

**parameters**: The parameters of this Function.  
· model_version: String identifying the PEDSnet model version (e.g., “2.1.3”).  
· name: Alternate name for the database to be created.  
· addsites: Comma‑separated list of site identifiers that should be added in addition to the default set.  
· skipsites: Comma‑separated list of site identifiers that should be omitted from the default set.  
· idname: Identifier name for the alternate ID namespace (e.g., “omop”).  
· alt_id_only: Boolean flag; when True and idname is not “dcc”, only the alternate ID schemas are created.  
· owner: Database role that will own the created schemas and be granted permissions.  
· pedsnet_only: Boolean flag; when True only PEDSnet schemas (no PCORnet) are created.  
· inc_ext: Boolean flag; when True external site schemas are included in the creation process.  
· new: Boolean flag; when True the database is created with the newer locale/template settings.  
· limit: Boolean flag; when True database and schema permissions are granted in a limited‑privilege mode.  
· dcc_only: Boolean flag; when True only the central DCC schemas are created (no site schemas).  
· pwprompt: Boolean flag; when True the function prompts the user for a database password on the command line.  
· dburi: PostgreSQL URI that identifies the server and initial connection database (e.g., “postgresql://user@host/dbname”).

**Code Description**:  
1. The function begins with a docstring that explains its purpose and the expected format of the PostgreSQL URI.  
2. It imports the core routine `prepare_database_altname` from `pedsnetdcc.prepdb`. This routine performs the actual database creation, schema generation, and permission granting.  
3. The parameters `new` and `limit` are normalised to explicit Boolean values: if the caller passed a truthy value they become `True`; otherwise they become `False`. This guarantees that the downstream call receives proper booleans.  
4. A local variable `password` is initialised to `None`. If the caller set `pwprompt` to `True`, the function uses `click.prompt` to request a password from the user, hiding the input. The entered password is stored in `password`.  
5. The helper `make_conn_str` (from `pedsnetdcc.utils`) is called with the supplied `dburi` and the optional `password`. `make_conn_str` parses the URI and returns a libpq‑compatible connection string that can be used by `psycopg2`.  
6. The function then calls `prepare_database_altname` with all user‑supplied options plus the generated connection string. The arguments are passed in the exact order expected by the callee:  
   - `model_version`  
   - `conn_str` (the libpq connection string)  
   - `name` (alternate database name)  
   - `addsites`  
   - `skipsites`  
   - `idname`  
   - `alt_id_only`  
   - `owner`  
   - `pedsnet_only`  
   - `inc_ext`  
   - `new` (boolean)  
   - `limit` (boolean)  
   - `update=False` (explicitly indicates that a new database should be created)  
   - `dcc_only=dcc_only` (passed as a named argument)  
   The return value `success` is a boolean indicating whether the preparation succeeded.  
7. If `success` is `False`, the function terminates the process with a non‑zero exit status (`sys.exit(1)`), signalling an error to the operating system or calling script.  
8. If `success` is `True`, the function exits cleanly with status `0` (`sys.exit(0)`). No further processing is performed after the call to `prepare_database_altname`.  

**Relationship with callees**:  
- `prepare_database_altname` (in `pedsnetdcc/prepdb.py`) carries out the heavy‑lifting: creating the database, granting permissions, building site and alternate‑ID schemas, and cleaning up external schemas as required.  
- `make_conn_str` (in `pedsnetdcc/utils.py`) converts the user‑provided PostgreSQL URI into a libpq connection string, handling optional password overrides and search‑path insertion.  
- `click.prompt` is used only when `pwprompt` is true, providing an interactive password prompt.  
- `sys.exit` is used to return an appropriate exit code to the shell based on the success of the operation.  

**Note**:  
- The function does not perform any validation of the textual parameters (`addsites`, `skipsites`, `name`, etc.); malformed input may cause the downstream preparation routine to fail.  
- The explicit conversion of `new` and `limit` to booleans ensures that values such as `0`, `1`, or string representations are correctly interpreted before being passed to the core routine.  
- When `pwprompt` is enabled, the password is read securely (input hidden) and injected into the connection string; otherwise the password component of the URI, if present, is used.  
- The function relies on the caller having sufficient privileges on the PostgreSQL server to create new databases and assign ownership to the specified `owner`.  
- Exit codes follow the conventional pattern: `0` for success, `1` for any failure in the preparation step.
## FunctionDef cleanup(backup_dir, site_root)
**cleanup**: The function of cleanup is to invoke the site‑directory cleanup process and terminate the program with an appropriate exit status.

**parameters**:  
· **backup_dir**: Path to the root directory where backups should be stored. This value is passed directly to `cleanup_site_directories`; if it is falsy, the default `BACKUP_ROOT` defined in the cleanup module will be used.  
· **site_root**: Path to the root directory that contains the site sub‑directories. This value is also forwarded to `cleanup_site_directories`; if it is falsy, the default `SITE_ROOT` defined in the cleanup module will be used.

**Code Description**:  
1. The function calls `cleanup_site_directories(backup_dir, site_root)`. This helper performs the actual work of backing up each configured site’s older data directories and deleting the originals after a successful backup.  
2. The return value from `cleanup_site_directories` is stored in the local variable `success`.  
3. If `success` is `False`, indicating that the cleanup process failed (for example, because a required root directory was missing or a site backup could not be completed), the function invokes `sys.exit(1)`. This causes the entire program to terminate with a non‑zero status, signalling an error to the operating system or calling script.  
4. If `success` is `True`, the function proceeds to `sys.exit(0)`, terminating the program with a zero status to indicate successful completion.

**Relationship with other objects**:  
- **Callee**: `cleanup_site_directories` (defined in `pedsnetdcc/cleanup.py`). `cleanup` acts as a thin wrapper that delegates all substantive work to this function and then handles the program’s exit code based on the Boolean result.  
- **External dependency**: The standard library module `sys` is used solely for the `sys.exit` calls that end the process.

**Note**:  
- `cleanup` does not perform any validation of its arguments itself; it relies entirely on `cleanup_site_directories` to handle missing or invalid paths.  
- Because the function always calls `sys.exit`, it should be used only as the entry point of a script or command‑line tool, not as a library function that other code expects to continue executing after the call.  
- The exit codes are conventional: `0` for success, `1` for any failure. Adjustments to these codes would require changes in the calling environment that interprets the program’s result.
## FunctionDef map_external_ids(dburi, in_file, search_path, out_file, table_name, pwprompt)
**map_external_ids**: The function of map_external_ids is to read a CSV file containing external identifiers, map those identifiers to internal DCC IDs using the database, and optionally write the mapping results to an output CSV file.

**parameters**:  
· dburi: PostgreSQL URI string that identifies the target database (e.g., `postgresql://user:pass@host:port/dbname`).  
· in_file: Path to the input CSV file that holds the external IDs to be mapped.  
· search_path: Schema search_path to be applied when connecting to the database; it is incorporated into the connection string.  
· out_file: Path to the output CSV file where the mapping results will be written. If omitted, a file name is derived from `in_file` by appending “_RESULTS.csv”.  
· table_name: Name of the database table that stores the external‑to‑DCC ID mapping.  
· pwprompt: Boolean flag indicating whether the user should be prompted for the database password at runtime.

**Code Description**:  
1. The function imports `map_external_ids` from `pedsnetdcc.external_id_mapper`. This imported function performs the actual database lookup and CSV generation.  
2. It initializes a local variable `password` to `None`.  
3. If `pwprompt` is true, the function uses `click.prompt` to request the database password from the user, hiding the input. The entered password is stored in `password`.  
4. When `out_file` is not supplied (i.e., evaluates to false), the function derives a default output filename: it removes the “.csv” suffix from `in_file` and appends “_RESULTS.csv”. This ensures that a result file is always produced.  
5. The function calls `make_conn_str` (defined in `pedsnetdcc.utils`) with the supplied `dburi`, the optional `password`, and the optional `search_path`. `make_conn_str` returns a libpq‑compatible connection string that incorporates the password and search_path overrides.  
6. With the constructed connection string (`conn_str`), the function invokes the external mapper:  
   `success = map_external_ids(conn_str, str(in_file), str(out_file), str(table_name), search_path)`  
   - `conn_str` provides the database connection details.  
   - `in_file`, `out_file`, and `table_name` are converted to strings to guarantee the correct type for the mapper.  
   - `search_path` is passed again because the mapper may need it for schema‑qualified queries.  
   The mapper returns a boolean `success` indicating whether the mapping completed without error.  
7. If `success` is false, the function terminates the process with a non‑zero exit code (`sys.exit(1)`), signalling failure to the calling environment.  
8. If `success` is true, the function exits cleanly with status 0 (`sys.exit(0)`).  

**Relationship with Callees**:  
- **make_conn_str** (pedsnetdcc.utils): Generates a connection string that respects the supplied `search_path` and `password`. This string is essential for the external mapper to open a database session.  
- **pedsnetdcc.external_id_mapper.map_external_ids**: Performs the core mapping logic—reading the input CSV, querying the specified table for DCC IDs, and writing the results to the output CSV. The CLI wrapper (`map_external_ids` in `main.py`) handles argument preparation, user interaction, and process termination, while delegating the heavy lifting to this callee.  

**Note**:  
- The function does not perform any validation on the existence or format of `in_file`; such checks are expected to be handled by the external mapper.  
- If `out_file` already exists, it will be overwritten without warning.  
- When `pwprompt` is true, the password prompt occurs before any connection string is built, ensuring that the password is available for `make_conn_str`.  
- The exit codes follow Unix conventions: `0` for success, `1` for failure. This behavior allows the command to be used in scripts and CI pipelines where the return status is inspected.  
- All path arguments are cast to `str` before being passed to the mapper, guaranteeing compatibility even if callers provide `pathlib.Path` objects.  
- The function relies on the global `sys` and `click` modules; they must be imported elsewhere in the module (as is typical for the CLI entry points).
## FunctionDef grant_permissions(dburi, pwprompt, full)
**grant_permissions**: The function of grant_permissions is to apply the required PostgreSQL permissions for all site schemas, vocabulary schemas, and optionally the loading‑user role on a target database.

**parameters**:  
· dburi: a PostgreSQL URI string that identifies the target database (e.g., `postgresql://user[:password]@host[:port]/dbname`).  
· pwprompt: a boolean flag; when True the function prompts the user for the database password on the command line.  
· full: a boolean flag; when True the function also grants loading‑user permissions in addition to the standard schema and vocabulary permissions.

**Code Description**:  
1. **Import permission helpers** – The function imports three permission‑granting utilities from `pedsnetdcc.permissions`:  
   * `grant_schema_permissions` – grants schema‑ and table‑level privileges for all site schemas.  
   * `grant_vocabulary_permissions` – grants privileges on the vocabulary schemas and tables.  
   * `grant_loading_user_permissions` – grants the special loading‑user role the privileges needed to load data.  

2. **Password handling** – If `pwprompt` is True, the function uses `click.prompt` with `hide_input=True` to request the database password from the user. The entered password is stored in the local variable `password`; otherwise `password` remains `None`.

3. **Build connection string** – The helper `make_conn_str` (from `pedsnetdcc.utils`) is called with the supplied `dburi` and the optional `password`. It returns a libpq‑style connection string (`conn_str`) that will be used by the permission‑granting functions.

4. **Full‑grant option** – If the `full` flag is True, `grant_loading_user_permissions(conn_str)` is executed first. This step grants the loading‑user role the necessary privileges on all site schemas (core and external, depending on defaults used by the callee).

5. **Grant schema permissions** – `grant_schema_permissions(conn_str)` is called next. This function iterates over the list of site schemas and executes a series of GRANT/ALTER statements to give the appropriate PEDSnet roles access to tables and schemas.

6. **Grant vocabulary permissions** – Finally, `grant_vocabulary_permissions(conn_str)` is invoked to apply a predefined set of GRANT statements on all vocabulary‑related schemas and tables.

7. **Execution flow** – Each of the three callee functions logs its start and completion, builds a `StatementList` of individual SQL commands, executes them serially (non‑transactionally), checks for errors, and logs elapsed time. `grant_permissions` itself does not perform any SQL directly; it merely orchestrates the calls.

**Note**:  
- The function does not return a value; success is indicated by the absence of an exception from any of the called helpers.  
- When `pwprompt` is False and the URI already contains a password, that password is used; otherwise the connection will be attempted without a password, which may fail if authentication is required.  
- The `full` flag is intended for situations where the loading‑user role must be (re)‑granted its privileges, such as after a manual schema change or when troubleshooting permission issues.  
- All permission‑granting operations are performed outside of a transaction because PostgreSQL does not allow many GRANT/ALTER commands inside a transaction block.  
- Errors raised by any of the callee functions propagate up to the CLI entry point, causing the command to terminate with an appropriate error message.
## FunctionDef vocab_indexes(pwprompt, searchpath, force, model_version, dburi)
**vocab_indexes**: The function of vocab_indexes is to coordinate the execution of the vocabulary‑index update process for a PEDSnet database, handling password prompting, connection‑string creation, and exit‑code handling.

**parameters**:  
· pwprompt (bool): When True, the function prompts the user on the command line for the database password; the entered password is hidden as it is typed.  
· searchpath (str): The PostgreSQL schema search_path that should be applied to the connection; it is incorporated into the libpq connection string.  
· force (bool): Passed through to the underlying index‑update routine; if True, benign errors such as “object already exists” or “object does not exist” are ignored.  
· model_version (str): The PEDSnet data‑model version identifier (e.g., “2.3.0”) that determines which index specifications are applied.  
· dburi (str): A PostgreSQL URI (e.g., “postgresql://user@host/dbname”) that serves as the base for building the libpq connection string.

**Code Description**:  
1. **Password handling** – The variable `password` is initialised to `None`. If `pwprompt` is true, the function uses `click.prompt` to request the database password from the user, hiding the input. The entered value is stored in `password`.  
2. **Connection‑string construction** – The helper `make_conn_str` (from `pedsnetdcc.utils`) is called with `dburi`, the supplied `searchpath`, and the possibly‑prompted `password`. This produces a libpq‑compatible connection string (`conn_str`) that encodes host, port, database name, user, password, and the `options` clause containing the `search_path`.  
3. **Invocation of the index‑update routine** – The function imports `run_vocab_indexes` from `pedsnetdcc.transform_runner` and calls it with the generated `conn_str`, the `model_version`, the original `searchpath`, and the `force` flag. `run_vocab_indexes` performs the actual addition and removal of vocabulary indexes and returns a boolean indicating success.  
4. **Exit‑code handling** – If `run_vocab_indexes` returns a falsy value (indicating failure), the function terminates the process with `sys.exit(1)`. If the call succeeds, the function terminates with `sys.exit(0)`. No further cleanup or exception handling is performed; any exception raised inside `run_vocab_indexes` propagates upward and results in a non‑zero exit status.  

**Note**:  
- The function is a thin CLI wrapper; it does not contain any business logic beyond password prompting and connection‑string preparation.  
- Because the function calls `sys.exit` directly, it should be used only as the entry point of a command‑line command; importing it for reuse in other Python code will cause the interpreter to exit.  
- The `force` flag is forwarded unchanged to `run_vocab_indexes`; callers must decide whether to tolerate benign index‑creation or index‑drop errors.  
- No exception handling is performed inside `vocab_indexes`; any unexpected error (e.g., malformed URI, failure in `make_conn_str`, or an exception inside `run_vocab_indexes`) will cause the process to exit with a non‑zero status, which is appropriate for a CLI tool.  
- The function relies on the external `click` library for password prompting and on the `make_conn_str` utility to correctly embed the `search_path` into the connection string; ensure these dependencies are available in the runtime environment.
## FunctionDef run_r_query(pwprompt, searchpath, site, package, model_version, copy, dburi)
**run_r_query**: The function of run_r_query is to orchestrate the execution of an R script by preparing a temporary Argos configuration file and invoking the R‑query runner with a properly constructed database connection string.

**parameters**:  
· pwprompt (bool): If True, the user will be prompted on the command line for the database password; the entered password is hidden as it is typed.  
· searchpath (str or None): The PostgreSQL schema search_path that should be injected into the connection string.  
· site (str): Identifier of the site (used to build the temporary Argos JSON filename).  
· package (str): Name of the package (used to build the temporary Argos JSON filename).  
· model_version (str): Version identifier of the data model; passed through to the R‑query runner.  
· copy (bool): Flag indicating whether the R script should copy data; passed through to the R‑query runner.  
· dburi (str): PostgreSQL URI that identifies the target database (e.g., `postgresql://user:pass@host:port/dbname`).  

**Code Description**:  
1. The function initializes a local variable `password` to `None`.  
2. If `pwprompt` is True, it uses `click.prompt` to request the database password from the user, hiding the input; the entered value is stored in `password`.  
3. It calls the utility `make_conn_str` (defined in *pedsnetdcc/utils.py*) with three arguments: the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. `make_conn_str` returns a libpq‑compatible connection string that incorporates the search_path and password overrides.  
4. It composes a temporary Argos configuration filename by concatenating `site`, an underscore, `package`, and the suffix `_argos_temp.json`. This file name is expected by the downstream R‑query runner.  
5. The function imports `run_r_query` from the module `pedsnetdcc.r_query`. This imported callable is distinct from the current function; it is the actual implementation that launches the R script.  
6. It invokes the imported `run_r_query` with the following arguments, in order:  
   - `config_file` (the temporary Argos JSON filename),  
   - `conn_str` (the connection string produced in step 3),  
   - `site`, `package`, `password`, `searchpath`, `model_version`, and `copy`.  
   The imported function returns a boolean `success` indicating whether the R script completed without error.  
7. If `success` is False, the process terminates with exit status 1 (`sys.exit(1)`), signalling failure to the operating system.  
8. If `success` is True, the process terminates with exit status 0 (`sys.exit(0)`), signalling successful completion.  

**Relationship with callees**:  
- **make_conn_str** (pedsnetdcc/utils.py) is called to translate the user‑provided PostgreSQL URI into a libpq connection string, applying any overrides for `search_path` and `password`. This ensures that the R script receives a connection string that matches the command‑line options.  
- **pedsnetdcc.r_query.run_r_query** is the core executor of the R script. It consumes the temporary Argos configuration file and the connection string, then runs the appropriate R code. The boolean result from this callee determines the exit status of the current function.  

**Note**:  
- The function does not create or write the Argos JSON file itself; it only determines the filename. The actual creation of the file must be performed elsewhere before this function is called.  
- Password handling is deliberately interactive only when `pwprompt` is True; otherwise the password must be embedded in the `dburi` or supplied via the `password` argument to `make_conn_str`.  
- The function exits the entire Python process using `sys.exit`; therefore it should be used as a top‑level command‑line entry point rather than as a library routine that expects to return control to the caller.  
- Because the imported `run_r_query` is executed after the import statement, any side effects (such as module‑level imports or configuration) occur at call time.  
- The temporary Argos filename follows the pattern `<site>_<package>_argos_temp.json`; callers must ensure that this naming convention does not clash with existing files in the working directory.  
## FunctionDef run_r_lab_loinc(pwprompt, searchpath, site, model_version, copy, dburi)
**run_r_lab_loinc**: The function of run_r_lab_loinc is to execute the Lab LOINC R script for a given site and, if the script succeeds, perform the required post‑processing steps.

**parameters**: The parameters of this Function.  
· **pwprompt** (bool): If True, the function will prompt the user on the command line for the database password; the entered password is not echoed.  
· **searchpath** (str): PostgreSQL schema search path that will be incorporated into the connection string and passed to the R script.  
· **site** (str): Identifier of the site for which the derivation is being run; used to name temporary files and for logging.  
· **model_version** (str): Version string of the PEDSnet data model (e.g., “2.3.0”). It is forwarded to the R‑script runner but not used directly inside this function.  
· **copy** (bool): When True, the R‑script runner will copy the generated results directory to the global output location.  
· **dburi** (str): PostgreSQL URI that identifies the target database (e.g., `postgresql://user:pass@host:port/dbname`).  

**Code Description**:  
1. **Package identification** – The variable `package` is set to the literal string `'lab_loinc'`. This value is used when constructing file names and when invoking downstream helpers.  
2. **Password handling** – If `pwprompt` is True, the function uses `click.prompt` to request the database password from the user, hiding the input. The result is stored in the local variable `password`; otherwise `password` remains `None`.  
3. **Connection string creation** – `make_conn_str` is called with the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. The helper returns a libpq‑compatible connection string (`conn_str`) that contains host, port, dbname, user, password (if provided) and an `options` clause that sets the search_path.  
4. **Argos configuration file name** – `config_file` is built by concatenating the site identifier, the package name, and the suffix `"_argos_temp.json"` (e.g., `umich_lab_loinc_argos_temp.json`). This temporary JSON file will be written by the R‑script runner.  
5. **Run the Lab LOINC R script** – The function imports `run_r_query` from `pedsnetdcc.r_query` and calls it with the arguments: `config_file`, `conn_str`, `site`, `package`, `password`, `searchpath`, `model_version`, and `copy`.  
   - `run_r_query` prepares a site‑specific directory, creates the Argos file, substitutes placeholders in the R helper files, executes the R script, and optionally copies results.  
   - The return value `success` is a boolean indicating whether the R script completed without raising an exception.  
6. **Check R‑script outcome** – If `success` is False, the function terminates the whole process with `sys.exit(1)`, causing a non‑zero exit status for the command‑line invocation.  
7. **Post‑processing** – Upon successful R execution, the function imports `run_post_lab_loinc` from `pedsnetdcc.lab_loinc` and calls it with `conn_str`, `site`, and `searchpath`.  
   - `run_post_lab_loinc` adds a primary key to the temporary `updated_measurement` table, renames tables, adjusts foreign‑key constraints, and creates indexes on the final `measurement` table.  
   - Its return value `success` is again a boolean.  
8. **Check post‑processing outcome** – If the post‑processing step returns False, the function exits with `sys.exit(1)`.  
9. **Successful completion** – If both the R script and the post‑processing succeed, the function ends with `sys.exit(0)`, indicating a clean exit.  

**Note**:  
- The function does not catch any exceptions raised by its callees; any unexpected exception will propagate and cause the Python interpreter to exit with a non‑zero status.  
- Password handling is minimal: when `pwprompt` is False, the password is taken from the URI (or from the optional `password` argument) and stored in plain text inside the generated Argos JSON file. Ensure that file system permissions restrict access to the temporary directory.  
- The temporary Argos file (`*_argos_temp.json`) is created in the site‑specific working directory under `/app/<package>/<site>`. It is not removed by this function; cleanup is left to the surrounding execution environment.  
- The function relies on `sys.exit` to communicate success or failure to the operating system. This design makes the function suitable only as a top‑level command‑line entry point; it should not be called from other Python code that expects a return value.  
- All downstream helpers (`make_conn_str`, `run_r_query`, `run_post_lab_loinc`) must be available in the import path; missing imports will raise `ImportError` before any work is performed.  
- The `model_version` argument is forwarded unchanged to `run_r_query`; future versions of the R script may use it, but the current implementation does not.  
- The `copy` flag only influences the behavior of `run_r_query`; it does not affect the post‑processing step.  
## FunctionDef run_r_obs_covid(pwprompt, searchpath, site, model_version, idname, copy, limit, owner, dburi)
**run_r_obs_covid**: The function of run_r_obs_covid is to prepare a database connection string and invoke the R‑based COVID observation script, exiting with an appropriate status code.

**parameters**:  
· pwprompt (bool): If True, the function will prompt the user for the database password on the command line.  
· searchpath (str or None): Schema search_path to be injected into the connection string via the `options` parameter.  
· site (str): Identifier of the site for which the R script should be executed.  
· model_version (str): Version string of the model to be used by the R script.  
· idname (str): Name of the identifier column that the R script will reference.  
· copy (bool): Flag indicating whether the R script should copy the generated data to a target location.  
· limit (int or None): Optional row limit to be passed to the R script; None means no limit.  
· owner (str): Database role that will own the objects created by the R script.  
· dburi (str): PostgreSQL URI that describes how to connect to the database (e.g., `postgresql://user:pass@host:port/dbname`).  

**Code Description**:  
1. The function initializes a local variable `password` to `None`.  
2. If `pwprompt` is True, it uses `click.prompt` to request the database password from the user, hiding the input; the entered value is stored in `password`.  
3. It calls the utility `make_conn_str` (defined in `pedsnetdcc/utils.py`) with three arguments: the supplied `dburi`, the optional `searchpath`, and the possibly‑prompted `password`. `make_conn_str` returns a libpq‑compatible connection string that incorporates the search_path and password as needed. The result is stored in `conn_str`.  
4. The function imports the concrete implementation of the R script runner: `run_r_obs_covid` from the module `pedsnetdcc.r_obs_covid`. This import is performed locally to avoid circular imports and to keep the CLI entry point lightweight.  
5. It invokes the imported `run_r_obs_covid` with the prepared connection string and all other parameters (`site`, `password`, `searchpath`, `model_version`, `idname`, `copy`, `limit`, `owner`). The called function returns a boolean `success` indicating whether the R script completed without error.  
6. If `success` is False, the function terminates the process with exit code 1 (`sys.exit(1)`), signalling failure to the operating system.  
7. If `success` is True, the function terminates with exit code 0 (`sys.exit(0)`), indicating successful execution.  

**Note**:  
- The function does not perform any validation of the non‑database parameters; it forwards them directly to the R script runner, which is responsible for handling invalid values.  
- Password handling is deliberately minimal: when `pwprompt` is False, the password must already be embedded in `dburi` or omitted entirely; when True, the entered password overrides any password present in the URI.  
- The local import of `pedsnetdcc.r_obs_covid.run_r_obs_covid` ensures that the heavy R‑related dependencies are loaded only when this command is executed.  
- The exit behavior (`sys.exit`) makes this function suitable for use as a Click command entry point; any exception raised inside the called R script should be caught and translated into a `False` return value to trigger the appropriate non‑zero exit status.
## FunctionDef run_r_obs_recover(pwprompt, searchpath, site, model_version, idname, copy, noidx, nofk, skip_query, limit, owner, dburi)
**run_r_obs_recover**: The function of run_r_obs_recover is to prepare a database connection string, invoke the R‑based observation‑period recovery routine, and exit with an appropriate status code.

**parameters**:  
· pwprompt (bool): If True, the user is prompted on the command line for the database password; the input is hidden.  
· searchpath (str or None): Schema search_path that should be injected into the connection string via the PostgreSQL `options` parameter.  
· site (str): Identifier of the site (e.g., “site1”) that is passed to the R recovery script.  
· model_version (str): Version string of the data model to be used by the recovery script.  
· idname (str): Name of the identifier column that the recovery script should operate on.  
· copy (bool): Flag indicating whether the script should copy data instead of moving it.  
· noidx (bool): When True, the script skips creation of indexes after recovery.  
· nofk (bool): When True, the script skips creation of foreign‑key constraints after recovery.  
· skip_query (bool): If True, the script does not execute the initial SELECT query that identifies rows to recover.  
· limit (int or None): Optional maximum number of rows to process; passed directly to the R script.  
· owner (str or None): Database role that should own any objects created by the script.  
· dburi (str): PostgreSQL URI that identifies the target database (e.g., `postgresql://user@host/dbname`).  

**Code Description**:  
1. The function initializes a local variable `password` to `None`.  
2. If `pwprompt` is True, it uses `click.prompt` to request the database password from the user, hiding the input; the entered value is stored in `password`.  
3. It calls the utility `make_conn_str` (defined in `pedsnetdcc.utils`) with three arguments: the supplied `dburi`, the optional `searchpath`, and the possibly‑prompted `password`. `make_conn_str` returns a libpq‑compatible connection string that incorporates the search_path and password as needed.  
4. The function imports `run_r_obs_recover` from the module `pedsnetdcc.r_obs_recover`. This import is performed inside the function to avoid unnecessary loading when the command is not used.  
5. It invokes the imported `run_r_obs_recover` with the constructed connection string and all other command‑line parameters (`site`, `password`, `searchpath`, `model_version`, `idname`, `copy`, `noidx`, `nofk`, `skip_query`, `limit`, `owner`). The called routine executes the actual R script that performs observation‑period recovery and returns a Boolean `success`.  
6. If `success` is False, the function terminates the process with exit code 1 (`sys.exit(1)`), signalling an error to the shell.  
7. If `success` is True, the function exits cleanly with code 0 (`sys.exit(0)`).  

From a functional perspective, `run_r_obs_recover` acts as a thin command‑line wrapper: it handles user interaction (password prompting), prepares a proper connection string via its callee `make_conn_str`, delegates the heavy lifting to the R‑based recovery implementation, and translates the outcome into standard Unix exit codes.

**Note**:  
- The function relies on the external `click` library for password prompting; ensure `click` is installed in the execution environment.  
- The `dburi` must be a valid PostgreSQL URI; malformed URIs will cause `make_conn_str` to raise a `ValueError`.  
- Password handling follows a precedence rule: an explicitly prompted password overrides any password embedded in the URI.  
- The `searchpath` argument overrides any `search_path` option already present in the URI’s query string.  
- Because the function calls `sys.exit`, it should be used only as a top‑level command entry point; invoking it from other Python code will terminate the interpreter unless the exit calls are caught or the function is refactored.  
- All Boolean flags (`copy`, `noidx`, `nofk`, `skip_query`) are passed unchanged to the R script; their interpretation is defined in `pedsnetdcc.r_obs_recover.run_r_obs_recover`.  
- The function does not perform any validation of the non‑string parameters (`limit`, `owner`); any required checks are expected to be performed inside the R script.  
## FunctionDef run_r_dose(pwprompt, searchpath, site, model_version, copy, limit, owner, dburi)
**run_r_dose**: The function of run_r_dose is to orchestrate the execution of the R dose calculation script by preparing a database connection string, optionally prompting for a password, invoking the R script runner, and exiting with an appropriate status code.

**parameters**:  
· pwprompt: Boolean flag indicating whether the user should be prompted for the database password on the command line.  
· searchpath: String containing a PostgreSQL schema search_path that will be incorporated into the connection string.  
· site: Identifier of the site for which the R dose script should be run; passed directly to the R script.  
· model_version: Version string of the model to be used by the R script.  
· copy: Boolean or flag controlling whether the R script should copy data; passed directly to the R script.  
· limit: Integer limiting the number of records processed by the R script; passed directly to the R script.  
· owner: String specifying the database owner or role; passed directly to the R script.  
· dburi: PostgreSQL URI that identifies the target database (e.g., `postgresql://user:pass@host:port/dbname?...`).  

**Code Description**:  
1. The function begins by initializing a local variable `password` to `None`.  
2. If `pwprompt` is true, the function uses `click.prompt` to request the database password from the user, hiding the input. The entered password is stored in `password`.  
3. It calls the utility function `make_conn_str` (defined in `pedsnetdcc/utils.py`) with three arguments: the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. `make_conn_str` returns a libpq‑compatible connection string that incorporates the search_path and password as needed. The result is stored in `conn_str`.  
4. Inside the function body, the module `pedsnetdcc.r_dose` is imported, and the `run_r_dose` function from that module is retrieved. This import is performed locally to avoid circular imports and to delay loading of heavy R‑related dependencies until the command is actually executed.  
5. The imported `run_r_dose` is invoked with the following arguments: `conn_str`, `site`, `password`, `searchpath`, `model_version`, `copy`, `limit`, and `owner`. The call returns a Boolean `success` indicating whether the R script completed without error.  
6. If `success` is `False`, the function terminates the process with exit code 1 by calling `sys.exit(1)`.  
7. If `success` is `True`, the function terminates the process with exit code 0 by calling `sys.exit(0)`.  

**Relationship with Callees**:  
- **make_conn_str** (pedsnetdcc/utils.py): Generates a libpq‑style connection string from the supplied DBURI, optionally overriding the search_path and password. This string is required by the R script to connect to the database.  
- **pedsnetdcc.r_dose.run_r_dose**: Executes the actual R script that performs dose calculations. It receives the prepared connection string and all user‑provided parameters, runs the R code, and returns a success flag.  

**Note**:  
- The function exits the entire Python process; it should be used as a top‑level command entry point rather than as a library function.  
- Password prompting occurs only when `pwprompt` is true; otherwise the password must be present in the DBURI or omitted if not required.  
- Importing `pedsnetdcc.r_dose.run_r_dose` inside the function prevents unnecessary loading of R‑related modules when the command is not invoked.  
- The exit status follows the conventional Unix convention: `0` for success, `1` for failure, allowing external scripts or CI pipelines to detect errors.  
- All parameters are passed through to the R script unchanged; validation of values (e.g., ensuring `limit` is a positive integer) is expected to be performed by the R script or upstream CLI argument parsing.
## FunctionDef run_r_pcornet_peds_slice(pwprompt, searchpath, site, source_schema, target_schema, dburi)
**run_r_pcornet_peds_slice**: The function of run_r_pcornet_peds_slice is to prepare a database connection string, create a temporary Argos configuration file name, invoke the R‑based pcornet pediatric slice script, and exit with an appropriate status code.

**parameters**:  
· pwprompt (bool): If True, the function will prompt the user for the database password on the command line.  
· searchpath (str or None): Optional PostgreSQL schema search_path that will be incorporated into the connection string.  
· site (str): Identifier for the site; used to build the temporary Argos JSON filename and passed to the R script.  
· source_schema (str): Name of the source schema that the R script will read from.  
· target_schema (str): Name of the target schema that the R script will write to.  
· dburi (str): PostgreSQL URI that identifies the database (e.g., `postgresql://user@host/dbname`).  

**Code Description**:  
1. The function initializes a local variable `password` to `None`.  
2. If `pwprompt` is True, it uses `click.prompt` to request the database password from the user, hiding the input; the entered value is stored in `password`.  
3. It calls `make_conn_str` (from `pedsnetdcc.utils`) with the supplied `dburi`, `searchpath`, and the possibly‑provided `password`. `make_conn_str` returns a libpq‑compatible connection string that incorporates the search_path and password as needed. The result is stored in `conn_str`.  
4. It constructs a temporary Argos configuration filename by concatenating the `site` value, the literal string `"_pcornet_peds_slice_argos_temp.json"`, and assigns this to `config_file`. This file name is expected by the downstream R script.  
5. The function imports `run_r_pcornet_peds_slice` from the module `pedsnetdcc.r_pcornet_peds_slice`. This imported function is the actual wrapper that executes the R script.  
6. It calls the imported `run_r_pcornet_peds_slice` with the arguments `config_file`, `conn_str`, `site`, `password`, `source_schema`, and `target_schema`. The call returns a Boolean `success` indicating whether the R script completed without error.  
7. If `success` is False, the function terminates the process with exit code 1 by calling `sys.exit(1)`.  
8. If `success` is True, the function terminates the process with exit code 0 by calling `sys.exit(0)`.  

**Note**:  
- The function does not write the Argos JSON file itself; it only creates the filename. The R script is responsible for generating or reading the file.  
- Password handling is optional; if `pwprompt` is False and no password is supplied, `make_conn_str` will use any password embedded in `dburi`.  
- The `searchpath` argument overrides any `search_path` option present in the URI, because `make_conn_str` gives precedence to the explicit argument.  
- The function relies on `click` for interactive prompting and on `sys.exit` for process termination; callers should be aware that the function will always exit the interpreter, making it suitable for command‑line entry points but not for library use.  
- Any exception raised inside `make_conn_str` (e.g., malformed URI) or inside the imported R‑script wrapper will propagate and cause the command to abort before reaching the explicit `sys.exit` calls.  
- The imported `run_r_pcornet_peds_slice` must accept exactly the six positional arguments provided; mismatched signatures will raise a `TypeError`.  
## FunctionDef run_r_pcornet_adult_slice(pwprompt, searchpath, site, source_schema, target_schema, dburi)
**run_r_pcornet_adult_slice**: The function of run_r_pcornet_adult_slice is to prepare a temporary Argos configuration file, build a PostgreSQL connection string, invoke the R‑based PCORnet adult slice script, and terminate the process with an appropriate exit code.

**parameters**:  
· pwprompt: Boolean flag indicating whether the user should be prompted for the database password on the command line.  
· searchpath: Optional schema search_path that will be incorporated into the connection string via the `options` parameter.  
· site: Identifier for the site (used to construct the temporary Argos JSON filename).  
· source_schema: Name of the source schema that contains the input data for the R script.  
· target_schema: Name of the target schema where the R script should write its results.  
· dburi: PostgreSQL URI that identifies the database to connect to (e.g., `postgresql://user:pass@host:port/dbname`).  

**Code Description**:  
1. Initialise a local variable `password` to `None`.  
2. If `pwprompt` is true, invoke `click.prompt` to request the database password from the user, hiding the input; the entered value is stored in `password`.  
3. Call the utility function `make_conn_str` (defined in `pedsnetdcc.utils`) with the supplied `dburi`, `searchpath`, and the possibly‑prompted `password`. This function returns a libpq‑compatible connection string that incorporates the search_path and password overrides.  
4. Construct a temporary Argos configuration filename by concatenating the `site` value with the suffix `"_pcornet_adult_slice_argos_temp.json"`. This file is expected to be created elsewhere in the workflow and consumed by the R script.  
5. Dynamically import the function `run_r_pcornet_adult_slice` from the module `pedsnetdcc.r_pcornet_adult_slice`. This imported function encapsulates the actual execution of the R script.  
6. Execute the imported `run_r_pcornet_adult_slice`, passing six arguments in order: the temporary Argos config file name, the connection string, the `site` identifier, the password (or `None`), the `source_schema`, and the `target_schema`. The function returns a Boolean `success` flag.  
7. If `success` is `False`, terminate the Python process with exit code 1 (`sys.exit(1)`), signalling an error to the calling environment.  
8. If `success` is `True`, terminate the process with exit code 0 (`sys.exit(0)`), indicating normal completion.  

The function therefore acts as a thin command‑line wrapper that handles user interaction (password prompting), connection‑string construction, and delegation to the R‑script runner, while ensuring that the overall command returns a conventional Unix exit status.

**Note**:  
- The function always ends the interpreter via `sys.exit`; it should be used as a top‑level command entry point, not as a library call within longer Python workflows.  
- Password handling respects the precedence order: explicit prompt (`pwprompt`) overrides any password embedded in `dburi`.  
- The temporary Argos file name is deterministic based on `site`; callers must ensure that the file is created before this function runs, otherwise the R script may fail.  
- The return value of the imported `run_r_pcornet_adult_slice` must be a Boolean; any non‑True value triggers an error exit.  
- Because `make_conn_str` is used, any malformed `dburi` (e.g., duplicate query parameters) will raise a `ValueError` before the R script is invoked.  
- The function imports the R‑script runner lazily (inside the function) to avoid unnecessary import overhead when the command is not executed.
## FunctionDef run_r_pcornet_covid_slice(pwprompt, searchpath, site, source_schema, target_schema, dburi)
**run_r_pcornet_covid_slice**: The function of run_r_pcornet_covid_slice is to prepare a database connection string, invoke the R‑based PCORnet COVID slice script, and exit with an appropriate status code.

**parameters**:  
· pwprompt (bool): If True, the function will prompt the user on the command line for the database password.  
· searchpath (str or None): Optional PostgreSQL schema search_path that will be incorporated into the connection string.  
· site (str): Identifier of the site for which the slice is being generated; also used to name a temporary Argos JSON configuration file.  
· source_schema (str): Name of the source schema that contains the raw data to be processed.  
· target_schema (str): Name of the target schema where the slice results will be written.  
· dburi (str): PostgreSQL URI that identifies the database (e.g., `postgresql://user@host/dbname`).  

**Code Description**:  
1. The function initializes a local variable `password` to `None`.  
2. If `pwprompt` is True, it uses `click.prompt` to request the database password from the user, hiding the input; the entered value is stored in `password`.  
3. It calls the utility `make_conn_str` (defined in `pedsnetdcc.utils`) with the supplied `dburi`, `searchpath`, and the possibly‑prompted `password`. `make_conn_str` returns a libpq‑compatible connection string that incorporates the search_path and password as needed. The result is stored in `conn_str`.  
4. A temporary Argos configuration filename is constructed by concatenating the `site` value with the suffix `"_pcornet_covid_slice_argos_temp.json"`. This file name is passed to the R‑script runner.  
5. The function imports `run_r_pcornet_covid_slice` from the module `pedsnetdcc.r_pcornet_covid_slice`. This imported function is the actual wrapper that launches the R script.  
6. It invokes the imported `run_r_pcornet_covid_slice` with the arguments: `config_file`, `conn_str`, `site`, `password`, `source_schema`, and `target_schema`. The call returns a boolean `success` indicating whether the R script completed without error.  
7. If `success` is False, the function terminates the Python process with exit code 1 (`sys.exit(1)`), signalling failure to the operating system.  
8. If `success` is True, the function terminates with exit code 0 (`sys.exit(0)`), indicating successful completion.  

**Note**:  
- The function does not perform any validation of the input parameters beyond what `make_conn_str` does; callers must ensure that `dburi` is a valid PostgreSQL URI.  
- The temporary Argos JSON file name is deterministic based on `site`; if the file already exists it will be overwritten by the downstream R‑script logic.  
- Password handling is optional; if `pwprompt` is False and no password is supplied via `dburi`, the connection string will omit a password, relying on other authentication methods (e.g., .pgpass).  
- The function exits the entire process using `sys.exit`; therefore it should be used as a top‑level command‑line entry point rather than as a library function called from other Python code.  
- All side effects (prompting, file name creation, process termination) occur before the R script is launched; any exception raised by `make_conn_str` or the imported `run_r_pcornet_covid_slice` will propagate and cause the program to terminate with a traceback.
## FunctionDef subset_by_cohort(searchpath, pwprompt, dburi, model_version, force, source_schema, target_schema, cohort_table, concept_create, drug_dose, measurement, covid_obs, inc_hash, split_measure, index_create, fk_create, notable, nopk, limit, owner, nonull, pre_split, drug_tpn)
**subset_by_cohort**: The function of subset_by_cohort is to orchestrate the creation of a subset schema based on a cohort table and optionally perform measurement splitting and partitioning.

**parameters**  
· **searchpath** (str): Schema search path to be used when building the initial connection string.  
· **pwprompt** (bool): If True, the function will prompt the user for the database password on the command line.  
· **dburi** (str): PostgreSQL URI that identifies the target database (e.g., `postgresql://user@host/dbname`).  
· **model_version** (str): PEDSnet data‑model version (e.g., `2.3.0`). Used for logging and passed to downstream functions.  
· **force** (bool): When True, benign DDL errors (such as “object already exists”) are ignored by downstream calls.  
· **source_schema** (str): Name of the schema that contains the original PEDSnet tables.  
· **target_schema** (str): Name of the schema that will receive the subset tables.  
· **cohort_table** (str): Name of the table that holds the list of `person_id` values defining the cohort.  
· **concept_create** (bool): If True, concept‑group index‑replacement tables are created in the target schema.  
· **drug_dose** (bool): If True, drug‑dose derivation tables are copied into the target schema.  
· **measurement** (bool): If True, measurement derivation tables (or a subset) are copied into the target schema.  
· **covid_obs** (bool): If True, the COVID‑specific observation derivation table is copied.  
· **inc_hash** (bool): If True, the `hash_token` table is populated; otherwise it is created empty.  
· **split_measure** (bool): If True, after the subset is created the core `measurement` table is split into `measurement_anthro`, `measurement_labs`, and `measurement_vitals`.  
· **index_create** (bool): If True, indexes are created on the subset tables after they are populated.  
· **fk_create** (bool): If True, foreign‑key constraints are added to the subset tables.  
· **notable** (bool): If True, no tables are created (dry‑run mode).  
· **nopk** (bool): If True, primary‑key constraints are not added to the subset tables.  
· **nonull** (bool): If True, NOT NULL constraints are not added to the subset tables.  
· **limit** (bool): If True, permissions are granted only to the specified `owner`; otherwise default DCC roles receive permissions.  
· **owner** (str): Database role that receives permissions when `limit` is True.  
· **pre_split** (bool): If True, the measurement table is treated as already split; the split tables are created directly.  
· **drug_tpn** (bool): If True, the `drug_exposure_tpn` table is copied into the target schema.  

**Code Description**  
1. **Password handling** – If `pwprompt` is True, the function prompts the user for the database password using `click.prompt` with hidden input. The entered password is stored in the local variable `password`.  

2. **Connection‑string construction** – The helper `make_conn_str` is called with `dburi`, the supplied `searchpath`, and the optional `password`. This produces a libpq‑compatible connection string (`conn_str`) that incorporates the search path and password (if provided).  

3. **Subset creation** – The function imports `run_subset_by_cohort` from `pedsnetdcc.subset_by_cohort` and invokes it with the constructed `conn_str` and all flags that control which auxiliary objects are created. The call signature is:  
   `run_subset_by_cohort(conn_str, model_version, source_schema, target_schema, cohort_table, concept_create, drug_dose, measurement, covid_obs, inc_hash, index_create, fk_create, notable, nopk, nonull, limit, owner, pre_split, drug_tpn, force)`  
   The return value `success` is a boolean indicating whether the subset operation completed without raising an exception. If `success` is False, the script terminates with exit code 1.  

4. **Optional measurement split** – If either `split_measure` or `pre_split` is True, additional processing is performed:  
   a. The function imports `conn_str_with_search_path` from `pedsnetdcc.subset_by_cohort`.  
   b. A new search path is built as a comma‑separated list containing the `target_schema` followed by the `vocabulary` schema (`new_search_path = ','.join((target_schema, 'vocabulary'))`).  
   c. `conn_str_with_search_path` is called to produce `new_conn_str`, a connection string that sets the search path to the newly created target schema and the vocabulary schema.  

   **Split measurement table** – If `split_measure` is True and `pre_split` is False, the function imports `split_measurement_table` from `pedsnetdcc.split_measurement` and calls it:  
   `split_measurement_table(new_conn_str, False, False, model_version, new_search_path, limit, owner)`  
   The first two `False` arguments correspond to `truncate` and `view`, meaning the original measurement table is left untouched and no unified view is created. If the split operation fails (`success` is False), the script exits with code 1.  

5. **Partition measurement tables** – Regardless of whether a split was performed, the function always partitions the measurement tables. It imports `partition_measurement_table` from `pedsnetdcc.partition_measurement` and calls:  
   - If `measurement` is True: `partition_measurement_table(new_conn_str, model_version, new_search_path, False, False, False)`  
   - If `measurement` is False: `partition_measurement_table(new_conn_str, model_version, new_search_path, False, True, False)`  
   The boolean flags correspond to `dcc`, `site3`, and `no_vital` respectively. When `measurement` is False the function creates a DCC‑style partition set that omits the measurement tables (i.e., only creates the core table without partitioning). If the partitioning step fails, the script exits with code 1.  

6. **Process termination** – After all requested steps have completed successfully, the function calls `sys.exit(0)` to indicate normal termination.  

**Relationship with callees**  
- `make_conn_str` (utility) builds the initial connection string from the URI, search path, and optional password.  
- `run_subset_by_cohort` performs the heavy‑lifting of creating the subset schema, handling table creation, primary‑key, NOT NULL, index, foreign‑key, permission, and vacuum steps.  
- `conn_str_with_search_path` adjusts the connection string so that subsequent DDL operates in the newly created target schema plus the vocabulary schema.  
- `split_measurement_table` (module `pedsnetdcc.split_measurement`) creates the three split measurement tables (`measurement_anthro`, `measurement_labs`, `measurement_vitals`) when requested.  
- `partition_measurement_table` (module `pedsnetdcc.partition_measurement`) adds check constraints, inheritance, and routing triggers to the measurement tables, optionally creating a DCC‑style or site‑specific partition layout.  

**Note**  
- The function does not perform any transaction management; each downstream call executes its own DDL statements in autocommit mode.  
- If any downstream function returns `False` or raises an exception, the script terminates immediately with a non‑zero exit code, preventing subsequent steps from running.  
- The `split_measure` flag controls whether the core `measurement` table is first split; the `pre_split` flag indicates that the measurement table is already split and skips the split step but still requires partitioning.  
- The `measurement` flag influences the arguments passed to `partition_measurement_table`; when `measurement` is False the function creates a DCC‑style partition set that does not include measurement partitions.  
- All permission handling (owner vs. default DCC roles) is delegated to `run_subset_by_cohort`; the `limit` and `owner` arguments are passed through unchanged.  
- The function assumes that the PostgreSQL server is reachable and that the supplied URI, schemas, and cohort table exist; validation of those objects occurs inside the called functions.
## FunctionDef subset_pcornet_by_cohort(searchpath, pwprompt, dburi, model_version, force, source_schema, target_schema, cohort_table, inc_hash, index_create, fk_create, notable, nopk, limit, owner)
**subset_pcornet_by_cohort**: The function of subset_pcornet_by_cohort is to prepare a database connection string, optionally prompt for a password, invoke the core subset operation, and terminate the process with an appropriate exit status.

**parameters**:  
· **searchpath**: Schema search_path to be injected into the connection string; may be None.  
· **pwprompt**: Boolean flag; when True the function prompts the user on the command line for the database password (input hidden).  
· **dburi**: PostgreSQL URI that identifies the target database (e.g., `postgresql://user@host/dbname`).  
· **model_version**: String indicating the PEDSnet data model version (e.g., `"2.3.0"`).  
· **force**: Boolean flag passed through to the downstream routine; when True benign errors are ignored.  
· **source_schema**: Name of the schema that contains the original PCORnet tables.  
· **target_schema**: Name of the schema where the subset tables will be created.  
· **cohort_table**: Name of the table that holds the list of `patid` values defining the cohort.  
· **inc_hash**: Boolean flag; if True the `hash_token` table is included in the subset.  
· **index_create**: Boolean flag; if True predefined indexes are created on the subset tables.  
· **fk_create**: Boolean flag; if True predefined foreign‑key constraints are created on the subset tables.  
· **notable**: Boolean flag; if True the subset operation skips table creation (dry‑run mode).  
· **nopk**: Boolean flag; if True primary‑key constraints are not added to the subset tables.  
· **limit**: Boolean flag; if True permission grants are limited to the user specified by `owner`.  
· **owner**: Database role that receives GRANT privileges when `limit` is True; defaults to `'loading_user'`.  

**Code Description**:  
1. **Password handling** – The variable `password` is initialised to `None`. If `pwprompt` is true, the function uses `click.prompt` to request the database password from the user, hiding the input. The obtained password (or `None`) is stored in `password`.  
2. **Connection string construction** – The helper `make_conn_str` (from `pedsnetdcc.utils`) is called with `dburi`, the supplied `searchpath`, and the possibly‑prompted `password`. This produces a libpq‑compatible connection string (`conn_str`) that incorporates host, port, database name, user, password, and any `options` such as `search_path`.  
3. **Import of core routine** – The function imports `run_subset_pcornet_by_cohort` from `pedsnetdcc.subset_pcornet_by_cohort`. Import is performed locally to avoid unnecessary loading when the command is not used.  
4. **Invocation of core routine** – `run_subset_pcornet_by_cohort` is called with the prepared `conn_str` and all command‑line arguments (model version, schemas, cohort table, and the various Boolean flags). The call returns a Boolean `success` indicating whether the subset operation completed without raising an exception.  
5. **Process termination** – If `success` is false, the function calls `sys.exit(1)` to signal an error to the operating system. If `success` is true, it calls `sys.exit(0)` to indicate normal completion. No further cleanup is performed; the downstream routine is responsible for committing or rolling back transactions and for logging.  

**Relationship with callees**:  
- **make_conn_str** (pedsnetdcc.utils) builds the libpq connection string from the supplied URI, optional search_path, and optional password.  
- **run_subset_pcornet_by_cohort** (pedsnetdcc.subset_pcornet_by_cohort) performs the heavy‑lifting: creating subset tables, adding primary keys, indexes, foreign keys, granting permissions, and vacuuming. All flags passed to `subset_pcornet_by_cohort` are forwarded unchanged to this routine.  
- **click.prompt** is used for interactive password entry when `pwprompt` is true.  
- **sys.exit** terminates the CLI process with the appropriate exit code.  

**Note**:  
- The function does not perform any validation of the arguments beyond what is done inside `make_conn_str` and `run_subset_pcornet_by_cohort`; invalid schema names, missing tables, or malformed URIs will cause downstream errors.  
- When `pwprompt` is true, the function blocks waiting for user input; this is unsuitable for non‑interactive automation unless the password is supplied via the `password` argument of `make_conn_str` (i.e., by embedding it in the URI).  
- The exit status is binary: `0` for success, `1` for any failure. The underlying `run_subset_pcornet_by_cohort` may raise exceptions; those propagate up to this wrapper and result in a non‑zero exit.  
- The function assumes that the environment has the `click` library installed; missing this dependency will raise an ImportError before any database work begins.  
- Because the connection string is built once and reused for all subsequent SQL execution, any change to the password or search_path after the initial call will not be reflected.  
- The function is intended to be used as a Click command entry point in the CLI; it should not be imported and called directly in library code without handling the `sys.exit` side effects.
## FunctionDef create_index_replace(searchpath, pwprompt, dburi, model_version)
**create_index_replace**: The function of create_index_replace is to orchestrate the creation of index‑replacement tables for a specified PEDSnet model version by building a database connection string, optionally prompting for a password, and invoking the workflow that performs the actual DDL work.

**parameters**:  
· **searchpath**: a string (or ``None``) that, when supplied, is inserted into the PostgreSQL connection string as the ``search_path`` option.  
· **pwprompt**: a boolean flag. If ``True``, the function will prompt the user on the command line for the database password, hiding the input.  
· **dburi**: a PostgreSQL URI in the form ``postgresql://[user[:password]@][host][:port]/dbname[?param=value...]`` that identifies the target database.  
· **model_version**: a string identifying the PEDSnet data model version (e.g., ``"2.3.0"``) for which the index‑replacement tables should be created.

**Code Description**:  
1. **Password handling** – The variable ``password`` is initialised to ``None``. If ``pwprompt`` is true, the function uses ``click.prompt`` to request the password from the user, storing the hidden input in ``password``.  
2. **Connection‑string construction** – The helper ``make_conn_str`` (from ``pedsnetdcc.utils``) is called with ``dburi``, ``searchpath`` and the possibly‑provided ``password``. This routine parses the URI, merges or overrides the ``search_path`` and password, and returns a libpq‑compatible connection string (e.g., ``host=... port=... dbname=... user=... password=... options='-c search_path=...'``).  
3. **Import of the workflow function** – ``run_index_replace`` is imported from ``pedsnetdcc.subset_by_cohort``. Import is performed locally to avoid unnecessary imports when the command is not used.  
4. **Execution of the index‑replacement workflow** – ``run_index_replace`` is called with the constructed ``conn_str`` and the supplied ``model_version``. The called function creates the auxiliary tables, logs progress, and returns ``True`` on success or raises an exception on failure.  
5. **Process termination based on outcome** – If ``run_index_replace`` returns a falsy value (which it never does in the current implementation, but the check guards against future changes), the script exits with status code ``1`` indicating failure. Otherwise it exits with status code ``0`` indicating success. The explicit ``sys.exit`` calls ensure that the command‑line entry point terminates the Python interpreter with the appropriate exit status.

**Note**:  
- The function terminates the entire process via ``sys.exit``; it is intended to be used as a command‑line entry point, not as a library call.  
- ``make_conn_str`` enforces strict validation of the URI; malformed URIs or duplicate query parameters raise ``ValueError`` before any database interaction occurs.  
- If ``pwprompt`` is ``False`` and the URI does not contain a password, the resulting connection string will omit the ``password`` parameter, relying on other authentication methods (e.g., .pgpass).  
- ``run_index_replace`` does not catch exceptions from the underlying DDL creation; any database error will propagate up, causing this function to exit with a non‑zero status.  
- The ``searchpath`` argument overrides any ``search_path`` option already present in the URI; this is useful for directing the operation to a specific schema without modifying the original URI.  
- Because the function imports ``run_index_replace`` only when executed, the module load time for the CLI is reduced, which can be beneficial for commands that do not need this functionality.
## FunctionDef run_recover_cohort(pwprompt, searchpath, site, model_version, dburi)
**run_recover_cohort**: The function of run_recover_cohort is to orchestrate the execution of the RECOVER cohort derivation using command‑line arguments and a PostgreSQL connection string.

**parameters**:  
· pwprompt (bool): If True, the function will interactively prompt the user for the database password.  
· searchpath (str or None): The PostgreSQL schema search_path that should be applied to the connection. It is passed to the connection‑string builder.  
· site (str): Identifier of the site for which the cohort is being derived; forwarded to the underlying RECOVER derivation routine.  
· model_version (str): Version identifier of the data model to be used by the derivation routine.  
· dburi (str): A PostgreSQL URI that specifies the database host, port, name, user, and optional query parameters (e.g., `postgresql://user:pass@host:port/dbname?param=value`).  

**Code Description**:  
1. The function initializes a local variable `password` to `None`.  
2. If `pwprompt` is True, it uses `click.prompt` to request the database password from the user, hiding the input. The entered value is stored in `password`.  
3. It calls the utility `make_conn_str` (defined in *pedsnetdcc/utils.py*) with three arguments: the supplied `dburi`, the `searchpath`, and the possibly‑prompted `password`. `make_conn_str` returns a libpq‑compatible connection string that incorporates the search_path and password overrides. The result is stored in `conn_str`.  
4. The function imports `run_recover_cohort` from the module `pedsnetdcc.recover_cohort`. This imported function is the actual implementation that performs the cohort derivation logic.  
5. It invokes the imported `run_recover_cohort` with four arguments: the constructed `conn_str`, the `site`, the `searchpath`, and the `model_version`. The call returns a Boolean `success` indicating whether the derivation completed without error.  
6. If `success` is False, the function terminates the process with exit status 1 (`sys.exit(1)`), signalling an error to the operating system.  
7. If `success` is True, the function terminates normally with exit status 0 (`sys.exit(0)`).  

From a functional perspective, `run_recover_cohort` acts as a thin command‑line wrapper: it prepares authentication details, builds a proper connection string, delegates the heavy lifting to the core derivation routine, and translates the routine’s Boolean result into conventional Unix exit codes.

**Note**:  
- The password prompt is only displayed when `pwprompt` is True; otherwise the password must be embedded in the `dburi` or omitted if the database allows password‑less connections.  
- `make_conn_str` enforces a single occurrence for each query parameter in the URI; malformed URIs will raise a `ValueError` before the derivation is started.  
- The function relies on the external `click` library for interactive prompting and on the standard `sys` module for process termination.  
- Exit status 0 indicates successful cohort derivation; any non‑zero status (currently only 1) indicates failure and can be used by calling scripts or CI pipelines to detect errors.  
- The imported `run_recover_cohort` from `pedsnetdcc.recover_cohort` must accept the same signature (`conn_str, site, searchpath, model_version`) and return a Boolean; mismatches will raise runtime errors.
## FunctionDef generate_transform_statements(dburi, pwprompt, searchpath, model_version, name, stringid, logged, site)
**generate_transform_statements**: The function of generate_transform_statements is to generate and display the SQL statements required to create transformed copies of source tables for a given model version and site.

**parameters**:  
· dburi: connection string (unused in the current implementation).  
· pwprompt: boolean flag; when True the function prompts the user for a database password.  
· searchpath: PostgreSQL search_path string that contains a comma‑separated list of schemas.  
· model_version: string identifying the PEDSnet model version (e.g., “2.3.0”).  
· name: identifier for the ID set that will be used in the transformation (passed to the transform pipeline).  
· stringid: boolean flag; when True the site‑specific ID column is treated as a string type, otherwise as a BigInteger.  
· logged: boolean flag; when True the generated tables will be created as logged tables, otherwise as UNLOGGED tables.  
· site: name of the site for which the transformation is being performed (e.g., “stlouis”).

**Code Description**:  
1. The function imports two internal utilities: `_transform_select_sql` from `pedsnetdcc.transform_runner` and `create_schema`, `primary_schema` from `pedsnetdcc.schema`.  
2. If `pwprompt` is true, the function invokes `click.prompt` to request a database password from the user, hiding the input. The retrieved password is stored in the local variable `password` but is not used elsewhere in the function.  
3. The primary schema is derived from the supplied `searchpath` by calling `primary_schema(searchpath)`. This returns the first schema name in the comma‑separated list. The result is stored in the variable `schema`.  
4. The identifier type for the transformation is selected based on `stringid`:  
   – If `stringid` is true, `idtype` is set to the string `'String'`.  
   – Otherwise, `idtype` is set to `'BigInteger'`.  
5. A temporary target schema name is constructed by concatenating the primary schema, an underscore, and the literal string `'transformed'`. The resulting name is stored in `tmp_schema`.  
6. The core work is delegated to `_transform_select_sql`. The function calls `_transform_select_sql` with the following arguments: `model_version`, `site`, `tmp_schema`, `name`, `idtype`, and `logged`. This call returns a set of tuples, each tuple containing an SQL statement string and a descriptive message.  
7. The function iterates over the returned set. For each `(sql, msg)` pair, it prints the message prefixed with `"msg: "` followed by the SQL statement. The current implementation prints only the SQL; the `msg` variable is not used beyond being concatenated to the output string.  
8. The function does not create the temporary schema, does not execute any of the generated SQL, and does not return a value. Its sole side effect is printing the generated statements to standard output.

**Relationship with callees**:  
- `primary_schema` is used to extract the primary schema name from the provided `searchpath`. This value is essential for constructing the temporary transformed schema name.  
- `_transform_select_sql` performs the heavy lifting of building the CREATE TABLE (or CREATE UNLOGGED TABLE) statements for each source table, applying the full transformation pipeline (index, age, concept name, ID mapping, site name). The `generate_transform_statements` function merely forwards the necessary parameters and consumes the resulting set.  
- `click.prompt` is conditionally used for interactive password entry, but the password is not passed to any subsequent function in the current code path.  
- `create_schema` is imported but never invoked; therefore no schema creation occurs within this function.

**Note**:  
- The function prints the generated SQL statements but does not execute them; callers must run the statements separately if they wish to materialize the transformed tables.  
- Because `create_schema` is not called, the temporary schema (`tmp_schema`) must already exist or be created by another part of the workflow before the printed statements are executed.  
- The `dburi` and the password obtained via `click.prompt` are currently unused, which may indicate incomplete implementation or a placeholder for future enhancements.  
- The order of statements printed is nondeterministic because `_transform_select_sql` returns a set; if a specific execution order is required, the caller should sort the set before processing.  
- Errors that could arise during the generation of SQL (e.g., issues in the transformation pipeline) are propagated as exceptions from `_transform_select_sql`; this function does not catch them.
