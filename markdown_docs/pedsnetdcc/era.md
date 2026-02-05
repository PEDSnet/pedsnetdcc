## FunctionDef _fill_concept_names(conn_str, era_type, site)
**_fill_concept_names**: The function of _fill_concept_names is to populate the concept name columns of a specific era table (condition_era, drug_era, etc.) with the human‑readable names from the vocabulary and to record the site identifier for each row.

**parameters**:  
· conn_str: a libpq‑style PostgreSQL connection string used to open a database connection.  
· era_type: the base name of the era table to be updated (e.g., “condition”, “drug”, or “drug_scdf”).  
· site: a string identifying the site for which the era derivation is being performed; this value is written into the “site” column of the target table.

**Code Description**:  
1. **SQL template construction** – The function defines a multi‑line SQL string (`fill_concept_names_sql`) that performs an UPDATE on `{0}_era` (where `{0}` is replaced by `era_type`). The UPDATE sets two columns:  
   * `{2}_concept_name` – the column that stores the concept name for the era type (`condition_concept_name` or `drug_concept_name`).  
   * `site` – set to the literal value supplied in the `site` argument.  
   The UPDATE uses a derived table (`v`) that selects the era identifier (`{2}_era_id`) and the matching concept name from `vocabulary.concept` by joining on the era’s `{2}_concept_id`. The placeholder `{2}` is derived from `temp_era_type`, which is normally the same as `era_type` except when `era_type` is `"drug_scdf"`; in that case `temp_era_type` is set to `"drug"` so that the correct column names (`drug_era_id`, `drug_concept_name`, etc.) are used.

2. **Message preparation** – `fill_concept_names_msg` holds a short description (“adding concept names”) that will be attached to the `Statement` object for logging purposes.

3. **Statement creation** – The SQL template is formatted with the actual values (`era_type`, `site`, `temp_era_type`) and wrapped in a `Statement` instance:  
   `add_era_ids_stmt = Statement(fill_concept_names_sql.format(era_type, site, temp_era_type), fill_concept_names_msg)`.  
   This object now contains the final SQL command and a human‑readable message.

4. **Execution** – The statement’s `execute` method is called with `conn_str`. The method opens a new database connection, runs the UPDATE, and stores the result metadata (rowcount, any error, etc.) inside the `Statement` instance.

5. **Error handling** – Immediately after execution, `check_stmt_err` is invoked with the statement and a context string (`'add concept names'`). `check_stmt_err` inspects `add_era_ids_stmt.err`; if an error is present it logs the failure and raises a `DatabaseError`. If no error is found, execution proceeds.

6. **Return value** – When the UPDATE completes without raising an exception, the function returns `True` to signal success to the caller.

**Reference Relationships**:  
*Caller* – The function is called from `run_era` (in `pedsnetdcc/era.py`). After the era derivation query and optional ID assignment, `run_era` invokes `_fill_concept_names` when the `no_concept` flag is not set. The boolean result is used by `run_era` to decide whether to continue or abort the overall era processing workflow.  
*Callees* – Within `_fill_concept_names` the only external utilities used are the `Statement` class (for SQL encapsulation and execution) and the helper `check_stmt_err` (for centralized error detection). No other project functions are called.

**Note**:  
- The function assumes that the target era table already contains the columns `{2}_concept_id`, `{2}_concept_name`, and `site`. If any of these columns are missing, the UPDATE will fail and `check_stmt_err` will raise an exception.  
- When `era_type` is `"drug_scdf"` the function deliberately maps it to `"drug"` for column naming; this is required because the underlying table is named `drug_scdf_era` but the concept columns follow the drug naming convention.  
- The UPDATE statement runs in autocommit mode (via the `Statement` execution path), so changes are permanent as soon as the command succeeds.  
- The function does not return the number of rows updated; callers should rely on the boolean success indicator (`True`) and on any logging produced by the `Statement` infrastructure.  
- If the database connection cannot be established, `Statement.execute` will store the connection error in `err`, and `check_stmt_err` will raise a `DatabaseError`.  

**Output Example**:  
```python
result = _fill_concept_names(conn_str, "condition", "SiteA")
print(result)   # -> True (indicates that concept names were successfully added)
```
## FunctionDef _copy_to_dcc_table(conn_str, era_type, schema)
**_copy_to_dcc_table**: The function of _copy_to_dcc_table is to copy a generated condition or drug era table from a source schema into the central `dcc_pedsnet` schema, handling possible conflicts gracefully.

**parameters**:  
· `conn_str` (str): PostgreSQL connection string used to open a database connection for the copy operation.  
· `era_type` (str): Type of era to copy; accepted values are `"condition"`, `"drug"`, or `"drug_scdf"`. Determines which source table and SQL template are used.  
· `schema` (str): Name of the source schema that contains the era table to be copied (e.g., `vocab_2023`).  

**Code Description**:  
1. Three SQL template strings are defined:  
   * `copy_to_condition_sql` – inserts rows from `{schema}.condition_era` into `dcc_pedsnet.condition_era`.  
   * `copy_to_drug_sql` – inserts rows from `{schema}.drug_era` into `dcc_pedsnet.drug_era`.  
   * `copy_to_drug_scdf_sql` – inserts rows from `{schema}.drug_scdf_era` into `dcc_pedsnet.drug_era`.  
   All statements use `ON CONFLICT DO NOTHING` to ignore duplicate primary‑key violations.  

2. A generic log message template `copy_to_msg` is prepared: `"copying {0}_era to dcc_pedsnet"`.

3. The function selects the appropriate SQL template based on `era_type`:  
   * If `era_type == "condition"` → `copy_to_condition_sql` is formatted with `schema`.  
   * If `era_type == "drug_scdf"` → `copy_to_drug_scdf_sql` is formatted with `schema`.  
   * For any other value (including `"drug"`), `copy_to_drug_sql` is used.  
   The selected template and a formatted message are used to instantiate a `Statement` object (`copy_to_stmt`).  

4. `copy_to_stmt.execute(conn_str)` runs the INSERT statement against the database. The `Statement` class creates a new connection from `conn_str`, executes the SQL, and stores any error in `copy_to_stmt.err`.  

5. Immediately after execution, `check_stmt_err(copy_to_stmt, 'insert {0}_era data'.format(era_type))` is called. This helper inspects `copy_to_stmt.err`; if an error is present it logs the failure and raises a `DatabaseError`.  

6. If no exception is raised, the function returns `True` to indicate successful copying.  

**Reference Relationships**:  
*Callers* – The function is invoked by two higher‑level routines:  
  * `run_era` (in `pedsnetdcc/era.py`) when the user requests copying of era results to the central repository (`copy=True`).  
  * `copy_era_dcc` (in the same module) which is a thin wrapper that logs start/end messages and then calls `_copy_to_dcc_table`.  

*Callees* – Internally the function relies on:  
  * `Statement` (from `pedsnetdcc/db.py`) to encapsulate and execute the INSERT SQL.  
  * `check_stmt_err` (from `pedsnetdcc/utils.py`) to validate execution and raise a standardized exception on failure.  

**Note**:  
- The function does not perform any schema validation; it assumes that the source tables (`condition_era`, `drug_era`, or `drug_scdf_era`) exist in the supplied `schema`.  
- Because `ON CONFLICT DO NOTHING` is used, rows that already exist in the target `dcc_pedsnet` tables are silently skipped; no warning is emitted.  
- The `era_type` value `"drug_scdf"` is treated specially and copies from `drug_scdf_era` into the standard `dcc_pedsnet.drug_era` table. Any other non‑`condition` value falls back to the generic drug copy.  
- Errors occurring during connection establishment or SQL execution are captured in the `Statement` object and re‑raised as `DatabaseError` by `check_stmt_err`. Callers should be prepared to handle this exception.  

**Output Example**:  
```python
result = _copy_to_dcc_table(conn_str, "condition", "my_schema")
print(result)   # -> True (indicates that rows were inserted or already existed)
```
## FunctionDef _copy_to_drug_era_table(conn_str, schema)
**_copy_to_drug_era_table**: The function of _copy_to_drug_era_table is to copy rows from the temporary `drug_scdf_era` table into the permanent `{schema}.drug_era` table and confirm that the operation succeeded.

**parameters**:  
· conn_str: a libpq‑style PostgreSQL connection string used to open a database connection.  
· schema: the name of the target schema (e.g., `dcc_pedsnet`) that will receive the copied rows.

**Code Description**:  
1. **SQL command construction** – The function defines a multi‑line string `copy_to_drug_era_sql` that performs an `INSERT INTO {schema}.drug_era (…) SELECT … FROM {schema}.drug_scdf_era` with an `ON CONFLICT DO NOTHING` clause. The placeholders `{0}` are later replaced by the supplied `schema` argument. A human‑readable log message template `copy_to_msg` is also defined (`"copying drug_scdf_era to {0}.drug_era"`).  

2. **Statement object creation** – A `Statement` instance (`copy_to_stmt`) is created by passing the formatted SQL (`copy_to_drug_era_sql.format(schema)`) and the formatted log message (`copy_to_msg.format(schema)`). The `Statement` class encapsulates the SQL, a descriptive message, and execution metadata (rowcount, error, etc.).  

3. **Execution** – The statement’s `execute` method is called with `conn_str`. This opens a new PostgreSQL connection, runs the INSERT command in autocommit mode, and stores any database‑level exception in `copy_to_stmt.err`.  

4. **Error handling** – Immediately after execution, the helper `check_stmt_err` is invoked with the statement object and the context string `'insert drug_scdf_era data'`. `check_stmt_err` inspects `copy_to_stmt.err`; if an error is present it logs the failure and raises a `DatabaseError`. If no error is present, execution continues.  

5. **Return value** – When the INSERT completes without raising an exception, the function returns `True` to signal success to the caller.

**Reference Relationships**:  
- **Caller** – The function is invoked exclusively from `run_era` (in `pedsnetdcc/era.py`) when the era type is `"drug_scdf"`. In that context, `run_era` logs the start of the copy operation, calls `_copy_to_drug_era_table(conn_str, schema)`, and checks the Boolean return to decide whether to continue processing.  
- **Callee** – Internally the function relies on the `Statement` class (defined in `pedsnetdcc/db.py`) for SQL execution and on the utility `check_stmt_err` (defined in `pedsnetdcc/utils.py`) for standardized error detection and logging. No other project functions are called directly.

**Note**:  
- The INSERT uses `ON CONFLICT DO NOTHING`, so duplicate rows (based on the primary key or unique constraint of `drug_era`) are silently ignored. This behavior is intentional to make the copy idempotent.  
- Because the statement runs in autocommit mode, the inserted rows are committed immediately; there is no transaction rollback available from this function.  
- Any database‑level error (e.g., missing source table, permission issues, schema not found) will be captured in `copy_to_stmt.err` and cause `check_stmt_err` to raise a `DatabaseError`. Callers must be prepared to handle that exception.  
- The function does not modify the input parameters; it only returns a Boolean flag. Re‑invoking the function with the same arguments is safe and will have no effect on rows already present in the target table.

**Output Example**:  
```python
result = _copy_to_drug_era_table(conn_str, "dcc_pedsnet")
print(result)   # -> True (indicates the copy succeeded)
```
## FunctionDef _renumber_drug_era_table(conn_str, schema)
**_renumber_drug_era_table**: The function of _renumber_drug_era_table is to renumber the site_id column in the drug_era table so that the values form a contiguous sequence ordered by person_id.

**parameters**:  
· conn_str (str): PostgreSQL connection string used to open a database connection.  
· schema (str): Name of the schema that contains the drug_era table; it is interpolated into the SQL statements.

**Code Description**:  
1. **SQL preparation** – A multi‑line string renumber_drug_era_sql is defined. The statement updates {schema}.drug_era (alias d) and sets site_id to a newly generated sequential number. The new number is obtained from a sub‑query (alias nn) that selects drug_era_id, person_id and computes row_number() over an ordering by person_id. The sub‑query therefore assigns 1, 2, 3,… to each row in the order of person_id, and the outer UPDATE matches rows by drug_era_id to apply the new value.

2. **Message preparation** – renumber_drug_era_msg holds a human‑readable log message template that will be formatted with the schema name.

3. **Statement construction** – An instance of Statement (from pedsnetdcc.db) is created with the formatted SQL and message:
   ```python
   renumber_stmt = Statement(renumber_drug_era_sql.format(schema),
                             renumber_drug_era_msg.format(schema))
   ```
   The Statement object encapsulates the SQL, a descriptive message, and provides execution methods.

4. **Execution** – renumber_stmt.execute(conn_str) opens a new connection using the supplied conn_str, runs the UPDATE, and stores execution metadata (rowcount, any error, etc.) inside the Statement instance.

5. **Error handling** – After execution, the helper check_stmt_err(renumber_stmt, 'renumber site_id') is called. This function inspects renumber_stmt.err; if an error is present it logs the failure and raises a DatabaseError. If no error is found, execution proceeds.

6. **Return value** – If the UPDATE completes without raising an exception, the function returns True to signal successful renumbering.

**Reference Relationships**:  
- **Caller** – The function is invoked from run_era (in pedsnetdcc/era.py) when the era type is not "condition". run_era logs the start of renumbering, calls _renumber_drug_era_table(conn_str, schema), and logs completion. This places the function in the overall era derivation workflow after the derivation query, ID assignment, and concept‑name addition.  
- **Callee** – The function relies on the Statement class to execute the SQL and on check_stmt_err to validate the execution result. No other project functions are called directly.

**Note**:  
- The UPDATE rewrites site_id for every row in drug_era; ensure that no downstream processes depend on the original values before this step.  
- Because the statement uses row_number() ordered by person_id, the new site_id values are deterministic for a given data snapshot.  
- The function assumes that the target schema and table exist and that the executing user has UPDATE privileges.  
- Any exception raised by Statement.execute or check_stmt_err will propagate out of the function, causing run_era to abort and log the error.  
- The function does not commit or rollback explicitly; the Statement execution runs in autocommit mode, so the UPDATE is applied immediately.

**Output Example**:  
```python
result = _renumber_drug_era_table(conn_str="dbname=pedsnet host=localhost", schema="dcc")
print(result)   # -> True
```
## FunctionDef run_era(era_type, conn_str, site, copy, neg_ids, no_ids, no_concept, search_path, model_version, id_name, notable, nopk, novac)
**run_era**: The function of run_era is to orchestrate the full derivation workflow for a Condition or Drug era, including SQL execution, ID assignment, concept‑name population, optional copying to the central `dcc_pedsnet` schema, and final vacuuming.

**parameters**:  
· era_type (str): type of derivation – `"condition"`, `"drug"` or `"drug_scdf"`.  
· conn_str (str): PostgreSQL connection string used for every database operation.  
· site (str): identifier of the site for which the derivation is being performed; written into the `site` column of the era table.  
· copy (bool): if True, the resulting era table is copied into the `dcc_pedsnet` schema after derivation.  
· neg_ids (bool): if True, the ID sequence is created to generate negative identifiers; otherwise a normal positive sequence is used.  
· no_ids (bool): if True, the step that assigns identifiers to rows is skipped.  
· no_concept (bool): if True, the step that populates concept name columns is skipped.  
· search_path (str): PostgreSQL `search_path` string; the first schema in the list is used as the target schema for all operations.  
· model_version (str): PEDSnet data‑model version (e.g., `"2.3.0"`); required for metadata lookup.  
· id_name (str): prefix for the last‑ID tracking table (e.g., `"dcc"` or `"onco"`).  
· notable (bool, default False): if True, the derivation query itself is not executed (useful for dry‑run or testing).  
· nopk (bool, default False): if True, the primary‑key constraint is not added after IDs are assigned.  
· novac (bool, default False): if True, the final `VACUUM ANALYZE` step is omitted.

**Code Description**:  

1. **Connection information & logging start** – The function extracts connection metadata with `get_conn_info_dict(conn_str)`. It logs a message “starting … era calculation” together with the site and connection details, then records the start time (`time.time()`). The primary schema is obtained from `search_path` via `primary_schema`.

2. **StatementSet preparation** – An empty `StatementSet` (`stmts`) is created to hold DDL statements that must be executed before the derivation query.

3. **Drop primary key & null constraints (when applicable)** –  
   * If `era_type` is not `"drug_scdf"` and `no_ids` is False, a `DROP CONSTRAINT` statement (`DROP_PK_CONSTRAINT_ERA_SQL`) is added to `stmts`. The statement is executed immediately (outside the parallel framework) with error checking via `check_stmt_err`.  
   * After the primary‑key drop, a `DROP NULL` statement (`DROP_NULL_ERA_SQL`) is added and executed in the same manner. These steps ensure that the table can be updated without violating existing constraints.

4. **Run the derivation query** –  
   * If `notable` is False, the function builds the appropriate derivation SQL:  
     - For `"condition"` → `CONDITION_ERA_SQL`.  
     - For `"drug_scdf"` → `DRUG_ERA_SCDF_SQL`.  
     - Otherwise (standard drug) → `DRUG_ERA_SQL`.  
   * The selected SQL is wrapped in a `Statement` with a descriptive message and executed. `check_stmt_err` validates success. Logging records the start and completion of the query.

5. **Add identifiers** –  
   * If `no_ids` is False, `_add_era_ids` is called with all relevant arguments (`era_type`, `conn_str`, `site`, `neg_ids`, `search_path`, `model_version`, `id_name`).  
   * `_add_era_ids` performs counting of rows needing IDs, reserves a block of IDs in the last‑ID tracking table, creates (or re‑creates) the sequence, sets its start value, and updates the era table. It returns `True` on success; any failure raises an exception and causes `run_era` to return `False`.

6. **Populate concept names** –  
   * If `no_concept` is False, `_fill_concept_names(conn_str, era_type, site)` is invoked. This function updates the `{era_type}_era` table, copying the human‑readable concept name from `vocabulary.concept` and setting the `site` column. It returns `True` on success; otherwise `run_era` aborts.

7. **Copy drug_scdf to drug era** –  
   * When `era_type == "drug_scdf"`, `_copy_to_drug_era_table(conn_str, schema)` copies rows from the temporary `drug_scdf_era` table into the permanent `{schema}.drug_era` table using an `INSERT … ON CONFLICT DO NOTHING`. Failure returns `False`.

8. **Copy to central dcc_pedsnet schema** –  
   * If `copy` is True, `_copy_to_dcc_table(conn_str, era_type, schema)` inserts the derived rows into the corresponding table in the `dcc_pedsnet` schema, also using `ON CONFLICT DO NOTHING`. Errors raise an exception; the function returns `True` on success.

9. **Cleanup of drug_scdf temporary table** –  
   * For `"drug_scdf"` the function drops the temporary table after it has been copied to the permanent drug era table, using a `Statement` built from `drop_drug_scdf_era_sql`.

10. **Renumber site_id for drug eras** –  
    * If `era_type` is not `"condition"` (i.e., drug or drug_scdf), `_renumber_drug_era_table(conn_str, schema)` is called. This updates `site_id` to a contiguous sequence ordered by `person_id`.

11. **Add primary‑key constraint** –  
    * Unless `nopk` is True and the era type is not `"drug_scdf"`, `_add_primary_key(era_type, conn_str, schema)` adds a primary‑key constraint on the `{era_type}_era_id` column (or `drug_era_id` for `drug_scdf`). Errors are raised via `check_stmt_err`.

12. **Vacuum / analyze** –  
    * Unless `novac` is True, the function calls `vacuum(conn_str, model_version, analyze=True, tables=[era_table])`, where `era_table` is the concrete table name (`condition_era`, `drug_era`, or `drug_era` for `drug_scdf`). This refreshes planner statistics and reclaims storage.

13. **Logging end & return** –  
    * A final log entry records “finished … era calculation” together with the elapsed time (`secs_since(start_time)`). If no exception was raised, the function returns `True` to indicate successful completion.

**Reference Relationships**  

*Callers* – `run_era` is invoked by the command‑line entry points in `pedsnetdcc/main.py`:  
  - `run_derivations` (runs all derivations sequentially).  
  - `run_drug_era`, `run_drug_scdf_era`, `run_condition_era`, and `run_drug_condition_era` (individual or combined era runs).  

*Internal callees* – `run_era` relies on a number of helper functions and classes:  
  - `primary_schema` (pedsnetdcc/schema.py) to resolve the target schema.  
  - `Statement`, `StatementSet` (pedsnetdcc/db.py) for SQL encapsulation and execution.  
  - `check_stmt_err`, `check_stmt_data` (pedsnetdcc/utils.py) for error handling.  
  - `_add_era_ids`, `_fill_concept_names`, `_copy_to_drug_era_table`, `_copy_to_dcc_table`, `_renumber_drug_era_table`, `_add_primary_key` (all defined in the same module) for the individual processing steps.  
  - `vacuum` (pedsnetdcc/utils.py) for post‑processing maintenance.  
  - Logging utilities (`logger`, `combine_dicts`, `secs_since`) for structured log messages.

**Note**  
- The function assumes that the era tables (`condition_era`, `drug_era`, `drug_scdf_era`) already exist in the primary schema and that the necessary columns (`*_era_id`, `*_concept_id`, `site`, etc.) are present.  
- When `no_ids` is True, the primary‑key drop and subsequent ID‑related steps are skipped; however, the function still attempts to drop the primary‑key constraint if it exists, which may raise an error if the constraint is absent.  
- The `notable` flag bypasses the derivation query only; all subsequent steps (ID assignment, concept name addition, copying, vacuum) are still performed unless their own flags (`no_ids`, `no_concept`, `copy`, `novac`) suppress them.  
- `neg_ids` creates a sequence that generates negative identifiers in the range `-2147483647 … 0`; this mirrors the historic PEDSnet convention for “negative” IDs.  
- All DDL statements are executed with autocommit isolation level, making them irreversible within the function.  
- Errors from any helper raise `DatabaseError` (or `MissingDataError` from `check_stmt_data`); `run_era` catches them only implicitly via the return‑value checks performed after each helper call. If an exception propagates, the caller (e.g., the CLI wrapper) will exit with a non‑zero status.

**Output Example**  
```python
result = run_era(
    era_type="condition",
    conn_str="host=pg host=localhost dbname=pedsnet user=loader password=secret options='-c search_path=dcc_pedsnet,public'",
    site="dcc",
    copy=True,
    neg_ids=False,
    no_ids=False,
    no_concept=False,
    search_path="dcc_pedsnet, public",
    model_version="2.3.0",
    id_name="dcc"
)
print(result)   # -> True (indicates the entire condition‑era derivation completed successfully)
```
## FunctionDef _add_primary_key(era_type, conn_str, schema)
**_add_primary_key**: The function of _add_primary_key is to add a primary‑key constraint on the *_era_id* column of a specific era table in the target schema.

**parameters**:  
· era_type (str): the type of era being processed (e.g., “condition”, “drug”, or “drug_scdf”).  
· conn_str (str): a libpq‑style PostgreSQL connection string used to open a database connection.  
· schema (str): the PostgreSQL schema that contains the era table.

**Code Description**:  
1. **SQL template preparation** – Two format strings are defined:  
   * `pk_era_id_sql` = `"alter table {0}.{1}_era add primary key ({2}_era_id)"` – the DDL that will be executed.  
   * `pk_era_id_msg` = `"making {0}_era_id the primary key"` – a human‑readable log message.  

2. **Special handling for the drug_scdf era** – When `era_type` equals `'drug_scdf'` the primary‑key column name is still `drug_era_id`. The code therefore substitutes `temp_era_type = 'drug'` for the column placeholder while keeping the table name as `drug_scdf_era`. For all other era types `temp_era_type` is left unchanged.

3. **Logging start of operation** – An informational log entry with the message `'begin add primary key'` is emitted.

4. **Statement construction** – A `Statement` object is instantiated with the formatted SQL and the formatted log message:  
   ```python
   pk_era_id_stmt = Statement(
       pk_era_id_sql.format(schema, era_type, temp_era_type),
       pk_era_id_msg.format(temp_era_type)
   )
   ```

5. **Execution** – The statement’s `execute` method is called with `conn_str`. This opens a new autocommit connection, runs the ALTER TABLE command, and stores any exception in `pk_era_id_stmt.err`.

6. **Error checking** – `check_stmt_err` is invoked with the statement and a context string that includes the era type. If the execution raised an error, `check_stmt_err` logs the failure and raises a `DatabaseError`, causing the calling function to abort. If no error is present, execution continues.

7. **Logging success** – An informational log entry `'primary key created'` is emitted after the statement succeeds.

8. **Return value** – The function returns `True` to indicate that the primary‑key addition completed without raising an exception.

**Reference Relationships**:  
*Caller*: `_add_primary_key` is called from `run_era` (pedsnetdcc/era.py) when the `nopk` flag is False and the era type is not `'drug_scdf'`. It is part of the final steps of the era derivation workflow, after IDs have been added, concept names filled, and any necessary table copies performed.  
*Callees*:  
- `Statement` (pedsnetdcc/db.py) – used to encapsulate the ALTER TABLE command.  
- `Statement.execute` – runs the command on a fresh database connection.  
- `check_stmt_err` (pedsnetdcc/utils.py) – validates that the execution did not produce an error and raises a `DatabaseError` if it did.  
- The module‑level `logger` – records start, success, and any error information.

**Note**:  
- The function assumes that the target table (`{era_type}_era` or `drug_scdf_era`) already exists in the specified schema.  
- If the primary‑key constraint already exists, PostgreSQL will raise an error; this will be caught by `check_stmt_err` and propagated as a `DatabaseError`.  
- The special case for `'drug_scdf'` only changes the column name used in the ALTER statement; the table name remains `{schema}.drug_scdf_era`.  
- No explicit transaction handling is required because the statement is executed with autocommit isolation level.  
- The function always returns `True` when it reaches the return statement; any failure results in an exception before that point.

**Output Example**:  
```python
result = _add_primary_key('condition', conn_str, 'dcc_pedsnet')
print(result)   # -> True
```
## FunctionDef _add_era_ids(era_type, conn_str, site, neg_ids, search_path, model_version, id_name)
**_add_era_ids**: The function of _add_era_ids is to assign unique identifiers to rows in an era table (condition_era, drug_era, or drug_scdf_era) and to make the identifier column the primary key.

**parameters**:  
· era_type (str): type of era derivation – “condition”, “drug”, or “drug_scdf”.  
· conn_str (str): PostgreSQL libpq connection string used for all database operations.  
· site (str): site identifier that is part of the schema-qualified table names.  
· neg_ids (bool): if True, creates a sequence that generates negative IDs; otherwise creates a normal positive‑ID sequence.  
· search_path (str): PostgreSQL schema search_path; the first schema is used as the target schema.  
· model_version (str): PEDSnet model version (e.g., “2.3.0”) used to obtain the stock metadata.  
· id_name (str): prefix for the last‑ID tracking table (e.g., “dcc” or “onco”).

**Code Description**:  

1. **Preparation and logging** – The function extracts connection information with `get_conn_info_dict`, merges it with the site identifier via `combine_dicts`, and logs the start of the ID assignment. The start time is recorded for elapsed‑time logging.

2. **Schema and table name resolution** – `primary_schema(search_path)` returns the primary schema name. `table_name` is built as `{era_type}_era`. For the special case `era_type == 'drug_scdf'`, the function treats the underlying table as `drug_era` and uses `temp_era_type = 'drug'` for column naming.

3. **Metadata acquisition** – `stock_metadata(model_version)` loads a SQLAlchemy `MetaData` object for the requested model version. The target table object is obtained from `metadata.tables[temp_table_name]` (used later only for existence checking; the actual SQL statements operate directly on the database).

4. **Count needed IDs** – A `Statement` is created with the SQL  
   ```sql
   SELECT COUNT(*) FROM {era_type}_era WHERE {temp_era_type}_era_id IS NULL
   ```  
   and executed. `check_stmt_err` validates that the execution succeeded, and `check_stmt_data` ensures a result row was returned. The count (`new_id_count`) is stored in `tpl_vars`.

5. **Reserve a block of IDs** – Two statements are placed in a `StatementList`:
   * `LOCK {last_id_table_name}` – locks the last‑ID tracking table for the given `id_name`.
   * `UPDATE {last_id_table_name} AS new SET last_id = new.last_id + '{new_id_count}'::bigint FROM {last_id_table_name} AS old RETURNING old.last_id, new.last_id` – increments the stored last ID by the number of IDs needed and returns the old and new values.  
   The list is executed with `serial_execute(conn_str, transaction=True)`, guaranteeing that the lock and update occur atomically. After execution, the old and new last IDs are extracted from the second statement’s result and logged.

6. **Create the ID sequence** – Depending on `neg_ids`, either a normal sequence or a negative‑only sequence is created with `CREATE SEQUENCE IF NOT EXISTS {schema}.{site}_{era_type}_era_id_seq`. For negative IDs the sequence is defined with `INCREMENT 1 START -2147483647 MINVALUE -2147483647 MAXVALUE 0`. The creation statement is wrapped in a `Statement` and executed; errors are caught by `check_stmt_err`.

7. **Set the sequence start value** – The sequence is reset to start at the previously reserved `old_last_id` using  
   ```sql
   ALTER SEQUENCE {schema}.{site}_{era_type}_era_id_seq RESTART WITH {old_last_id};
   ```  
   This ensures that the next call to `nextval` will produce the first ID in the newly reserved block.

8. **Assign IDs to rows** – An update statement assigns IDs to all rows that still have a NULL identifier:  
   ```sql
   UPDATE {schema}.{era_type}_era
   SET {temp_era_type}_era_id = nextval('{schema}.{site}_{era_type}_era_id_seq')
   WHERE {temp_era_type}_era_id IS NULL;
   ```  
   Execution is performed via a `Statement`; any error raises a `DatabaseError` through `check_stmt_err`.

9. **Finalize logging** – The function logs completion, including the elapsed time calculated by `secs_since(start_time)`, and returns `True` to indicate success.

**Reference relationships**  

* **Callers** – The only direct caller in the project is `run_era` (pedsnetdcc/era.py). `run_era` invokes `_add_era_ids` after the era derivation query has been executed and before concept names are added, primary keys are restored, or vacuuming occurs. The caller passes all parameters required for ID assignment and checks the Boolean return value to decide whether to continue.

* **Callees / utilities** –  
  * `Statement` and `StatementList` from `pedsnetdcc/db.py` are used to build and execute the individual SQL commands.  
  * `check_stmt_err` and `check_stmt_data` from `pedsnetdcc/utils.py` provide error detection and missing‑data validation.  
  * `get_conn_info_dict` and `combine_dicts` supply connection metadata for structured logging.  
  * `primary_schema` (pedsnetdcc/schema.py) extracts the target schema from the search_path.  
  * `stock_metadata` (pedsnetdcc/utils.py) loads the model definition needed to reference the era table.  
  * `secs_since` (pedsnetdcc/dict_logging.py) formats elapsed seconds for log messages.

**Note**  
* The function assumes that a last‑ID tracking table named `{id_name}_{table_name}_id` already exists in the target schema; it does not create this table.  
* The sequence name is `{schema}.{site}_{era_type}_era_id_seq`. If the sequence already exists, `CREATE SEQUENCE IF NOT EXISTS` leaves it unchanged, but the subsequent `ALTER SEQUENCE … RESTART WITH` will reset its next value to the reserved start.  
* Negative ID generation is only supported when `neg_ids` is True; the sequence is created with a fixed negative range that matches the original PEDSnet convention.  
* All SQL statements are executed with autocommit (isolation level 0). The lock‑update pair is wrapped in a transaction via `StatementList.serial_execute(..., transaction=True)` to avoid race conditions when multiple processes allocate IDs concurrently.  
* The function returns a plain Boolean (`True` on success). Any failure raises either `DatabaseError` (via `check_stmt_err`) or `MissingDataError` (via `check_stmt_data`), which propagates to the caller.

**Output Example**  
```python
result = _add_era_ids(
    era_type='condition',
    conn_str='host=pg host=localhost dbname=demo user=loader password=secret options=\'-c search_path=dds\'',
    site='dcc',
    neg_ids=False,
    search_path='dcc, public',
    model_version='2.3.0',
    id_name='dcc'
)
print(result)   # -> True
```
## FunctionDef copy_era_dcc(era_type, conn_str, site, search_path)
**copy_era_dcc**: The function of copy_era_dcc is to copy a derived condition or drug era table from a source schema into the central `dcc_pedsnet` schema while logging the operation.  

**parameters**:  
· **era_type** (str): Type of derivation to copy; accepted values are `"condition"` or `"drug"` (the caller may also pass `"drug_scdf"` but the wrapper only uses the first two).  
· **conn_str** (str): PostgreSQL connection string used for all database interactions.  
· **site** (str): Identifier of the site for which the era data is being copied; included in log messages.  
· **search_path** (str): PostgreSQL schema search path; the first schema in this list is treated as the source schema for the copy.  

**Code Description**:  
1. **Parse connection information** – `get_conn_info_dict(conn_str)` extracts user, host, port, dbname, and search_path from the connection string and returns a dictionary (`conn_info_dict`).  
2. **Prepare start‑up log entry** – A message template `'{0} {1} era entries'` is combined with a dictionary that merges the site identifier and the connection info (`log_dict`). The start of the copy operation is logged with `logger.info`.  
3. **Record start time** – `time.time()` is stored in `start_time` for later elapsed‑time calculation.  
4. **Determine source schema** – `primary_schema(search_path)` returns the first schema name from the supplied search path; this value is stored in `schema`.  
5. **Copy data to dcc_pedsnet** –  
   * An informational log entry `"copy {era_type}_era to dcc_pedsnet"` is emitted.  
   * The helper `_copy_to_dcc_table(conn_str, era_type, schema)` is called. This helper builds the appropriate INSERT … SELECT statement (condition, drug, or drug_scdf), executes it via a `Statement` object, checks for errors with `check_stmt_err`, and returns `True` on success.  
   * If `_copy_to_dcc_table` returns a falsy value, `copy_era_dcc` returns `False` immediately, indicating failure.  
   * On success, a log entry confirming that the era table has been copied is emitted.  
6. **Log completion** – The elapsed time since `start_time` is calculated with `secs_since(start_time)` and included in a final log entry that marks the end of the copy operation.  
7. **Return status** – If no exception was raised and the copy succeeded, the function returns `True`.  

**Reference Relationships**:  
*Callers* – `copy_era_dcc` is invoked by the command‑line entry points defined in `pedsnetdcc/main.py`:  
  * `copy_drug_era` calls `copy_era_dcc("drug", ...)`.  
  * `copy_condition_era` calls `copy_era_dcc("condition", ...)`.  
These wrappers construct the connection string, obtain the search path, and forward the parameters.  

*Callees* – Within its body, `copy_era_dcc` relies on several utility functions:  
  * `get_conn_info_dict` (pedsnetdcc/utils.py) for extracting connection metadata used in logging.  
  * `combine_dicts` (pedsnetdcc/utils.py) for merging dictionaries that form the log context.  
  * `primary_schema` (pedsnetdcc/schema.py) to select the source schema from the search path.  
  * `_copy_to_dcc_table` (pedsnetdcc/era.py) to perform the actual INSERT … SELECT operation.  
  * `secs_since` (pedsnetdcc/dict_logging.py) to format the elapsed time for the final log entry.  

**Note**:  
- The function does not perform validation of `era_type` beyond passing it to `_copy_to_dcc_table`; an unsupported value will be treated as a generic drug copy by the callee.  
- All logging is performed through the module‑level `logger`; callers must ensure that the logger is configured appropriately for the desired output destination.  
- Errors raised inside `_copy_to_dcc_table` (e.g., SQL execution failures) propagate as `DatabaseError`; `copy_era_dcc` does not catch them, so the caller should be prepared to handle this exception.  
- The elapsed‑time string returned by `secs_since` is zero‑padded to four characters but will expand if the duration exceeds 9999 seconds.  

**Output Example**:  
```python
result = copy_era_dcc("condition", conn_str, "stlouis", "my_schema, public")
print(result)   # -> True  (indicates the condition_era rows were copied successfully)
```
