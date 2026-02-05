## FunctionDef _make_index_name(table_name, column_name)
**_make_index_name**: The function of _make_index_name is to generate a deterministic, length‑restricted index name for a specific table‑column pair, suitable for Oracle databases.  

**parameters**:  
· table_name (str): The name of the database table for which the index is being created.  
· column_name (str): The name of the column within the table that the index will cover.  

**Code Description**:  
The function builds an index identifier that complies with Oracle’s maximum object‑name length (defined elsewhere as NAME_LIMIT, typically 30 characters).  
1. **Table abbreviation** – It prefixes the first three characters of table_name with the literal “mea_”, producing a short table token (e.g., “mea_mea” for a table named “measurement”).  
2. **Column abbreviation** – It splits column_name on underscores and concatenates the first character of each segment, yielding a compact column token (e.g., “mci” for “measurement_concept_id”).  
3. **Hash component** – It creates an MD5 hash of the string “{table_name}.{column_name}”, encodes it as UTF‑8, and extracts a slice of the hexadecimal digest. The slice length is calculated as:  
   `hashlen = NAME_LIMIT - (len(table_abbrev) + len(column_abbrev) + 3 * len('_') + len('ix'))`  
   This ensures that the final assembled name never exceeds the Oracle limit.  
4. **Final assembly** – The function joins the three parts (table abbreviation, column abbreviation, truncated hash) with underscores and appends the literal “ix” to indicate an index, returning a string such as “mea_mea_mci_ae1fd5b22b92397ca9_ix”.  

**Relationship with Callers**:  
In pedsnetdcc/subset_by_cohort.py, the measure_index function iterates over a predefined list of measurement‑related columns. For each column it calls _make_index_name to obtain a unique, Oracle‑compatible index name, which is then embedded in a CREATE INDEX statement. This centralizes naming logic, guaranteeing that all measurement‑like indexes share a consistent format and avoid name collisions across tables that have similarly named columns (e.g., provider.gender_source_concept_name vs. person.gender_source_concept_name).  

**Note**:  
- The constant NAME_LIMIT must be defined in the module’s scope; if it is smaller than the combined length of the static parts, hashlen could become zero or negative, causing an invalid index name.  
- The function is intended for internal use (indicated by the leading underscore) and should not be called directly from external code without ensuring the appropriate constants are available.  
- Because the hash is derived from the full table‑column string, the same table‑column pair will always produce the same index name, which aids reproducibility.  

**Output Example**:  
Calling `_make_index_name('measurement', 'measurement_concept_id')` might return:  
`mea_mea_mci_ae1fd5b22b92397ca9_ix`   (exact hash segment will vary with the MD5 digest).
## FunctionDef run_subset_by_cohort(conn_str, model_version, source_schema, target_schema, cohort_table, concept_create, drug_dose, measurement, covid_obs, inc_hash, index_create, fk_create, notable, nopk, nonull, limit, owner, pre_split, drug_tpn, force)
**run_subset_by_cohort**: The function of run_subset_by_cohort is to create a subset of a PEDSnet database schema based on a cohort table, optionally adding auxiliary objects such as concept‑group tables, drug dose tables, measurement tables, COVID observation tables, indexes, primary keys, NOT NULL constraints, and foreign‑key constraints.

**parameters**:  
· conn_str (str): libpq‑style PostgreSQL connection string used for all database operations.  
· model_version (str): PEDSnet model version identifier (e.g., “2.3.0”).  
· source_schema (str): name of the schema that contains the original PEDSnet tables.  
· target_schema (str): name of the schema that will receive the subset tables.  
· cohort_table (str): name of the table that holds the cohort (person_id) list.  
· concept_create (bool, default False): if True, create concept‑group index‑replacement tables in the target schema.  
· drug_dose (bool, default False): if True, copy drug dose derivation tables into the target schema.  
· measurement (bool, default False): if True, copy measurement derivation tables (or a subset of them) into the target schema.  
· covid_obs (bool, default False): if True, copy the COVID‑specific observation derivation table.  
· inc_hash (bool, default False): if True, include the hash_token table in the subset; otherwise the table is created empty.  
· index_create (bool, default False): if True, create indexes on the subset tables after they are populated.  
· fk_create (bool, default False): if True, add foreign‑key constraints to the subset tables.  
· notable (bool, default False): if True, skip creation of all tables (used for dry‑run or testing).  
· nopk (bool, default False): if True, do not add primary‑key constraints to the subset tables.  
· nonull (bool, default False): if True, do not add NOT NULL constraints to the subset tables.  
· limit (bool, default False): if True, grant permissions only to the specified owner; otherwise grant to the standard DCC user group.  
· owner (str, default 'loading_user'): database role that will receive the granted permissions.  
· pre_split (bool, default False): if True, treat the measurement table as already split and create the split measurement tables directly.  
· drug_tpn (bool, default False): if True, copy the drug_exposure_tpn table into the target schema.  
· force (bool, default False): if True, ignore benign errors (e.g., “object already exists”) during DDL execution.

**Code Description**:  

1. **Logging initialization** – A module logger is obtained. A log dictionary (`log_dict`) is built by merging the supplied `model_version` with connection‑information extracted from `conn_str` via `get_conn_info_dict`. An INFO message “starting subset by cohort” is emitted and the start time is recorded (`start_time = time.time()`).

2. **Metadata acquisition** – The function calls `stock_metadata(model_version)` to obtain a SQLAlchemy `MetaData` object that describes the stock PEDSnet schema for the requested model version. An empty `StatementSet` (`stmts`) is created to hold DDL statements.

3. **Table selection logic** –  
   * `select_all` lists tables that are copied without a join to the cohort table (e.g., location, provider).  
   * `special_handling` lists tables that require custom join or filter logic (e.g., fact_relationship, hash_token, cohort).  
   * If `measurement` and `pre_split` are both True, the full set of split measurement tables is used; otherwise a reduced set is selected based on the flags.

4. **Initial table creation (unless `notable`)** –  
   * For every table in the metadata that is not a vocabulary table and not in `special_handling`, a CREATE‑TABLE AS SELECT statement is built.  
   * Tables listed in `select_all` are created as a straight copy of the source table. All other tables are created with an inner join to the target cohort table on `person_id`.  
   * The generated SQL strings are stored in `create_dict`; the table names are added to `grant_vacuum_tables` for later permission and vacuum steps.  
   * All statements are added to `stmts`, executed in parallel (`stmts.parallel_execute(conn_str)`), and each statement is checked with `check_stmt_err`. Failure aborts the function with a logged fatal error.

5. **Special‑handling tables** – After the initial tables are created, the function repeats the process for tables in `special_handling`. Each table receives a tailored SQL statement:
   * `fact_relationship` includes a WHERE clause that limits rows to those referencing entities present in the newly created subset tables.  
   * `location_history` joins to the cohort table on `entity_id`.  
   * `visit_payer` joins to `visit_occurrence`.  
   * `hash_token` is either joined to the cohort (if `inc_hash` is True) or created with `WHERE FALSE` (empty).  
   * `cohort` and `cohort_definition` join to the cohort table on `subject_id`.  
   * Each statement is executed in parallel and errors are handled as in step 4.

6. **Optional auxiliary tables** –  
   * **Drug TPN table** – If `drug_tpn` is True, a CREATE‑TABLE AS SELECT for `drug_exposure_tpn` is generated, joined to the cohort, executed, and logged.  
   * **Drug dose tables** – If `drug_dose` is True, two tables (`drug_exposures_mgkg_derivations`, `drug_exposures_mgkg_metadata`) are created. The derivations table joins to the cohort; the metadata table joins to `drug_exposure`. Execution follows the same parallel pattern.  
   * **Measurement tables** – If `measurement` or `pre_split` is True, the appropriate set of measurement tables (determined in step 3) is created, each joined to the cohort. Execution and error handling are identical to previous steps.  
   * **COVID observation table** – If `covid_obs` is True, `observation_derivation_covid` is created with a join to the cohort and executed.

7. **Search‑path adjustment for target schema** – A new connection string (`new_conn_str`) is built with `conn_str_with_search_path`, setting the `search_path` to `<target_schema>,vocabulary`. This ensures subsequent DDL statements operate on the target schema without needing to qualify schema names.

8. **Primary‑key addition** – If `nopk` is False, `add_primary_keys(new_conn_str, model_version, force)` is called to add primary‑key constraints to all core tables. When measurement tables are present, `measure_pk` is invoked for each measurement table to add a measurement‑specific primary key.

9. **NOT NULL constraints** – If `nonull` is False, `set_not_nulls(new_conn_str, model_version)` adds NOT NULL constraints to core tables. When measurement tables are present, `add_measurement_not_nulls` adds NOT NULL constraints to the measurement‑like tables.

10. **Index creation** – If `index_create` is True, `add_indexes(new_conn_str, model_version, force)` creates standard indexes, `drop_unneeded_indexes` removes indexes that are no longer required, and for each measurement table `measure_index` creates Oracle‑compatible indexes on a predefined column set.

11. **Foreign‑key creation** – If `fk_create` is True, foreign‑key constraints are added:
    * When `pre_split` is True, measurement‑like foreign keys are added first (`add_measurement_like_fks`), then a measurement‑organism FK (`add_measurement_org_lab_fk`), and finally generic foreign keys via `add_foreign_keys` with `skip_meas_org_meas=True`.
    * When `pre_split` is False, generic foreign keys are added directly with `add_foreign_keys`.

12. **Concept‑group index‑replacement tables** – If `concept_create` is True, `create_index_replacement_tables` is invoked to generate auxiliary tables for concept‑group columns and to add primary keys to them. The list of tables that will receive permissions is extended with the concept‑group tables.

13. **Permission granting** – A fresh `StatementSet` is prepared. For each table in `grant_vacuum_tables` (core tables plus any auxiliary tables created above), a GRANT statement (`GRANT_TABLE_SQL.format(target_schema, target_table, usr)`) is added for each user. The user list is either the single `owner` (if `limit` is True) or the default DCC user group (`pre_prod_dcc`, `pcor_et_user`, `peds_staff`, `dcc_analytics`). All GRANT statements are executed sequentially (no parallel execution) and errors are logged and raised.

14. **VACUUM/ANALYZE** – `vacuum(new_conn_str, model_version, analyze=True, tables=grant_vacuum_tables)` runs `VACUUM ANALYZE` on every table that was created, ensuring fresh statistics for downstream queries.

15. **Final logging and return** – An INFO message “finished subset by cohort” with elapsed time is logged. The function returns `True` to indicate successful completion.

**Reference relationships**:  

*Callers* – The function is invoked by the CLI entry point `pedsnetdcc/main.py/subset_by_cohort`. The CLI parses command‑line options, builds a connection string, and forwards all user‑specified flags to `run_subset_by_cohort`. After a successful run, the CLI may optionally invoke `split_measurement_table` and `partition_measurement_table` using the newly created target schema.

*Internal callees* –  
- `stock_metadata` – obtains the canonical model definition.  
- `Statement` and `StatementSet` – encapsulate and batch‑execute all generated DDL statements.  
- `check_stmt_err` – validates each executed statement and raises `DatabaseError` on failure.  
- `add_primary_keys`, `measure_pk` – add primary‑key constraints.  
- `set_not_nulls`, `add_measurement_not_nulls` – apply NOT NULL constraints.  
- `add_indexes`, `drop_unneeded_indexes`, `measure_index` – manage indexes.  
- `add_foreign_keys`, `add_measurement_like_fks`, `add_measurement_org_lab_fk` – create foreign‑key constraints.  
- `create_index_replacement_tables` – generate concept‑group auxiliary tables.  
- `conn_str_with_search_path` – builds a connection string that sets the appropriate `search_path`.  
- `vacuum` – performs VACUUM ANALYZE on the created tables.  
- Utility functions `combine_dicts`, `get_conn_info_dict`, `secs_since` – support structured logging and elapsed‑time calculation.

**Note**:  
- The function assumes that the source schema contains a fully populated PEDSnet model matching `model_version`.  
- All DDL statements are executed with autocommit; they cannot be rolled back as a single transaction.  
- Errors in any step abort the process; the caller must handle `DatabaseError` exceptions.  
- The `limit` flag restricts GRANT statements to a single owner; otherwise a predefined DCC user group receives permissions.  
- The `force` flag only influences primary‑key, index, and foreign‑key creation functions that accept it; it does not affect table‑creation steps.  
- When `pre_split` is True, measurement tables are treated as already partitioned, and measurement‑specific foreign keys are added before generic foreign keys.  
- The function does not return any data structures; success is indicated solely by the boolean return value.

**Output Example**:  
```python
result = run_subset_by_cohort(
    conn_str="host=pg port=5432 dbname=pedsnet user=loader password=secret",
    model_version="2.3.0",
    source_schema="dcc",
    target_schema="dcc_subset",
    cohort_table="cohort_mystudy",
    concept_create=True,
    drug_dose=False,
    measurement=True,
    covid_obs=False,
    inc_hash=False,
    index_create=True,
    fk_create=True,
    notable=False,
    nopk=False,
    nonull=False,
    limit=False,
    owner="loader",
    pre_split=False,
    drug_tpn=False,
    force=False
)
print(result)   # -> True (indicates the subset schema was created successfully)
```
## FunctionDef run_index_replace(conn_str, model_version)
**run_index_replace**: The function of run_index_replace is to orchestrate the creation of index‑replacement tables for a given PEDSnet model version and report success.

**parameters**  
· conn_str: a libpq‑style PostgreSQL connection string that identifies the target database and schema.  
· model_version: a string identifying the PEDSnet data model version (e.g., “2.3.0”).

**Code Description**  
1. **Logger preparation** – The function obtains a module‑level logger (`logging.getLogger(__name__)`).  
2. **Log dictionary assembly** – It builds a base dictionary `log_dict` by merging the supplied `model_version` with connection information extracted via `get_conn_info_dict(conn_str)`. This dictionary is used for all subsequent structured log entries.  
3. **Start log entry** – An informational log message “starting subset by cohort” is emitted together with `log_dict`. The current epoch time is stored in `start_time` to later calculate elapsed time.  
4. **Metadata acquisition** – `stock_metadata(model_version)` is called to retrieve a SQLAlchemy `MetaData` object that represents the canonical PEDSnet schema for the requested version. The returned `metadata` object is not directly used later in this function, but its creation ensures that the model version is valid and may be required by downstream utilities.  
5. **StatementSet initialization** – An empty `StatementSet` instance (`stmts`) is created. In the current implementation the set remains unused; it is likely retained for future extensions or to keep a consistent pattern with other workflow functions.  
6. **Index‑replacement table creation** – The core operation is delegated to `create_index_replacement_tables(conn_str, model_version)`. This helper builds auxiliary tables for concept‑group columns and adds primary‑key constraints, executing the DDL statements in parallel. Any exception raised inside this call propagates out of `run_index_replace`, causing the function to terminate without reaching the final log entry.  
7. **Finish log entry** – After the helper returns successfully, an informational log message “finished subset by cohort” is emitted. The elapsed time is calculated with `secs_since(start_time)` and added to the log dictionary.  
8. **Return value** – If execution reaches this point without raising an exception, the function returns `True` to indicate successful completion.

**Reference Relationships**  
*Callers* –  
- `pedsnetdcc/main.py/create_index_replace` parses command‑line arguments, builds a connection string, and invokes `run_index_replace`. It exits the process with status 0 on success or status 1 on failure.  
- The documentation for `create_index_replacement_tables` describes `run_index_replace` as a thin wrapper that forwards its arguments, indicating that other higher‑level workflows (e.g., subset creation scripts) may also call it indirectly.

*Callees* –  
- `pedsnetdcc/concept_group_tables.py/create_index_replacement_tables` performs the actual DDL work.  
- Utility functions `pedsnetdcc/utils.py/combine_dicts`, `pedsnetdcc/utils.py/get_conn_info_dict`, and `pedsnetdcc/dict_logging.py/secs_since` are used for structured logging and elapsed‑time calculation.  
- `pedsnetdcc/utils.py/stock_metadata` supplies the model metadata.  
- `pedsnetdcc/db.py/StatementSet` is instantiated but not populated; its presence aligns the function with the project’s pattern of using statement collections.

**Note**  
- The function does not catch exceptions from `create_index_replacement_tables`; any database error will cause the function to abort and propagate the exception to the caller.  
- A valid PostgreSQL connection string and a supported model version are required; otherwise the helper may raise a `DatabaseError` or a connection‑related exception.  
- The `StatementSet` object created in step 5 is currently unused; future extensions might add additional statements to be executed in the same workflow.  
- Logging is performed with structured dictionaries, which facilitates downstream log aggregation and analysis.

**Output Example**  
```python
result = run_index_replace("host=pghost port=5432 dbname=mydb user=loader", "2.3.0")
print(result)   # -> True
```
## FunctionDef measure_pk(conn_str, model_version, schema, m_type)
**measure_pk**: The function of measure_pk is to add a “measurement‑like” primary‑key constraint to a specific measurement table in the target schema.

**parameters**  
· conn_str: PostgreSQL connection string used to open a database connection.  
· model_version: string identifying the PEDSnet model version (e.g., “2.3.0”).  
· schema: name of the target schema that contains the measurement table.  
· m_type: suffix of the measurement table name (the part after “measurement_”), used to build the appropriate SQL statement.

**Code Description**  
1. **Logging preparation** – A module‑level logger is obtained for the current file. The helper combine_dicts merges a dictionary containing the supplied model_version with the connection‑info dictionary returned by get_conn_info_dict(conn_str). This combined dictionary ( log_dict ) is used for all subsequent log entries so that each message carries model version and connection metadata.  

2. **Start‑up log** – An INFO log records that the “measurement like primary key” step is beginning, and the current epoch time is stored in start_time for later elapsed‑time reporting.  

3. **StatementSet creation** – A new StatementSet instance stmts is created and immediately cleared (the clear call is defensive; a freshly constructed set is already empty).  

4. **SQL statement construction** – The constant PK_MEASURE_LIKE_TABLE_SQL is a format string that expects the target schema and the measurement type. It is formatted with the supplied schema and m_type to produce a complete DDL statement that creates the primary‑key‑like constraint on the measurement table. The resulting SQL string is wrapped in a Statement object idx_stmt.  

5. **Queue the statement** – The Statement object is added to the StatementSet via stmts.add(idx_stmt).  

6. **Parallel execution** – stmts.parallel_execute(conn_str, 1) launches a single worker process (pool size 1) to execute the queued statement. Using a single worker guarantees sequential execution while still reusing the parallel‑execution infrastructure (queues, logging thread, result collection).  

7. **Error handling** – After execution, the function iterates over the (single) statement in stmts. For each statement it calls check_stmt_err(stmt, 'Run measurement like PK'). If the statement’s err attribute is not None, check_stmt_err logs a fatal error, adds the elapsed time (computed with secs_since(start_time)), and raises a DatabaseError. The surrounding try/except captures any exception, logs the offending SQL and error message, logs the total elapsed time, and re‑raises the exception so the caller is aware of the failure.  

8. **Success log and return** – If no error is detected, an INFO log records that the primary‑key addition is complete and the function returns True to indicate success.

**Reference relationships**  
- **Callers**: The only direct caller in the codebase is run_subset_by_cohort (in pedsnetdcc/subset_by_cohort.py). After primary keys are added to all tables, run_subset_by_cohort loops over each measurement table (e.g., measurement_bmi, measurement_ht_z) and invokes measure_pk to create the measurement‑specific primary‑key constraint.  
- **Callees / dependencies**:  
  * PK_MEASURE_LIKE_TABLE_SQL – a module‑level SQL template string that defines the DDL for the measurement primary key.  
  * Statement – encapsulates the formatted SQL and provides execute logic.  
  * StatementSet – aggregates the Statement and runs it via parallel_execute.  
  * combine_dicts, get_conn_info_dict – utilities used to build the logging dictionary.  
  * check_stmt_err – validates the execution result and raises a DatabaseError if needed.  
  * secs_since – formats the elapsed time for log entries.  

**Note**  
- The function always uses a pool size of 1; therefore the statement runs in a separate process but not concurrently with other statements. This design isolates the execution environment while keeping the parallel‑execution pattern consistent across the project.  
- The returned value is a boolean True only when the primary‑key statement executes without error. Any failure results in an exception being raised; the caller will not receive True.  
- The function does not modify the input parameters; it only constructs and runs a DDL command.  
- Because the SQL is generated from a format string, the caller must ensure that schema and m_type are trusted values to avoid SQL injection.  

**Output Example**  
```python
result = measure_pk(conn_str,
                    model_version='2.3.0',
                    schema='subset_schema',
                    m_type='bmi')
# If the statement succeeds, result is True
# If the statement fails, a DatabaseError is raised and the function does not return.
```
## FunctionDef measure_index(conn_str, model_version, schema, m_type)
**measure_index**: The function of measure_index is to create Oracle‑compatible indexes on a set of measurement‑like columns for a specified measurement table type.

**parameters**:  
· conn_str – PostgreSQL libpq connection string used to connect to the database.  
· model_version – string identifying the PEDSnet model version (e.g., “2.3.0”).  
· schema – name of the target schema that contains the measurement‑like table.  
· m_type – suffix of the measurement table (e.g., “anthro”, “labs”, “vitals”) that determines which table the indexes are built on.

**Code Description**:  
1. A module‑level logger is obtained and a log dictionary is built by merging the model version with connection‑info extracted via `get_conn_info_dict`. An informational log entry marks the start of the index‑creation step.  
2. `start_time` records the current epoch time for later elapsed‑time reporting.  
3. A `StatementSet` instance (`stmts`) is created to hold the DDL statements that will create the indexes.  
4. `col_index` is a tuple listing the measurement columns that require indexes:  
   - measurement_age_in_months  
   - measurement_concept_id  
   - measurement_date  
   - measurement_type_concept_id  
   - person_id  
   - site  
   - visit_occurrence_id  
   - measurement_source_value  
   - value_as_concept_id  
   - value_as_number  
5. For each column name in `col_index` the function:  
   a. Calls the internal helper `_make_index_name(m_type, col)` to generate a deterministic, length‑restricted index name that complies with Oracle’s object‑name limit.  
   b. Formats the constant `IDX_MEASURE_LIKE_TABLE_SQL` with the generated index name, the target `schema`, the measurement type (`m_type`), and the column name. The resulting SQL is a `CREATE INDEX … ON … (column)` statement.  
   c. Wraps the SQL string in a `Statement` object and adds it to the `StatementSet`.  
6. After all index statements are collected, `stmts.parallel_execute(conn_str, 5)` launches five worker processes that each execute the stored statements in parallel. The `StatementSet` handles task queuing, result collection, and logging thread management internally.  
7. Once execution finishes, the function iterates over each `Statement` in the set and calls `check_stmt_err(stmt, 'Run measurement like indexes')`. If any statement has its `err` attribute set, `check_stmt_err` logs the failure (including the SQL text and error message) and raises a `DatabaseError`. The surrounding `try/except` block catches the exception, logs a fatal error with elapsed time, and re‑raises, causing the whole `measure_index` call to fail.  
8. If no errors are encountered, an informational log entry “add indexes complete” is emitted.  
9. The function returns `True` to indicate successful index creation.

**Reference relationships**:  
- **Callers**: `run_subset_by_cohort` invokes `measure_index` when the user requests index creation (`index_create=True`) and measurement tables are part of the subset (`measurement=True` or `pre_split=True`). The caller supplies the connection string, model version, target schema, and the measurement type derived from the table name.  
- **Callees / helpers**:  
  * `_make_index_name` – builds a unique, Oracle‑compatible index identifier.  
  * `combine_dicts` and `get_conn_info_dict` – assemble logging metadata.  
  * `Statement` – encapsulates each `CREATE INDEX` SQL command.  
  * `StatementSet` – aggregates the statements and runs them in parallel via its `parallel_execute` method.  
  * `check_stmt_err` – inspects each statement for execution errors and raises a `DatabaseError` if needed.  
  * `secs_since` – used only in error‑logging paths to report elapsed time.  

**Note**:  
- The constant `IDX_MEASURE_LIKE_TABLE_SQL` must be a format string with four placeholders (`{}`) that accept the index name, schema, measurement type, and column name; otherwise a `IndexError` or malformed SQL will occur.  
- The function always uses a fixed pool size of five workers; if the number of columns is less than five the extra workers will simply idle.  
- Index names are deterministic; the same `(schema, m_type, column)` combination will always produce the same name, which helps idempotent runs.  
- Errors in any single `CREATE INDEX` statement abort the whole operation after logging; partial index creation may remain in the database. callers should be prepared to handle or clean up such partial state.  
- The function returns a boolean `True` on success; it never returns `False`. Any failure is signaled by an exception, not by a return value.

**Output Example**:  
When called with valid arguments and no execution errors, the function completes silently aside from log messages and returns:

```python
result = measure_index(conn_str, "2.3.0", "dcc_subset", "anthro")
print(result)   # True
```
## FunctionDef add_measurement_like_fks(conn_str, model_version, schema)
**add_measurement_like_fks**: The function of add_measurement_like_fks is to create and apply foreign‑key constraints for the “measurement‑like” tables (anthro, labs, vitals) in a target schema.

**parameters**:  
· **conn_str** (str): PostgreSQL connection string used to open a database connection.  
· **model_version**: PEDSnet model version identifier (e.g., “2.3.0”). It is only used for logging.  
· **schema** (str): Name of the target schema that contains the measurement‑like tables to which the foreign keys will be added.

**Code Description**:  
1. **Logging setup** – A logger for the current module is obtained. The helper `combine_dicts` merges a dictionary containing the `model_version` with the connection‑info dictionary returned by `get_conn_info_dict(conn_str)`. An informational log entry records the start of the foreign‑key addition step. The current wall‑clock time is saved in `start_time` for later elapsed‑time reporting.

2. **Statement container** – An empty `StatementSet` instance (`stmts`) is created. This container will hold a `Statement` object for each foreign‑key DDL command that must be executed.

3. **Foreign‑key definition** –  
   * `col_fk` lists the column names that appear in the measurement table and therefore need foreign‑key references in the measurement‑like tables.  
   * `measure_like_tables` is a set containing the three table prefixes: `'anthro'`, `'labs'`, and `'vitals'`.  
   * The nested loops iterate over each measurement‑like table and each column name in `col_fk`. For each combination the code determines the referenced table and column:  
     - If the column name contains “concept_id”, the reference is always `vocabulary.concept(concept_id)`. The base name for the foreign‑key name is derived by stripping the trailing “_concept_id” from the column name.  
     - Otherwise the reference table is built from the column name itself (everything before the last underscore) and the reference column is the column name itself.  
   * A foreign‑key constraint name is assembled as `fk_meas_<base_name>_<measure_like_table>`.  
   * The SQL template `FK_MEASURE_LIKE_TABLE_SQL` (defined elsewhere in the project) is formatted with the target `schema`, the current `measure_like_table`, the generated constraint name, the column name, the reference table, and the reference column.  
   * A `Statement` object is instantiated with the formatted SQL and the default message, then added to `stmts`.

4. **Parallel execution** – `stmts.parallel_execute(conn_str)` launches a pool of worker processes (up to 25) that each execute one of the generated `Statement` objects. The execution is performed in autocommit mode, suitable for DDL statements.

5. **Error handling** – After the parallel run, the function iterates over each `Statement` in `stmts`. For each statement it calls `check_stmt_err(stmt, 'Measurement like table FKs')`. If a statement has an error (`stmt.err` is not `None`), `check_stmt_err` logs a detailed error message (including the SQL, the statement message, and the exception) and raises a `DatabaseError`. The surrounding `try/except` block catches any such exception, logs a fatal error with the offending SQL and error text, logs the elapsed time using `secs_since(start_time)`, and re‑raises the exception to abort the calling workflow.

6. **Success logging** – If all statements complete without error, an informational log entry “foreign keys added” is emitted.

**Reference relationships**:  
* **Caller** – `run_subset_by_cohort` invokes `add_measurement_like_fks` when the `fk_create` flag is true and the `pre_split` flag is set. This occurs after the subset tables have been created and before the generic foreign‑key addition performed by `add_foreign_keys`.  
* **Callees / utilities** –  
  - `Statement` (from `pedsnetdcc.db`) to encapsulate each ALTER TABLE … ADD CONSTRAINT command.  
  - `StatementSet` (from `pedsnetdcc.db`) to batch the statements and run them in parallel.  
  - `combine_dicts` and `get_conn_info_dict` (from `pedsnetdcc.utils`) for constructing enriched log dictionaries.  
  - `check_stmt_err` (from `pedsnetdcc.utils`) to detect and raise errors after execution.  
  - `secs_since` (from `pedsnetdcc.dict_logging`) to compute elapsed seconds for log messages.  
  - The constant `FK_MEASURE_LIKE_TABLE_SQL` (defined elsewhere in the package) provides the DDL template used to create each foreign‑key constraint.

**Note**:  
- The function does not return a value; its success is indicated by the absence of an exception.  
- All foreign‑key statements are generated dynamically; any change to the column list (`col_fk`) or the set of measurement‑like tables must be reflected here.  
- Because the statements are executed in parallel, the order of constraint creation is nondeterministic; however, each constraint is independent, so ordering does not affect correctness.  
- Errors are propagated immediately after detection; the caller (`run_subset_by_cohort`) will abort the entire subset operation if any foreign‑key addition fails.  
- The function assumes that the target schema already contains the measurement‑like tables with the appropriate columns; otherwise the generated ALTER TABLE statements will fail.  
- The maximum number of parallel workers is limited to 25 by `StatementSet.parallel_execute`; for a very large number of constraints this limit prevents excessive process creation.
## FunctionDef add_measurement_not_nulls(conn_str, model_version, schema, measure_like_tables)
**add_measurement_not_nulls**: The function of add_measurement_not_nulls is to apply NOT NULL constraints to a predefined list of columns in all measurement‑like tables of a target schema.

**parameters**: The parameters of this Function.  
· **conn_str** (str): database connection string in libpq format.  
· **model_version**: PEDSnet model version identifier (e.g., “2.3.0”).  
· **schema** (str): name of the target schema that contains the measurement‑like tables.  
· **measure_like_tables** (set[str]): collection of table names that follow the “measurement_…” naming pattern and should receive the NOT NULL constraints.

**Code Description**:  
The function begins by creating a module‑level logger for the current file and by building a logging dictionary (`log_dict`) that merges the supplied `model_version` with connection‑info extracted from `conn_str` via `get_conn_info_dict`. An informational log entry marks the start of the NOT NULL operation and the current time is recorded in `start_time`.

A `StatementSet` instance (`stmts`) is instantiated to hold the individual ALTER TABLE statements that will be executed in parallel. The list of columns to be forced NOT NULL (`col_not_null`) is defined explicitly and matches the columns required by the core `measurement` table:  

- `measurement_concept_id`  
- `measurement_date`  
- `measurement_datetime`  
- `measurement_source_value`  
- `measurement_type_concept_id`  
- `person_id`  
- `value_source_value`

For each table name in `measure_like_tables`, the function extracts the measurement type suffix by stripping the leading “measurement_” prefix (`m_type = measure_like_table[12:]`). It then iterates over each column in `col_not_null`, formats a SQL ALTER TABLE command using the global template `SET_COLUMN_NOT_NULL` (which expects schema, measurement type, and column name), wraps the command in a `Statement` object, and adds the statement to the `StatementSet`.

After all statements are collected, `stmts.parallel_execute(conn_str)` launches a pool of worker processes (up to 25 or the number of statements) that execute the ALTER TABLE commands concurrently. Once execution completes, the function iterates over each `Statement` in the set, invoking `check_stmt_err(stmt, 'Measurement like tables set not null')`. If any statement reports an error (`stmt.err` is not `None`), the error is logged with the SQL text and the elapsed time since `start_time`, and the exception is re‑raised, causing the whole operation to fail. If no errors are encountered, a final informational log entry confirms that the columns have been set NOT NULL.

**Reference Relationships**:  
- **Caller**: `run_subset_by_cohort` in the same module invokes `add_measurement_not_nulls` when the `nonull` flag is false and either `measurement` or `pre_split` is true. This occurs after primary keys have been added and before index creation or foreign‑key addition.  
- **Callees**:  
  * `combine_dicts` – merges dictionaries for logging.  
  * `get_conn_info_dict` – parses the connection string for log enrichment.  
  * `Statement` – encapsulates each ALTER TABLE command.  
  * `StatementSet` – aggregates the statements and provides parallel execution.  
  * `StatementSet.parallel_execute` – runs the statements in separate processes.  
  * `check_stmt_err` – inspects each statement for execution errors and raises a `DatabaseError` if needed.  
  * `secs_since` – formats the elapsed time for log messages.  
  * Global constant `SET_COLUMN_NOT_NULL` – SQL template used to generate the ALTER TABLE statements.

**Note**:  
- The function assumes that every table name in `measure_like_tables` begins with the prefix “measurement_”; otherwise the derived `m_type` will be incorrect.  
- The NOT NULL constraints are applied using plain ALTER TABLE … ALTER COLUMN … SET NOT NULL; if a column already contains NULL values, the statement will fail and the error will be propagated.  
- Parallel execution is limited to a maximum of 25 worker processes; for very large numbers of tables the pool size will be capped.  
- Errors are not suppressed; any failure aborts the entire operation after logging detailed information, ensuring schema integrity.  
- The function does not provide a “force” option; callers must guarantee that the target columns are free of NULLs before invoking this function.
## FunctionDef add_measurement_org_lab_fk(conn_str, model_version, schema)
**add_measurement_org_lab_fk**: The function of add_measurement_org_lab_fk is to add a foreign‑key constraint that links the measurement organism to the measurement_labs table in the specified schema.

**parameters**:  
· conn_str (str): PostgreSQL connection string used to open a database connection.  
· model_version: PEDSnet model version identifier (e.g., “2.3.0”). It is only used for logging context.  
· schema (str): Name of the target schema where the foreign‑key will be created.

**Code Description**:  
1. **Logging setup** – A module‑level logger is obtained with `logging.getLogger(__name__)`.  
2. **Context dictionary** – `combine_dicts` merges a dictionary containing the supplied `model_version` with the result of `get_conn_info_dict(conn_str)`. The combined dictionary (`log_dict`) holds connection metadata (user, host, port, dbname, search_path) and is used to enrich all subsequent log entries.  
3. **Start message** – An informational log entry is emitted indicating that the process of adding the measurement organism foreign key to `measurement_labs` is beginning.  
4. **StatementSet preparation** – A new `StatementSet` instance (`stmts`) is created and immediately cleared (the clear call is redundant because the set is empty at construction). This object could be used to batch multiple statements, but in this function only a single statement is executed.  
5. **SQL statement creation** – The constant `ADD_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE_LABS` (defined elsewhere in the project) contains a template SQL `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY …` command. The template is formatted with the supplied `schema` to produce a complete DDL statement. A `Statement` object (`add_fk_measurement_org`) is instantiated with this SQL string and a human‑readable message describing the operation.  
6. **Execution** – The `execute` method of the `Statement` object is called with `conn_str`. This method opens a new autocommit connection, runs the SQL, captures any error in `add_fk_measurement_org.err`, and closes the connection.  
7. **Error handling** – Immediately after execution, `check_stmt_err` is invoked with the statement and a caller name string. If the statement’s `err` attribute is not `None`, `check_stmt_err` logs the error and raises a `DatabaseError`, causing the surrounding workflow to abort.  
8. **Completion log** – If no exception is raised, an informational log entry records that the foreign‑key addition succeeded.

**Reference relationships**:  
- **Callers** – The function is invoked from `run_subset_by_cohort` (in the same module) when the `fk_create` flag is true and the `pre_split` flag is also true. In that context it is called after measurement tables have been created but before the generic foreign‑key addition routine (`add_foreign_keys`).  
- **Callees / utilities** –  
  * `combine_dicts` (pedsnetdcc/utils.py) – merges dictionaries for logging.  
  * `get_conn_info_dict` (pedsnetdcc/utils.py) – extracts connection details for log enrichment.  
  * `Statement` (pedsnetdcc/db.py) – encapsulates the DDL command and provides execution semantics.  
  * `StatementSet` (pedsnetdcc/db.py) – container for statements; used here only to follow the project’s pattern.  
  * `check_stmt_err` (pedsnetdcc/utils.py) – validates the execution result and raises an exception on failure.  
  * `ADD_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE_LABS` – a module‑level SQL template constant that defines the exact ALTER TABLE command.  

**Note**:  
- The function does not return a value; success is indicated by the absence of an exception.  
- Because the statement is executed with autocommit, the foreign‑key is applied immediately and cannot be rolled back by the caller.  
- Any database‑level error (e.g., missing referenced table, naming conflict, insufficient privileges) will be captured in `add_fk_measurement_org.err` and propagated as a `DatabaseError` by `check_stmt_err`.  
- The temporary `StatementSet` instance is unnecessary for a single statement but maintains consistency with other transformation functions that batch multiple statements.  
- The logging calls rely on the project’s structured‑logging convention; the `log_dict` ensures that every log entry includes model version and connection metadata for easier troubleshooting.
