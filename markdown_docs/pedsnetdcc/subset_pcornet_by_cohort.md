## FunctionDef run_subset_pcornet_by_cohort(conn_str, model_version, source_schema, target_schema, cohort_table, inc_hash, index_create, fk_create, notable, nopk, limit, owner, force)
**run_subset_pcornet_by_cohort**: The function of run_subset_pcornet_by_cohort is to create a PCORnet‑style subset of tables in a target schema based on a cohort table, optionally adding hashes, primary keys, indexes, and foreign keys, and to grant appropriate permissions.

**parameters**:  
· conn_str (str): libpq connection string used for all database operations.  
· model_version (str): PEDSnet data model version (e.g., “2.3.0”).  
· source_schema (str): Schema that contains the original PCORnet tables.  
· target_schema (str): Schema where the subset tables will be created.  
· cohort_table (str): Name of the table that holds the cohort (list of patids).  
· inc_hash (bool, optional, default False): If True, include rows from the `hash_token` table that match the cohort.  
· index_create (bool, optional, default False): If True, create predefined indexes on the subset tables after they are built.  
· fk_create (bool, optional, default False): If True, create predefined foreign‑key constraints on the subset tables after they are built.  
· notable (bool, optional, default False): If True, skip the creation of all tables (used for dry‑run or testing).  
· nopk (bool, optional, default False): If True, skip adding primary‑key constraints to the subset tables.  
· limit (bool, optional, default False): If True, grant permissions only to the user specified by `owner`; otherwise grant to the default `pcor_et_user`.  
· owner (str, optional, default 'loading_user'): Database role that will receive the granted permissions when `limit` is True.  
· force (bool, optional, default False): If True, benign errors are ignored (passed through to downstream error handling).  

**Code Description**:  
1. **Logging setup** – A logger for the module is obtained. `combine_dicts` merges the model version and connection‑info dictionary (produced by `get_conn_info_dict`) into `log_dict`. An informational log entry marks the start of the subset operation.  

2. **Timing** – `start_time` records the epoch time for later elapsed‑time logging.  

3. **Statement containers** – A `StatementSet` instance (`stmts`) is created to hold DDL statements that will be executed in parallel.  

4. **Table classification** –  
   * `select_patid` – set of tables whose rows must be filtered by `patid` present in the cohort table.  
   * `select_all` – tuple of tables (`provider`, `harvest`) that are copied completely without a join to the cohort.  
   * `special_handling` – set of tables that require additional joins or conditional inclusion (`lab_history`, `private_address_geocode`, `hash_token`).  

5. **Initial table creation (unless `notable`)** – For each table in `select_patid` a CREATE‑TABLE AS SELECT statement is built:  
   * Base SQL: `create table <target_schema>.<table> as select t.* from <source_schema>.<table> t`.  
   * If the table is not in `select_all`, a join to the cohort table (`c.patid = t.patid`) is added.  
   * The statement string is stored in `create_dict` and the table name is added to `grant_vacuum_tables` (list of tables that will later receive permissions and be vacuumed).  
   * All statements are wrapped in `Statement` objects, added to `stmts`, and executed in parallel via `stmts.parallel_execute(conn_str)`.  
   * After execution each statement is inspected with `check_stmt_err`; any fatal error is logged and re‑raised, aborting the function.  

6. **Special‑handling tables** – After the initial pass, the function clears the previous collections and builds statements for the three tables in `special_handling`:  
   * `lab_history` joins to the newly created `lab_result_cm` on `lab_loinc`.  
   * `private_address_geocode` joins to `lds_address_history` on `addressid`.  
   * `hash_token` is included only if `inc_hash` is True; otherwise the statement adds `WHERE FALSE` to create an empty table.  
   * These statements are executed in parallel and error‑checked in the same manner as the initial tables.  

7. **Primary‑key addition** – If `nopk` is False, the function iterates over the constant list `ADD_PRIMARY_KEYS_SQL` (defined elsewhere in the package). For each template, it formats the target schema, creates a `Statement`, adds it to `stmts`, and runs the set with a pool size of 5. Errors are handled with `check_stmt_err`.  

8. **Index creation** – If `index_create` is True, the function similarly processes `ADD_INDEXES_SQL` to add indexes, using a parallel pool of 5 workers and error checking.  

9. **Foreign‑key creation** – If `fk_create` is True, the function processes `ADD_FOREIGN_KEYS_SQL` in the same fashion as indexes.  

10. **Permission granting** –  
    * A new connection string (`new_conn_str`) is built with `conn_str_with_search_path` to set the search_path to the target schema.  
    * For each table in `grant_vacuum_tables` and for each user (either the single `owner` when `limit` is True, or the default `pcor_et_user`), a GRANT statement (`GRANT_TABLE_SQL`) is created, wrapped in a `Statement`, and added to `stmts`.  
    * The statements are executed sequentially (no parallel execution here) by iterating over `stmts` and calling `stmt.execute(conn_str)`. Errors are logged and raised via `check_stmt_err`.  

11. **VACUUM/ANALYZE** – The function calls `vacuum` with the new connection string, the model version, `analyze=True`, and the list of tables in `grant_vacuum_tables`. This re‑analyzes the freshly created tables for optimal planner statistics.  

12. **Final logging and return** – A concluding log entry records the total elapsed time. If the function reaches this point without raising an exception, it returns `True` to indicate success.  

**Reference relationships**:  
* **Callers** – The function is invoked by `pedsnetdcc/main.py/subset_pcornet_by_cohort`, which prepares the connection string and forwards command‑line arguments.  
* **Callees** –  
  * `Statement` and `StatementSet` (from `pedsnetdcc/db.py`) for building and parallel execution of SQL commands.  
  * `check_stmt_err` (from `pedsnetdcc/utils.py`) for error detection after each execution batch.  
  * `combine_dicts`, `get_conn_info_dict`, `secs_since` (from `pedsnetdcc/utils.py`) for logging context and elapsed‑time calculation.  
  * `conn_str_with_search_path` (from `pedsnetdcc/utils.py`) to adjust the search_path for permission statements.  
  * `vacuum` (from `pedsnetdcc/utils.py`) to perform VACUUM ANALYZE on the created tables.  
  * Constants `ADD_PRIMARY_KEYS_SQL`, `ADD_INDEXES_SQL`, `ADD_FOREIGN_KEYS_SQL`, and `GRANT_TABLE_SQL` (defined elsewhere in the package) provide the DDL templates used for PK, index, FK, and permission statements.  

**Note**:  
* The function assumes that the source schema contains all PCORnet tables listed in `select_patid` and `special_handling`. Missing tables will cause the CREATE statement to fail and be reported by `check_stmt_err`.  
* When `notable` is True, no tables are created; the function will still attempt to add PKs, indexes, FKs, and permissions if the corresponding flags are set, which may result in errors because the target tables do not exist.  
* `limit` controls which database role receives GRANT privileges; setting it to True restricts permissions to a single `owner`, otherwise the default `pcor_et_user` receives them.  
* The parallel execution pool size defaults to the number of statements (capped at 25) for initial table creation, but a fixed pool size of 5 is used for PK, index, and FK creation to limit concurrent DDL load.  
* Errors from any stage cause an immediate exception; the function does not attempt partial rollback.  

**Output Example**:  
When called with valid arguments and all steps succeed, the function returns the boolean value `True`. If an error occurs (e.g., a CREATE statement fails), a `DatabaseError` is raised and the function does not return.  

```python
success = run_subset_pcornet_by_cohort(
    conn_str="host=... dbname=... user=... password=...",
    model_version="2.3.0",
    source_schema="pcornet",
    target_schema="pcornet_subset",
    cohort_table="my_cohort",
    inc_hash=True,
    index_create=True,
    fk_create=False,
    notable=False,
    nopk=False,
    limit=False,
    owner="loader",
    force=False
)
# success == True
```
