## FunctionDef run_recover_cohort(conn_str, site, search_path, model_version)
**run_recover_cohort**: The function of run_recover_cohort is to execute the RECOVER cohort SQL query for a given site and log its progress.

**parameters**: The parameters of this Function.  
· **conn_str** (str): database connection string in libpq format.  
· **site** (str): identifier of the site for which the RECOVER cohort derivation is run.  
· **search_path** (str): PostgreSQL schema search path; the first schema in the list is used as the primary schema for the query.  
· **model_version** (str): version of the PEDSnet data model (e.g., “2.3.0”). This argument is currently not used inside the function but is kept for API consistency.

**Code Description**:  
1. **Connection information extraction** – The function calls `get_conn_info_dict(conn_str)` to parse the connection string into a dictionary containing user, host, port, database name, and search_path. This dictionary is later merged into log messages.  

2. **Start‑up logging** – A log message template `'{0} RECOVER Cohort for {1}'` is prepared. Using `combine_dicts`, a dictionary that merges `{'site': site}` with the connection‑info dictionary is built (`log_dict`). The function logs an INFO entry indicating that the RECOVER cohort process is starting for the supplied site, including all connection metadata.  

3. **Timing** – `time.time()` is called and stored in `start_time` to later compute elapsed time for the final log entry.  

4. **Schema resolution** – `primary_schema(search_path)` extracts the first schema name from the supplied `search_path`. The result is stored in `schema` and will be interpolated into the RECOVER cohort SQL template.  

5. **Statement preparation** – An empty `StatementSet` instance (`stmts`) is created, although it is not used further in this function (the set could be a placeholder for future extensions).  

6. **Execution logging** – An INFO log records that the RECOVER cohort query is about to run for the given site.  

7. **SQL statement creation** – `RECOVER_COHORT_SQL.format(schema)` produces the final SQL string by inserting the resolved primary schema into the module‑level SQL template (`RECOVER_COHORT_SQL`). A `Statement` object (`recover_query_stmt`) is instantiated with this SQL and a descriptive message `"running RECOVER cohort query for {site}"`.  

8. **Statement execution** – `recover_query_stmt.execute(conn_str)` opens a new autocommit PostgreSQL connection, runs the SQL, captures row count, fields, and any result data, and stores any exception in `recover_query_stmt.err`.  

9. **Error checking** – `check_stmt_err(recover_query_stmt, 'run RECOVER query for {0}'.format(site))` inspects the `err` attribute of the statement. If an error is present, it logs the failure and raises a `DatabaseError`, causing the function to abort.  

10. **Completion logging** – Upon successful execution, an INFO log records that the RECOVER cohort is complete for the site.  

11. **Final log entry** – The function logs another INFO message indicating that the RECOVER process has finished, merging the same `log_dict` with a message `"finished"` and the elapsed time calculated by `secs_since(start_time)`.  

12. **Return value** – If no exception was raised, the function returns `True` to signal successful completion.

**Note**:  
- The function does not perform any vacuum or cleanup of the output table; the docstring mentions “Vacuum output table” but the implementation only runs the query.  
- `model_version` is accepted but not referenced; callers should provide it for interface compatibility.  
- All logging is performed through the module‑level `logger` and includes connection metadata for easier troubleshooting.  
- Errors from the SQL execution are propagated as `DatabaseError` via `check_stmt_err`; callers must be prepared to handle this exception.  
- The `StatementSet` instance created (`stmts`) is currently unused; it may be a remnant of a previous implementation.  

**Output Example**:  
```python
result = run_recover_cohort(conn_str, site='stlouis', search_path='dcc_pedsnet, public', model_version='2.3.0')
print(result)   # True
```
