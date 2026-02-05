## FunctionDef check_fact_relationship(conn_str, output, pool_size)
**check_fact_relationship**: The function of check_fact_relationship is to verify the referential integrity of the fact_relationship table and return a boolean indicating whether the table is valid.

**parameters**:  
· **conn_str** (str): the database connection string used for all SQL executions.  
· **output** (str, optional): controls which checks are performed. Accepted values are `'percent'`, `'samples'`, or `'both'`. The default is `'both'`.  
· **pool_size** (int or None, optional): the number of parallel worker processes to use when executing the SQL statements. If None, a default of 5 workers is applied.

**Code Description**:  
1. **Initialization** – The function logs the start of the check and records the start time. A `StatementSet` instance (`stmts`) is created to hold all SQL statements that will be executed. If `pool_size` is not supplied it is set to 5.  

2. **Statement construction** –  
   * When `output` includes `'percent'` or `'both'`, the function adds a series of `Statement` objects that count total rows (`tot_*_sql`) and bad rows (`bad_*_sql`) for each of the eight fact domains: observation, measurement, visit, drug, device, condition, person, and procedure.  
   * When `output` includes `'samples'` or `'both'`, the function adds additional `Statement` objects that retrieve a single example of a bad record for each domain and each fact‑id field (`fact_id_1` and `fact_id_2`).  

   Each `Statement` is created with the appropriate SQL string (e.g., `tot_obs_sql`) and a human‑readable message (e.g., `tot_obs_msg`). The messages are later used to route the results into the correct place in the result data structure.

3. **Parallel execution** – `stmts.parallel_execute(conn_str, pool_size)` runs all statements concurrently using the `StatementSet.parallel_execute` method. This method spawns a pool of worker processes, each executing the `Statement.execute` method, and collects the modified `Statement` objects (with `data`, `fields`, `err`, etc.) back into the set.

4. **Result aggregation** – A dictionary named `results` is pre‑populated with a sub‑dictionary for each domain (`obs`, `meas`, `visit`, `drug`, `dev`, `cond`, `person`, `proc`). Each sub‑dictionary holds placeholders for `total`, `bad`, `percent`, a list of `samples`, and a `valid` flag (initially `True`).  

   The function iterates over every `Statement` in `stmts`. For each statement it:
   * Calls `check_stmt_err(stmt, 'fact relationship check')` to raise a `DatabaseError` if the statement encountered an execution error.
   * For statements that are expected to return a scalar count (`tot_*_msg` and `bad_*_msg`), it calls `check_stmt_data` to ensure data is present, then stores the count (`stmt.data[0][0]`) in the appropriate field of `results`.
   * For sample‑retrieval statements (`bad_*_1_msg` and `bad_*_2_msg`), it checks whether any rows were returned. If a row exists, it builds a dictionary mapping column names (`stmt.fields`) to values (`stmt.data[0]`) and appends a record containing the field name (`fact_id_1` or `fact_id_2`) and the row data to `results[domain]['samples']`.

5. **Derived calculations** – After all statements have been processed, the function computes the percentage of bad rows for each domain (`percent = bad * 100 // total`) when both `bad` and `total` are available. It also sets `valid` to `False` for any domain that has a non‑zero bad count or at least one sample record.

6. **Logging of domain results** – For each domain the function emits either an `info` log entry (when `valid` is `True`) or a `warn` log entry (when `valid` is `False`). The warning log combines a generic message with the full domain result dictionary using `combine_dicts`.

7. **Overall validity** – The overall table validity (`valid`) is the logical AND of the `valid` flags of all eight domains. A final `info` log entry records the overall result, the boolean `valid`, and the elapsed time calculated by `secs_since(starttime)`.

8. **Return value** – The function returns the overall boolean `valid`. `True` indicates that every fact_relationship reference is valid; `False` indicates at least one domain contains invalid references.

**Reference relationships**:  
* **Callers** – `check_fact_relationship` is invoked by `pedsnetdcc/main.py` in the `post_load` workflow, which runs it after data loading. It is also exercised by the unit test `tests/check_fact_relationship_test.py/CheckFactRelationshipTest.test_fact_relationship_results`.  
* **Callees** – The function relies on the following utilities and classes:  
  * `Statement` and `StatementSet` (from `pedsnetdcc/db.py`) for building and executing SQL in parallel.  
  * `check_stmt_err` and `check_stmt_data` (from `pedsnetdcc/utils.py`) for error and data validation.  
  * `combine_dicts` (from `pedsnetdcc/utils.py`) for constructing log dictionaries.  
  * `secs_since` (from `pedsnetdcc/dict_logging.py`) for elapsed‑time formatting.  
  * The module‑level `logger` for structured logging.  

**Note**:  
* The function assumes that all SQL strings (`*_sql`) and their corresponding message strings (`*_msg`) are defined elsewhere in the module. Missing definitions will raise a `NameError`.  
* The default `pool_size` of 5 may be overridden; however, the underlying `StatementSet.parallel_execute` caps the worker pool at 25 processes.  
* If any statement fails (i.e., `stmt.err` is set), `check_stmt_err` raises a `DatabaseError` and the function aborts immediately; no partial results are returned.  
* The percentage calculation uses integer division, truncating any fractional part.  
* The `samples` list contains dictionaries with keys `'field'` and `'row'`; each `'row'` is itself a dictionary mapping column names to values.  
* The function logs both informational and warning messages; downstream callers (e.g., the test suite) may inspect the logger’s message handler to verify expected output.

**Output Example**:  
```python
# Example call
valid = check_fact_relationship(conn_str, output='both', pool_size=4)

# Possible return values
True   # all domains have zero bad references
False  # at least one domain contains invalid fact_relationship rows
```
