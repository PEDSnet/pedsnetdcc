## FunctionDef create_index_replacement_tables(conn_str, model_version)
**create_index_replacement_tables**: The function of create_index_replacement_tables is to generate auxiliary “index replacement” tables for selected concept‑group columns and then add primary‑key constraints to those tables.

**parameters**:  
· **conn_str** (str): a libpq‑style PostgreSQL connection string that will be used for all database operations.  
· **model_version** (str): the PEDSnet model version identifier (e.g., “2.3.0”) that is included in log messages for traceability.

**Code Description**:  
1. **Logging preparation** – The function obtains a logger for its module and builds a base log dictionary (`log_dict`) that merges the supplied `model_version` with connection‑information extracted by `get_conn_info_dict(conn_str)`. It logs a “starting index replacement tables” message and records the start time (`start_time = time.time()`).  

2. **SQL templates** – Two template strings are defined:  
   * `create_concept_group_sql` – creates a new table by aggregating distinct values of a column together with the corresponding concept‑id column and a count, grouped by both columns.  
   * `pk_concept_group_sql` – adds a composite primary key on the same two columns.  

3. **Mapping of source tables to columns** – `create_by_table` maps each base fact table to the pair of columns that require replacement tables:  
   * `condition_occurrence` → (`condition_source_value`, `condition_concept_name`)  
   * `drug_exposure` → (`drug_source_value`, `drug_concept_name`)  
   * `measurement` → (`measurement_source_value`, `measurement_concept_name`)  
   * `procedure_occurrence` → (`procedure_concept_name`, `procedure_source_value`)  

4. **Statement construction** – Two `StatementSet` instances are created: `table_stmts` for the CREATE‑TABLE statements and `pk_stmts` for the ALTER‑TABLE primary‑key statements.  
   For each entry in `create_by_table`, the function iterates over the two columns:  
   * Derives a short column name by stripping the leading prefix up to the first underscore (e.g., “source_value” from “condition_source_value”).  
   * Constructs the new table name as `<base_table>_<short_name>` (e.g., `condition_occurrence_source_value`).  
   * Determines the associated concept‑id column: if the short name ends with “name”, the id column is the first two parts of the original column plus “_id” (e.g., `condition_concept_id`); otherwise it is the first part plus “_concept_id” (e.g., `condition_source_concept_id`).  
   * Instantiates a `Statement` with the formatted `create_concept_group_sql` and adds it to `table_stmts`.  
   * For every new table except `drug_exposure_source_value`, a primary‑key `Statement` is created with `pk_concept_group_sql` and added to `pk_stmts`. The explicit exclusion avoids adding a PK to the `drug_exposure_source_value` table, which is handled elsewhere in the code base.  

5. **Parallel execution of CREATE statements** – The function logs “begin creating index replacement tables” and calls `table_stmts.parallel_execute(conn_str)`. This spawns a pool of worker processes (up to 25) that each execute one `Statement`.  

6. **Error handling for CREATE statements** – After execution, each `Statement` in `table_stmts` is examined with `check_stmt_err`. If any statement has a non‑null `err` attribute, the error is logged (including the offending SQL) and a `DatabaseError` is raised, aborting the function.  

7. **Parallel execution of PRIMARY‑KEY statements** – Upon successful table creation, the function logs “begin adding primary keys to index replacement tables” and runs `pk_stmts.parallel_execute(conn_str)` in the same parallel fashion.  

8. **Error handling for PK statements** – Each `Statement` in `pk_stmts` is similarly checked with `check_stmt_err`. Any failure is logged and re‑raised as a `DatabaseError`.  

9. **Completion logging** – After both phases succeed, the function logs “finished adding primary keys to index replacement tables” and then a final message “finished index replacement tables” that includes the elapsed time calculated by `secs_since(start_time)`.  

10. **Return value** – If no exception is raised, the function returns `True` to indicate successful creation of all replacement tables and primary keys.

**Reference relationships**:  
* **Callers** – The function is invoked by higher‑level workflow functions that orchestrate a full data merge or subset creation:  
  * `merge_site_data` and `merge_data_to_schema` call it when the `norep` flag is not set, ensuring that index replacement tables are built after the main DCC tables are populated.  
  * `run_subset_by_cohort` calls it when the `concept_create` flag is true, creating the same replacement tables in a subset schema.  
  * `run_index_replace` is a thin wrapper that simply forwards its arguments to this function.  
* **Callees** – Within its body the function uses:  
  * `Statement` and `StatementSet` (from `pedsnetdcc.db`) to encapsulate and batch SQL commands.  
  * `combine_dicts` and `get_conn_info_dict` (from `pedsnetdcc.utils`) for structured logging.  
  * `check_stmt_err` (also from `pedsnetdcc.utils`) to detect and raise database errors.  
  * `secs_since` (from `pedsnetdcc.dict_logging`) to format elapsed‑time strings for log entries.  

**Note**:  
* The function assumes that the target database user has sufficient privileges to create tables and alter them.  
* Primary‑key creation is deliberately skipped for the `drug_exposure_source_value` table; this exception is hard‑coded and must be kept in sync with any schema changes that affect that table.  
* All SQL statements are executed with autocommit (isolation level 0), so they are safe for DDL operations but cannot be rolled back as a single transaction.  
* Errors from individual statements are not aggregated; the first encountered error aborts the process after being logged.  
* The parallel execution pool size is automatically limited to a maximum of 25 workers to avoid resource exhaustion.  

**Output Example**:  
```python
result = create_index_replacement_tables(conn_str, "2.3.0")
print(result)   # -> True
```
