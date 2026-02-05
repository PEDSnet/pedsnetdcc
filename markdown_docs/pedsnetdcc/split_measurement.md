## FunctionDef _make_index_name(table_name, column_name)
**_make_index_name**: The function of _make_index_name is to generate a deterministic, length‑limited index name for a given table and column combination, suitable for Oracle’s identifier length restrictions.  

**parameters**:  
· table_name (str): The name of the table for which the index is being created.  
· column_name (str): The name of the column on which the index will be built.  

**Code Description**:  
The function constructs an index identifier that conforms to the constant `NAME_LIMIT`, which represents the maximum identifier length allowed by Oracle databases.  
1. **Table abbreviation** – It prefixes the table abbreviation with `"mea_"` and appends the first three characters of `table_name`. For example, if `table_name` is `"anthro"`, the table abbreviation becomes `"mea_ant"`.  
2. **Column abbreviation** – It creates a short column token by taking the first character of each underscore‑separated segment of `column_name`. For `"measurement_age_in_months"` the abbreviation becomes `"mai"`.  
3. **Hash component** – To guarantee uniqueness while keeping the name short, the function computes an MD5 hash of the string `"table_name.column_name"` (e.g., `"anthro.measurement_age_in_months"`). The hash is expressed as a hexadecimal string.  
4. **Length calculation** – The variable `hashlen` is derived by subtracting the lengths of the table abbreviation, column abbreviation, three separator underscores, and the suffix `"ix"` from `NAME_LIMIT`. This determines how many characters of the MD5 hash can be safely included without exceeding the limit.  
5. **Final assembly** – The parts are joined with underscores:  
   `table_abbrev + "_" + column_abbrev + "_" + md5[:hashlen] + "_ix"`  
   The result is a deterministic, collision‑resistant index name such as `"mea_ant_mai_ae1fd5b22b92397ca9_ix"`.  

**Relationship with Callers**:  
`_make_index_name` is invoked inside `split_measurement_table` (found in `pedsnetdcc/split_measurement.py`). When the split measurement tables (`measurement_anthro`, `measurement_labs`, `measurement_vitals`) are being indexed, the caller iterates over a predefined list of column names and calls `_make_index_name(measure_like_table, col)` to obtain a unique index identifier for each `(table, column)` pair. The generated name is then interpolated into the SQL statement template `IDX_MEASURE_LIKE_TABLE_SQL` and executed against the database. By centralising the naming logic, the caller ensures consistent index names across all split tables and avoids name collisions that could arise from similarly named columns in different tables (e.g., `provider.gender_source_concept_name` vs. `person.gender_source_concept_name`).  

**Note**:  
- The function relies on the global constant `NAME_LIMIT`; if this constant is changed, the hash truncation length will adjust automatically.  
- The MD5 hash guarantees that even tables with identical first‑three‑letter prefixes and column abbreviations will receive distinct index names, provided the full `table_name.column_name` strings differ.  
- The returned name always ends with the suffix `"_ix"` to clearly indicate its purpose as an index identifier.  

**Output Example**:  
Calling `_make_index_name("anthro", "measurement_age_in_months")` might return:  

`mea_ant_mai_ae1fd5b22b92397ca9_ix`   (exact hash portion depends on the MD5 of `"anthro.measurement_age_in_months"`).
## FunctionDef split_measurement_table(conn_str, truncate, view, model_version, search_path, limit, owner, skip_split, skip_index, skip_fk, skip_not_null)
**split_measurement_table**: The function of split_measurement_table is to split the core *measurement* table into three specialized tables (anthro, labs, vitals), apply constraints, indexes, permissions, optionally truncate the original table, optionally create a unified view, and finally vacuum the new tables.

**parameters**:  
· **conn_str** (str): PostgreSQL connection string used for all database operations.  
· **truncate** (bool): If True, the original *measurement* table is truncated after the split tables are created.  
· **view** (bool): If True, a view named *measurements* that unions the three split tables is created.  
· **model_version**: PEDSnet data model version (e.g., “2.3.0”). Used for logging and for the final vacuum call.  
· **search_path** (str): PostgreSQL schema search path; the first schema is used as the primary schema for table creation.  
· **limit** (bool, default False): If True, permissions are granted only to the role specified by *owner*; otherwise permissions are granted to the default PEDSnet staff roles.  
· **owner** (str, default 'loading_user'): Role that receives permissions when *limit* is True.  
· **skip_split** (bool, default False): If True, the function skips the creation of the split tables and only performs subsequent steps (indexes, foreign keys, permissions, etc.).  
· **skip_index** (bool, default False): If True, index creation on the split tables is omitted.  
· **skip_fk** (bool, default False): If True, foreign‑key constraints on the split tables are omitted.  
· **skip_not_null** (bool, default False): If True, NOT‑NULL constraints on the split tables are omitted.

**Code Description**:  
1. **Logging start** – Combines the model version and connection‑info (extracted by `get_conn_info_dict`) into a dictionary and logs the start of the split operation. The start time is recorded for elapsed‑time reporting.  
2. **Determine primary schema** – Calls `primary_schema(search_path)` to obtain the first schema name; this schema is used as the target for all DDL statements.  
3. **Define split logic** –  
   * `measure_like_tables` maps each new table name to the SQL “IN” or “NOT IN” clause that determines which concept IDs belong to the table.  
   * `concept_id` holds the concrete concept‑ID tuples for anthro, labs, and vitals. These IDs are interpolated into the CREATE‑TABLE‑LIKE statements.  
4. **Create split tables** (executed only when *skip_split* is False):  
   * For each target table, builds a `Statement` with `CREATE_MEASURE_LIKE_TABLE_SQL` (template not shown) that creates a table like *measurement* filtered by the appropriate concept IDs.  
   * Adds each statement to a `StatementSet` and runs them in parallel (`parallel_execute` with a pool of 5 workers).  
   * After execution, iterates over the statements and calls `check_stmt_err` to raise a `DatabaseError` if any statement failed.  
5. **Set primary keys** (still under *skip_split* False):  
   * Generates a `Statement` for each table using `PK_MEASURE_LIKE_TABLE_SQL` and executes them in parallel, checking for errors as above.  
6. **Add indexes** (unless *skip_index* is True):  
   * Clears the `StatementSet`, defines a list of columns (`col_index`) that require indexes, and for each table/column pair creates an index name via `_make_index_name`.  
   * Constructs a `Statement` with `IDX_MEASURE_LIKE_TABLE_SQL` for each index, adds to the set, and runs them in parallel. Errors are checked with `check_stmt_err`.  
7. **Add foreign keys** (unless *skip_fk* is True):  
   * Clears the set, defines a list of columns (`col_fk`) that need foreign‑key constraints, determines the referenced table and column for each (concept‑id columns reference `vocabulary.concept`, others reference tables derived from the column name).  
   * Generates a deterministic foreign‑key name (`fk_meas_<base>_<table>`) and creates a `Statement` with `FK_MEASURE_LIKE_TABLE_SQL`. Execution and error handling follow the same parallel pattern.  
8. **Set NOT NULL constraints** (unless *skip_not_null* is True):  
   * Clears the set, iterates over a list of columns (`col_not_null`) and creates a `Statement` using `SET_COLUMN_NOT_NULL` for each table/column pair. Executes in parallel and checks for errors.  
9. **Adjust measurement organism foreign key** (only when *skip_split* is False):  
   * Drops the existing foreign key from *measurement* to *measurement_organism* (`DROP_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE`).  
   * Adds a new foreign key from *measurement_labs* to *measurement_organism* (`ADD_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE_LABS`). Both statements are executed sequentially with explicit error checking.  
10. **Grant permissions** (only when *skip_split* is False):  
    * Determines the list of users: either the single *owner* (when *limit* True) or the default roles `('peds_staff', 'dcc_analytics')`.  
    * For each split table and each user, creates a `Statement` with `GRANT_MEASURE_LIKE_TABLE_SQL` and executes them sequentially, checking for errors.  
11. **Truncate original measurement table** (if *truncate* True):  
    * Executes a single `Statement` with `TRUNCATE_MEASUREMENT_SQL` to remove all rows from *measurement*. Errors are checked.  
12. **Create measurements view** (if *view* True):  
    * Builds a `Statement` containing a `CREATE VIEW measurements AS SELECT * FROM measurement_anthro UNION ALL SELECT * FROM measurement_labs UNION ALL SELECT * FROM measurement_vitals`.  
    * Executes the statement and checks for errors.  
13. **Vacuum** – Calls the project‑wide `vacuum` utility with `analyze=True` on the three new tables (`measurement_anthro`, `measurement_labs`, `measurement_vitals`). This refreshes planner statistics and reclaims storage.  
14. **Logging end** – Logs completion with elapsed time calculated by `secs_since(start_time)`.  
15. **Return** – Returns `True` to indicate successful completion; any failure raises an exception before this point.

**Reference relationships**:  
*Callers* –  
- `pedsnetdcc/main.py/split_measurement` invokes `split_measurement_table` after constructing a connection string.  
- `pedsnetdcc/main.py/subset_by_cohort` may call `split_measurement_table` on a newly created target schema when the user requests a split after a subset operation.  

*Callees* –  
- `Statement` and `StatementSet` from `pedsnetdcc/db.py` are used to encapsulate and parallel‑execute all DDL statements.  
- Utility functions `combine_dicts`, `get_conn_info_dict`, `primary_schema`, `secs_since`, `check_stmt_err`, and `vacuum` provide logging, connection parsing, schema extraction, timing, error handling, and post‑operation maintenance.  
- Helper `_make_index_name` generates deterministic index identifiers for the index‑creation step.  
- SQL template constants (`CREATE_MEASURE_LIKE_TABLE_SQL`, `PK_MEASURE_LIKE_TABLE_SQL`, `IDX_MEASURE_LIKE_TABLE_SQL`, `FK_MEASURE_LIKE_TABLE_SQL`, `SET_COLUMN_NOT_NULL`, `DROP_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE`, `ADD_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE_LABS`, `GRANT_MEASURE_LIKE_TABLE_SQL`, `TRUNCATE_MEASUREMENT_SQL`) are interpolated into the `Statement` objects; these constants are defined elsewhere in the module.  

**Note**:  
- The function relies on the existence of the core *measurement* table and the concept‑ID lists; if those IDs change between model versions, the SQL templates must be updated accordingly.  
- Parallel execution uses a fixed pool size of 5 workers for table creation, primary‑key, index, foreign‑key, and NOT‑NULL steps; this number can be adjusted by modifying the `parallel_execute` call.  
- All DDL statements are executed in autocommit mode (via `Statement.execute`), which is required for commands such as `VACUUM` and `CREATE INDEX`.  
- Errors are not silently ignored; any failure in a statement triggers `check_stmt_err`, which logs the error and raises a `DatabaseError`, aborting the function.  
- Permission granting respects the *limit* flag; when False, the default PEDSnet roles receive access, otherwise only the specified *owner* role is granted.  
- The function does not return any data; the boolean return value is merely an indicator that the process reached the end without raising an exception.  

**Output Example**:  
```python
success = split_measurement_table(
    conn_str="host=pg host=5432 dbname=dds user=loader",
    truncate=True,
    view=True,
    model_version="2.3.0",
    search_path="dcc_pedsnet, vocabulary",
    limit=False,
    owner="loading_user",
    skip_split=False,
    skip_index=False,
    skip_fk=False,
    skip_not_null=False
)
print(success)   # -> True (if all steps completed without error)
```
