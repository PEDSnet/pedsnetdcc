## FunctionDef _add_primary_key(conn_str, schema)
**_add_primary_key**: The function of _add_primary_key is to add a primary‑key constraint on the column measurement_id of the table updated_measurement in the specified schema.

**parameters**:  
· conn_str: a PostgreSQL connection string used to open a database connection.  
· schema: the name of the schema that contains the updated_measurement table; it is interpolated into the ALTER TABLE statement.

**Code Description**:  
1. The function defines the DDL command that will be executed.  
   * pk_sql holds the template `"alter table {0}.updated_measurement add primary key (measurement_id)"`.  
   * pk_msg is a human‑readable description `"making measurement_id the primary key"`.  
2. A log entry with the message `'begin add primary key'` is written using the module‑level logger.  
3. The SQL string is formatted with the supplied schema value, producing a concrete statement such as `alter table my_schema.updated_measurement add primary key (measurement_id)`.  
4. A Statement object is instantiated with the formatted SQL and the descriptive message pk_msg. The Statement class encapsulates the SQL, tracks execution metadata, and provides the execute method.  
5. The statement’s execute method is called with conn_str. Internally, Statement.execute opens a new autocommit connection, runs the SQL, captures any database‑level exception in stmt.err, and closes the connection.  
6. Immediately after execution, check_stmt_err is invoked with the statement object and a context string `'make measurement_id the primary key'`.  
   * If the DDL failed, check_stmt_err logs the error and raises a DatabaseError, causing the calling function to abort.  
   * If no error occurred, execution proceeds.  
7. A second log entry with the message `'primary key created'` records successful completion.  
8. The function returns the boolean value True to signal that the operation finished without raising an exception.

**Reference Relationships**:  
*Caller*: `run_post_lab_loinc` in the same module invokes _add_primary_key as the first step of its post‑lab‑LOINC workflow. The caller relies on the returned True only to confirm that the function completed; any failure is propagated as an exception from check_stmt_err.  
*Callees*: `Statement` (the constructor and its execute method) and `check_stmt_err` (the error‑checking utility). Both are part of the project’s standard pattern for executing SQL statements and handling errors uniformly.

**Note**:  
- The function does not perform any explicit transaction management; the underlying Statement.execute uses autocommit, so the primary‑key addition is applied immediately.  
- If the target table already has a primary key or the column measurement_id does not exist, the ALTER TABLE will fail, check_stmt_err will raise a DatabaseError, and the calling workflow will be aborted.  
- The function always returns True when it reaches the end; it never returns False. The boolean return value is therefore only an indicator that no exception was raised.  
- Logging is performed at INFO level before and after the DDL execution; any error details are logged by check_stmt_err at ERROR level.

**Output Example**:  
```python
result = _add_primary_key(conn_str, 'dcc_pedsnet')
print(result)   # -> True
```
## FunctionDef _rename_table(conn_str, schema, current_name, new_name)
**_rename_table**: The function of _rename_table is to rename an existing table within a specified schema in a PostgreSQL database.

**parameters**:  
· conn_str: a libpq‑style connection string that identifies the target database.  
· schema: the name of the schema that contains the table to be renamed.  
· current_name: the current name of the table that will be renamed.  
· new_name: the desired new name for the table.

**Code Description**:  
1. **SQL preparation** – Two format strings are defined:  
   * `rename_sql` = `"alter table {0}.{1} rename to {2}"` – the actual DDL command that will be sent to PostgreSQL.  
   * `rename_msg` = `"renaming table {0} to {1}"` – a human‑readable description used for logging.  
   The placeholders are later filled with `schema`, `current_name`, and `new_name`.

2. **Logging start of operation** – A log entry with the message *begin rename table* is written at INFO level using the module‑level `logger`. The log entry is a dictionary so that downstream log processors can extract structured data.

3. **Statement creation** – An instance of `pedsnetdcc.db.Statement` is created:  
   ```python
   rename_stmt = Statement(
       rename_sql.format(schema, current_name, new_name),
       rename_msg.format(current_name, new_name)
   )
   ```  
   The first argument is the fully‑rendered SQL command; the second argument is the descriptive message.

4. **Execution** – `rename_stmt.execute(conn_str)` opens a new PostgreSQL connection (autocommit mode), runs the ALTER TABLE command, and stores the result in the `Statement` object. No result set is expected, so `rename_stmt.data` remains `None`; any database‑level exception is captured in `rename_stmt.err`.

5. **Error checking** – Immediately after execution, `check_stmt_err(rename_stmt, 'rename table {0} to {1}'.format(current_name, new_name))` is called. This helper inspects `rename_stmt.err`; if an error is present it logs the failure and raises a `DatabaseError`. If no error is present the function returns silently.

6. **Logging success** – Upon successful completion, another INFO‑level log entry with the message *table renamed* is emitted.

7. **Return value** – The function returns the boolean literal `True` to indicate that the rename operation finished without raising an exception.

**Reference relationships**:  
*Caller*: The only direct caller in the current code base is `run_post_lab_loinc` in `pedsnetdcc/lab_loinc.py`. Within that workflow the function is invoked twice: first to rename the original `measurement` table to `measurement_orig`, and second to rename `updated_measurement` to `measurement`. The surrounding function logs start/end messages, measures elapsed time, and relies on the boolean return value only to confirm successful execution (any failure would be raised as an exception before reaching the return).  

*Callee*:  
- `Statement` (from `pedsnetdcc/db.py`) – provides the encapsulated execution of the ALTER TABLE command.  
- `check_stmt_err` (from `pedsnetdcc/utils.py`) – validates that the statement completed without error and raises a `DatabaseError` if needed.  
- The module‑level `logger` – records progress and outcomes.

**Note**:  
- The function does not perform any quoting or escaping of identifiers; callers must ensure that `schema`, `current_name`, and `new_name` are safe PostgreSQL identifiers.  
- Because the ALTER TABLE is executed in autocommit mode, the rename is permanent as soon as the command succeeds; there is no transaction rollback within this function.  
- Any exception raised by `Statement.execute` (e.g., connection failure) or by `check_stmt_err` will propagate out of `_rename_table`, causing the calling workflow to abort.  
- The function always returns `True` when it reaches the end; the return value is not used to convey detailed status.

**Output Example**:  
```python
result = _rename_table(conn_str, 'dcc_pedsnet', 'measurement', 'measurement_orig')
print(result)   # -> True
```
## FunctionDef _make_index_name(table_name, column_name)
**_make_index_name**: The function of _make_index_name is to generate a deterministic, length‑constrained index name for a given table and column pair.

**parameters**:  
· table_name (str): The name of the database table for which the index will be created.  
· column_name (str): The name of the column within the table that the index will cover.

**Code Description**:  
The function builds an index identifier that complies with the Oracle identifier length restriction (defined elsewhere as `NAME_LIMIT`). The steps are:

1. **Table abbreviation** – Prefixes the first three characters of `table_name` with the literal `"mea_"`.  
   ```python
   table_abbrev = "mea_" + table_name[:3]
   ```
   This yields a short, predictable prefix (e.g., for `table_name='upd'` the result is `'mea_upd'`).

2. **Column abbreviation** – Splits `column_name` on underscores and concatenates the first character of each segment.  
   ```python
   column_abbrev = ''.join([x[0] for x in column_name.split('_')])
   ```
   For `column_name='measurement_age_in_months'` the abbreviation becomes `'mai'`.

3. **Hash component** – Computes an MD5 hash of the fully qualified name `"{table_name}.{column_name}"`, encodes it as UTF‑8, and obtains the hexadecimal digest.  
   ```python
   md5 = hashlib.md5(
       '{}.{}'.format(table_name, column_name).encode('utf-8')).hexdigest()
   ```

4. **Length calculation** – Determines how many characters of the hash can be used while keeping the final index name within `NAME_LIMIT`. The calculation subtracts the lengths of the table abbreviation, column abbreviation, three underscore characters, and the literal `'ix'` from the limit.  
   ```python
   hashlen = NAME_LIMIT - (len(table_abbrev) + len(column_abbrev) +
                           3 * len('_') + len('ix'))
   ```

5. **Final assembly** – Joins the table abbreviation, column abbreviation, the truncated hash, and the suffix `'ix'` with underscores.  
   ```python
   return '_'.join([table_abbrev, column_abbrev, md5[:hashlen], 'ix'])
   ```

The resulting string is deterministic (same inputs always give the same output) and highly unlikely to collide with other index names, even when tables have similarly named columns (e.g., `provider.gender_source_concept_name` vs. `person.gender_source_concept_name`).  

**Relationship with Callers**:  
`_make_index_name` is invoked by `run_post_lab_loinc` in `lab_loinc.py`. Within that routine, a list of measurement‑related columns is iterated, and for each column the function is called as `_make_index_name('upd', col)`. The generated index name is then embedded in a SQL statement that creates an index on the `measurement` table (which has been renamed to `upd` earlier in the process). Thus, `_make_index_name` supplies the unique, Oracle‑compatible identifier required for each index creation statement.

**Note**:  
- The function assumes that `NAME_LIMIT` is defined globally and reflects the maximum identifier length allowed by the target database (30 characters for Oracle).  
- Because the hash portion is truncated to fit the limit, the function must be used consistently; changing `NAME_LIMIT` or the abbreviation logic will alter the generated names.  
- The function does not validate that `table_name` or `column_name` are non‑empty strings; callers should ensure valid inputs.  
- The returned name is not intended to be human‑readable; it is optimized for uniqueness and length compliance.

**Output Example**:  
```python
>>> _make_index_name('upd', 'measurement_age_in_months')
'mea_upd_mai_ae1fd5b22b92397ca9_ix'
```  
(Exact hash segment will vary depending on the `NAME_LIMIT` value, but the format will always be `mea_<tbl>_<col>_<hash>_ix`.)
## FunctionDef run_post_lab_loinc(conn_str, site, search_path)
**run_post_lab_loinc**: The function of run_post_lab_loinc is to perform the post‑processing steps required after the Lab LOINC R script has populated the `updated_measurement` table, including adding a primary key, renaming tables, adjusting foreign‑key constraints, and creating indexes on the final `measurement` table.

**parameters**:  
· **conn_str** (str): PostgreSQL connection string used to open a database connection for all SQL statements.  
· **site** (str): Identifier of the site for which the derivation is being run; included in log messages.  
· **search_path** (str): PostgreSQL schema search path; the first schema in this list is used as the target schema for all DDL operations.

**Code Description**:  
1. **Connection information extraction** – `get_conn_info_dict(conn_str)` parses the connection string into a dictionary (`conn_info_dict`) that is later merged into log entries.  
2. **Logging start** – A structured log entry is emitted indicating the start of the post‑lab‑LOINC process, and the current time is recorded in `start_time`.  
3. **Schema determination** – `primary_schema(search_path)` returns the first schema name from the supplied search path; this schema is stored in `schema` and used for all subsequent DDL statements.  
4. **Statement collection** – A `StatementSet` instance (`stmts`) is created to hold the individual SQL commands that will be executed, either sequentially or in parallel.  

5. **Add primary key** – `_add_primary_key(conn_str, schema)` builds and executes an `ALTER TABLE … ADD PRIMARY KEY` statement on `updated_measurement.measurement_id`. The helper logs its own start/end messages and raises a `DatabaseError` via `check_stmt_err` if the operation fails.  

6. **Rename tables** –  
   - `_rename_table(conn_str, schema, 'measurement', 'measurement_orig')` renames the existing `measurement` table to `measurement_orig`.  
   - `_rename_table(conn_str, schema, 'updated_measurement', 'measurement')` renames the newly populated `updated_measurement` table to `measurement`.  
   Both helpers use a `Statement` to run the `ALTER TABLE … RENAME TO …` command and invoke `check_stmt_err` for error handling.  

7. **Adjust foreign‑key constraint on measurement_organism** –  
   - The foreign key that previously referenced `measurement_orig` is dropped. A `Statement` (`drop_fk_measurement_org`) is created with the SQL template `DROP_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE` formatted with the target schema, executed, and its error status checked via `check_stmt_err`.  
   - A new foreign key that references the renamed `measurement` table is added. A `Statement` (`add_fk_measurement_org`) is created with the SQL template `ADD_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE`, executed, and checked similarly.  

8. **Create indexes on the new measurement table** –  
   - The list `col_index` enumerates the columns that should be indexed (e.g., `measurement_age_in_months`, `measurement_concept_id`, etc.).  
   - For each column, `_make_index_name('upd', col)` generates a deterministic index name that respects the Oracle identifier length limit (`NAME_LIMIT`). The function builds a name of the form `mea_<tbl>_<col>_<hash>_ix`.  
   - An `Statement` containing the index‑creation SQL (`IDX_MEASURE_LIKE_TABLE_SQL.format(idx_name, schema, col)`) is added to the `StatementSet`.  

9. **Parallel execution of index statements** – `stmts.parallel_execute(conn_str, 5)` launches five worker processes to run all index‑creation statements concurrently. The `StatementSet` handles task queuing, worker management, and result collection.  

10. **Error inspection for index creation** – After parallel execution, each statement in `stmts` is examined with `check_stmt_err`. If any statement reports an error, the error is logged (including the offending SQL and error message) and re‑raised as a `DatabaseError`, causing the function to abort. Successful completion logs “indexes added”.  

11. **Logging end** – A final log entry records the completion of the post‑lab‑LOINC steps together with the total elapsed time (`secs_since(start_time)`).  

12. **Return value** – If no exception is raised, the function returns `True` to indicate successful execution of all steps.

**Reference relationships**:  
- **Callers** – The function is invoked by `run_r_lab_loinc` in `pedsnetdcc/main.py` after the Lab LOINC R script finishes successfully. The caller supplies the same connection string, site identifier, and search path used for the R script.  
- **Callees** – Internally the function calls:  
  * `_add_primary_key` (adds PK on `updated_measurement`).  
  * `_rename_table` (renames `measurement` → `measurement_orig` and `updated_measurement` → `measurement`).  
  * `Statement` (encapsulates each DDL command).  
  * `check_stmt_err` (validates execution of each statement).  
  * `primary_schema` (extracts the target schema from `search_path`).  
  * `_make_index_name` (generates Oracle‑compatible index names).  
  * `StatementSet.parallel_execute` (runs index creation statements in parallel).  
  * `secs_since` (calculates elapsed time for logging).  

**Note**:  
- All DDL statements are executed in autocommit mode via `Statement.execute`; there is no transaction rollback, so any failure leaves the database in a partially modified state. The function relies on `check_stmt_err` to abort on the first error.  
- The index‑creation step uses a fixed pool size of five workers; this value can be changed by modifying the call to `parallel_execute`.  
- `_make_index_name` assumes the global constant `NAME_LIMIT` is defined and appropriate for the target database (e.g., 30 characters for Oracle).  
- The function does not catch `DatabaseError` exceptions; they propagate to the caller, which in `run_r_lab_loinc` results in a process exit with status 1.  
- The function returns a simple boolean (`True`) only when all steps succeed; it never returns `False`.  

**Output Example**:  
```python
result = run_post_lab_loinc(conn_str, site='stlouis', search_path='dcc_pedsnet, public')
print(result)   # -> True
```
