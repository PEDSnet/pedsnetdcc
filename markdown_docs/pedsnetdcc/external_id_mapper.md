## FunctionDef map_external_ids(conn_str, in_csv_file, out_csv_file, table_name, search_path)
**map_external_ids**: The function of **map_external_ids** is to read a list of external site identifiers from a CSV file, create a temporary table in the target database, determine which identifiers already have a DCC identifier, allocate new DCC identifiers for the unmapped rows, persist the new mappings, write the complete site‑to‑DCC mapping to an output CSV file, and return a boolean indicating success.

**parameters**:  
· **conn_str** – libpq‑style PostgreSQL connection string used for all database operations.  
· **in_csv_file** – path to the input CSV file that contains the external site identifiers (one identifier per row).  
· **out_csv_file** – path to the CSV file that will be written with two columns: `site_id` and `dcc_id`.  
· **table_name** – logical name of the mapping table (e.g., “measurement”, “observation”). It is used to build the names of the ID‑tracking and mapping tables.  
· **search_path** – PostgreSQL `search_path` string; the first schema in this list is treated as the primary schema for all temporary objects.

**Code Description**:  

1. **Timing and logging start** – The function records the current epoch time (`starttime`) and logs a “starting external id mapping” message together with the elapsed seconds (initially zero) using `secs_since`.

2. **Template variable preparation** – A dictionary `tpl_vars` is created with the supplied `table_name`. Two additional entries are added:  
   * `map_table_name` – formatted from the constant `MAP_TABLE_NAME_TMPL` using `tpl_vars`.  
   * `last_id_table_name` – formatted from `LAST_ID_TABLE_NAME_TMPL` using `tpl_vars`.  

   The primary schema is extracted from `search_path` by calling `primary_schema(search_path)` and stored in `schema`.

3. **Read input CSV** – The input file is opened in binary mode, read with `csv.reader`, and converted to a list of rows (`csv_data`). Each row is a list of string fragments representing a single external identifier.

4. **Generate a temporary table name** – A loop repeatedly calls `get_temp_table_name(conn_str, schema)` until a non‑empty name is returned. `get_temp_table_name` creates a random candidate name, checks the database for existence, and returns the name only if the table already exists; the loop therefore continues until a name that does **not** exist is found (the function returns an empty string when the candidate does not exist, causing the loop to iterate).

5. **Create and fill the temporary table** –  
   * `fill_temp_table(conn_str, schema, temp_table, csv_data)` creates a temporary table (`CREATE_TEMP_SQL`) in the identified schema and bulk‑loads the CSV rows into it in batches of up to 100 000 rows (`INSERT_TEMP_SQL`).  
   * Errors from the CREATE and INSERT statements are captured by `Statement` objects and validated with `check_stmt_err`.

6. **Retrieve current mappings** – `get_current_map_pairs(conn_str, schema, temp_table, tpl_vars['map_table_name'])` runs a SELECT (`SELECT_MAP_SQL`) that joins the temporary table with the permanent mapping table. It returns a list of dictionaries, each containing `site_id` (the external identifier) and `dcc_id` (the existing DCC identifier or `None`).

7. **Drop the temporary table** – `drop_temp_table(conn_str, schema, temp_table)` removes the temporary table now that its data have been extracted.

8. **Count unmapped rows** – The function iterates over `map_data` and increments `unmapped` for each entry where `dcc_id` is `None`. This count is stored in `tpl_vars['new_id_count']`.

9. **Reserve a block of new DCC identifiers** –  
   * A `StatementList` (`update_last_id_stmts`) is created and populated with two `Statement` objects: one that locks the last‑ID tracking table (`LOCK_LAST_ID_SQL`) and one that updates the table to reserve a contiguous block of identifiers (`UPDATE_LAST_ID_SQL`).  
   * `update_last_id_stmts.serial_execute(conn_str, transaction=True)` runs both statements inside a single transaction, guaranteeing atomicity of the lock‑update sequence.  
   * After execution, each statement is checked for errors with `check_stmt_err` and the second statement is checked for returned data with `check_stmt_data`. The result row contains the old and new last‑ID values; these are stored in `tpl_vars['old_last_id']` and `tpl_vars['new_last_id']` and logged.

10. **Assign new DCC identifiers** – Starting from `old_last_id + 1`, the function walks the `map_data` list again. For each entry whose `dcc_id` is `None`, it:  
    * Sets `dcc_id` to the next available identifier.  
    * Calls `insert_mapping_row(tpl_vars, site_id, dcc_id, conn_str)` to insert a row into the permanent mapping table (`INSERT_NEW_MAPS_SQL`). The helper updates `tpl_vars` with the current `site_id` and `dcc_id`, builds the INSERT statement, executes it, and validates the result with `check_stmt_err`.  
    * Increments the running `dcc_id` counter.

11. **Write output CSV** – The function opens `out_csv_file` in binary write mode, creates a `csv.writer`, writes the header row `['site_id', 'dcc_id']`, and then writes one row per mapping pair. Each row is also logged at INFO level showing the site and assigned DCC identifier.

12. **Final logging and return** – A completion message containing the target table name and total elapsed seconds is logged. The function returns `True` to signal successful completion.

**Note**:  
- The input CSV must contain exactly one column per row (the external identifier). Additional columns are concatenated without separators, which may produce malformed identifiers.  
- Temporary tables are created with `CREATE TEMP TABLE` and are session‑scoped. Because each `Statement.execute` opens a new connection, the temporary table must be defined with `ON COMMIT PRESERVE ROWS` (the default) so that it remains visible across the separate connections used for the INSERT batches.  
- The loop that obtains a temporary table name relies on `get_temp_table_name` returning an empty string when the generated name does **not** exist; therefore the loop continues until a non‑existent name is found and then uses that name for creation.  
- All database interactions are performed via the `Statement` and `StatementList` abstractions; errors are captured in the `err` attribute and must be checked with `check_stmt_err`. Missing result data are verified with `check_stmt_data`. Failure in any of these checks raises a `DatabaseError` or `MissingDataError`, aborting the mapping process.  
- The allocation of new DCC identifiers is performed atomically by locking and updating the last‑ID tracking table inside a single transaction; this prevents race conditions when multiple processes run the mapper concurrently.  
- The function returns a simple boolean (`True`). It does not raise its own exceptions; any database‑related problem propagates as `DatabaseError` or `MissingDataError` from the helper utilities.

**Output Example**:  
```python
result = map_external_ids(
    conn_str='postgresql://loader:pw@dbhost/pedsnet',
    in_csv_file='site_ids.csv',
    out_csv_file='mapped_ids.csv',
    table_name='measurement',
    search_path='dcc_pedsnet, public'
)
print(result)   # -> True
# The file 'mapped_ids.csv' now contains:
# site_id,dcc_id
# EXT001,100001
# EXT002,100002
# EXT003,100003
```
## FunctionDef insert_mapping_row(tpl_vars, site_id, dcc_id, conn_str)
**insert_mapping_row**: The function of insert_mapping_row is to insert a single external‑to‑DCC identifier mapping into the database and verify that the insertion succeeded.

**parameters**:  
· tpl_vars (dict): a dictionary of template variables used to build the SQL statement; it is modified in‑place to include the current `site_id` and `dcc_id`.  
· site_id (str or int): the external identifier (site‑specific) that will be stored in the mapping row.  
· dcc_id (str or int): the newly allocated DCC identifier that will be paired with `site_id`.  
· conn_str (str): a libpq‑style PostgreSQL connection string used to obtain a database connection for executing the insert.

**Code Description**:  
1. The function updates the supplied `tpl_vars` dictionary by adding two keys, `'site_id'` and `'dcc_id'`, assigning them the values passed in `site_id` and `dcc_id`. This prepares the variable set required by the SQL template that performs the insert.  
2. `INSERT_NEW_MAPS_SQL.format(**tpl_vars)` is evaluated to produce a concrete SQL INSERT statement. The template string `INSERT_NEW_MAPS_SQL` is defined elsewhere in the module and contains placeholders that correspond to the keys in `tpl_vars`.  
3. A `Statement` object is instantiated with the rendered SQL string. The `Statement` class (from `pedsnetdcc.db`) encapsulates the command, a default message, and a unique immutable identifier, and provides execution methods.  
4. The `execute` method of the `Statement` instance is called with `conn_str`. This method opens a new PostgreSQL connection, runs the SQL in autocommit mode, captures any execution error in `stmt.err`, and then closes the connection.  
5. Immediately after execution, `check_stmt_err` (from `pedsnetdcc.utils`) is invoked with the statement and the context string `'id mapping pre-transform'`. `check_stmt_err` inspects `stmt.err`; if an error is present it logs the failure and raises a `DatabaseError`. Thus, `insert_mapping_row` will propagate any database‑level problem to its caller.  
6. The function does not return a value; its purpose is side‑effectful (database insertion) and error handling is performed via the raised exception.

**Reference relationships**:  
- **Caller**: `map_external_ids` (in the same module) iterates over unmapped rows, computes a new `dcc_id` for each, and calls `insert_mapping_row` to persist the mapping. The caller relies on the function to raise an exception on failure, which would abort the overall mapping process.  
- **Callee**: The function uses the `Statement` class to encapsulate and run the SQL, and `check_stmt_err` to enforce error handling. Both utilities are part of the core database execution framework used throughout the project.  

**Note**:  
- `tpl_vars` is mutated; callers should be aware that the dictionary will contain the added `'site_id'` and `'dcc_id'` after the call.  
- The SQL template `INSERT_NEW_MAPS_SQL` must contain placeholders matching the keys in `tpl_vars`; otherwise a `KeyError` will be raised during formatting.  
- Because `Statement.execute` runs in autocommit mode, the insert is committed immediately; there is no transaction rollback within this function.  
- Any database error (e.g., constraint violation, connection problem) will cause `check_stmt_err` to raise a `DatabaseError`, terminating the mapping loop unless caught by the caller.  
- The function does not perform any validation of `site_id` or `dcc_id`; it assumes the caller has supplied appropriate, non‑null values.
## FunctionDef get_temp_table_name(conn_str, schema)
**get_temp_table_name**: The function of **get_temp_table_name** is to generate a candidate temporary table name, verify its existence in the database, and return the name only if the table is found; otherwise it returns an empty string.

**parameters**:  
· **conn_str**: a libpq‑style PostgreSQL connection string used to connect to the target database.  
· **schema**: the name of the schema in which the temporary table existence check will be performed.

**Code Description**:  
1. **Template variables preparation** – A dictionary `tpl_vars` is built containing the supplied `schema` and a randomly generated identifier for the temporary table. The identifier is created with `random.getrandbits(62)` and prefixed with `'t'`, producing a string such as `'t1234567890123456'`. This ensures a low probability of name collision.  
2. **SQL statement construction** – The function formats the module‑level constant `TABLE_EXISTS_SQL` with `tpl_vars`. `TABLE_EXISTS_SQL` is a SQL template that queries the PostgreSQL catalog (e.g., `pg_tables` or `information_schema.tables`) to determine whether a table with the generated name exists in the given schema.  
3. **Statement execution** – A `Statement` object is instantiated with the formatted SQL. The `execute` method of `Statement` opens a new connection using `conn_str`, runs the query, and stores any result rows in `exist_statement.data`.  
4. **Error handling** – Immediately after execution, `check_stmt_err` is called with the statement and a context string `'check temp table exists'`. If the execution raised an error, `check_stmt_err` logs the problem and raises a `DatabaseError`, causing the caller to abort.  
5. **Result interpretation** – The variable `exists` receives the value of `exist_statement.data`. In PostgreSQL, a SELECT that returns rows yields a non‑empty list; a SELECT that returns no rows yields `None` or an empty list. The function treats any truthy `exists` value as an indication that the temporary table already exists.  
6. **Return value** – If `exists` is truthy, the generated temporary table name (`tpl_vars['temp_table_name']`) is returned. If the table does not exist, an empty string (`''`) is returned, signalling the caller to try again with a new random name.

**Reference Relationships**:  
- **Caller** – The only direct caller in the project is `map_external_ids` (in `pedsnetdcc/external_id_mapper.py`). That function repeatedly invokes `get_temp_table_name` inside a `while temp_table == ''` loop until a non‑empty name is obtained, i.e., until a generated name corresponds to an existing temporary table. The returned name is then used by `fill_temp_table`, `drop_temp_table`, and other helper functions that manipulate the temporary table.  
- **Callees** – Internally the function relies on two utilities:  
  * `Statement` from `pedsnetdcc/db.py` for executing the existence‑check SQL.  
  * `check_stmt_err` from `pedsnetdcc/utils.py` for centralized error detection and logging.  
  It also depends on the constant `TABLE_EXISTS_SQL`, which is defined elsewhere in the module and contains the actual SQL used for the existence test.

**Note**:  
- The function does **not** create the temporary table; it only checks whether a table with the generated name already exists. Callers must handle the case where an empty string is returned (typically by looping, as `map_external_ids` does).  
- Because the name is generated with 62 random bits, the probability of a collision is extremely low, but the existence check guards against the unlikely event that the name is already in use.  
- Any database error during the existence check (e.g., connection failure, malformed SQL) will cause `check_stmt_err` to raise a `DatabaseError`, terminating the calling workflow unless the exception is caught.  
- The function assumes that `TABLE_EXISTS_SQL` is a valid SELECT statement that returns rows when the table exists; if the template is malformed, the function will raise an error via `check_stmt_err`.

**Output Example**:  
```python
# Suppose the generated name is 't9876543210' and the table exists
temp_name = get_temp_table_name(conn_str, 'public')
print(temp_name)   # -> 't9876543210'

# If the generated name does not correspond to any table
temp_name = get_temp_table_name(conn_str, 'public')
print(temp_name)   # -> ''   (empty string, indicating no such table)
```
## FunctionDef fill_temp_table(conn_str, schema, table_name, csv_data)
**fill_temp_table**: The function of **fill_temp_table** is to create a temporary table in the target PostgreSQL schema and bulk‑load a list of site identifiers (provided as CSV rows) into that table in batches.

**parameters**:  
· **conn_str** – a libpq‑style connection string that identifies the PostgreSQL database to which the temporary table will be created.  
· **schema** – the name of the schema (e.g., `dcc_pedsnet`) in which the temporary table will be created.  
· **table_name** – the identifier that will be used for the temporary table; this value is substituted into the SQL templates that create and populate the table.  
· **csv_data** – a list of rows read from a CSV file; each row is itself an iterable of string fragments that together represent a single site identifier.  

**Code Description**:  
1. **Template preparation** – A dictionary `tpl_vars` is built containing the target `schema` and the temporary table name (`temp_table_name`). These variables are later interpolated into the SQL statement templates `CREATE_TEMP_SQL` and `INSERT_TEMP_SQL` (both defined elsewhere in the module).  

2. **Create the temporary table** –  
   * `CREATE_TEMP_SQL.format(**tpl_vars)` produces a `CREATE TEMP TABLE …` statement that is wrapped in a `Statement` object.  
   * `create_statement.execute(conn_str)` runs the statement against the database using the `Statement.execute` method, which opens a new connection, runs the SQL in autocommit mode, and stores any error in `create_statement.err`.  
   * `check_stmt_err(create_statement, 'create temp table')` is called immediately after execution; if the statement produced an error, `check_stmt_err` logs the problem and raises a `DatabaseError`, aborting the function.  

3. **Transform CSV rows into SQL value tuples** –  
   * An empty list `site_id_list` is created.  
   * The function iterates over each `site_id` entry in `csv_data`. For each entry, it concatenates the fragments (`''.join(site_id)`) and surrounds the result with parentheses, yielding a string such as `'(12345)'`. This string is appended to `site_id_list`. The resulting list therefore contains one SQL‑compatible tuple per CSV row.  

4. **Batch the inserts** –  
   * Because the number of site identifiers can be very large, the list is split into sub‑lists of at most 100 000 elements (`n = 100000`).  
   * The list comprehension `[site_id_list[i * n:(i + 1) * n] for i in range((len(site_id_list) + n - 1) // n)]` produces `split_site_id_list`, a list of batches.  

5. **Insert each batch** – For every `site_id_list` batch:  
   * The batch is joined into a single comma‑separated string and stored in `tpl_vars['site_id']`.  
   * `INSERT_TEMP_SQL.format(**tpl_vars)` creates an `INSERT INTO … VALUES …` statement that inserts the entire batch in one command.  
   * A new `Statement` object (`insert_statement`) is instantiated with this SQL and executed via `insert_statement.execute(conn_str)`.  
   * `check_stmt_err(insert_statement, 'fill temp table')` validates the execution; any error raises an exception and stops further processing.  

6. **Result** – When the function returns (implicitly `None`), the temporary table exists in the current session and contains all site identifiers from the CSV file, ready for subsequent queries (e.g., `get_current_map_pairs`).  

**Reference relationships**:  
* **Caller** – `fill_temp_table` is invoked by `map_external_ids` (in the same module). `map_external_ids` obtains a temporary table name via `get_temp_table_name`, reads the CSV file into `csv_data`, and then calls `fill_temp_table` to populate the temporary table before performing mapping logic.  
* **Callees** – Within `fill_temp_table` the following objects are used:  
  * `Statement` (from `pedsnetdcc.db`) – to encapsulate and execute the CREATE and INSERT SQL commands.  
  * `check_stmt_err` (from `pedsnetdcc.utils`) – to verify that each executed statement succeeded and to raise a `DatabaseError` on failure.  
  * The SQL template strings `CREATE_TEMP_SQL` and `INSERT_TEMP_SQL` – external constants that define the DDL and DML required for the temporary table.  

**Note**:  
- The temporary table is created with the `CREATE TEMP TABLE` command, which means it lives only for the duration of the database session (the connection used by the subsequent statements). All statements in this function share the same connection string, but each `Statement.execute` call opens its own connection; therefore the temporary table must be created in a session that is visible to the later INSERT statements. In practice, PostgreSQL treats temporary tables as session‑wide, and because each `Statement` opens a new connection, the temporary table must be created with the `ON COMMIT PRESERVE ROWS` semantics (the default) so that it persists across the separate connections used for the inserts.  
- Errors are not raised automatically by `Statement`; they are captured in the `err` attribute and must be checked with `check_stmt_err`. Failure to call `check_stmt_err` after a statement could allow silent errors.  
- The batch size of 100 000 is hard‑coded; if the CSV contains more rows, the function will automatically split the data into multiple INSERT statements to avoid exceeding PostgreSQL’s maximum query length or memory limits.  
- The function does not return any value; callers should rely on the existence of the temporary table and on the absence of raised exceptions to determine success.  
- The CSV rows are assumed to contain only the site identifier fragments; any additional columns in the CSV will be concatenated without separators, which may produce malformed identifiers if the input format deviates from expectations.  
## FunctionDef update_temp_table(conn_str, schema, table_name, map_table_name)
**update_temp_table**: The function of update_temp_table is to execute an UPDATE statement that copies DCC identifiers from a mapping table into a temporary table.

**parameters**:  
· conn_str: a libpq‑style PostgreSQL connection string used to open a database connection.  
· schema: the name of the database schema that contains the tables involved in the update.  
· table_name: the name of the temporary table that will receive the updated DCC identifiers.  
· map_table_name: the name of the mapping table that holds the source DCC identifiers.

**Code Description**:  
1. A dictionary named `tpl_vars` is built containing the three identifiers required by the SQL template: the target `schema`, the `temp_table_name` (taken from the `table_name` argument), and the `map_table_name`.  
2. The function formats a pre‑defined SQL template `UPDATE_DCC_SQL` with the values from `tpl_vars`. The resulting string is a complete UPDATE command that references the supplied schema, temporary table, and mapping table.  
3. An instance of `Statement` is created with the formatted SQL. The `Statement` class encapsulates the SQL command together with a default descriptive message and provides execution metadata such as `rowcount`, `err`, etc.  
4. The `execute` method of the `Statement` object is called with `conn_str`. This method opens a new PostgreSQL connection, sets autocommit mode, runs the SQL on the connection, captures any database‑level error in `stmt.err`, and then closes the connection.  
5. After execution, the helper `check_stmt_err` is invoked with the statement object and the context string `'update available dcc_ids'`. `check_stmt_err` inspects `stmt.err`; if an error is present it logs the failure and raises a `DatabaseError`. If no error occurred, the function simply returns (implicitly `None`).  

**Relationship with callees**:  
- The function relies on the `Statement` class from `pedsnetdcc.db` to perform the actual database interaction. All aspects of connection handling, logging, and result capture are delegated to `Statement.execute`.  
- Error handling is delegated to `pedsnetdcc.utils.check_stmt_err`, which standardises logging and exception raising for any statement execution failure.  
- The SQL text itself is supplied by the module‑level constant `UPDATE_DCC_SQL`; the function only performs string interpolation and does not contain any SQL logic beyond that.

**Note**:  
- The function does not return any value; callers should rely on the absence of an exception to infer success.  
- Because `Statement.execute` runs with autocommit, the UPDATE is committed immediately; there is no transaction rollback within this function.  
- If the supplied `conn_str` is invalid or the database is unreachable, the connection error will be captured in `stmt.err` and propagated as a `DatabaseError` by `check_stmt_err`.  
- The caller must ensure that `schema`, `table_name`, and `map_table_name` correspond to existing objects in the database; otherwise the UPDATE statement will fail and be reported by the error handling logic.
## FunctionDef get_current_map_pairs(conn_str, schema, table_name, map_table_name)
**get_current_map_pairs**: The function of get_current_map_pairs is to retrieve the current site‑to‑DCC identifier mappings for a temporary table that holds external IDs.

**parameters**:  
· conn_str: PostgreSQL connection string used to open a database connection.  
· schema: Name of the schema that contains the temporary and mapping tables.  
· table_name: Name of the temporary table that stores the external (site) IDs to be mapped.  
· map_table_name: Name of the permanent mapping table that stores the relationship between site IDs and DCC IDs.

**Code Description**:  
1. A dictionary `tpl_vars` is built containing the three identifiers required by the SQL template: the target `schema`, the temporary table name (`temp_table_name`), and the mapping table name (`map_table_name`).  
2. The SQL statement to fetch the current mapping is created by formatting the constant `SELECT_MAP_SQL` with `tpl_vars`. The resulting string is passed to the `Statement` class, producing a `Statement` object (`mapping_statement`).  
3. `mapping_statement.execute(conn_str)` runs the formatted SELECT against the database. The `Statement.execute` method opens a new connection, runs the query, stores any error in `mapping_statement.err`, and places the fetched rows in `mapping_statement.data`.  
4. Immediately after execution, `check_stmt_err(mapping_statement, 'select current mapping')` is called (by the caller of this function). This helper inspects `mapping_statement.err` and raises a `DatabaseError` if the query failed, ensuring that the function proceeds only when the SELECT succeeded.  
5. An empty list `map_data` is prepared. The function iterates over `mapping_statement.data`, which is a sequence of rows returned by the SELECT. Each row is expected to contain two columns: the external site identifier and the corresponding DCC identifier. For every row, a dictionary `{'site_id': result[0], 'dcc_id': result[1]}` is created and appended to `map_data`.  
6. After processing all rows, `map_data`—a list of dictionaries mapping each site ID to its current DCC ID (or `None` if not yet assigned)—is returned to the caller.

**Reference Relationships**:  
- **Caller**: `map_external_ids` (in the same module) invokes `get_current_map_pairs` after loading external IDs into a temporary table. The returned list is used to determine which records already have a DCC ID and which need new IDs assigned.  
- **Callee**: The function relies on the `Statement` class from `pedsnetdcc.db` to execute the SELECT query, and on the `check_stmt_err` utility from `pedsnetdcc.utils` (indirectly, via the caller) to validate execution success. No other internal functions are called.

**Note**:  
- The SELECT statement (`SELECT_MAP_SQL`) must return exactly two columns in the order *site_id*, *dcc_id*. If the query returns a different column order or additional columns, the mapping dictionaries will be constructed incorrectly.  
- The function does not perform its own error handling; it expects the caller to invoke `check_stmt_err` (or an equivalent check) after execution. If `mapping_statement.err` is not `None`, the function will still return an empty list, but downstream code will likely raise an exception.  
- The returned list preserves the order of rows as produced by the database query; callers should not rely on any specific ordering unless the SELECT includes an ORDER BY clause.  
- All database resources (connections, cursors) are managed by the `Statement` class, so the function itself does not need explicit cleanup.  

**Output Example**:  
```python
# Example return value when the temporary table contains three external IDs,
# two of which are already mapped.
[
    {'site_id': 'EXT123', 'dcc_id': 1001},
    {'site_id': 'EXT456', 'dcc_id': None},
    {'site_id': 'EXT789', 'dcc_id': 1002}
]
```
## FunctionDef drop_temp_table(conn_str, schema, table_name)
**drop_temp_table**: The function of drop_temp_table is to remove a temporary table that was created earlier in the external‑id‑mapping workflow.

**parameters**:  
· conn_str: a libpq‑style PostgreSQL connection string that identifies the target database.  
· schema: the name of the database schema that contains the temporary table.  
· table_name: the name of the temporary table to be dropped.

**Code Description**:  
1. The function builds a dictionary named `tpl_vars` that holds the values for the SQL template placeholders. It stores the supplied `schema` under the key `'schema'` and the temporary table name under `'temp_table_name'`.  
2. `DROP_TEMP_SQL` (defined elsewhere in the module) is a SQL string that contains placeholders for the schema and temporary table name. The function formats this template with `tpl_vars` using Python’s `str.format` method, producing a concrete `DROP TABLE` statement.  
3. A `Statement` object is instantiated with the formatted SQL. The `Statement` class encapsulates the SQL command together with execution metadata (message, unique identifier, result storage, error handling, etc.).  
4. The `execute` method of the `Statement` instance is called with `conn_str`. This opens a new PostgreSQL connection, runs the DROP command in autocommit mode, captures any execution error in `stmt.err`, and then closes the connection.  
5. After execution, the helper function `check_stmt_err` is invoked with the statement object and the context string `'drop temp table'`. `check_stmt_err` inspects `stmt.err`; if an error occurred, it logs the failure and raises a `DatabaseError`. If no error is present, the function simply returns, indicating that the temporary table has been successfully removed.

**Reference Relationships**:  
- **Caller**: `drop_temp_table` is called from `map_external_ids` (in the same module). In the mapping process, after the temporary table has been filled and the current mapping retrieved, `map_external_ids` invokes `drop_temp_table` to clean up the temporary resource before proceeding with ID assignment.  
- **Callee**: The function relies on the `Statement` class from `pedsnetdcc.db` to execute the DROP command, and on `check_stmt_err` from `pedsnetdcc.utils` to validate the execution result and raise a consistent exception on failure. No other functions are directly called.

**Note**:  
- The DROP operation is executed in autocommit mode; therefore it cannot be rolled back. Ensure that the temporary table name is correct to avoid unintentionally dropping a permanent table.  
- Any error during execution (e.g., table does not exist, insufficient privileges) will be captured by the `Statement` object and propagated as a `DatabaseError` by `check_stmt_err`. Callers should be prepared to handle this exception.  
- The function does not return a value; its success is implied by the absence of an exception.  
- The SQL template `DROP_TEMP_SQL` must be defined in the module and must contain the placeholders `{schema}` and `{temp_table_name}`; otherwise a `KeyError` will be raised during formatting.
