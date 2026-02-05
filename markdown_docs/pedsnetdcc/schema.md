## FunctionDef create_schema_statement(schema)
**create_schema_statement**: The function of create_schema_statement is to generate a Statement object that encapsulates the SQL command required to create a PostgreSQL schema.

**parameters**  
· schema: the name of the database schema to be created. It must be a valid PostgreSQL identifier (e.g., no spaces, starts with a letter or underscore).

**Code Description**  
The function receives a single argument, schema, and builds a raw SQL string using Python’s format method:  

```python
sql = 'CREATE SCHEMA {0}'.format(schema)
```  

The resulting string is exactly the DDL command that PostgreSQL expects to create a new schema. The function then instantiates a Statement object (defined in pedsnetdcc/db.py) with this SQL string and returns the Statement instance. No custom message is supplied, so the Statement uses its default msg value of “executing SQL”.  

The returned Statement object does not execute the command; it merely holds the command and the metadata required for later execution. Execution is performed by calling execute or execute_on_conn on the Statement instance, as seen in the callers:

* **create_schema** (pedsnetdcc/schema.py) calls create_schema_statement(schema).execute(conn_str) to actually create the schema, handling errors such as duplicate‑schema when force=True.  
* Various transformation runner functions (e.g., run_transformation, run_target_transformation, run_age_transformation, run_concept_transformation, run_site_transformation, run_id_transformation, run_index_transformation) use create_schema_statement directly when they need to create a temporary or backup schema (e.g., backup_schema = schema + '_backup'; create_schema_statement(backup_schema)). These statements are collected into a StatementList and executed in a single transaction.  

Thus, create_schema_statement acts as a low‑level factory for DDL Statement objects, while higher‑level functions manage execution flow, error handling, and transaction control.

**Note**  
- The function does not validate the supplied schema name; callers must ensure it conforms to PostgreSQL naming rules.  
- Because the Statement is created with the default message, log entries will show “executing SQL” unless the caller overrides the msg attribute after creation.  
- Errors during execution are stored in the Statement.err attribute; callers must inspect this attribute (as create_schema does) to decide whether to raise a DatabaseError or ignore the error (e.g., when force=True and the error code is DUPLICATE_SCHEMA).  
- The returned Statement object is immutable with respect to its identifier id_; attempts to modify id_ raise RuntimeError as defined in the Statement class.

**Output Example**  

```python
stmt = create_schema_statement('research')
print(stmt)                     # -> Statement for executing SQL
print(stmt.sql)                 # -> CREATE SCHEMA research
print(stmt.id_)                 # -> 3fa85f64-5717-4562-b3fc-2c963f66afa6  (example UUID)
```
## FunctionDef create_schema(conn_str, schema, force)
**create_schema**: The function of create_schema is to create a PostgreSQL schema identified by *schema* using the database connection described by *conn_str*.

**parameters**:  
· conn_str (str): libpq‑style connection string that identifies the target PostgreSQL database.  
· schema (str): name of the schema to be created; must be a valid PostgreSQL identifier.  
· force (bool, optional, default False): when True, the function suppresses the error that occurs if the schema already exists (PostgreSQL error code 42P06, condition DUPLICATE_SCHEMA).

**Code Description**:  
1. The function builds a *Statement* object that contains the SQL command `CREATE SCHEMA {schema}` by calling `create_schema_statement(schema)`.  
2. The statement is executed against the database by invoking the `execute` method of the *Statement* class: `stmt = create_schema_statement(schema).execute(conn_str)`. The `execute` method opens a new connection (autocommit), runs the SQL, and stores any exception in `stmt.err`.  
3. After execution the function checks `stmt.err`.  
   * If an error is present and `force` is True, the helper `pg_error(stmt)` is called to translate the PostgreSQL error code to its symbolic name. If the name equals `'DUPLICATE_SCHEMA'`, the error is ignored and the function returns `True`.  
   * If an error is present and either `force` is False or the error is not a duplicate‑schema condition, a formatted message `failed to create schema `{schema}`: {stmt.err}` is constructed and a `DatabaseError` (subclass of `StatementError`) is raised with that message.  
4. If no error occurred, the function simply returns `True` to indicate successful creation.

**Reference relationships**:  
*Callers* – `create_schema` is used throughout the package wherever a new schema must be provisioned:  
  - Unit tests in `pedsnetdcc/tests/schema_test.py` (tests for normal creation, duplicate‑schema handling, and forced creation).  
  - Transformation pipelines in `pedsnetdcc/transform_runner.py` (`run_transformation`, `run_target_transformation`, `run_age_transformation`, `run_concept_transformation`, `run_site_transformation`, `run_id_transformation`, `run_index_transformation`). Each runner creates a temporary schema `<primary>_transformed` before loading transformed tables.  
*Callees* – `create_schema` relies on:  
  - `create_schema_statement` to generate the appropriate `Statement` object containing the DDL.  
  - `Statement.execute` (from `pedsnetdcc/db.py`) to run the statement against the database.  
  - `pg_error` (from `pedsnetdcc/utils.py`) to map a PostgreSQL error to its symbolic name for the `force` logic.  
  - `DatabaseError` (from `pedsnetdcc/utils.py`) to raise a consistent exception type for callers.

**Note**:  
- The function does not perform any validation of the *schema* name; callers must ensure the name complies with PostgreSQL identifier rules.  
- Because the underlying `Statement.execute` runs with autocommit, the schema creation is committed immediately and cannot be rolled back by the caller.  
- When `force=False` and the schema already exists, a `DatabaseError` is raised; this behavior is exercised in the unit test `test_create`.  
- The return value is always a boolean `True`; any failure results in an exception, never a `False` return.  
- The function only suppresses the specific duplicate‑schema error; other errors (e.g., permission denied) are always propagated as `DatabaseError`.

**Output Example**:  
```python
# Successful creation
result = create_schema(conn_str, 'research')
print(result)          # => True

# Duplicate schema with force=False raises DatabaseError
create_schema(conn_str, 'research')   # raises DatabaseError

# Duplicate schema with force=True is ignored
result = create_schema(conn_str, 'research', force=True)
print(result)          # => True
```
## FunctionDef drop_schema_statement(schema, if_exists, cascade)
**drop_schema_statement**: The function of drop_schema_statement is to construct a SQL DROP SCHEMA command wrapped in a Statement object, optionally adding IF EXISTS and CASCADE clauses.

**parameters**: The parameters of this Function.  
· **schema** (str): The name of the database schema to be dropped.  
· **if_exists** (bool): When True, the generated SQL includes the IF EXISTS clause, preventing an error if the schema does not exist.  
· **cascade** (bool): When True, the generated SQL includes the CASCADE clause, causing all objects within the schema (tables, views, etc.) to be dropped automatically.

**Code Description**:  
The function builds the optional clauses first:  
- `if_exists_clause` is set to the literal string 'IF EXISTS' if if_exists is True; otherwise it is an empty string.  
- `cascade_clause` is set to 'CASCADE' if cascade is True; otherwise it is an empty string.  

It then assembles the final command using Python’s str.format method:

```
sql = 'DROP SCHEMA {if_exists} {schema} {cascade}'.format(
          if_exists=if_exists_clause, schema=schema, cascade=cascade_clause)
```

The placeholders are replaced with the previously computed clause strings and the supplied schema name. Even when a clause is omitted, the format string leaves a space, resulting in a syntactically valid statement such as `DROP SCHEMA my_schema ` or `DROP SCHEMA  my_schema `.

Finally, the function creates and returns a Statement instance (from pedsnetdcc/db.py) initialized with the constructed sql string. The Statement object encapsulates the command and provides execution methods (`execute`, `execute_on_conn`) as well as metadata fields (`sql`, `msg`, `id_`, `data`, `rowcount`, `err`).

**Reference Relationships**:  
- **Callers**:  
  - `drop_schema` (in pedsnetdcc/schema.py) calls `drop_schema_statement(...).execute(conn_str)` to actually drop a schema.  
  - Numerous transformation runner functions (e.g., `run_transformation`, `run_target_transformation`, `run_age_transformation`, `run_concept_transformation`, `run_site_transformation`, `run_id_transformation`, `run_index_transformation`, `undo_transformation`) build a StatementList and append `drop_schema_statement` instances for dropping backup, temporary, or original schemas as part of their workflow.  
- **Callee**:  
  - The function directly instantiates the `Statement` class defined in pedsnetdcc/db.py. No other internal helpers are used.

**Note**:  
- The function does not perform any quoting or validation of the schema parameter; callers must ensure the schema name is a valid PostgreSQL identifier.  
- When both if_exists and cascade are False, the generated SQL contains two consecutive spaces before the schema name; PostgreSQL tolerates this, but the extra whitespace is harmless.  
- The returned Statement object must be executed (e.g., via `.execute(conn_str)`) or added to a StatementList for batch execution. After execution, callers should inspect `stmt.err` to detect any failure, as Statement does not raise exceptions automatically.  
- Because Statement generates a unique immutable id_, the same drop_schema_statement call can be safely reused in parallel execution collections.

**Output Example**:  

```python
stmt = drop_schema_statement('research_data', if_exists=True, cascade=True)
print(stmt.sql)
# Output: DROP SCHEMA IF EXISTS research_data CASCADE

print(isinstance(stmt, Statement))   # True
print(stmt.msg)                      # defaults to 'executing SQL'
```
## FunctionDef drop_schema(conn_str, schema, if_exists, cascade)
**drop_schema**: The function of drop_schema is to drop a database schema using a generated SQL statement and raise a DatabaseError on failure.  

**parameters**:  
· **conn_str** (str): libpq‑style connection string that identifies the target PostgreSQL database.  
· **schema** (str): name of the schema to be dropped.  
· **if_exists** (bool): when True, the generated DROP SCHEMA statement includes the `IF EXISTS` clause, preventing an error if the schema does not exist.  
· **cascade** (bool): when True, the generated statement includes the `CASCADE` clause, causing all objects within the schema (tables, views, etc.) to be dropped automatically.  

**Code Description**:  
1. The function calls **drop_schema_statement(schema, if_exists, cascade)**, which builds a `DROP SCHEMA` SQL command string based on the supplied flags and returns a **Statement** object containing that SQL.  
2. The returned **Statement** object’s **execute(conn_str)** method is invoked. `Statement.execute` opens a new PostgreSQL connection using the supplied `conn_str`, sets the isolation level to autocommit, and runs the SQL via `Statement.execute_on_conn`. The execution result (including any error) is stored in the **Statement** instance’s `err` attribute.  
3. After execution, the function checks `stmt.err`. If an error is present, it formats an error message: `'failed to create schema `{0}`: {1}'` where `{0}` is the schema name and `{1}` is the error captured by the statement. It then raises **DatabaseError** with that message.  
4. If no error occurs, the function simply returns **True** (the literal `True` value).  

**Reference Relationships**:  
- **Callees**:  
  * **drop_schema_statement** (pedsnetdcc/schema.py): constructs the appropriate DROP SCHEMA SQL and returns a Statement.  
  * **Statement.execute** (pedsnetdcc/db.py): runs the SQL against the database and records any execution error.  
  * **DatabaseError** (pedsnetdcc/utils.py): exception class raised when the DROP operation fails.  
- **Callers**:  
  * Unit tests in **pedsnetdcc/tests/schema_test.py** invoke `drop_schema` to verify normal dropping, conditional dropping with `if_exists`, and cascading drops.  
  * Higher‑level schema management functions (e.g., transformation runners) may call `drop_schema` when cleaning up temporary or backup schemas as part of a pipeline.  

**Note**:  
- The function does not perform any quoting or validation of the `schema` argument; callers must ensure the schema name is a valid PostgreSQL identifier.  
- The function returns **True** only when the DROP statement succeeds; any failure results in a raised **DatabaseError**, not a returned False.  
- Because `Statement.execute` uses autocommit, the DROP operation is applied immediately and cannot be rolled back.  
- When `if_exists` is False and the schema does not exist, PostgreSQL raises an error, which is captured and re‑raised as `DatabaseError`. Using `if_exists=True` suppresses this error.  
- When `cascade=False` and the schema contains objects, PostgreSQL raises an error; setting `cascade=True` forces removal of all dependent objects.  

**Output Example**:  

```python
# Successful drop
result = drop_schema(conn_str, 'research_data', if_exists=False, cascade=False)
print(result)          # True

# Failure example (schema does not exist and if_exists=False)
try:
    drop_schema(conn_str, 'missing_schema', if_exists=False, cascade=False)
except DatabaseError as e:
    print(e)           # failed to create schema `missing_schema`: schema "missing_schema" does not exist
```
## FunctionDef primary_schema(search_path)
**primary_schema**: The function of primary_schema is to extract and return the first schema name from a PostgreSQL search_path string.

**parameters**: The parameters of this function.  
· search_path: a string containing a comma‑separated list of PostgreSQL schemas as defined in the connection’s search_path.

**Code Description**:  
The function begins by removing any leading or trailing whitespace from the supplied *search_path* using `strip()`.  
If the resulting string is empty, the variable *schemas* is set to `None`; otherwise the string is split on commas, producing a list of individual schema names stored in *schemas*.  
A validation step follows: if *schemas* is falsy (i.e., `None` or an empty list), a `ValueError` is raised with the message `'search_path must be non‑empty'`. This ensures that callers always receive a valid schema name.  
When the list is non‑empty, the function returns the first element of the list after applying `strip()` again to remove any surrounding whitespace from that individual schema name. The returned value is a plain string representing the primary schema that will be used for subsequent SQL operations.

**Relationship with Callers**:  
`primary_schema` is invoked by many high‑level data‑processing functions throughout the project, including but not limited to:
- `run_bmi_calc` (pedsnetdcc/bmi.py) – determines the target schema for BMI configuration files and table creation.  
- `_add_measurement_ids` (pedsnetdcc/bmi.py) – obtains the schema to locate the measurement table and related ID‑tracking tables.  
- `copy_bmi_measurement` (pedsnetdcc/bmi.py) – uses the primary schema to copy BMI results into the core measurement table.  
- `run_era` and `_add_era_ids` (pedsnetdcc/era.py) – extract the schema for condition and drug era derivations, sequence creation, and ID assignment.  
- `populate_last_id` (pedsnetdcc/id_maps.py) – derives the schema for populating ID‑mapping tables.  
- `run_r_dose`, `run_r_drug_era`, `run_r_obs_covid`, `run_r_obs_recover`, `run_r_query`, `run_r_pcornet_*`, and other R‑script wrappers – all call `primary_schema` to embed the correct schema name into Argos configuration files and SQL statements.  
- Partitioning, splitting, and transformation utilities (e.g., `partition_measurement_table`, `split_measurement_table`, `run_transformation`, `run_target_transformation`, etc.) – rely on the first schema to construct temporary or transformed schema names and to reference core tables during schema manipulation.  

In each case, the function provides a single source of truth for the “primary” schema, simplifying the handling of PostgreSQL search_path strings and preventing repetitive parsing logic across the codebase.

**Note**:  
- The function assumes that the search_path follows PostgreSQL conventions (comma‑separated schema names). It does not validate individual schema names beyond stripping whitespace.  
- If the input contains only whitespace or is an empty string, a `ValueError` is raised; callers must be prepared to handle this exception.  
- The returned schema name is not quoted; callers should apply appropriate quoting or identifier handling when embedding the value in SQL statements.  

**Output Example**:  
Given `search_path = "dcc_pedsnet, public, vocabulary"`, the call `primary_schema(search_path)` returns the string `"dcc_pedsnet"`.  
If `search_path = "   analytics   "`, the function returns `"analytics"`.  
If `search_path = ""` (or only spaces), the function raises `ValueError: search_path must be non‑empty`.
## FunctionDef schema_exists(conn_str, schema)
**schema_exists**: The function of schema_exists is to determine whether a given PostgreSQL schema is present in the connected database and return a Boolean result.  

**parameters**:  
· conn_str (str): a libpq‑style connection string that identifies the target PostgreSQL database.  
· schema (str): the name of the schema whose existence is to be checked.  

**Code Description**:  
The function builds a simple SQL query that selects the constant 1 from the PostgreSQL catalog view information_schema.schemata where the schema_name matches the supplied schema argument. The query string is constructed with Python’s format method, inserting the schema name directly into the SQL text.  

A Statement object (from pedsnetdcc.db.Statement) is instantiated with the generated SQL. The Statement encapsulates the SQL command and provides execution metadata such as data, err, rowcount, etc.  

`stmt.execute(conn_str)` is called, which opens a new database connection using the provided connection string, runs the SQL, and populates the Statement attributes. If any error occurs during execution (for example, a connection failure or a SQL error), the Statement.err attribute is set and the error is logged.  

After execution, the function checks whether the Statement.err attribute is truthy. If an error is present, a DatabaseError (from pedsnetdcc.utils.DatabaseError) is raised with a formatted message that includes the schema name, the executed SQL, and the original error object. This ensures that callers receive a clear exception rather than a silent failure.  

If no error occurred, the function returns the Boolean expression `len(stmt.data) > 0`. Because the query returns a single row when the schema exists, stmt.data will be a non‑empty list in that case; otherwise it will be empty, yielding False.  

**Reference Relationships**:  
- **Callers**:  
  * SchemaTest._schema_exists (in pedsnetdcc/tests/schema_test.py) invokes schema_exists to verify schema presence during unit tests.  
  * TransformRunnerTest.test_without_data (in pedsnetdcc/tests/transform_runner_test.py) calls schema_exists to assert that the temporary public_transformed schema does not exist after a transformation run.  
  * undo_transformation (in pedsnetdcc/transform_runner.py) uses schema_exists to confirm that the backup schema exists before attempting a rollback.  
- **Callees**:  
  * Statement (from pedsnetdcc.db) is used to execute the SQL query.  
  * DatabaseError (from pedsnetdcc.utils) is raised when the underlying statement reports an error.  

**Note**:  
- The schema name is interpolated directly into the SQL string without quoting; callers must ensure that schema contains only a valid identifier to avoid SQL injection.  
- The function relies on the Statement class to manage connection handling, error capture, and result fetching; therefore any configuration that affects Statement behaviour (such as custom logging queues) also applies here.  
- A DatabaseError is raised only when the execution of the query itself fails; the function returns False when the schema simply does not exist.  
- The function does not modify the database; it performs a read‑only check.  

**Output Example**:  
```python
exists = schema_exists("host=localhost dbname=mydb user=app", "dcc_pedsnet")
print(exists)   # -> True if the schema dcc_pedsnet is present, otherwise False
```
## FunctionDef tables_in_schema(conn_str, schema)
**tables_in_schema**: The function of tables_in_schema is to return the set of table names that exist in a given PostgreSQL schema.

**parameters**:  
· conn_str (str): a libpq‑style connection string that identifies the target database.  
· schema (str): the name of the schema whose tables should be listed.

**Code Description**:  
The function builds a simple SQL query that selects `table_name` from the `information_schema.tables` view, filtered by the supplied `schema` value. The query string is created with Python’s `format` method, inserting the `schema` directly into the SQL text.

```python
sql = "select table_name from information_schema.tables " \
      "where table_schema = '{}'".format(schema)
```

A `Statement` object is instantiated with this SQL string. The `Statement` class encapsulates the SQL command together with execution metadata (such as `data`, `err`, `rowcount`, etc.) and provides the `execute` method to run the command against the database.

```python
stmt = Statement(sql)
stmt.execute(conn_str)
```

After execution, the function checks the `err` attribute of the `Statement`. If an error occurred during execution, a `DatabaseError` is raised with a formatted message that includes the schema name, the SQL that was run, and the underlying error object.

```python
if stmt.err:
    tpl = 'error listing tables for schema {sch} ({sql}): {err}'
    raise DatabaseError(tpl.format(sch=schema, sql=stmt.sql, err=stmt.err))
```

When the execution succeeds, the function iterates over `stmt.data`, which contains the rows returned by the query. Each row is a tuple whose first element is the table name. These names are added to a Python `set` called `tables`. Using a set guarantees that the result contains unique table names and provides O(1) lookup for callers.

```python
tables = set()
for row in stmt.data:
    tables.add(row[0])
return tables
```

**Reference Relationships**:  

*Callers* – `tables_in_schema` is used by several higher‑level schema management functions:  
- `drop_schema` (in the same module) calls it to verify that a schema contains tables before attempting a drop, and to raise a `DatabaseError` when the drop fails.  
- `undo_transformation` (in `pedsnetdcc/transform_runner.py`) calls it to obtain the list of tables present in a backup schema and compare it against the expected model tables.  
- Unit tests in `pedsnetdcc/tests/schema_test.py` invoke it directly to confirm that a newly created table appears in the schema.

*Callees* – The function relies on two external objects:  
- `Statement` (defined in `pedsnetdcc/db.py`) for executing the SQL and capturing results or errors.  
- `DatabaseError` (defined in `pedsnetdcc/utils.py`) for propagating database‑level failures to the caller.

**Note**:  
- The function does not perform any quoting or escaping of the `schema` argument; callers must ensure that the schema name is a valid identifier and does not contain malicious characters.  
- Because the SQL is constructed with simple string formatting, passing a non‑existent schema will simply return an empty set rather than raising an error; only execution‑level problems (e.g., loss of connection) trigger a `DatabaseError`.  
- The returned value is a `set` of strings, not a list; callers should treat it as an unordered collection.  
- If the database connection cannot be established, the underlying `Statement.execute` will set `stmt.err`, and this function will raise `DatabaseError` with the original exception attached.  

**Output Example**:  

```python
>>> tables = tables_in_schema(conn_str, 'dcc_pedsnet')
>>> print(tables)
{'person', 'visit_occurrence', 'condition_occurrence', 'measurement'}
```
