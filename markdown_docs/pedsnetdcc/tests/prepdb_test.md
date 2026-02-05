## FunctionDef _introspect_schemas(conn_str)
**_introspect_schemas**: The function of _introspect_schemas is to return a list of user‑defined schemas present in a PostgreSQL database, excluding system and temporary schemas.

**parameters**:  
· conn_str: a libpq/psycopg2 connection string that identifies the target database (type: str)

**Code Description**:  
1. **Initialisation** – An empty list `schemas` is created to collect the names of schemas that will be returned. A tuple `exclude_schemas` defines patterns for schemas that must be ignored:  
   * `pg_catalog` – the core PostgreSQL system catalog  
   * `pg_toast.*` – TOAST tables (internal storage)  
   * `pg_temp.*` – temporary schemas created for each session  
   * `information_schema` – the ANSI‑standard information schema  

2. **SQL statement preparation** – A `Statement` object is instantiated with the SQL query  
   ```sql
   select schema_name from information_schema.schemata
   ```  
   This query retrieves the name of every schema known to the database.

3. **Execution of the query** – `stmt.execute(conn_str)` is called. The `execute` method (defined in *pedsnetdcc/db.py*) opens a new database connection using the supplied `conn_str`, runs the SQL, captures the result rows in `stmt.data`, and then closes the connection. No result or logging queues are supplied here, so the call is a simple synchronous execution.

4. **Iterating over result rows** – For each row returned by the query (`row` is a tuple where `row[0]` holds the schema name):
   * The variable `schema` receives the schema name.
   * A flag `should_exclude` is set to `False`.
   * The code loops over each pattern `pat` in `exclude_schemas`. `re.fullmatch(pat, schema)` is used to test whether the schema name matches the pattern exactly. If a match is found, `should_exclude` becomes `True` and the inner loop breaks.
   * If `should_exclude` remains `False` after checking all patterns, the schema name is appended to the `schemas` list.

5. **Return value** – After all rows have been processed, the function returns the populated `schemas` list, which contains only the schemas that are not filtered out.

**Reference Relationships**:  
* **Callee** – The function relies on `Statement.execute` from *pedsnetdcc/db.py* to run the SQL query and retrieve data. `Statement.execute` handles connection creation, autocommit mode, error capture, and result storage.  
* **Caller** – `_introspect_schemas` is invoked by the test method `TestPrepareDatabase.check` in *pedsnetdcc/tests/prepdb_test.py*. In that test, the returned list is converted to a set and compared against an expected set of schema names to verify that the database was created with the correct schema structure. No other parts of the production code call this helper directly.

**Note**:  
* The function excludes schemas using regular‑expression full‑match. Patterns such as `pg_toast.*` will match any schema that starts with `pg_toast`, and `pg_temp.*` matches temporary schemas that include a session‑specific suffix.  
* Because the function uses `Statement.execute` without a result queue, any connection error will raise an exception inside `execute`; however, `execute` stores the error in `stmt.err` and returns the `Statement` object. In the current usage (within a unit test), a connection error would cause the test to fail before reaching the comparison step.  
* The returned list preserves the order in which PostgreSQL reports schemas; order is not guaranteed to be alphabetical. If deterministic ordering is required, callers should sort the list themselves.  
* The function does not attempt to retrieve tables or other objects within each schema; it only reports schema names. Future extensions could return a nested structure (e.g., `{schema: [tables]}`) as hinted in the docstring.

**Output Example**:  
```python
>>> conn_str = "host=localhost dbname=mydb user=myuser password=mypass"
>>> schemas = _introspect_schemas(conn_str)
>>> schemas
['public', 'vocabulary', 'site1_pcornet', 'site1_pedsnet',
 'site1_id_maps', 'site2_pcornet', 'site2_pedsnet',
 'site2_id_maps', 'dcc_ids']
```
## FunctionDef setUpModule
**setUpModule**: The function of setUpModule is to create a globally‑available PostgreSQL factory that caches an initialized database for use by the test suite.

**parameters**: This function does not accept any parameters.

**Code Description**:  
- The function begins with a comment explaining its purpose: it will generate a PostgreSQL class that caches the initialized database across multiple temporary database cluster instances.  
- The `global Postgresql` statement declares that the name `Postgresql` refers to a module‑level variable, allowing the function to assign to it and make the resulting object accessible to all test functions in the module.  
- `testing.postgresql.PostgresqlFactory` is called with the keyword argument `cache_initialized_db=True`. This call returns a factory class (or callable) that, when instantiated, will spin up a temporary PostgreSQL server. The `cache_initialized_db=True` flag tells the factory to keep a copy of the database after the first initialization, so subsequent instances can start from the same pre‑populated state, dramatically reducing setup time for each test.  
- The returned factory is assigned to the global name `Postgresql`. After `setUpModule` finishes, any test in the module can create a temporary PostgreSQL instance simply by calling `Postgresql()`.

**Note**:  
- `setUpModule` is automatically invoked by the `unittest` framework before any test cases in the module are executed; it should not be called manually.  
- The `testing.postgresql` package must be installed in the environment; otherwise an `ImportError` will be raised at import time.  
- Because the factory caches the initialized database, changes made to the database in one test may persist across subsequent tests if the test does not properly clean up or roll back its transactions. Ensure each test either uses a fresh instance (`Postgresql()`) or explicitly resets the database state.  
- The global variable `Postgresql` will remain bound for the lifetime of the test run; redefining it elsewhere may interfere with the intended caching behavior.
## FunctionDef tearDownModule(self)
**tearDownModule**: The function of tearDownModule is to clean up the test environment by clearing any cached PostgreSQL database instances after all tests have run.  

**parameters**:  
· self: The instance reference passed automatically when the method is invoked; it is not used within the function body.  

**Code Description**:  
The implementation consists of a single statement: `Postgresql.clear_cache()`. This call invokes the `clear_cache` class method of the `Postgresql` helper class, which is responsible for removing any in‑memory or on‑disk caches that store initialized database connections or temporary test databases. By executing this method at the end of the test suite, the function ensures that subsequent test runs start with a clean slate, preventing interference from leftover state. No additional logic, error handling, or return values are present.  

**Note**:  
- The function must be called after all test cases have completed; it is typically registered as a module‑level teardown hook in the unittest framework.  
- Because the `self` argument is unused, the method could be defined as a plain function without a class context, but the current signature matches the surrounding test class structure.  
- Ensure that the `Postgresql` class provides a correctly implemented `clear_cache` method; otherwise, this teardown step will have no effect.  
## ClassDef TestPrepareDatabase
**TestPrepareDatabase**: The function of TestPrepareDatabase is to verify that the `prepare_database` routine correctly creates the expected schemas and roles for a given model version, both in normal and DCC‑only modes.

**attributes**:  
· **roles** – class‑level tuple containing the role names that must exist before grants are applied (`'peds_staff'`, `'achilles_user'`, `'dqa_user'`, `'pcor_et_user'`, `'loading_user'`, `'dcc_owner'`, `'pcornet_sas'`).  
· **model_version** – instance attribute set in `setUp`; the version string of the data model to be prepared (e.g., `'2.2.0'`).  
· **dbname** – instance attribute derived from `model_version` by calling `_make_database_name`; the name of the database that `prepare_database` will create.  
· **postgresql** – instance attribute holding a temporary PostgreSQL server created by the `testing.postgresql` library; used for isolated testing.  
· **dburi** – instance attribute containing the connection URI of the temporary PostgreSQL instance (`self.postgresql.url()`).  
· **conn_str** – instance attribute built from `dburi` via `make_conn_str`; a libpq‑style connection string that does **not** include the target database name.

**Code Description**:  
The class inherits from `unittest.TestCase`, making it a collection of unit tests that are automatically discovered and run by the Python unittest framework.

1. **create_roles** – Executes a `CREATE ROLE` statement for each role listed in the class‑level `roles` tuple. It opens a connection using `self.conn_str`, iterates over the role names, and runs the SQL command. The connection is closed explicitly after the `with` blocks.

2. **setUp** – Runs before each test method. It performs four key actions:  
   a. Assigns a fixed model version (`'2.2.0'`).  
   b. Generates the expected database name with `_make_database_name`.  
   c. Starts a fresh PostgreSQL instance (`Postgresql()`) and builds a connection string (`make_conn_str`).  
   d. Calls `create_roles` to ensure all required roles exist in the temporary server.  
   If any step raises an exception, the test is aborted and `tearDown` will not be called.

3. **tearDown** – Runs after each test method (provided `setUp` succeeded). It stops the temporary PostgreSQL server, which automatically drops the test database and any objects created during the test.

4. **check** – Core validation helper. It connects to the newly created database (`_conn_str_with_database`) and builds a set of expected schema names:
   - For each site returned by `_sites_and_dcc` (optionally limited to DCC‑only when `dcc_only=True`), it creates three schemas: `<site>_pcornet`, `<site>_pedsnet`, and `<site>_id_maps`.
   - Adds the universal schemas `vocabulary`, `public`, and `dcc_ids`.
   The method then retrieves the actual schemas present in the database via `_introspect_schemas` and asserts that the two sets are identical.

5. **check_conn_str** – Ensures the connection string used for the temporary server does not already contain the target database name (which would prevent `prepare_database` from creating it). It also verifies that a connection can be opened with the string; any failure raises an exception.

6. **test_prep_db** – The primary test case. It calls `prepare_database` with the model version and connection string, then invokes `check()` to confirm that all expected schemas were created.

7. **test_prep_db_dcc_only** – Variant test that runs `prepare_database` with the `dcc_only=True` flag, then validates the schema set using `check(dcc_only=True)`. This ensures the DCC‑only code path behaves correctly.

**Note**:  
- The temporary PostgreSQL instance is isolated; no external database is affected.  
- The `create_roles` method assumes the test user has permission to create roles; this is true for the `testing.postgresql` fixture.  
- `tearDown` is not executed if `setUp` raises an exception; therefore, any failure in role creation or connection string generation will leave the temporary server running until the test process exits.  
- The helper functions (`_make_database_name`, `_sites_and_dcc`, `_conn_str_with_database`, `_introspect_schemas`, `make_conn_str`, `prepare_database`) are imported from the surrounding package and must behave as documented for the tests to pass.

**Output Example**:  
When both tests succeed, the unittest runner produces output similar to:

```
..
----------------------------------------------------------------------
Ran 2 tests in 0.73s

OK
```
### FunctionDef create_roles(self)
**create_roles**: The function of create_roles is to create database roles required for the test suite so that subsequent permission grants do not fail.  

**parameters**:  
· self: the instance of the TestPrepareDatabase test case; it provides the connection string (`self.conn_str`) and the list of role names (`TestPrepareDatabase.roles`).  

**Code Description**:  
The method opens a PostgreSQL connection using the connection string stored in `self.conn_str`. The connection is managed with a context manager (`with psycopg2.connect(self.conn_str) as conn:`), which guarantees that the connection is committed or rolled back automatically when the block exits. Inside the connection block, a cursor is obtained (`with conn.cursor() as cursor:`) to execute SQL statements.  

The method then iterates over the class‑level iterable `TestPrepareDatabase.roles`. For each role name, it builds a simple SQL command `CREATE ROLE <role>` and executes it with `cursor.execute`. Because the cursor is used inside a context manager, each `CREATE ROLE` statement is executed within the same transaction. After the loop finishes, the outer `with` block ends, causing the connection to be closed automatically. An explicit `conn.close()` call follows, which is harmless because the connection is already closed by the context manager.  

From a functional perspective, `create_roles` is invoked by the `setUp` method of the same test class. `setUp` prepares the test environment, builds the connection string, and then calls `self.create_roles()` to ensure that all roles required by the test database schema exist before any grant statements are executed. This ordering guarantees that role‑related operations in later test steps will not raise “role does not exist” errors.  

**Note**:  
- The PostgreSQL user used in `self.conn_str` must have sufficient privileges (typically a superuser) to create roles; otherwise, `cursor.execute` will raise a permission error.  
- If a role already exists, the `CREATE ROLE` command will fail with a duplicate‑object error, causing the whole transaction to roll back. In a real test environment, you may want to use `CREATE ROLE IF NOT EXISTS` (PostgreSQL 9.5+) or drop existing roles before creation.  
- The method does not return any value; its purpose is side‑effects only.  

**Output Example**:  
The function returns `None`. No direct output is produced; its effect is the creation of the specified roles in the test database.
***
### FunctionDef setUp(self)
**setUp**: The function of setUp is to prepare the test environment by initializing version information, creating a temporary PostgreSQL instance, building a connection string, and creating required database roles.

**parameters**:  
· self: the instance of the TestPrepareDatabase test case; it holds attributes that will be populated during set‑up (model_version, dbname, postgresql, dburi, conn_str).

**Code Description**:  
1. The method assigns a fixed model version string `'2.2.0'` to `self.model_version`.  
2. It calls the helper function `_make_database_name` with this version to compute the canonical test database name and stores the result in `self.dbname`. The helper converts the version to a short “XY” form and formats it as `pedsnet_dcc_vXY`.  
3. A temporary PostgreSQL server is started by instantiating `Postgresql()` (provided by the `testing.postgresql` package). The instance is saved in `self.postgresql`.  
4. The URI of the temporary server is obtained via `self.postgresql.url()` and stored in `self.dburi`.  
5. The URI is transformed into a libpq‑compatible connection string by calling `make_conn_str(self.dburi)`. The resulting string is saved in `self.conn_str`. This conversion parses the URI, extracts host, port, database name, user, password, and any query options, then assembles them into the format required by psycopg2.  
6. Finally, the method invokes `self.create_roles()`. This helper opens a connection using `self.conn_str`, iterates over the class‑level list `TestPrepareDatabase.roles`, and executes a `CREATE ROLE <role>` statement for each entry, ensuring that all roles needed by subsequent tests exist.

The method may raise a `ValueError` if any of the called helpers encounter malformed input (e.g., an invalid version string passed to `_make_database_name` or duplicate query parameters in the URI processed by `make_conn_str`).

**Note**:  
- The temporary PostgreSQL instance created by `Postgresql()` is isolated for the test run; it is automatically torn down when the test case finishes (handled by the test framework’s teardown).  
- `self.create_roles()` requires that the user specified in the connection string have sufficient privileges (typically a superuser) to create roles; otherwise a permission error will be raised.  
- If the database name derived from the version already exists, the test suite is responsible for cleaning it up before re‑running; this method only computes the name, it does not create the database itself.  
- The method does not return a value; its purpose is side‑effects that configure the test case’s attributes for later test methods.
***
### FunctionDef tearDown(self)
**tearDown**: The function of tearDown is to clean up resources after a test case has finished executing.  

**parameters**: The parameters of this Function.  
· self: the instance of the TestPrepareDatabase test case class; it provides access to the test fixture’s attributes, such as the PostgreSQL helper object.  

**Code Description**:  
The method is defined within the `TestPrepareDatabase` test class and serves as the counterpart to `setUp`. Its docstring explains that it performs cleanup after tests and explicitly notes that it will not be invoked if an exception occurs during `setUp`. The only operational statement in the body is `self.postgresql.stop()`. This call delegates to the `stop` method of the `postgresql` attribute, which is a helper object responsible for managing a PostgreSQL instance used during testing. By invoking `stop()`, the method shuts down the PostgreSQL server (or container), releases any held connections, and removes temporary resources such as roles that may have been created for the test. Consequently, the test environment is returned to a clean state, preventing side effects from leaking into subsequent tests.  

**Note**:  
- `tearDown` is automatically called by the unittest framework after each test method, provided that `setUp` completes without raising an exception.  
- If `setUp` fails, `tearDown` will not run, so any partial resources allocated in `setUp` must be handled within the exception path of `setUp` itself.  
- The cleanup performed here is limited to stopping the PostgreSQL instance; any additional resources created elsewhere in the test suite must be cleaned up separately.
***
### FunctionDef check(self, dcc_only)
**check**: The function of **check** is to verify that a freshly prepared PostgreSQL database contains exactly the set of schemas expected for the given model version and site configuration.

**parameters**:  
· **self** – the instance of the test case (`TestPrepareDatabase`).  
· **dcc_only** (bool, optional, default False) – when True, the verification is performed for a DCC‑only scenario; only the “dcc” site identifier is used when constructing the expected schema list.

**Code Description**:  
1. **Build a connection string that points to the test database** –  
   `conn_str = _conn_str_with_database(self.conn_str, self.dbname)` calls the helper `_conn_str_with_database` to take the base connection string stored in the test instance (`self.conn_str`) and inject the name of the database that was created by the preceding call to `prepare_database`. This yields a libpq‑style connection string that can be used to connect directly to the new database.

2. **Create the list of schemas that should exist** –  
   * An empty list `schema_list` is initialised.  
   * `schema_suffixes` is defined as the tuple `('pcornet', 'pedsnet', 'id_maps')`.  
   * The function iterates over each site identifier returned by `_sites_and_dcc(dcc_only=dcc_only)`.  
     - `_sites_and_dcc` always returns a tuple that starts with `'dcc'`. If `dcc_only` is False it also returns the internal site identifiers (e.g., `'site1'`, `'site2'`).  
   * For every combination of site and suffix, a schema name of the form `"{site}_{suffix}"` is appended to `schema_list`.  
   * After the nested loops, the names `'vocabulary'` and `'public'` are appended once, and then the tuple `('vocabulary', 'public', 'dcc_ids')` is extended onto the list. Because the list is later converted to a set, the duplicate `'vocabulary'` and `'public'` entries do not affect the final comparison.

3. **Convert the expected list to a set** –  
   `expected_schemas = set(schema_list)` removes any duplicates and provides an unordered collection suitable for direct equality comparison.

4. **Introspect the actual schemas present in the database** –  
   `actual_schemas = set(_introspect_schemas(conn_str))` runs the helper `_introspect_schemas`, which executes a simple `SELECT schema_name FROM information_schema.schemata` query, filters out system schemas (`pg_catalog`, `pg_toast.*`, `pg_temp.*`, `information_schema`), and returns the remaining user‑defined schema names as a list. Converting that list to a set yields `actual_schemas`.

5. **Assert equality** –  
   `self.assertEqual(actual_schemas, expected_schemas)` uses the standard `unittest.TestCase` assertion to fail the test if any schema is missing or any unexpected schema is present.

**Reference Relationships**:  
- **Callers**: The method is invoked by the two test cases in the same file:  
  * `TestPrepareDatabase.test_prep_db` – calls `prepare_database` with default arguments and then `self.check()`.  
  * `TestPrepareDatabase.test_prep_db_dcc_only` – calls `prepare_database` with `dcc_only=True` and then `self.check(dcc_only=True)`.  
  In both cases the database creation step precedes the verification step, ensuring that the schemas have been materialised before `check` runs.

- **Callees**:  
  * `_conn_str_with_database` – adjusts the base connection string to target the newly created database.  
  * `_sites_and_dcc` – supplies the list of site identifiers that drive the expected schema naming scheme; the `dcc_only` flag is propagated to this helper.  
  * `_introspect_schemas` – queries the database to retrieve the actual set of user‑defined schemas, excluding PostgreSQL system schemas.  

- **Testing Framework**: The method relies on `unittest.TestCase.assertEqual` for the final comparison, integrating the verification directly into the test suite.

**Note**:  
- The expected schema list deliberately includes `'vocabulary'`, `'public'`, and `'dcc_ids'` in addition to the per‑site schemas. Because the list is converted to a set, duplicate entries (e.g., `'vocabulary'` and `'public'` added twice) have no effect on the outcome.  
- The `dcc_only` flag limits the site iteration to the single identifier `'dcc'`; consequently the expected schemas in that mode are only the three suffix‑based schemas for `'dcc'` plus the shared `'vocabulary'`, `'public'`, and `'dcc_ids'`.  
- The function assumes that `self.conn_str` and `self.dbname` have been correctly initialised in the test’s `setUp` method and that the database creation step succeeded; otherwise the connection or schema introspection will raise an exception and cause the test to error.  
- Schema names are compared as sets, so ordering is irrelevant; any additional schema not listed in `expected_schemas` will cause the assertion to fail.
***
### FunctionDef check_conn_str(self)
**check_conn_str**: The function of check_conn_str is to validate that the provided PostgreSQL connection string does not reference the target database name and that a connection can be successfully established.

**parameters**:  
· self: Instance of the class containing the attributes `conn_str` (the PostgreSQL connection string) and `dbname` (the name of the database that will be created).

**Code Description**:  
1. The function begins by searching the connection string (`self.conn_str`) for a `dbname` parameter using a regular expression `r'\bdbname=([^ ]+)'`.  
2. If a `dbname` parameter is found and its value matches `self.dbname`, a `ValueError` is raised with the message *'URL should not contain the database to be created'*. This prevents the test from attempting to connect to a database that is supposed to be created later in the workflow.  
3. If the `dbname` check passes, the function attempts to open a connection to the PostgreSQL server using `psycopg2.connect(self.conn_str)`.  
4. The `with` statement ensures that the connection is automatically closed when the block exits. Inside the block, no operations are performed (`pass`).  
5. After exiting the `with` block, an explicit `conn.close()` is called as a safeguard, although the context manager has already closed the connection.  
6. If the connection attempt fails, `psycopg2.connect` raises `psycopg2.OperationalError`, which propagates out of the function.  
7. The function returns `None` on successful validation.

**Note**:  
- The function assumes that `self.conn_str` is a well‑formed PostgreSQL connection string.  
- Only the presence of a `dbname` parameter equal to `self.dbname` triggers a `ValueError`; other parameters are not examined.  
- The explicit `conn.close()` after the `with` block is redundant but harmless; it may be retained for clarity.  
- Any `psycopg2.OperationalError` raised by a failed connection attempt is not caught within the function, so callers must handle it if needed.

**Output Example**:  
When the connection string is valid and a connection can be made, the function completes silently and returns `None`.  

```python
result = instance.check_conn_str()
print(result)   # Output: None
```
***
### FunctionDef test_prep_db(self)
**test_prep_db**: The function of test_prep_db is to invoke the database preparation routine for a specific model version and then verify that the resulting database contains the expected schemas.

**parameters**:  
· self – the instance of the TestPrepareDatabase test case; provides access to configuration attributes such as `model_version`, `conn_str`, and any other state set up in the test’s `setUp` method.

**Code Description**:  
1. The method calls `prepare_database` from `pedsnetdcc.prepdb` with three arguments taken from the test instance:  
   – `self.model_version` – a string representing the PEDSnet model version (e.g., “2.1”).  
   – `self.conn_str` – a libpq‑style PostgreSQL connection string that has permission to create a new database.  
   – The optional arguments `update` and `dcc_only` are omitted, so they default to `False`.  
   This call creates a fresh PEDSnet database, builds all required site and DCC schemas, and applies the standard role‑based permissions. The function returns `True` on success; any exception would cause the test to error out.

2. Immediately after the database has been prepared, the test invokes `self.check()`. The `check` method (also defined in the same test class) builds a connection string that points to the newly created database, constructs the set of schema names that should exist for the given model version and site configuration, introspects the actual schemas present in the database, and asserts that the two sets are equal. If the assertion fails, the test case reports a failure; otherwise the test passes.

3. No explicit return value is defined for `test_prep_db`; as a unittest method, its success is determined by the absence of uncaught exceptions and by the assertion inside `self.check()`.

**Note**:  
- The method relies on the `prepare_database` function to perform all heavy‑lifting (database creation, schema generation, permission grants). It assumes that the test environment provides a PostgreSQL server reachable via `self.conn_str` and that the role “loading_user” exists with sufficient privileges.  
- Because `prepare_database` may raise exceptions on any SQL error, `test_prep_db` does not need additional error handling; a failure will automatically be reported by the unittest framework.  
- The verification performed by `self.check()` uses set comparison, so the order of schemas is irrelevant; any missing or extra schema will cause the test to fail.  
- This test covers the default (full‑site) preparation path. A separate test (`test_prep_db_dcc_only`) exercises the `dcc_only=True` branch.
***
### FunctionDef test_prep_db_dcc_only(self)
**test_prep_db_dcc_only**: The function of test_prep_db_dcc_only is to invoke the database preparation routine in DCC‑only mode and then verify that the resulting database contains exactly the expected schemas for that mode.  

**parameters**:  
· **self** – the instance of the unittest.TestCase subclass `TestPrepareDatabase`.  

**Code Description**:  
1. The method calls `prepare_database` (defined in `pedsnetdcc/prepdb.py`) with three arguments:  
   - `self.model_version` – the model version string supplied by the test fixture.  
   - `self.conn_str` – a libpq‑style connection string that has permission to create a new database.  
   - `dcc_only=True` – a flag that tells `prepare_database` to create only the central DCC schema and to omit all site‑specific schemas.  
   The call returns a boolean success flag, but the test does not inspect the return value because any exception raised by `prepare_database` would cause the test to error automatically.  

2. Immediately after the database has been prepared, the test calls `self.check(dcc_only=True)`. The `check` method (also defined in this test class) builds a connection string that points to the newly created database, constructs the expected set of schema names for a DCC‑only configuration, introspects the actual schemas present in the database, and asserts that the two sets are equal.  

3. By passing `dcc_only=True` to both `prepare_database` and `check`, the test ensures that the preparation logic respects the DCC‑only flag and that the verification logic expects only the schemas that should exist in that scenario (i.e., `dcc_pcornet`, `dcc_pedsnet`, `dcc_id_maps`, plus the shared `vocabulary`, `public`, and `dcc_ids` schemas).  

**Relationship with Callees**:  
- **prepare_database** – creates the new PostgreSQL database, runs the DDL to create the DCC schema, grants the necessary permissions, and skips creation of any site schemas because `dcc_only=True`.  
- **self.check** – validates the outcome of `prepare_database` by comparing the actual schema list in the database against the expected list for a DCC‑only setup. Internally, `check` calls `_conn_str_with_database`, `_sites_and_dcc`, and `_introspect_schemas`, but those details are encapsulated within the test helper.  

**Note**:  
- The test assumes that the test fixture’s `setUp` method has already populated `self.model_version`, `self.conn_str`, and `self.dbname` with valid values and that the PostgreSQL role “loading_user” exists with sufficient privileges.  
- If `prepare_database` raises any exception (for example, due to permission issues or SQL errors), the test will fail before reaching the `check` call.  
- The verification step uses `unittest.TestCase.assertEqual` on sets, so the order of schemas is irrelevant; any missing or extra schema will cause the assertion to fail.  
- This test isolates the DCC‑only path; other tests in the same file cover the full‑site scenario.
***
## FunctionDef fullmatch(regex, string, flags)
**fullmatch**: The function of fullmatch is to emulate Python 3.4’s `re.fullmatch` by matching an entire string against a regular expression.  

**parameters**:  
· **regex**: The regular‑expression pattern (as a string) that the entire target string must satisfy.  
· **string**: The input string to be tested against the pattern.  
· **flags**: (optional) Bitwise OR of regex flag constants from the `re` module (default = 0).  

**Code Description**:  
The function constructs a new pattern by wrapping the supplied `regex` in a non‑capturing group `(?: … )` and appending the end‑of‑string anchor `\Z`. The resulting pattern is `"(?:" + regex + r")\Z"`. The `\Z` anchor asserts that the match must occur at the very end of the string, while the implicit start‑of‑string position is provided by `re.match`, which always attempts to match at the beginning of the string. The composed pattern is then passed to `re.match` together with the original `string` and any `flags`. `re.match` returns a match object if the whole string conforms to the pattern; otherwise it returns `None`. This behavior reproduces what `re.fullmatch` does in later Python versions.  

**Note**:  
- The function relies on `re.match`, so it works only with patterns that are compatible with Python’s regular‑expression engine.  
- Because the pattern is built via string concatenation, any backslashes in `regex` must be correctly escaped by the caller.  
- The `flags` argument is forwarded unchanged to `re.match`, allowing the use of options such as `re.IGNORECASE` or `re.MULTILINE`.  
- This implementation is intended for environments where `re.fullmatch` is unavailable (e.g., Python 3.3 or earlier).  

**Output Example**:  
```python
result = fullmatch(r'\d+', '123')
# result is a re.Match object:
# <re.Match object; span=(0, 3), match='123'>
```
