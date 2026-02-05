## FunctionDef setUpModule
**setUpModule**: The function of setUpModule is to create a module‑level PostgreSQL factory that caches an initialized database for reuse across multiple temporary database instances during testing.  

**parameters**: This function does not accept any parameters.  

**Code Description**:  
- The function begins by declaring `global Postgresql`, which tells Python that the name `Postgresql` refers to a variable at the module scope rather than a local one.  
- It then assigns to `Postgresql` an instance of `testing.postgresql.PostgresqlFactory`.  
- The factory is instantiated with the argument `cache_intialized_db=True`. This flag directs the factory to keep a copy of the database after it has been initialized the first time, allowing subsequent temporary PostgreSQL clusters to start from this cached state instead of performing a full initialization each time.  
- As a result, any test that needs a fresh PostgreSQL instance can obtain one from this factory, benefiting from faster startup times while still working with a clean, pre‑initialized database.  

**Note**:  
- The function must be executed before any test that relies on the `Postgresql` factory; typically the test runner calls `setUpModule` automatically when the module is imported.  
- The `testing.postgresql` package must be installed in the environment; otherwise an `ImportError` will be raised.  
- Because `Postgresql` is a global variable, it can be accessed by other test functions or fixtures within the same module, but care should be taken not to reassign it unintentionally.  
- The caching behavior assumes that the initial database state is suitable for all tests; if a test requires a different schema or data, it should create its own isolated instance or reset the cache accordingly.
## FunctionDef tearDownModule
**tearDownModule**: The function of tearDownModule is to clear the cached initialized PostgreSQL database after all tests in the module have finished.

**parameters**: The parameters of this Function.  
· *None*: tearDownModule does not accept any arguments.

**Code Description**:  
The function consists of a single operational step. It calls the class method `clear_cache()` on the `Postgresql` class. This method is responsible for removing any cached connections or state that were created during the test suite execution. By invoking `Postgresql.clear_cache()` at the end of the test module, the function ensures that subsequent test runs start with a clean slate, preventing interference from leftover database connections or cached data.

**Note**:  
- This teardown function is automatically recognized by the unittest framework when placed at the module level; it will be executed once after all test cases in the module have been run.  
- Ensure that the `Postgresql` class provides a `clear_cache` method; otherwise, an `AttributeError` will be raised.  
- No return value is produced; the purpose is solely side‑effect cleanup.
## ClassDef AddPrimaryKeysTest
**AddPrimaryKeysTest**: The function of AddPrimaryKeysTest is to verify that primary key constraints are correctly added to tables in a PostgreSQL database for a given OMOP CDM model version.

**attributes**:
· postgresql – an instance of the temporary PostgreSQL server created for the test suite.  
· dburi – the database URI string returned by the temporary PostgreSQL server.  
· conn_str – a connection string derived from `dburi` using `make_conn_str`.  
· model_version – the CDM model version string used for the test (set to `'2.3.0'`).  
· engine – a SQLAlchemy engine created from `dburi`.  
· metadata – SQLAlchemy MetaData object populated with the CDM schema for `model_version` via `stock_metadata`.

**Code Description**:
The class inherits from `unittest.TestCase` and defines a series of unit tests that exercise the `add_primary_keys` function.

- **setUp(self)**: Executed before each test method. It starts a temporary PostgreSQL instance, builds the database URI, creates a SQLAlchemy engine, loads the CDM metadata for the specified `model_version`, and creates all tables in the temporary database. This provides an isolated environment for each test.

- **tearDown(self)**: Executed after each test method. It stops the temporary PostgreSQL instance, ensuring that resources are released and the temporary database is removed.

- **test_pk_names(self)**: Retrieves the list of primary key objects for the current `model_version` via `_primary_keys_from_model_version`. It extracts their names and asserts that a predefined set of expected primary key names (e.g., `'xpk_death'`, `'xpk_person'`, etc.) are present. This confirms that the primary‑key definitions are correctly loaded from the model.

- **_make_update_tables(self, conn_str)**: Helper method used by `test_add_primary_keys`. It iterates over all primary‑key objects for the model version, constructs a `CREATE TABLE … AS SELECT * FROM …` statement for each underlying table, and executes the statement using the `Statement` utility. It asserts that each execution completes without error, thereby creating duplicate tables that will later be modified.

- **_check_primary_keys(self, dburi)**: Helper method that validates that primary‑key constraints exist after `add_primary_keys` has been run. It creates a new SQLAlchemy engine from `dburi`, iterates over all tables defined in `self.metadata` (excluding vocabulary tables listed in `VOCAB_TABLES`), reflects each table from the database, and asserts that the reflected table’s primary key name matches the expected name from the original metadata.

- **test_add_primary_keys(self)**: The primary functional test. It creates a new schema named `'target'`, adjusts the search path to include this schema and `public`, and generates duplicate tables in the target schema via `_make_update_tables`. It then calls `add_primary_keys` with the modified connection string and the model version. After the operation, it builds a new database URI that forces the search path to the target schema and invokes `_check_primary_keys` to verify that the primary‑key constraints were added correctly.

- **test_double_add_primary_keys(self)**: Ensures that attempting to add primary keys a second time raises an error. It first runs `test_add_primary_keys` to add the keys, then re‑creates the connection string with the same search path and calls `add_primary_keys` again inside a `with self.assertRaises(psycopg2.ProgrammingError)` block. The test passes only if a `ProgrammingError` is raised, confirming that the function does not allow duplicate primary‑key additions.

**Note**:
- The tests rely on auxiliary utilities (`Postgresql`, `make_conn_str`, `Statement`, `conn_str_with_search_path`, `_primary_keys_from_model_version`, `add_primary_keys`, `stock_metadata`, and the constant `VOCAB_TABLES`) that must be available in the test environment.  
- The temporary PostgreSQL instance provides isolation but may increase test execution time; ensure sufficient resources when running the full test suite.  
- The `test_double_add_primary_keys` expects a `psycopg2.ProgrammingError`; any change in the underlying exception type will cause the test to fail.  
- The search path manipulation is essential for directing `add_primary_keys` to the correct schema; altering the order or contents of the search path may affect test outcomes.
### FunctionDef setUp(self)
**setUp**: The function of setUp is to create a temporary PostgreSQL database, configure a SQLAlchemy engine, and load the stock PEDSnet metadata for the specified model version.

**parameters**:  
· self: the instance of the test case (AddPrimaryKeysTest) on which the setup is performed.

**Code Description**:  
1. `self.postgresql = Postgresql()` – Instantiates a temporary PostgreSQL server (provided by the `testing.postgresql` package). The server runs in a temporary directory and is automatically cleaned up when the test finishes.  
2. `self.dburi = self.postgresql.url()` – Retrieves the connection URI of the temporary server, e.g. `postgresql://user:pass@host:port/dbname`.  
3. `self.conn_str = make_conn_str(self.dburi)` – Calls **make_conn_str** (defined in `pedsnetdcc/utils.py`) to translate the URI into a libpq‑compatible connection string. This string is later used by command‑line utilities and other test helpers that expect a raw connection string rather than a SQLAlchemy URL.  
4. `self.model_version = '2.3.0'` – Sets the PEDSnet data‑model version that the test will operate against.  
5. `self.engine = sqlalchemy.create_engine(self.dburi)` – Creates a SQLAlchemy `Engine` bound to the temporary PostgreSQL instance. The engine provides DB‑API connections for subsequent operations.  
6. `self.metadata = stock_metadata(self.model_version)` – Calls **stock_metadata** (also in `pedsnetdcc/utils.py`) to obtain a fresh `MetaData` object that contains the untransformed PEDSnet schema for version 2.3.0. The function contacts the data‑model service, downloads the schema definition, and populates the `MetaData` with tables, columns, primary‑key constraints, foreign‑key constraints, etc.  
7. `self.metadata.create_all(self.engine)` – Instructs SQLAlchemy to create all tables defined in the `MetaData` within the temporary database. This populates the database with the stock schema, providing a clean baseline for the primary‑key tests.

**Note**:  
- The temporary PostgreSQL instance is isolated per test run; no external database is affected.  
- `make_conn_str` produces a connection string that includes host, port, dbname, user, password, and any query parameters present in the original URI. It does not modify the database itself.  
- `stock_metadata` does not cache results; each call re‑downloads the model definition. The returned `MetaData` reflects the stock (untransformed) schema, which is then materialized in the temporary database by `create_all`.  
- Because the test creates its own engine and metadata, any changes made by the test (e.g., adding primary keys) are confined to the temporary database and are discarded when the test case tears down.  
- The method does not accept additional arguments; all configuration is hard‑coded for the purpose of the test suite.
***
### FunctionDef tearDown(self)
**tearDown**: The function of tearDown is to stop and clean up the temporary PostgreSQL instance used during the test.  

**parameters**: This method does not accept any parameters.  

**Code Description**:  
- The method is defined as an instance method of the test class (likely a subclass of `unittest.TestCase`).  
- Inside the method, a single operation is performed: `self.postgresql.stop()`.  
- `self.postgresql` is an object that represents a running PostgreSQL server that was started in the test’s `setUp` method.  
- Calling `stop()` on this object terminates the PostgreSQL process and releases any resources (such as open sockets, temporary files, or allocated memory) associated with the test database.  
- By stopping the database in `tearDown`, the test ensures that each test case runs in isolation and that no leftover state persists between tests, which helps maintain test reliability and prevents resource leaks.  

**Note**:  
- This method must be called after each test case execution; the unittest framework automatically invokes `tearDown` if it is defined.  
- Ensure that `self.postgresql` has been correctly instantiated before `tearDown` runs; otherwise, calling `stop()` may raise an AttributeError.  
- If additional cleanup (e.g., removing temporary files) is required, it should be added to this method after the `stop()` call.
***
### FunctionDef test_pk_names(self)
**test_pk_names**: The function of test_pk_names is to verify that the set of primary‑key constraint names returned for a given model version includes all expected PEDSnet primary‑key names.

**parameters**: This method does not accept any external parameters; it operates on the instance attribute `self.model_version` that is set up by the test case.

**Code Description**:  
1. The test calls the internal helper `_primary_keys_from_model_version`, passing `self.model_version`. This helper returns a list of SQLAlchemy `PrimaryKeyConstraint` objects that belong to the stock PEDSnet schema for the specified version, excluding vocabulary tables.  
2. From each constraint object the test extracts the `name` attribute, building the list `pk_names`.  
3. A hard‑coded set `some_expected_pk_names` contains the canonical primary‑key constraint names that should be present in a complete PEDSnet model (e.g., `xpk_person`, `xpk_condition_occurrence`, etc.).  
4. The test iterates over `some_expected_pk_names` and uses `self.assertIn` to assert that each expected name appears in `pk_names`. If any expected name is missing, the unit test will fail, indicating a discrepancy in the primary‑key enumeration logic.

**Relationship with Callees**:  
- **Callee**: `_primary_keys_from_model_version` (located in `pedsnetdcc/primary_keys.py`). This function supplies the raw primary‑key constraint objects that the test inspects. The correctness of `test_pk_names` therefore depends directly on the accurate operation of `_primary_keys_from_model_version`.  
- **Caller**: `test_pk_names` is invoked automatically by the unittest framework as part of the `AddPrimaryKeysTest` test suite. No other project components call this method.

**Note**:  
- The test assumes that the stock model version referenced by `self.model_version` contains exactly the primary‑key names listed in `some_expected_pk_names`. If the PEDSnet data model is updated with new tables or renamed constraints, the test must be updated accordingly.  
- Vocabulary tables are deliberately excluded by `_primary_keys_from_model_version`; consequently, any primary‑key constraints that might exist on vocabulary tables are not validated by this test.  
- Because the test relies on a network call inside `stock_metadata` (used by `_primary_keys_from_model_version`), failures unrelated to primary‑key logic (e.g., connectivity issues) can cause this test to error rather than fail. Consider mocking the metadata source in environments where network access is unreliable.
***
### FunctionDef _make_update_tables(self, conn_str)
**_make_update_tables**: The function of _make_update_tables is to create duplicate tables for each primary‑key‑defined table in the target schema, using a simple “CREATE TABLE … AS SELECT * FROM …” statement, and to verify that each creation succeeds without error.

**parameters**:  
· self: the instance of the test case (AddPrimaryKeysTest) on which the method is invoked.  
· conn_str (str): a libpq‑style PostgreSQL connection string that points to the database where the duplicate tables will be created.

**Code Description**:  
1. **Retrieve primary‑key constraints** – The method calls `_primary_keys_from_model_version(self.model_version)` to obtain a list of `PrimaryKeyConstraint` objects that belong to the stock PEDSnet model for the version stored in `self.model_version`. A list comprehension filters out any falsy entries, producing the list `pks`.  
2. **Iterate over each primary‑key constraint** – For every `pk` in `pks` the method extracts the associated table name via `pk.table.name`.  
3. **Build a CREATE‑TABLE‑AS‑SELECT statement** – The template string `'create table {tbl} as select * from {tbl}'` is formatted with the table name, yielding a SQL command that creates a new table with the same name as the original and populates it with all rows from the original table. Because the new table name is identical to the source, the command effectively creates a copy of the original table in the current search_path (the test schema).  
4. **Execute the statement** – A `Statement` object is instantiated with the generated SQL and its `execute(conn_str)` method is called. `Statement.execute` opens a new database connection using the supplied `conn_str`, runs the SQL in autocommit mode, and returns the same `Statement` instance populated with execution results and any error (`stmt.err`).  
5. **Assert successful execution** – The test method immediately checks `self.assertIsNone(stmt.err)`. If the statement produced an error (e.g., the table already exists, permission issues, or connection problems), the assertion will fail and the test will stop, signalling that the update‑table preparation step did not succeed.

**Reference Relationships**:  
- **Callers** – The method is invoked by `AddPrimaryKeysTest.test_add_primary_keys`. In that test the workflow first creates a new schema, adjusts the search_path, calls `_make_update_tables` to set up the duplicate tables, and then calls `add_primary_keys` to re‑apply primary‑key constraints.  
- **Callees** –  
  * `_primary_keys_from_model_version` (pedsnetdcc/primary_keys.py) – supplies the list of primary‑key constraint objects for the specified model version.  
  * `Statement.execute` (pedsnetdcc/db.py) – runs the generated DDL statement against the database and records any error.  
  * `self.assertIsNone` – the unittest assertion method used to enforce that no error occurred.  

**Note**:  
- The SQL uses the same table name for the new table as for the source table; therefore the command succeeds only when the target schema’s search_path does not already contain a table with that name. In the test suite the schema is freshly created, guaranteeing that the names are available.  
- Because `Statement.execute` opens a new connection with autocommit, the CREATE‑TABLE statements are applied immediately and cannot be rolled back by the test.  
- Any failure in `_primary_keys_from_model_version` (e.g., an invalid model version) will result in an empty `pks` list, causing the method to perform no work and the subsequent primary‑key addition step to operate on an unchanged schema.  
- The method does not clean up the tables it creates; cleanup is handled by the overall test teardown (dropping the temporary schema).  
- The function is intended solely for test preparation; it should not be used in production code where naming collisions or schema side‑effects could cause data loss.
***
### FunctionDef _check_primary_keys(self, dburi)
**_check_primary_keys**: The function verifies that every non‑vocabulary table in the test schema has a primary key whose name matches the expected name defined in the model metadata.

**parameters**:  
· **self** – the instance of the test case class (`AddPrimaryKeysTest`). It provides access to the SQLAlchemy `MetaData` object (`self.metadata`) that describes the expected schema.  
· **dburi** – a SQLAlchemy database URI string that includes any required connection options (e.g., a `search_path` parameter). This URI is used to create a temporary engine for inspecting the live database.

**Code Description**:  
1. **Engine creation** – The method starts by constructing a new SQLAlchemy engine with `sqlalchemy.create_engine(dburi)`. An engine is required because SQLAlchemy’s reflection utilities need a live connection to the database; a plain connection string is insufficient.  
2. **Iterating over expected tables** – `self.metadata.sorted_tables` yields the tables defined in the model metadata in dependency order. The loop processes each table `t`.  
3. **Skipping vocabulary tables** – Tables whose names appear in the global `VOCAB_TABLES` collection are ignored because they are managed separately and are not subject to primary‑key validation in this test.  
4. **Filtering tables with a primary key** – Only tables that already have a primary key defined in the metadata (`t.primary_key`) are examined. This ensures the test does not attempt to validate tables that are intentionally key‑less.  
5. **Reflecting the live table** – For each qualifying table, a fresh `sqlalchemy.Table` object is created with `autoload=True` and `autoload_with=new_engine`. This forces SQLAlchemy to read the actual table definition from the database, including its primary‑key constraint.  
6. **Assertion** – The test asserts that the reflected table (`tbl`) possesses a primary key (`tbl.primary_key`) and that the name of this primary key (`tbl.primary_key.name`) is identical to the name recorded in the metadata (`t.primary_key.name`). The assertion is performed with `self.assertTrue`, causing the test to fail if the condition is not met.  

**Relationship with callers**:  
`_check_primary_keys` is invoked by the test method `test_add_primary_keys`. After the test creates a temporary schema, populates it with tables, and runs the production function `add_primary_keys` to add missing primary‑key constraints, the test builds a new database URI that includes the appropriate `search_path`. It then calls `_check_primary_keys` with this URI to confirm that the primary keys were added correctly. Thus, `_check_primary_keys` serves as a verification step that validates the side effects of `add_primary_keys` within the same test case.

**Note**:  
- The method relies on the global `VOCAB_TABLES` constant; ensure this list accurately reflects tables that should be excluded from primary‑key checks.  
- Because the method uses `autoload=True`, it requires that the database user associated with `dburi` has sufficient privileges to read table definitions.  
- The assertion checks only the primary‑key **name**, not the column composition. If column order or composition is important, additional checks would be needed.  
- This helper is intended for unit‑test environments; it should not be used in production code where raising assertions could disrupt normal operation.
***
### FunctionDef test_add_primary_keys(self)
**test_add_primary_keys**: The function of test_add_primary_keys is to verify that primary‑key constraints can be added to a freshly created schema and that the resulting tables contain the expected primary‑key definitions.

**parameters**:  
· self – the instance of the unittest.TestCase subclass AddPrimaryKeysTest on which the method operates.

**Code Description**:  
1. **Create a target schema** – The method builds a SQL statement `create schema target` and executes it with `Statement(...).execute(self.conn_str)`. The `execute` method opens a new PostgreSQL connection, runs the DDL in autocommit mode, and stores any exception in `stmt.err`. The test immediately asserts that `stmt.err` is `None`, confirming that the schema was created successfully.  

2. **Configure the search_path** – A PostgreSQL `search_path` string is assembled as `'target,public'`. The helper `conn_str_with_search_path` is called with the original connection string (`self.conn_str`) and this search_path. The helper returns a new libpq‑style connection string that includes an `options` parameter setting `search_path=target,public`. This new connection string (`new_conn_str`) ensures that subsequent statements operate inside the newly created schema.  

3. **Create duplicate tables** – The private method `_make_update_tables(new_conn_str)` is invoked. It iterates over all primary‑key constraints defined for the current model version (`self.model_version`), and for each table executes a `CREATE TABLE <tbl> AS SELECT * FROM <tbl>` statement in the target schema. Each execution is performed with `Statement(...).execute`, and the test asserts that no error occurs. The result is a set of tables in the target schema that have the same data as the original tables but lack primary‑key constraints.  

4. **Add primary‑key constraints** – The production function `add_primary_keys(new_conn_str, self.model_version)` is called. This function generates `ALTER TABLE … ADD CONSTRAINT` statements for every primary‑key defined in the model metadata (excluding vocabulary tables) and runs them in parallel. If any statement fails (except for benign duplicate‑key errors when `force=True`), the function raises an exception.  

5. **Build a database URI with search_path** – The test constructs a SQLAlchemy‑compatible URI (`new_dburi`) by appending a URL‑encoded `options` query parameter to the original `self.dburi`. The encoding is performed with `urllib.quote_plus`, embedding the same `search_path` used for the connection string. This URI is required because SQLAlchemy’s engine creation expects a URL rather than a raw libpq connection string.  

6. **Verify that primary keys were added** – The private method `_check_primary_keys(new_dburi)` is called. It creates a temporary SQLAlchemy engine from `new_dburi`, reflects each non‑vocabulary table defined in the test metadata, and asserts that the reflected table possesses a primary‑key whose name matches the name recorded in the metadata. Any mismatch causes the test to fail.  

7. **Overall outcome** – If all assertions pass and no exception is raised, the test confirms that `add_primary_keys` correctly adds primary‑key constraints to a schema that initially contains duplicate tables without keys.

**Reference Relationships**:  
- **Callees** – `Statement.execute` (runs the `CREATE SCHEMA` DDL), `conn_str_with_search_path` (injects the search_path into the connection string), `_make_update_tables` (creates duplicate tables), `add_primary_keys` (adds the primary‑key constraints), `urllib.quote_plus` (URL‑encodes the options clause), `_check_primary_keys` (validates the resulting constraints).  
- **Callers** – The method is invoked directly by the test runner as part of the `AddPrimaryKeysTest` suite. It is also called by `test_double_add_primary_keys`, which first runs `test_add_primary_keys` and then attempts to add the same primary keys a second time to verify error handling.  

**Note**:  
- The test creates a fresh schema named `target`; therefore it must be run in an environment where the test user has permission to create and drop schemas.  
- All DDL statements are executed with autocommit, so changes are permanent for the duration of the test and are cleaned up only by the test teardown (dropping the temporary schema).  
- The verification step checks only the primary‑key **name**, not the column composition; the underlying `add_primary_keys` function is responsible for generating the correct column list.  
- If `add_primary_keys` raises an exception, the test will fail immediately, providing a clear indication that the primary‑key addition logic is broken.  
- The use of `conn_str_with_search_path` ensures that both the raw libpq connection string (used by `Statement.execute`) and the SQLAlchemy URI (used by `_check_primary_keys`) operate within the same schema context.
***
### FunctionDef test_double_add_primary_keys(self)
**test_double_add_primary_keys**: The function of test_double_add_primary_keys is to verify that invoking add_primary_keys a second time on a schema that already contains the required primary‑key constraints raises a psycopg2.ProgrammingError when the force flag is not set.

**parameters**  
· self – the instance of the unittest.TestCase subclass AddPrimaryKeysTest on which the method operates.

**Code Description**  
1. The method begins by calling self.test_add_primary_keys().  
   – test_add_primary_keys creates a fresh schema named target, populates it with duplicate tables that lack primary‑key constraints, and then calls add_primary_keys to add the constraints successfully.  
   – After this call the target schema already contains all primary‑key definitions defined for the current model version.  

2. The test prepares the same connection context that was used in the first run:  
   – target_schema is set to the literal string 'target'.  
   – search_path is built as 'target,public' so that subsequent statements operate inside the newly created schema while still having access to the public vocabulary schema.  
   – new_conn_str is obtained by calling conn_str_with_search_path(self.conn_str, search_path). This utility injects the options parameter with the desired search_path into the original libpq connection string.  

3. The core verification uses the unittest assertRaises context manager:  

   ```python
   with self.assertRaises(psycopg2.ProgrammingError):
       add_primary_keys(new_conn_str, self.model_version)
   ```  

   – The call to add_primary_keys is executed with the same connection string and model version as before, but without the force=True argument.  
   – Because the primary‑key constraints already exist in the target schema, the underlying DDL statements generated by add_primary_keys attempt to add duplicate primary‑key constraints.  
   – The add_primary_keys implementation treats duplicate‑key errors (PostgreSQL error code 42P16) as fatal unless force is True, so it propagates the database error as a psycopg2.ProgrammingError.  
   – The assertRaises block confirms that this exception is indeed raised, indicating correct error handling for repeated primary‑key addition.  

4. No further assertions or cleanup are performed inside this method; the surrounding test framework’s tearDown method will drop the temporary schema after the test suite completes.

**Relationship with callees**  
- Calls self.test_add_primary_keys (the sibling test method) to set up the initial state.  
- Calls conn_str_with_search_path (to construct a connection string that includes the appropriate search_path).  
- Calls add_primary_keys (the production function that generates and executes ALTER TABLE … ADD CONSTRAINT statements).  
- Relies on the psycopg2.ProgrammingError exception class to capture the expected database error.  

**Note**  
- The test deliberately does **not** pass force=True to add_primary_keys; this is essential because the function only suppresses duplicate‑key errors when force is true.  
- The test assumes that the database user has permission to create schemas, tables, and constraints, and that the underlying PostgreSQL server returns error code 42P16 for duplicate primary‑key definitions.  
- Because the test re‑uses the same search_path and connection string, it validates that the search‑path handling performed by conn_str_with_search_path remains consistent across multiple calls.  
- If the implementation of add_primary_keys changes to silently ignore duplicate primary‑key errors without requiring force=True, this test will fail, signaling a behavioral change.
***
