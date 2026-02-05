## FunctionDef setUpModule
**setUpModule**: The function of setUpModule is to create and expose a PostgreSQL factory that caches an initialized database for reuse across multiple temporary database instances during testing.

**parameters**: This function does not accept any parameters.

**Code Description**:  
- The function begins by declaring `Postgresql` as a global variable, allowing the assignment to affect the module‑level namespace.  
- It then assigns `Postgresql` to an instance of `testing.postgresql.PostgresqlFactory` with the argument `cache_initialized_db=True`.  
- `PostgresqlFactory` is a helper class provided by the `testing.postgresql` package; it creates temporary PostgreSQL server instances on demand.  
- The `cache_initialized_db=True` flag instructs the factory to keep a copy of a freshly initialized database in memory (or on disk) after the first creation, so subsequent instances can be started much faster by reusing this cached state instead of performing a full initialization each time.  
- By exposing this factory as a global variable, other test modules can import `Postgresql` from this module and use it to spin up isolated PostgreSQL instances for their own test cases, ensuring a consistent and efficient test environment.

**Note**:  
- The function must be executed before any test that relies on the `Postgresql` factory; typically the test runner calls `setUpModule` automatically at module import time.  
- The `testing.postgresql` package must be installed in the test environment; otherwise an `ImportError` will be raised.  
- Because the factory caches the initialized database, changes made to the database in one test will not affect subsequent tests that obtain a new instance from the factory, preserving test isolation.  
- Modifying the global `Postgresql` variable after this setup may interfere with other tests that expect the cached factory behavior.
## FunctionDef tearDownModule(self)
**tearDownModule**: The function of tearDownModule is to clean up the test environment by clearing the cached initialized PostgreSQL database after all tests have run.  

**parameters**:  
· self: The instance of the test class invoking the teardown; required by the test framework to access class attributes if needed.  

**Code Description**:  
The method consists of a single operational step. First, it includes a comment that explains its purpose: “Clear cached init-ed database at end of tests.” The subsequent line calls the class method `clear_cache()` on the `Postgresql` utility class. This call removes any in‑memory or on‑disk cache that was created when the test database was initialized, ensuring that subsequent test runs start with a fresh, uncached state. No return value is produced, and the method relies entirely on the side effect of the `clear_cache` call.  

**Note**:  
- This teardown is intended to be executed once after the entire test module has completed; it should not be called manually within individual test cases.  
- The effectiveness of the cleanup depends on the correct implementation of `Postgresql.clear_cache()`. If that method does not fully reset the cached state, residual data may affect later tests.  
- Ensure that any resources allocated during test setup that rely on the PostgreSQL cache are no longer needed before this method runs.
## ClassDef TestMerge
**TestMerge**: The function of TestMerge is to verify that the data‑merge and data‑clear utilities correctly create and remove tables in a temporary PostgreSQL database for the PEDSnet data model.

**attributes**:  
· model_version – string indicating the PEDSnet model version used for the tests (set to `'2.3.0'`).  
· metadata – SQLAlchemy MetaData object containing the schema definitions for the specified model version, after being processed by all transforms in `TRANSFORMS`.  
· postgresql – a `Postgresql` helper instance that starts a temporary PostgreSQL server in a temporary directory.  
· dburi – the database URI returned by `self.postgresql.url()`.  
· conn_str – a connection string built from `dburi` using `make_conn_str`.  
· engine – a SQLAlchemy engine created from `dburi` for executing DDL statements and reflecting metadata.

**Code Description**:  
The class inherits from `unittest.TestCase`, making it a standard unit‑test suite.  

- **setUp**: Executed before each test method. It fixes the model version, obtains the base metadata via `stock_metadata`, and applies every transformation in the global `TRANSFORMS` list to the metadata. It then launches a temporary PostgreSQL instance (`Postgresql()`), stores its connection URI, builds a plain connection string, and creates a SQLAlchemy engine bound to that URI. These resources are reused by the test methods.  

- **tearDown**: Executed after each test method. It stops the temporary PostgreSQL server, ensuring that the temporary database is destroyed and no resources are left dangling.  

- **test_merge**:  
  1. Creates a separate schema for each site listed in `SITES` plus a `dcc` schema, using the pattern `<site>_pedsnet`. It also creates a `vocabulary` schema.  
  2. For every site schema, it creates all non‑vocabulary tables defined in `self.metadata` (i.e., tables whose names are not in `VOCAB_TABLES`). The table objects are temporarily assigned the appropriate schema name, created via `table.create(self.engine)`, and then the schema attribute is cleared.  
  3. For the `vocabulary` schema, it creates only the tables listed in `VOCAB_TABLES`.  
  4. Calls `merge_site_data(self.model_version, self.conn_str)`, which is expected to copy or consolidate site‑specific tables into the central `dcc_pedsnet` schema.  
  5. Reflects the resulting `dcc_pedsnet` schema into a new `MetaData` instance and asserts that the core tables `person`, `visit_occurrence`, and `location` exist in that schema. It also asserts that the `concept` table (a vocabulary table) is **not** present, confirming that only non‑vocabulary tables are merged.  

- **test_clear**:  
  1. Creates only the `dcc_pedsnet` schema.  
  2. Populates it with all non‑vocabulary tables from the metadata, similar to the merge test but without any site schemas.  
  3. Performs a sanity check by reflecting the schema and confirming that the `person` table exists.  
  4. Calls `clear_dcc_data(self.model_version, self.conn_str)`, which should drop all tables in the `dcc_pedsnet` schema.  
  5. Reflects the schema again and verifies that it contains zero tables, confirming that the clear operation removed all data.  

Overall, the test suite validates that the merge utility correctly aggregates site‑specific data into the DCC schema while excluding vocabulary tables, and that the clear utility fully removes all DCC tables.

**Note**:  
- The tests rely on global constants `SITES`, `TRANSFORMS`, `VOCAB_TABLES`, and helper functions `stock_metadata`, `make_conn_str`, `Statement`, `merge_site_data`, and `clear_dcc_data`. Ensure these are correctly imported and configured before running the tests.  
- The temporary PostgreSQL instance is created for each test method; therefore, the tests may be slower than pure in‑memory tests.  
- The schema names are constructed by appending `'_pedsnet'` to site identifiers; any change to this naming convention must be reflected in the test expectations.  
- The `tearDown` method only stops the PostgreSQL server; if additional cleanup (e.g., file system temp directories) is required by the `Postgresql` helper, it should be handled within that helper.
### FunctionDef setUp(self)
**setUp**: The function of setUp is to prepare a fresh test environment by loading the PEDSnet model metadata, applying all registered schema transforms, and creating a temporary PostgreSQL database with a SQLAlchemy engine.

**parameters**:  
· **self** – the instance of the TestMerge test case; no additional arguments are required.

**Code Description**:  
1. **Model version selection** – The method fixes the model version to the string `'2.3.0'` and stores it in `self.model_version`.  
2. **Base metadata acquisition** – It calls `stock_metadata(self.model_version)` to retrieve a `sqlalchemy.MetaData` object that represents the unmodified PEDSnet schema for version 2.3.0. The returned object is assigned to `self.metadata`.  
3. **Schema transformation** – The global list `TRANSFORMS` contains Transform subclasses that implement site‑specific or feature‑specific alterations. For each transform `t` in this list, the method invokes `t.modify_metadata(self.metadata)`. The `modify_metadata` class method iterates over all non‑vocabulary tables in the supplied metadata and calls the subclass’s `modify_table` hook, mutating the metadata in‑place. The potentially mutated metadata is reassigned to `self.metadata` after each call, allowing successive transforms to build on the previous changes.  
4. **Temporary PostgreSQL instance** – `self.postgresql = Postgresql()` creates a new PostgreSQL server that lives in a temporary directory (managed by the `testing.postgresql` package). This server is isolated from any real database and will be torn down automatically when the test finishes.  
5. **Connection string construction** – `self.dburi = self.postgresql.url()` obtains the SQLAlchemy‑style URI for the temporary server. `self.conn_str = make_conn_str(self.dburi)` converts that URI into a libpq‑compatible connection string using the `make_conn_str` utility. This string is useful for tools that expect the traditional “host=… port=… dbname=…” format.  
6. **SQLAlchemy engine creation** – `self.engine = sqlalchemy.create_engine(self.dburi)` builds a SQLAlchemy `Engine` bound to the temporary database. The engine is stored on the test instance for use by individual test methods to execute DDL/DML statements against the fresh schema.

**Note**:  
- The method is executed before each test method in the `TestMerge` class, guaranteeing that every test starts with a clean, fully transformed schema and an isolated PostgreSQL instance.  
- Because `modify_metadata` mutates the `MetaData` object in‑place, the same `self.metadata` reference is reused after each transform; callers should not expect the original stock metadata to remain unchanged.  
- The temporary PostgreSQL server is created without any pre‑loaded data; tests that require fixture data must insert it explicitly after `setUp` runs.  
- The connection string produced by `make_conn_str` includes any options derived from the URI; if additional options (e.g., a custom `search_path`) are needed, they must be supplied when calling `make_conn_str`.  
- Cleanup of the temporary server and SQLAlchemy engine is typically handled in a corresponding `tearDown` method (not shown here).
***
### FunctionDef tearDown(self)
**tearDown**: The function of tearDown is to stop and clean up the temporary PostgreSQL instance used during the test.  

**parameters**: This method does not accept any parameters.  

**Code Description**:  
- The method is defined as an instance method of the `TestMerge` test case class.  
- Inside the method, a single operation is performed: `self.postgresql.stop()`.  
- `self.postgresql` is an object that represents a PostgreSQL server that was started for the test suite (typically created in the `setUp` method).  
- Calling the `stop()` method on this object terminates the PostgreSQL process and releases any resources associated with it, ensuring that the test environment is cleaned up after the test run.  

**Note**:  
- This teardown step is essential to prevent orphaned database processes that could interfere with subsequent tests or consume system resources.  
- Ensure that `self.postgresql` has been properly initialized before `tearDown` is invoked; otherwise, calling `stop()` may raise an exception.
***
### FunctionDef test_merge(self)
**test_merge**: The function of test_merge is to verify that the merge_site_data routine correctly creates merged tables in the DCC schema while leaving vocabulary tables untouched.

**parameters**: This test method takes no explicit parameters; it operates on the instance attributes provided by the TestMerge unittest class (e.g., self.conn_str, self.engine, self.metadata, self.model_version).

**Code Description**  
1. **Schema creation** – The test iterates over each site identifier in `SITES` plus the special identifier `'dcc'`. For each, it executes a `CREATE SCHEMA <site>_pedsnet` statement via `Statement(...).execute(self.conn_str)`. This establishes isolated schemas for each site and for the DCC target. A separate `vocabulary` schema is also created. The `Statement.execute` method opens a new PostgreSQL connection, runs the DDL in autocommit mode, logs any connection errors, and closes the connection.

2. **Data‑table creation in site schemas** – For every site in `SITES`, the test creates all non‑vocabulary tables defined in `self.metadata`. It obtains each table object, temporarily sets its `schema` attribute to `<site>_pedsnet`, calls `table.create(self.engine)` to emit a `CREATE TABLE` statement, and then resets the schema attribute to `None`. This populates each site schema with the expected PEDSnet tables.

3. **Vocabulary‑table creation** – All tables listed in `VOCAB_TABLES` are created once in the `vocabulary` schema using the same temporary schema assignment technique.

4. **Merge execution** – The core operation is invoked by calling `merge_site_data(self.model_version, self.conn_str)`. This function (described elsewhere) builds and executes `CREATE TABLE dcc_pedsnet.<table> AS SELECT … UNION ALL …` statements that combine rows from every site schema into the central DCC schema. It also runs optional post‑merge steps such as setting tables to LOGGED, adding primary keys, NOT‑NULL constraints, indexes, foreign keys, and VACUUM.

5. **Reflection of the DCC schema** – After the merge completes, a new `sqlalchemy.MetaData` object is instantiated with `schema='dcc_pedsnet'`. The `reflect` call inspects the live database and populates `md.tables` with all tables that now exist in the DCC schema.

6. **Verification of merged tables** – The test asserts that three core clinical tables—`person`, `visit_occurrence`, and `location`—are present in the reflected metadata by checking for the keys `'dcc_pedsnet.person'`, `'dcc_pedsnet.visit_occurrence'`, and `'dcc_pedsnet.location'`. These assertions confirm that the merge created the expected DCC tables.

7. **Verification that vocabulary tables were not merged** – The test also asserts that the `concept` table (a vocabulary table) is **not** present in the DCC schema (`'dcc_pedsnet.concept' not in md.tables`). This ensures that `merge_site_data` correctly excluded vocabulary tables from the merge process.

**Note**  
- The test relies on the global constants `SITES`, `VOCAB_TABLES`, and the `self.metadata` object that describes the full PEDSnet model for the given `model_version`.  
- All DDL statements are executed with autocommit via `Statement.execute`; therefore, any failure to create a schema or table will raise an exception and cause the test to abort.  
- The test does not clean up the created schemas or tables; it is intended to run in an isolated test database that is discarded after the test suite finishes.  
- Because `merge_site_data` is called with default flags, the test exercises the full merge pipeline, including table creation, logging, primary‑key addition, index creation, foreign‑key addition, and VACUUM. Any change in those default behaviors may require updates to the test expectations.
***
### FunctionDef test_clear(self)
**test_clear**: The function of test_clear is to verify that the clear_dcc_data routine correctly removes all non‑vocabulary tables from a freshly created DCC schema.

**parameters**: The parameters of this Function.  
· **self**: the TestMerge instance that provides the test fixture attributes (conn_str, engine, metadata, model_version) used throughout the test.

**Code Description**:  
1. **Create the DCC schema** – The test starts by issuing a raw SQL command `CREATE SCHEMA dcc_pedsnet` through the `Statement('CREATE SCHEMA dcc_pedsnet').execute(self.conn_str)` call. `Statement.execute` opens a new PostgreSQL connection using the supplied connection string, runs the DDL in autocommit mode, and then closes the connection. This ensures that a clean, empty schema named *dcc_pedsnet* exists for the remainder of the test.  

2. **Populate the schema with non‑vocab tables** – The test iterates over every table defined in the test metadata (`self.metadata.tables`) except those listed in the global `VOCAB_TABLES` collection. For each selected table:  
   - The table object's `schema` attribute is temporarily set to `'dcc_pedsnet'`.  
   - `table.create(self.engine)` is called, which issues the appropriate `CREATE TABLE` statements via SQLAlchemy’s engine bound to the test database.  
   - The `schema` attribute is reset to `None` to avoid side‑effects on subsequent operations.  
   This step builds a realistic set of DCC tables that the clear operation is expected to drop.  

3. **Sanity check before clearing** – A new `sqlalchemy.MetaData` object scoped to the `dcc_pedsnet` schema (`md = sqlalchemy.MetaData(schema='dcc_pedsnet')`) is reflected against the live database (`md.reflect(self.engine)`). The test asserts that the expected table `dcc_pedsnet.person` is present, confirming that the previous creation step succeeded.  

4. **Invoke the clear routine** – The core functionality under test is called: `clear_dcc_data(self.model_version, self.conn_str)`. This function (documented elsewhere) generates DROP statements for every non‑vocab table in the model version’s metadata and executes them in parallel. It uses the same connection string, which automatically points to the DCC schema because `clear_dcc_data` rewrites the connection string with `search_path=dcc_pedsnet`.  

5. **Validate that all tables were removed** – After `clear_dcc_data` returns, the test reflects the schema again (`md = sqlalchemy.MetaData(schema='dcc_pedsnet'); md.reflect(self.engine)`). It then asserts that `len(md.tables) == 0`, meaning no tables remain in the DCC schema. This confirms that the clear operation successfully dropped every non‑vocab table.  

**Relationship with callees**:  
- **Statement.execute**: Used to run the initial `CREATE SCHEMA` command. The method handles connection management, autocommit, and error logging.  
- **clear_dcc_data**: The primary function being exercised. It internally calls utilities such as `conn_str_with_search_path`, `stock_metadata`, `StatementSet.parallel_execute`, and `_check_stmt_err`. The test does not mock these calls; it relies on their real behavior to perform parallel DROP statements.  

**Note**:  
- The test only creates non‑vocabulary tables; vocabulary tables are deliberately excluded because `clear_dcc_data` is designed to preserve them.  
- The schema name is hard‑coded as `dcc_pedsnet`; any change to the global `DCC_SCHEMA` constant would require updating the test accordingly.  
- Because `clear_dcc_data` runs DROP statements in parallel, the test may involve multiple database connections; the test environment must allow enough concurrent connections for the worker pool.  
- The test assumes that the metadata object (`self.metadata`) contains the full set of tables for the specified model version and that the `VOCAB_TABLES` constant correctly identifies vocabulary tables.  
- Any failure in the `Statement.execute` call or in the parallel drop process will cause the test to raise an exception, resulting in a test failure.
***
