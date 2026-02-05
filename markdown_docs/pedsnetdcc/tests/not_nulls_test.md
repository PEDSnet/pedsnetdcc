## FunctionDef setUpModule
**setUpModule**: The function of setUpModule is to create a module‑level PostgreSQL factory that caches an initialized test database for reuse across multiple temporary database instances.

**parameters**: This function takes no parameters.

**Code Description**:  
- The function begins by declaring `global Postgresql`, indicating that the name `Postgresql` will refer to a module‑level variable rather than a local one.  
- It then assigns to `Postgresql` an instance of `testing.postgresql.PostgresqlFactory`. This factory class is part of the `testing.postgresql` package and is used to spin up temporary PostgreSQL server instances for testing purposes.  
- The factory is instantiated with the argument `cache_intialized_db=True`. With this flag enabled, the factory will create the database schema and any required seed data only once; subsequent temporary PostgreSQL instances will reuse this pre‑initialized state, dramatically reducing the overhead of setting up the test environment for each test case.  
- No explicit return statement is present, so the function returns `None`. The side effect of defining the global `Postgresql` variable is the primary purpose of the function.

**Note**:  
- `setUpModule` is automatically invoked by the unittest framework before any tests in the module are collected, ensuring that the `Postgresql` factory is ready for use by the test cases that follow.  
- Because the variable is declared global, other test functions or fixtures in the same module can import and instantiate temporary PostgreSQL servers via `Postgresql()`.  
- The caching behavior assumes that the initial database state is suitable for all tests; if a test modifies the database in a way that affects others, additional cleanup may be required.  
- Ensure that the `testing.postgresql` package is installed in the test environment; otherwise, an `ImportError` will be raised when this function executes.
## FunctionDef tearDownModule
**tearDownModule**: The function of tearDownModule is to clear the cached PostgreSQL test database after all tests in the module have finished running.  

**parameters**: This function does not accept any parameters.  

**Code Description**:  
- The function is defined without arguments, indicating it is intended to be called by the unittest framework automatically after the test suite in the module completes.  
- Inside the function, a single statement is executed: `Postgresql.clear_cache()`.  
- `Postgresql` is a helper class (provided elsewhere in the test utilities) that manages a temporary PostgreSQL instance used during testing.  
- The `clear_cache` method of `Postgresql` removes any cached connections or temporary database files that were created when the test environment was initialized. This ensures that subsequent test runs start with a clean state and that no leftover resources remain on the filesystem or in memory.  

**Note**:  
- This teardown function is automatically discovered by the unittest framework when placed at module level; it should not be called manually.  
- Ensure that the `Postgresql` class and its `clear_cache` method are correctly imported in the test module; otherwise, an `ImportError` will be raised at teardown time.  
- The function must remain side‑effect free apart from clearing the cache; adding additional logic could interfere with the test runner’s cleanup process.
## ClassDef NotNulls
**NotNulls**: The function of NotNulls is to verify that the utilities for dropping and restoring NOT NULL constraints work correctly on both vocabulary and non‑vocabulary tables in a temporary PostgreSQL database.

**attributes**: The attributes of this Class.  
· postgresql: a temporary PostgreSQL instance created by the `Postgresql` helper.  
· dburi: the database URI string obtained from the temporary PostgreSQL instance.  
· conn_str: a SQLAlchemy‑compatible connection string derived from `dburi` via `make_conn_str`.  
· model_version: the PEDSnet data model version used for the test (hard‑coded to `'2.2.0'`).  
· engine: a SQLAlchemy engine bound to the temporary database.  
· metadata: SQLAlchemy MetaData object representing the transformed PEDSnet schema for the specified model version.

**Code Description**:  
The `NotNulls` class inherits from `unittest.TestCase` and implements a single integration test that exercises the `drop_not_nulls` and `set_not_nulls` helper functions from the `pedsnetdcc` package.

1. **setUp** – Executed before each test method:  
   - Instantiates a temporary PostgreSQL server (`self.postgresql`).  
   - Retrieves its connection URI (`self.dburi`) and converts it to a SQLAlchemy connection string (`self.conn_str`).  
   - Sets `self.model_version` to `'2.2.0'`.  
   - Creates a SQLAlchemy engine (`self.engine`) using the URI.  
   - Loads the base PEDSnet metadata for the given model version via `stock_metadata`.  
   - Applies a series of schema transformations defined in the global `TRANSFORMS` list, each of which modifies the metadata in place.  
   - Calls `self.metadata.create_all(self.engine)` to materialize the transformed schema in the temporary database.

2. **tearDown** – Executed after each test method:  
   - Stops and cleans up the temporary PostgreSQL instance, ensuring no leftover resources.

3. **test_not_nulls** – The core test method performs the following sequence:  
   - Retrieves column collections for the `care_site` (non‑vocabulary) and `concept` (vocabulary) tables from the pre‑test metadata.  
   - Calls `drop_not_nulls` without the `vocabulary` flag, which should remove NOT NULL constraints only from non‑vocabulary tables.  
   - Reloads the `care_site` table definition from the live database and asserts that every column that was originally NOT NULL (and not a primary key) is now nullable. A sanity check confirms that at least one such column existed.  
   - Reloads the `concept` table and asserts that its original NOT NULL columns remain NOT NULL, confirming that vocabulary tables were untouched. A similar sanity check is performed.  
   - Calls `drop_not_nulls` with `vocabulary=True`, which should now also drop NOT NULL constraints from vocabulary tables. The test reloads `concept` and verifies that its previously NOT NULL columns are now nullable.  
   - Calls `set_not_nulls` without the `vocabulary` flag to restore NOT NULL constraints on non‑vocabulary tables only. The test reloads `care_site` and confirms that its original NOT NULL columns are again non‑nullable, while `concept` remains unchanged (still nullable).  
   - Calls `set_not_nulls` with `vocabulary=True` to restore NOT NULL constraints on vocabulary tables. The test reloads `concept` and verifies that its original NOT NULL columns are now non‑nullable again.

Throughout the test, the code uses SQLAlchemy's reflection (`autoload=True`, `autoload_with=self.engine`) to obtain the current table definitions after each operation, ensuring that the assertions are based on the actual database state.

**Note**:  
- The test depends on external utilities (`Postgresql`, `make_conn_str`, `stock_metadata`, `TRANSFORMS`, `drop_not_nulls`, `set_not_nulls`) that must be importable from the surrounding package.  
- The temporary PostgreSQL instance must be available on the test host; the `Postgresql` helper typically requires the `pg_ctl` binary and appropriate permissions.  
- The test assumes that the transformed schema contains at least one NOT NULL, non‑primary‑key column in both a non‑vocabulary table (`care_site`) and a vocabulary table (`concept`). If the schema changes, the sanity checks may need adjustment.  
- Running this test modifies the database schema; it should be executed in an isolated environment to avoid side effects on production data.
### FunctionDef setUp(self)
**setUp**: The function of setUp is to prepare a temporary PostgreSQL instance, build a SQLAlchemy engine, generate a transformed PEDSnet metadata schema, and create all tables in the temporary database for use in the test suite.  

**parameters**:  
· **self** – the test case instance; no external arguments are required.  

**Code Description**:  
1. **Create a temporary PostgreSQL server** – `self.postgresql = Postgresql()` instantiates a PostgreSQL server that runs in a temporary directory (provided by the `testing.postgresql` package). This server is isolated from any real database and will be torn down automatically when the test finishes.  
2. **Obtain the connection URI** – `self.dburi = self.postgresql.url()` retrieves the full PostgreSQL URI (e.g., `postgresql://user:pass@host:port/dbname`).  
3. **Build a libpq‑compatible connection string** – `self.conn_str = make_conn_str(self.dburi)` calls `pedsnetdcc.utils.make_conn_str`. The function parses the URI, extracts host, port, dbname, user, password, and any query options, then assembles a space‑separated connection string suitable for `psycopg2`. This string is stored for later use by other test helpers.  
4. **Set the model version** – `self.model_version = '2.2.0'` fixes the PEDSnet data‑model version that the test will operate against.  
5. **Create a SQLAlchemy engine** – `self.engine = sqlalchemy.create_engine(self.dburi)` builds an engine that points to the temporary PostgreSQL instance. The engine is later used to execute DDL and DML statements.  
6. **Load the stock (untransformed) metadata** – `self.metadata = stock_metadata(self.model_version)` invokes `pedsnetdcc.utils.stock_metadata`. This function contacts the PEDSnet data‑model service, downloads the schema for version 2.2.0, and returns a `sqlalchemy.MetaData` object containing all tables, columns, and constraints.  
7. **Apply all registered transforms** – The loop `for t in TRANSFORMS: self.metadata = t.modify_metadata(self.metadata)` iterates over every `Transform` subclass listed in the global `TRANSFORMS` collection. For each transform, `Transform.modify_metadata` (documented in `pedsnetdcc.abstract_transform.Transform`) is called, which walks through all non‑vocabulary tables and invokes the transform’s `modify_table` hook. The returned (potentially mutated) `MetaData` replaces the previous one, accumulating all schema modifications required for the test scenario.  
8. **Create the transformed schema in the temporary database** – `self.metadata.create_all(self.engine)` issues the appropriate `CREATE TABLE` (and related) statements against the temporary PostgreSQL instance, materialising the fully transformed schema. After this call the database contains all tables, indexes, and constraints that the test suite expects.  

**Note**:  
- The temporary PostgreSQL instance is created fresh for each test case; any data persisted during the test is isolated and will be removed when the test’s `tearDown` method (not shown) shuts down `self.postgresql`.  
- `make_conn_str` produces a libpq connection string; if additional options such as a custom `search_path` are needed, they must be supplied explicitly when calling `make_conn_str`.  
- The `TRANSFORMS` list must be defined before `setUp` runs; each entry must be a subclass of `Transform` that implements `modify_table`. If a transform does not modify the metadata, the loop still returns the original `MetaData` unchanged.  
- `stock_metadata` does not cache results; each call contacts the remote model service. In a test environment this is acceptable because the temporary database is short‑lived, but in production code callers may wish to cache the returned `MetaData`.  
- Errors raised by any of the callees (e.g., failure to start PostgreSQL, inability to fetch the model, or a transform raising an exception) will cause the test setup to fail, preventing the test case from running. Proper exception handling is delegated to the unittest framework.
***
### FunctionDef tearDown(self)
**tearDown**: The function of tearDown is to clean up the test environment by stopping the temporary PostgreSQL database that was started for the test case.  

**parameters**:  
· self: the instance of the test class, providing access to the `postgresql` attribute that represents the running PostgreSQL test container.  

**Code Description**:  
The `tearDown` method is executed after each test method in the `NotNulls` test suite. Its sole responsibility is to invoke `self.postgresql.stop()`. The `postgresql` attribute is an object (typically a test fixture or container manager) that controls a PostgreSQL instance used during testing. Calling `stop()` terminates the database process, releases any allocated resources, and ensures that no lingering connections remain. This guarantees that subsequent tests start with a clean slate and that system resources are not exhausted.  

**Note**:  
- `tearDown` must be defined in a unittest.TestCase subclass so that the unittest framework automatically calls it after each test method.  
- Ensure that `self.postgresql` has been properly initialized (e.g., in `setUp`) before `tearDown` runs; otherwise, calling `stop()` may raise an AttributeError.  
- This method does not return any value and does not raise exceptions under normal operation; any failure to stop the database should be investigated as a test environment issue.
***
### FunctionDef test_not_nulls(self)
**test_not_nulls**: The function of test_not_nulls is to verify that the drop_not_nulls and set_not_nulls utilities correctly modify NOT NULL constraints on both vocabulary and non‑vocabulary tables in a PEDSnet database.

**parameters**: The parameters of this Function.  
· **self**: instance of the unittest.TestCase subclass that provides access to the test database connection, engine, metadata, model version, and connection string.

**Code Description**:  
1. **Retrieve original column metadata** – The test obtains the column collections for the `care_site` (non‑vocabulary) and `concept` (vocabulary) tables from the pre‑loaded SQLAlchemy metadata (`self.metadata.tables`). These collections are stored in `care_site_cols` and `concept_cols` for later comparison.  

2. **Drop NOT NULL constraints on non‑vocabulary tables** – Calls `drop_not_nulls(self.conn_str, self.model_version)`. This removes NOT NULL constraints from all applicable columns in tables that are not listed in the vocabulary set.  

3. **Validate that non‑vocabulary columns became nullable** –  
   - Reflects the current `care_site` table definition from the live database using `sqlalchemy.Table(..., autoload=True, autoload_with=self.engine)`.  
   - Iterates over the original column metadata (`care_site_cols`). For each column that was originally NOT NULL and not a primary key, it increments `num_not_null` and asserts that the corresponding column in the reflected table is now `nullable`.  
   - After the loop, asserts that at least one column was examined (`self.assertNotEqual(num_not_null, 0)`) to ensure the test is meaningful.  

4. **Confirm that vocabulary tables were untouched** –  
   - Reflects the `concept` table in the same way.  
   - For each originally NOT NULL, non‑primary‑key column in `concept_cols`, it increments `num_not_null` and asserts that the column remains **not** nullable (`self.assertFalse(concept.columns[col.name].nullable)`).  
   - Performs a sanity check that `num_not_null` is non‑zero.  

5. **Drop NOT NULL constraints on vocabulary tables** – Calls `drop_not_nulls(self.conn_str, self.model_version, vocabulary=True)`. This targets only the tables listed in the vocabulary set.  

6. **Validate that vocabulary columns became nullable** – Reflects `concept` again and asserts that every originally NOT NULL, non‑primary‑key column is now nullable (`self.assertTrue(concept.columns[col.name].nullable)`).  

7. **Re‑apply NOT NULL constraints to all non‑vocabulary tables** – Calls `set_not_nulls(self.conn_str, self.model_version)`.  

8. **Verify that non‑vocabulary columns are again NOT NULL** – Reflects `care_site` and asserts that each originally NOT NULL, non‑primary‑key column is now non‑nullable (`self.assertFalse(care_site.columns[col.name].nullable)`).  

9. **Verify that vocabulary tables remain unchanged after non‑vocab set** – Reflects `concept` and asserts that its originally NOT NULL columns are still nullable (`self.assertTrue(concept.columns[col.name].nullable)`).  

10. **Re‑apply NOT NULL constraints to vocabulary tables** – Calls `set_not_nulls(self.conn_str, self.model_version, vocabulary=True)`.  

11. **Validate that vocabulary columns are now NOT NULL** – Reflects `concept` a final time and asserts that each originally NOT NULL, non‑primary‑key column is no longer nullable (`self.assertFalse(concept.columns[col.name].nullable)`).  

Throughout the test, the function uses SQLAlchemy’s reflection capabilities to obtain the current schema state after each operation, and it relies on the unittest `assertTrue`, `assertFalse`, and `assertNotEqual` methods to enforce expected outcomes.

**Relationship with callees**:  
- **drop_not_nulls** – invoked twice (once for non‑vocab tables, once with `vocabulary=True`) to remove NOT NULL constraints. The test checks that the function correctly respects the `vocabulary` flag.  
- **set_not_nulls** – invoked twice (once for non‑vocab tables, once with `vocabulary=True`) to restore NOT NULL constraints. The test confirms that the function restores constraints only on the intended set of tables.  

Both callees are part of the `pedsnetdcc.not_nulls` module and perform their work by generating and executing ALTER TABLE statements in parallel. The test does not inspect the internal implementation of these functions; it only validates their observable effects on the database schema.

**Note**:  
- The test assumes that the database schema for the specified `model_version` contains at least one NOT NULL, non‑primary‑key column in both a non‑vocabulary table (`care_site`) and a vocabulary table (`concept`). The sanity checks (`assertNotEqual(num_not_null, 0)`) will fail if this assumption is not met.  
- Primary‑key columns are deliberately excluded from all assertions because they are inherently NOT NULL and are not altered by the utilities.  
- The test uses the same connection string and engine for all operations; any change in connection parameters must be reflected in the test fixture setup.  
- Because `drop_not_nulls` and `set_not_nulls` execute statements in parallel, the order of column modifications is nondeterministic, but the final schema state is deterministic and verified by the assertions.  
- Any failure in the underlying utilities (e.g., insufficient privileges, malformed SQL templates) will cause the corresponding assertion to fail, providing immediate feedback that the utility did not behave as expected.
***
