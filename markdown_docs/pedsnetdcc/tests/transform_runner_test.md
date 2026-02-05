## FunctionDef setUpModule
**setUpModule**: The function of setUpModule is to create a reusable PostgreSQL factory that caches an initialized database for use across multiple test instances.  

**parameters**: This function does not accept any parameters.  

**Code Description**:  
- The function begins by declaring `Postgresql` as a global variable, allowing the assignment to affect the module‑level namespace.  
- It then assigns `Postgresql` to an instance of `testing.postgresql.PostgresqlFactory` with the argument `cache_intialized_db=True`.  
- `PostgresqlFactory` is a helper from the `testing.postgresql` package that creates temporary PostgreSQL server instances for testing. By enabling `cache_intialized_db`, the factory stores the state of the first initialized database and reuses that snapshot for subsequent temporary instances, dramatically reducing setup time for tests that need a fresh but pre‑populated database.  
- The function does not return a value; its purpose is side‑effectful initialization of the global `Postgresql` factory for the test module.  

**Note**:  
- The `testing.postgresql` package must be installed in the test environment; otherwise, an `ImportError` will be raised when this module is imported.  
- Because the factory caches the initialized database, any changes made to the database during a test will not affect other tests that later obtain a new instance from the same factory. This ensures test isolation while improving performance.  
- The global `Postgresql` variable should be used by individual test cases to obtain temporary PostgreSQL instances via `Postgresql()` or similar calls provided by the factory.
## FunctionDef tearDownModule
**tearDownModule**: The function of tearDownModule is to clear the cached initialized PostgreSQL database after all tests in the module have finished executing.  

**parameters**: The parameters of this Function.  
· None  

**Code Description**:  
The function `tearDownModule` is defined without any arguments, making it a module‑level teardown hook for the unittest framework. When the test runner completes execution of all test cases within the `transform_runner_test.py` module, it automatically invokes this function. Inside the function, a single statement is executed: `Postgresql.clear_cache()`. This call triggers the `clear_cache` class method of the `Postgresql` helper class, which is responsible for removing any cached database connections or initialized database state that were created during the test run. By clearing this cache, the function ensures that subsequent test runs start with a clean slate and that no residual state from previous tests interferes with later executions.  

**Note**:  
- The `Postgresql` class must be imported in the test module for this function to work; otherwise, a `NameError` will be raised.  
- This teardown is executed only once per test module, not after each individual test case.  
- If additional cleanup actions are required (e.g., removing temporary files), they should be added to this function or to separate teardown hooks.
## ClassDef TransformRunnerTest
**TransformRunnerTest**: The function of TransformRunnerTest is to verify that the transformation and rollback processes correctly modify database schemas and tables for a given model version.

**attributes**: The attributes of this Class.  
· postgresql: an instance of the temporary PostgreSQL server used for testing.  
· dburi: the database URI string obtained from the temporary PostgreSQL instance.  
· conn_str: a connection string built from the dburi, used by the Statement helper.  
· model_version: the version identifier of the data model under test (default `'2.3.0'`).  
· engine: a SQLAlchemy engine created from the dburi, used to create and inspect tables.

**Code Description**:  

- **setUp(self)** – Executed before each test method. It starts a temporary PostgreSQL server (`Postgresql()`), retrieves its connection URL, builds a connection string with `make_conn_str`, sets the model version to `'2.3.0'`, and creates a SQLAlchemy engine bound to the temporary database. This prepares an isolated environment for each test run.  

- **tearDown(self)** – Executed after each test method. It stops the temporary PostgreSQL server, ensuring that the temporary database is destroyed and resources are released.  

- **_expected_column_names(self, metadata, table_name)** – Helper that extracts the set of column names defined in a SQLAlchemy `MetaData` object for a specified table. It returns `set(metadata.tables[table_name].columns.keys())`. This represents the expected schema as defined by the model code.  

- **_actual_column_names(self, schema, table_name)** – Helper that queries the live PostgreSQL database to obtain the set of column names for a given table in a specific schema. It builds an SQL query against `information_schema.columns`, executes it via the `Statement` utility, asserts that no execution error occurred, and collects the column names into a set which it returns.  

- **test_without_data(self)** – The primary test case. It performs the following steps:  
  1. Creates the original (stock) metadata for the configured model version and materializes all tables in the temporary database (`orig_metadata.create_all(self.engine)`).  
  2. Calls `run_transformation` with the connection string, model version, a dummy site identifier (`'testsite'`), and a search path (`'public'`). This function is expected to copy the original tables to a backup schema (`public_backup`) and apply a series of transformations defined in `TRANSFORMS`, resulting in transformed tables in the `public` schema.  
  3. Asserts that the column definitions of the original `measurement` table match those in the `public_backup` schema by comparing the expected column set from `orig_metadata` with the actual column set retrieved from the database.  
  4. Constructs the transformed metadata by applying each transformation in `TRANSFORMS` to a fresh copy of the stock metadata, then asserts that the transformed `measurement` table in the `public` schema matches this expected transformed definition.  
  5. Verifies that the intermediate schema `public_transformed` does not exist after the transformation step.  
  6. Calls `undo_transformation` to roll back the changes, which should restore the original tables to the `public` schema and drop the backup schema.  
  7. Confirms that the original `measurement` table definition is now present in the `public` schema and that the `public_backup` schema has been removed.  

All assertions use `self.assertEqual`, `self.assertFalse`, and `self.assertIsNone` to enforce correctness. The test does not load any data into the tables; it validates only the structural changes performed by the transformation and rollback logic.

**Note**:  
- The test relies on external utilities (`Postgresql`, `make_conn_str`, `Statement`, `stock_metadata`, `run_transformation`, `undo_transformation`, `schema_exists`, and the `TRANSFORMS` collection). These must be importable and functional in the test environment.  
- The temporary PostgreSQL instance is created in a temporary directory; ensure the host system has sufficient permissions and the required binaries (`initdb`, `postgres`) available.  
- The test assumes that the `measurement` table exists in the stock metadata for the specified model version. If the model version changes, the test may need to be updated accordingly.  
- Because the test manipulates schemas (`public`, `public_backup`, `public_transformed`), it should be run in isolation to avoid interference with other tests that might use the same temporary database.

**Output Example**:  

When executed with `python -m unittest pedsnetdcc.tests.transform_runner_test.TransformRunnerTest`, a successful run produces output similar to:  

```
.
----------------------------------------------------------------------
Ran 1 test in 2.347s

OK
```
### FunctionDef setUp(self)
**setUp**: The function of setUp is to prepare a temporary PostgreSQL database and related SQLAlchemy objects for each test case.

**parameters**: This method does not accept any external parameters.

**Code Description**:  
1. **Create a temporary PostgreSQL instance** – `self.postgresql = Postgresql()` instantiates a lightweight PostgreSQL server that lives in a temporary directory. The server is started automatically by the `Postgresql` helper class.  
2. **Obtain the connection URI** – `self.dburi = self.postgresql.url()` retrieves the full PostgreSQL URI (e.g., `postgresql://user:pass@host:port/dbname`) that points to the temporary database.  
3. **Build a libpq‑compatible connection string** – `self.conn_str = make_conn_str(self.dburi)` calls the utility function `make_conn_str` (defined in `pedsnetdcc/utils.py`). This function parses the URI, extracts host, port, database name, user, and password, and assembles a space‑separated libpq connection string suitable for `psycopg2` and other PostgreSQL client libraries. No additional `search_path` or password overrides are supplied, so the connection string reflects the values present in `self.dburi`.  
4. **Set the model version** – `self.model_version = '2.3.0'` stores a static version identifier that may be used by the test suite to simulate a specific data model release.  
5. **Create a SQLAlchemy engine** – `self.engine = sqlalchemy.create_engine(self.dburi)` builds a SQLAlchemy `Engine` object bound to the temporary database URI. This engine provides a high‑level interface for executing SQL statements and managing connections during the test.  

The `setUp` method is executed automatically before each test method in the `TransformRunnerTest` class, ensuring that every test runs against a fresh, isolated database environment.

**Note**:  
- The temporary PostgreSQL instance is automatically cleaned up when the test case finishes (handled by the `Postgresql` object's destructor or context manager).  
- `make_conn_str` is used only to produce a libpq connection string; the resulting string (`self.conn_str`) is not directly used by SQLAlchemy, which works with the original URI. However, the string may be required by other components of the test suite that expect a libpq‑style connection string.  
- The hard‑coded `model_version` value should be updated if tests need to target a different schema version.  
- Because no explicit `search_path` or password overrides are provided, the connection string reflects exactly what the `Postgresql` helper supplies. If a test requires a custom schema search path, `make_conn_str` can be called with the `search_path` argument.
***
### FunctionDef tearDown(self)
**tearDown**: The function of tearDown is to clean up resources after a test case by stopping the PostgreSQL test database.  

**parameters**: This method does not accept any external parameters.  

**Code Description**:  
- The method is defined as an instance method of the `TransformRunnerTest` class.  
- Inside the method, a single operation is performed: `self.postgresql.stop()`.  
- `self.postgresql` is expected to be an object that manages a temporary PostgreSQL instance created for the test suite.  
- Calling `stop()` on this object terminates the PostgreSQL server process and releases any associated resources, effectively destroying the test database that was used during the test execution.  

**Note**:  
- Ensure that the `self.postgresql` attribute has been properly initialized (typically in `setUp`) before `tearDown` is invoked; otherwise, calling `stop()` may raise an exception.  
- This cleanup step is crucial for preventing resource leaks and ensuring that subsequent tests start with a clean environment.
***
### FunctionDef _expected_column_names(self, metadata, table_name)
**_expected_column_names**: The function of _expected_column_names is to return a set of column names for a specified table extracted from a SQLAlchemy MetaData object.  

**parameters**:  
· self – the instance of the test class (TransformRunnerTest) invoking the method.  
· metadata – a SQLAlchemy MetaData instance that holds Table objects representing the database schema.  
· table_name – a string containing the name of the table whose column names are to be retrieved.  

**Code Description**:  
The method accesses the `tables` dictionary of the supplied `metadata` object using the provided `table_name` as the key. This dictionary maps table names to SQLAlchemy `Table` objects. From the selected `Table` object, it obtains the `columns` collection, which is an ordered mapping of column names to `Column` objects. By calling `.keys()` on this collection, the method extracts an iterable of all column name strings. It then converts this iterable into a Python `set`, ensuring that the result contains each column name exactly once and provides fast membership testing. The resulting set is returned to the caller.  

In the test suite, `test_without_data` calls `_expected_column_names` twice: once with the original metadata (before any transformation) and once with the transformed metadata (after applying all defined transforms). The returned sets are compared against the actual column names retrieved from the live PostgreSQL schemas (`public_backup` and `public`) using the helper `_actual_column_names`. This comparison verifies that the transformation process preserves the expected schema structure.  

**Note**:  
- The method assumes that `metadata.tables` contains an entry for `table_name`; otherwise a `KeyError` will be raised.  
- The returned set contains only the column names as strings; it does not include data types, constraints, or ordering information.  
- Because a set is unordered, the comparison in the test relies solely on membership, not on column order.  

**Output Example**:  
If the `measurement` table has columns `measurement_id`, `person_id`, `measurement_concept_id`, and `value_as_number`, the call  

```python
_expected_column_names(metadata, 'measurement')
```  

would return  

```python
{'measurement_id', 'person_id', 'measurement_concept_id', 'value_as_number'}
```
***
### FunctionDef _actual_column_names(self, schema, table_name)
**_actual_column_names**: The function of _actual_column_names is to retrieve the set of column names that exist in a specified table within a given schema of the database.

**parameters**:  
· **schema**: The name of the database schema (e.g., 'public' or 'public_backup') that contains the target table.  
· **table_name**: The name of the table whose column names are to be fetched.

**Code Description**:  
The method builds an SQL query that selects the `column_name` field from the `information_schema.columns` view, filtering on the supplied `schema` and `table_name`. The query string is constructed using Python's `format` method to safely inject the schema and table identifiers.  

```python
tpl = ("select column_name from information_schema.columns "
       "where table_schema = '{sch}' and table_name = '{tbl}'")
sql = tpl.format(sch=schema, tbl=table_name)
```

The resulting SQL statement is executed via the `Statement` class, which is instantiated with the query string and then run against the database connection string stored in `self.conn_str`.  

```python
stmt = Statement(sql).execute(self.conn_str)
```

Immediately after execution, the test asserts that no error occurred (`stmt.err` must be `None`). This ensures that the query succeeded before proceeding.  

The method then iterates over `stmt.data`, which contains the rows returned by the query. For each row, the first element (`row[0]`)—the column name—is added to a Python `set` called `cols`. Using a set guarantees that each column name appears only once and provides efficient membership testing.  

Finally, the populated set of column names is returned to the caller.  

In the test suite (`TransformRunnerTest.test_without_data`), `_actual_column_names` is called multiple times to verify that after running a transformation, the expected columns are present in both the backup schema (`public_backup`) and the transformed schema (`public`). The returned sets are compared against expected column name sets generated by `_expected_column_names`. This comparison validates that the transformation process correctly creates or preserves the required table structures.

**Note**:  
- The function assumes that `self.conn_str` is a valid PostgreSQL connection string and that the `Statement` class is available in the test environment.  
- If the specified schema or table does not exist, the query will return no rows, and the function will return an empty set.  
- The method does not perform any quoting or escaping of schema/table names beyond simple string formatting; callers must ensure that the inputs are safe and correctly formatted.  
- The assertion `self.assertIsNone(stmt.err)` will raise an exception in the test if the query fails, halting further execution.

**Output Example**:  
```python
{'measurement_id', 'person_id', 'measurement_concept_id',
 'measurement_date', 'measurement_time', 'measurement_type_concept_id',
 'operator_concept_id', 'value_as_number', 'value_as_concept_id',
 'unit_concept_id', 'range_low', 'range_high', 'provider_id',
 'visit_occurrence_id', 'measurement_source_value',
 'measurement_source_concept_id', 'unit_source_value',
 'value_source_value'}
```
***
### FunctionDef test_without_data(self)
**test_without_data**: The function of test_without_data is to verify that a full transformation‑run (including backup creation, schema swapping, and subsequent undo) works correctly when the database contains only the stock schema and no site‑specific data.

**parameters**: This method is a unit‑test; it receives only the implicit `self` reference to the `TransformRunnerTest` instance. No additional parameters are defined.

**Code Description**:  
1. **Create the original (stock) metadata** –  
   `orig_metadata = stock_metadata(self.model_version)` obtains a fresh `MetaData` object that describes the untransformed PEDSnet model for the test’s model version.  
   `orig_metadata.create_all(self.engine)` materialises the tables in the temporary test database using the SQLAlchemy engine stored in `self.engine`. At this point the database contains only the original core tables in the default `public` schema.

2. **Run the transformation pipeline** –  
   Fixed values `site = 'testsite'` and `search_path = 'public'` are supplied.  
   `run_transformation(self.conn_str, self.model_version, site, search_path)` executes the complete transformation process: it creates a `<schema>_transformed` schema, copies the original tables into a `<schema>_backup` schema, builds transformed tables, applies post‑transform DDL (primary keys, NOT NULL, indexes, foreign keys, etc.), and finally swaps the transformed tables back into the original `public` schema.

3. **Validate the backup schema** –  
   The test asserts that the original table definitions are preserved in the backup schema.  
   `self._expected_column_names(orig_metadata, 'measurement')` extracts the set of column names for the `measurement` table from the original metadata.  
   `self._actual_column_names('public_backup', 'measurement')` queries the live database (`information_schema.columns`) to obtain the actual column names present in the `public_backup.measurement` table.  
   `self.assertEqual` confirms that the two sets are identical, proving that the backup schema contains an exact copy of the original table structure.

4. **Validate the transformed schema** –  
   A new `MetaData` instance `trans_metadata` is created from the stock model. All transformation objects listed in the global `TRANSFORMS` collection are applied sequentially:  
   ```python
   for t in TRANSFORMS:
       trans_metadata = t.modify_metadata(trans_metadata)
   ```  
   This yields the expected transformed metadata. The test then compares the expected column set for `measurement` (derived from `trans_metadata`) with the actual column set in the current `public` schema (`self._actual_column_names('public', 'measurement')`). Equality confirms that the transformation produced the correct table definition.

5. **Check that the temporary transformed schema was removed** –  
   `self.assertFalse(schema_exists(self.conn_str, 'public_transformed'))` ensures that the intermediate `<schema>_transformed` schema does not remain after the swap, as required by the transformation logic.

6. **Undo / rollback the transformation** –  
   `undo_transformation(self.conn_str, self.model_version, search_path)` restores the original tables from the backup schema, drops the backup schema, and removes any temporary objects created during the transformation.

7. **Validate restoration of the original schema** –  
   After undo, the test again compares the expected column set from the original metadata with the actual column set now present in the `public` schema, confirming that the original tables have been fully restored.

8. **Verify that the backup schema has been removed** –  
   `self.assertFalse(schema_exists(self.conn_str, 'public_backup'))` asserts that the backup schema no longer exists, indicating a clean rollback.

**Relationship with callees**:  
- Calls `stock_metadata` (pedsnetdcc.utils) to obtain the canonical model definition.  
- Calls `run_transformation` (pedsnetdcc.transform_runner) to execute the full transformation pipeline.  
- Calls `schema_exists` (pedsnetdcc.schema) to check for the presence of temporary schemas.  
- Calls `undo_transformation` (pedsnetdcc.transform_runner) to revert the transformation.  
- Iterates over the global `TRANSFORMS` list; each element is a subclass of `Transform` whose `modify_metadata` method (pedsnetdcc.abstract_transform) mutates the metadata to reflect site‑specific changes.  
- Uses the private helper methods `_expected_column_names` and `_actual_column_names` defined in the same test class to obtain column name sets from metadata and from the live database, respectively.

**Note**:  
- The test assumes that the temporary PostgreSQL instance represented by `self.conn_str` is empty except for the tables created by `orig_metadata.create_all`.  
- All schema names are hard‑coded (`public`, `public_backup`, `public_transformed`); any change to the default search_path in the test environment would require corresponding updates.  
- The comparison of column sets ignores column order and any additional constraints; it only verifies that the set of column names matches the expectation.  
- Because `run_transformation` and `undo_transformation` perform many DDL operations, any failure inside those functions will cause the test to abort with an exception, providing immediate feedback on transformation integrity.
***
