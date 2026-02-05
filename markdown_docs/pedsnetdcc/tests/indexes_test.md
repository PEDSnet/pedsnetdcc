## FunctionDef setUpModule
**setUpModule**: The function of setUpModule is to create a globally‑available PostgreSQL factory class that caches an initialized database for reuse across multiple temporary database instances during testing.  

**parameters**: This function does not accept any parameters.  

**Code Description**:  
- The function begins by declaring `global Postgresql`, indicating that the name `Postgresql` refers to a module‑level variable rather than a local one.  
- It then assigns to `Postgresql` an instance of `testing.postgresql.PostgresqlFactory`.  
- The factory is instantiated with the argument `cache_intialized_db=True`. This flag tells the factory to keep a copy of the database after it has been initialized the first time, allowing subsequent temporary PostgreSQL clusters to start from the cached state instead of re‑initializing from scratch.  
- No explicit return statement is present, so the function returns `None` implicitly.  

**Note**:  
- `setUpModule` is intended to be executed automatically by the unittest framework before any tests in the module are run, establishing the `Postgresql` factory for use by individual test cases.  
- The global variable `Postgresql` must be imported or referenced by the test suite after this setup; otherwise, attempts to create temporary PostgreSQL instances will fail.  
- The caching behavior improves test performance but assumes that the initial database state is suitable for all tests that rely on it. If a test modifies the cached state, it may affect subsequent tests, so tests should either avoid mutating the database or rely on the factory’s isolation mechanisms.  
## FunctionDef tearDownModule
**tearDownModule**: The function of tearDownModule is to clear the cached initialized PostgreSQL database after the test module has finished executing.  

**parameters**: This function does not accept any parameters.  

**Code Description**:  
- The function is defined without arguments and is intended to be invoked automatically by the unittest framework once all test cases in the module have been run.  
- Inside the function, a single statement is executed: `Postgresql.clear_cache()`.  
- `Postgresql.clear_cache()` is a class‑method (or static method) provided by the `Postgresql` helper class in the test suite. Its purpose is to remove any in‑memory or on‑disk cache that stores connections, schema information, or temporary data created during the test run.  
- By clearing this cache, the function ensures that subsequent test modules start with a clean database state, preventing cross‑test contamination and reducing the risk of flaky tests.  

**Note**:  
- The `Postgresql` class must be imported in the test module for this call to succeed; otherwise, a `NameError` will be raised.  
- The effectiveness of the cleanup depends on the implementation of `Postgresql.clear_cache()`. If additional resources (e.g., temporary files or external services) are allocated during tests, they should also be released within this or a separate teardown routine.  
- This function is automatically discovered by the unittest runner; it should not be called manually in test code.
## ClassDef IndexesTest
**IndexesTest**: The function of IndexesTest is to verify that the SQL generation for creating and dropping indexes behaves correctly for different scenarios.

**attributes**: The attributes of this Class.  
· model_version: a string representing the data model version used in the tests (set to `'2.2.0'` in `setUp`).  

**Code Description**:  
The `IndexesTest` class inherits from `unittest.TestCase` and contains three test methods that exercise the private helper function `_indexes_sql`.  

1. **setUp** – Executed before each test method. It initializes the instance attribute `self.model_version` with the value `'2.2.0'`. This version string is passed to `_indexes_sql` to generate the appropriate SQL statements for that model version.  

2. **test_add_indexes** – Calls `_indexes_sql(self.model_version)` to obtain the SQL statements for adding indexes.  
   - **sample_expected**: A tuple of three index‑creation statements that must be present in the generated SQL. The test iterates over each expected statement and asserts that it is included (`self.assertIn`).  
   - **sample_not_expected**: A tuple containing an index‑creation statement that should **not** be generated for this model version. The test asserts that this statement is absent (`self.assertNotIn`).  
   This method confirms that the function produces the correct subset of index creation statements and excludes unrelated ones.  

3. **test_drop_indexes** – Calls `_indexes_sql(self.model_version, drop=True)` to obtain the SQL statements for dropping indexes.  
   - **sample_expected**: A tuple of three `DROP INDEX` statements that must appear in the output. Each is verified with `self.assertIn`.  
   - **sample_not_expected**: A tuple containing a malformed `DROP INDEX` statement that should not be present. It is verified with `self.assertNotIn`.  
   This method ensures that the drop‑index branch of `_indexes_sql` generates the correct statements and does not include unintended syntax.  

4. **test_add_indexes_for_vocabulary** – Calls `_indexes_sql(self.model_version, vocabulary=True)` to generate index‑creation SQL specific to the vocabulary component of the model.  
   - **sample_expected**: Two index‑creation statements that are expected when the `vocabulary` flag is true. Each is checked for inclusion.  
   - **sample_not_expected**: Two index‑creation statements that belong to other parts of the model and must not appear when only vocabulary indexes are requested. Each is checked for exclusion.  
   This test validates that the `vocabulary=True` option correctly filters the generated SQL to only include vocabulary‑related indexes.  

Overall, the class systematically checks that `_indexes_sql` produces the right SQL fragments for adding and dropping indexes under normal, drop, and vocabulary‑specific conditions, and that it does not emit unrelated index statements.

**Note**:  
- The tests rely on the private function `_indexes_sql`; any change to its signature or behavior may require updating these tests.  
- The expected SQL strings include hash‑like suffixes (e.g., `89a4742c38ecb8ba35_ix`) that are part of the naming convention; the tests assume these exact suffixes remain stable.  
- The `# noqa` comments in the expected strings are irrelevant to the test logic; they are present only to silence style checkers in the source code.  
- No external resources or database connections are required; the tests operate purely on the generated SQL text.  
### FunctionDef setUp(self)
**setUp**: The function of setUp is to initialize test-specific attributes before each test case runs.  
**parameters**: This method does not accept any external parameters.  
**Code Description**:  
- The method is defined within the `IndexesTest` test class and is automatically invoked by the unittest framework before each test method execution.  
- Inside the method, a single instance attribute `self.model_version` is assigned the string value `'2.2.0'`. This attribute likely represents the version of the data model or schema that the test suite will operate against.  
- No other operations, such as database connections, mock setups, or configuration loading, are performed in this method. Its sole purpose is to ensure that `self.model_version` is consistently set for all test methods that depend on it.  
**Note**:  
- Because `setUp` runs before every test method, any modification to `self.model_version` within a test will be reset to `'2.2.0'` at the start of the next test.  
- If additional initialization steps become necessary (e.g., loading fixtures or configuring mocks), they should be added to this method to maintain a clean and repeatable test environment.
***
### FunctionDef test_add_indexes(self)
**test_add_indexes**: The function of test_add_indexes is to verify that the SQL generated for adding indexes contains the expected CREATE INDEX statements and does not contain unintended index statements.

**parameters**: This test method does not accept any external parameters.

**Code Description**:  
1. The method obtains a list of SQL statements by calling the helper function `_indexes_sql` with the current test class’s `model_version` attribute. The call uses the default arguments (`vocabulary=False`, `drop=False`), therefore it requests CREATE INDEX statements for the site‑specific (non‑vocabulary) tables of the specified PEDSnet model version.  
2. A tuple named `sample_expected` is defined, containing three literal CREATE INDEX strings that are known to be produced for the given model version. Each string follows the pattern `CREATE INDEX <generated_name> ON <table> (<column>)`.  
3. The method iterates over `sample_expected` and, for each entry, asserts that the string is present in the SQL list returned by `_indexes_sql` using `self.assertIn`. This confirms that the index‑generation logic correctly creates these three indexes.  
4. A second tuple, `sample_not_expected`, holds a single CREATE INDEX statement that should **not** be generated for the site schema (it belongs to the vocabulary schema).  
5. The method iterates over `sample_not_expected` and asserts that each string is absent from the generated SQL using `self.assertNotIn`. This ensures that the function respects the `vocabulary=False` flag and does not emit vocabulary‑table indexes.  

The test inherits from `unittest.TestCase`, so the assertions raise a failure if any expectation is violated, causing the test suite to report an error.

**Relationship with Callees**:  
- Calls `_indexes_sql` from `pedsnetdcc.indexes`, which builds the list of CREATE INDEX statements based on transformed metadata.  
- Relies on the `model_version` attribute defined in the test class (typically set in `setUp` or as a class constant).  
- Uses standard unittest assertion methods (`assertIn`, `assertNotIn`) provided by the base `TestCase` class.

**Note**:  
- The test only checks for the presence or absence of specific literal substrings; it does not validate the full set of generated indexes.  
- If the underlying index‑generation logic changes (e.g., index naming scheme or additional indexes are added), the expected strings must be updated accordingly.  
- The test assumes that the PostgreSQL dialect is used for compilation, as `_indexes_sql` compiles with `sqlalchemy.dialects.postgresql.dialect()`.  
- No database connection is performed; the test operates purely on the generated DDL strings.
***
### FunctionDef test_drop_indexes(self)
**test_drop_indexes**: The function of test_drop_indexes is to verify that the SQL statements generated for dropping indexes contain the expected DROP INDEX commands and exclude unrelated statements.

**parameters**: This test method does not accept any parameters.

**Code Description**:  
1. The method calls the helper function `_indexes_sql` with the current test class’s `model_version` attribute and the flag `drop=True`. This call returns a list of SQL strings that represent DROP INDEX statements for all non‑vocabulary indexes defined in the transformed PEDSnet schema for the specified model version.  
2. A tuple `sample_expected` is defined, containing three specific DROP INDEX statements that are known to be produced for the test model version.  
3. The method iterates over each string in `sample_expected` and uses `self.assertIn(sample, sql)` to assert that the generated SQL list includes each expected statement. This confirms that the drop‑index generation logic correctly creates statements for these indexes.  
4. A second tuple `sample_not_expected` holds a DROP INDEX statement that should **not** appear in the generated list (it targets a vocabulary table index).  
5. The method iterates over `sample_not_expected` and uses `self.assertNotIn(sample, sql)` to assert that the unwanted statement is absent, ensuring that the function respects the default `vocabulary=False` behavior and does not include vocabulary‑specific indexes when dropping non‑vocabulary indexes.  

**Relationship with callees**:  
- Calls `_indexes_sql` from `pedsnetdcc.indexes`. `_indexes_sql` builds the list of DROP INDEX statements by extracting index objects from transformed metadata and compiling them with SQLAlchemy’s `DropIndex` operation.  
- Relies on the test class’s `model_version` attribute to specify which model version’s metadata should be used.  

**Note**:  
- This test only checks the presence or absence of specific substrings within the generated SQL list; it does not validate the full ordering or completeness of all DROP statements.  
- The test assumes that the underlying metadata and transform definitions for the given `model_version` contain the three expected indexes and that no vocabulary indexes are included when `vocabulary` is left at its default value (`False`).  
- Because the test uses `assertIn` and `assertNotIn`, any change to the naming convention of generated indexes (e.g., a different hash suffix) will cause the test to fail. Adjust the expected strings accordingly if the index‑naming logic is modified.
***
### FunctionDef test_add_indexes_for_vocabulary(self)
**test_add_indexes_for_vocabulary**: The function of test_add_indexes_for_vocabulary is to verify that the SQL statements generated for vocabulary tables contain the expected CREATE INDEX commands and do not contain unrelated index statements.

**parameters**: This test method does not accept any external parameters. It operates on the instance attributes provided by the unittest framework (e.g., `self.model_version`).

**Code Description**:  
1. The method invokes the helper function `_indexes_sql` with two arguments: the model version stored in `self.model_version` and the flag `vocabulary=True`. This call returns a list of SQL strings that create indexes for the vocabulary portion of the transformed PEDSnet schema.  
2. A tuple `sample_expected` is defined, containing two exact CREATE INDEX statements that must be present in the generated SQL list:  
   - `CREATE INDEX idx_concept_class_id ON concept (concept_class_id)`  
   - `CREATE INDEX idx_concept_synonym_id ON concept_synonym (concept_id)`  
   The test iterates over this tuple and uses `self.assertIn` to assert that each expected statement appears somewhere in the `sql` list.  
3. A second tuple `sample_not_expected` is defined, containing two CREATE INDEX statements that are specific to non‑vocabulary tables and therefore must **not** be present when `vocabulary=True`:  
   - `CREATE INDEX con_lcn_f7a508db6a172c78291_ix ON concept_synonym (language_concept_name)`  
   - `CREATE INDEX con_s_d9ad76e415cb919c49e49_ix ON concept_class (site)`  
   The test iterates over this tuple and uses `self.assertNotIn` to confirm that each unwanted statement is absent from the `sql` list.  

The test therefore validates both the inclusion of required vocabulary indexes and the exclusion of site‑specific indexes when the vocabulary flag is active.

**Note**:  
- The test relies on the `self.model_version` attribute being correctly set in the test class setup; an incorrect version may cause the generated SQL to differ and lead to false failures.  
- The assertions compare full SQL strings; any change in formatting or naming conventions within `_indexes_sql` (e.g., index name generation) will require corresponding updates to the expected tuples.  
- This test does not interact with a database; it only checks the textual output of `_indexes_sql`.  
- The function under test, `_indexes_sql`, compiles index objects using the PostgreSQL dialect, so the expected strings are PostgreSQL‑specific.
***
## ClassDef IndexesDatabaseTest
**IndexesDatabaseTest**: The function of IndexesDatabaseTest is to verify that index creation and removal utilities work correctly for transformed PEDSnet CDM databases.

**attributes**:
· postgresql – a temporary PostgreSQL server instance created for the test suite.  
· dburi – the SQLAlchemy database URI pointing to the temporary PostgreSQL instance.  
· conn_str – a libpq connection string derived from dburi, used by the index utilities.  
· engine – a SQLAlchemy Engine object bound to the temporary database.  
· model_version – a string indicating the CDM version under test (default “2.2.0”).  
· metadata – a SQLAlchemy MetaData object representing the transformed PEDSnet schema after all registered transforms have been applied.

**Code Description**:
The class inherits from `unittest.TestCase` and defines a full lifecycle for testing index management functions (`add_indexes`, `drop_indexes`) that operate on a transformed PEDSnet CDM database.

*setUp* – Executed before each test method. It starts a fresh PostgreSQL instance using the `Postgresql` helper, builds the connection URI, creates a SQLAlchemy engine, and constructs the transformed metadata for the specified `model_version`. All transforms listed in `TRANSFORMS` are applied sequentially to the base metadata, ensuring the schema reflects the current transformation pipeline.

*tearDown* – Executed after each test method. It stops the temporary PostgreSQL server, automatically dropping the test database and releasing resources.

*expected_measurement_index_names* – Returns a hard‑coded `set` of index names that are expected to exist on the non‑vocabulary `measurement` table after the transformation pipeline has added its custom indexes. The list includes both generic indexes (e.g., `idx_measurement_concept_id`) and transformation‑specific indexes (e.g., `mea_pcn_74e171086ab53fdef03_ix`). This set is used as the reference for assertions in the test cases.

*expected_concept_index_names* – Returns a `set` of index names that belong to the vocabulary `concept` table. These indexes are deliberately **not** dropped by the default `drop_indexes` call, allowing the tests to verify that vocabulary indexes are preserved unless explicitly requested.

*test_drop* – Validates the drop‑index workflow for non‑vocabulary tables:
1. Calls `metadata.create_all(engine)` to materialize the transformed schema, including all indexes.
2. Reflects the `measurement` table and extracts its current index names.
3. Asserts that every name returned by `expected_measurement_index_names` is present.
4. Invokes `drop_indexes(conn_str, model_version)` to remove indexes from all non‑vocabulary tables.
5. Re‑reflects the `measurement` table and confirms that it now has zero indexes.
6. Reflects the `concept` table and checks that its indexes have **not** been removed (the set of actual concept indexes must differ from the expected set, proving they remain).
7. Ensures that a second call to `drop_indexes` without the `force` flag raises `psycopg2.ProgrammingError`, confirming that attempting to drop already‑dropped indexes is an error condition.

*test_add* – Verifies the add‑index workflow and interaction with the `vocabulary` flag:
1. Creates the full transformed schema.
2. Drops indexes from non‑vocabulary tables, then explicitly drops vocabulary indexes by passing `vocabulary=True`.
3. Confirms that both `measurement` and `concept` tables have zero indexes after the drops.
4. Calls `add_indexes(conn_str, model_version)` to recreate indexes on non‑vocabulary tables only.
5. Checks that the `measurement` table now contains exactly the indexes listed in `expected_measurement_index_names`.
6. Confirms that the `concept` table still has no indexes (vocabulary indexes were not re‑added).
7. Asserts that a second call to `add_indexes` without `force` raises `psycopg2.ProgrammingError`, demonstrating that duplicate index creation is prohibited.

*test_add_force* – Confirms that the `force=True` option allows idempotent index creation:
1. Builds the transformed schema (which already includes the required indexes).
2. Calls `add_indexes(..., force=True)`; no exception is raised even though the indexes already exist, proving that the `force` flag suppresses duplicate‑index errors.

*test_drop_force* – Confirms that the `force=True` option allows safe re‑dropping of indexes:
1. Builds the transformed schema.
2. Manually drops a single index (`idx_measurement_concept_id`) using a raw SQL `Statement`.
3. Verifies that the index is indeed missing from the reflected `measurement` table.
4. Calls `drop_indexes(..., force=True)`; the call succeeds without raising an error despite the missing index, demonstrating that `force` tolerates already‑absent indexes.

All test methods rely on SQLAlchemy’s reflection capabilities (`autoload=True`) to inspect the live database schema after each operation, and they use standard `unittest` assertions to validate expected conditions.

**Note**:
- The test suite assumes that the `add_indexes` and `drop_indexes` functions raise `psycopg2.ProgrammingError` when an operation would violate PostgreSQL’s index existence constraints, unless the `force` flag is set.
- The hard‑coded index name sets must be kept in sync with any changes to the transformation pipeline or CDM version; otherwise the tests will fail.
- The temporary PostgreSQL instance is created per test case, ensuring isolation but incurring a modest performance cost.

**Output Example**:
When `test_add` runs successfully, the assertion:

```python
self.assertEqual(self.expected_measurement_index_names(),
                 set([i.name for i in measurement.indexes]))
```

will compare two identical sets, for example:

```
{'idx_measurement_concept_id',
 'idx_measurement_person_id',
 'idx_measurement_visit_id',
 'mea_pcn_74e171086ab53fdef03_ix',
 'mea_maim_fafec5cb283b981155_ix',
 ...}
```

Both sets contain the same 15 index names, causing the test to pass without raising an exception.
### FunctionDef setUp(self)
**setUp**: The function of setUp is to prepare a temporary PostgreSQL database and a transformed PEDSnet metadata object for use in the test suite.

**parameters**:  
· **self** – the instance of the test case (IndexesDatabaseTest) on which the method is invoked.

**Code Description**:  
1. **Create a temporary PostgreSQL instance** – `self.postgresql = Postgresql()` starts a fresh PostgreSQL server in a temporary directory provided by the `testing.postgresql` package.  
2. **Build a database URI** – `self.dburi = self.postgresql.url()` retrieves the connection URI (e.g., `postgresql://user:pass@host:port/dbname`).  
3. **Convert the URI to a libpq connection string** – `self.conn_str = make_conn_str(self.dburi)` calls `pedsnetdcc.utils.make_conn_str`, which parses the URI and assembles a libpq‑compatible string that can be passed to `psycopg2` or SQLAlchemy.  
4. **Create a SQLAlchemy engine** – `self.engine = sqlalchemy.create_engine(self.dburi)` builds an engine bound to the temporary database, enabling execution of SQL statements during the tests.  
5. **Obtain the stock PEDSnet schema** – `self.model_version = '2.2.0'` selects the model version to test. `self.metadata = stock_metadata(self.model_version)` calls `pedsnetdcc.utils.stock_metadata`, which contacts the PEDSnet data‑model service and returns a fresh `sqlalchemy.MetaData` object containing the canonical tables, columns, and constraints for version 2.2.0.  
6. **Apply all registered schema transformations** – The loop `for t in TRANSFORMS: self.metadata = t.modify_metadata(self.metadata)` iterates over every `Transform` subclass listed in the global `TRANSFORMS` collection. For each transform, it invokes `Transform.modify_metadata`, which walks through all non‑vocabulary tables in the metadata and calls the subclass’s `modify_table` hook. The result is a metadata object that reflects all site‑specific or transformation‑specific alterations (additional columns, indexes, constraints, etc.) required for the test environment.

At the end of `setUp`, the test case has a live PostgreSQL instance (`self.postgresql`), a connection string (`self.conn_str`), a SQLAlchemy engine (`self.engine`), and a fully transformed metadata representation (`self.metadata`) ready for the individual test methods to use.

**Note**:  
- The temporary PostgreSQL server is automatically cleaned up by the `tearDown` method of the test case (not shown here).  
- `make_conn_str` may raise `ValueError` if the generated URI contains duplicate query parameters; the test environment assumes a well‑formed URI from `Postgresql`.  
- `stock_metadata` does not cache results; each call fetches the model definition anew, ensuring that tests always start from a clean, unmodified schema before transformations are applied.  
- The transformation loop mutates the original `MetaData` object in place; the reassignment `self.metadata = t.modify_metadata(self.metadata)` simply preserves the reference for readability.  
- Only non‑vocabulary tables are altered by `modify_metadata`; vocabulary tables remain unchanged, which is important for tests that verify that transformations do not affect those tables.
***
### FunctionDef tearDown(self)
**tearDown**: The function of tearDown is to clean up the test environment by shutting down the temporary PostgreSQL database after each test case.

**parameters**: This function does not accept any external parameters.

**Code Description**:  
- The method is defined as part of the `IndexesDatabaseTest` test class, which inherits from a unittest framework.  
- Inside the method, a single operation is performed: `self.postgresql.stop()`.  
- `self.postgresql` is an object that represents a running PostgreSQL instance created for the test suite (typically started in the `setUp` method).  
- Calling the `stop()` method on this object terminates the PostgreSQL server process, releases any allocated ports, and removes temporary data files associated with the test database.  
- By placing this call in `tearDown`, the test framework guarantees that the database is properly disposed of regardless of whether the test passed or failed, preventing resource leaks and ensuring isolation between test runs.

**Note**:  
- Ensure that `self.postgresql` has been correctly initialized before `tearDown` is invoked; otherwise, calling `stop()` may raise an exception.  
- This cleanup step is essential when running tests in parallel or in continuous integration environments to avoid leftover database processes that could interfere with subsequent test executions.
***
### FunctionDef expected_measurement_index_names(self)
**expected_measurement_index_names**: The function returns the complete set of index names that are expected to exist on the non‑vocabulary `measurement` table after the PEDSnet CDM transformations.

**parameters**: This function takes no parameters.

**Code Description**:  
- The method is defined inside the `IndexesDatabaseTest` test class.  
- It contains a single `return` statement that produces a Python `set` literal.  
- The set includes three generic index names that are created by the PEDSnet CDM core (`idx_measurement_concept_id`, `idx_measurement_person_id`, `idx_measurement_visit_id`).  
- In addition, the set lists a series of custom index names that follow the naming pattern `mea_<suffix>_<hash>_ix`. These correspond to indexes generated by the transformation scripts for the `measurement` table (e.g., `mea_pcn_74e171086ab53fdef03_ix`, `mea_maim_fafec5cb283b981155_ix`, etc.).  
- The comment above the return statement clarifies that the set represents *non‑vocabulary* indexes and that it may need to be updated if the CDM schema or transformation logic changes.

**Relationship with Callers**:  
- `IndexesDatabaseTest.test_drop` calls `expected_measurement_index_names()` to obtain the reference set of index names. After creating the transformed schema, the test verifies that each expected index is present on the `measurement` table before invoking `drop_indexes`. This ensures that the drop operation removes all of these indexes.  
- `IndexesDatabaseTest.test_add` also calls the function after re‑creating indexes with `add_indexes`. It compares the set returned by `expected_measurement_index_names()` with the actual index names on the `measurement` table to confirm that the add operation recreated exactly the expected indexes.  
- In both cases, the function serves as the authoritative source of expected index names, allowing the tests to assert correctness of the `add_indexes` and `drop_indexes` utilities.

**Note**:  
- The function is static in the sense that it does not depend on instance state; it simply returns a hard‑coded set.  
- If the PEDSnet CDM version is upgraded or if transformation scripts introduce new indexes or rename existing ones, this set must be updated accordingly to keep the tests accurate.  
- The returned set is unordered; callers typically convert it to a set or compare it directly with another set of index names.

**Output Example**:  
```python
{
    'idx_measurement_concept_id',
    'idx_measurement_person_id',
    'idx_measurement_visit_id',
    'mea_pcn_74e171086ab53fdef03_ix',
    'mea_maim_fafec5cb283b981155_ix',
    'mea_mcn_2396c11b8e9dc80fad6_ix',
    'mea_mraim_b3652804e85e68491_ix',
    'mea_ucn_a1d8526ef0526700f9b_ix',
    'mea_vacn_cdbccecc93bc04359c_ix',
    'mea_mtcn_0512b6f39c80e05694_ix',
    'mea_ocn_adee9ca63d3ce5cf5ca_ix',
    'mea_mscn_a15f3175cfbed7967a_ix',
    'mea_rlocn_49286b9222656be21_ix',
    'mea_s_c389be51cb02c33ef7d70_ix',
    'mea_rhocn_2ddf11b3636910434_ix',
}
```
***
### FunctionDef expected_concept_index_names(self)
**expected_concept_index_names**: The function of expected_concept_index_names is to return the set of index names that are expected to exist on the `concept` vocabulary table.

**parameters**: This function takes no parameters.

**Code Description**:  
The method is defined inside the `IndexesDatabaseTest` test class. It contains a single return statement that creates and returns a Python `set` literal. The set includes four string elements, each representing the name of an index that should be present on the `concept` table after the database schema is created:

- `'idx_concept_class_id'`
- `'idx_concept_code'`
- `'idx_concept_domain_id'`
- `'idx_concept_vocabulary_id'`

Because a set is unordered, the caller should treat the result as a collection of unique index names without relying on any specific ordering.

In the test suite, `expected_concept_index_names` is invoked by the `test_drop` method. After the test drops all non‑vocabulary indexes, it reloads the `concept` table metadata and extracts the actual index names. The test then asserts that the actual list of concept indexes **does not** equal the set returned by `expected_concept_index_names`. This check verifies that the `drop_indexes` utility correctly preserves vocabulary‑related indexes while removing others.

**Note**:  
- The function is intended solely for test verification; it does not interact with the database itself.  
- Since it returns a set, callers should compare it using set operations (e.g., `assertIn`, `assertEqual` with another set) rather than list indexing.  
- Modifying the returned set will not affect the original definition; each call creates a new set instance.

**Output Example**:  
```python
{
    'idx_concept_class_id',
    'idx_concept_code',
    'idx_concept_domain_id',
    'idx_concept_vocabulary_id'
}
```
***
### FunctionDef test_drop(self)
**test_drop**: The function of test_drop is to verify that the `drop_indexes` utility correctly removes all indexes from non‑vocabulary tables while preserving indexes on vocabulary tables, and that a second drop attempt raises an error when `force` is not enabled.  

**parameters**:  
· **self** – the instance of the `IndexesDatabaseTest` unittest class; provides access to the test database engine, connection string, model version, and helper methods used within the test.  

**Code Description**:  
1. **Create transformed schema** – `self.metadata.create_all(self.engine)` materialises the full PEDSnet CDM schema (including all tables and indexes) in the temporary test database.  
2. **Load measurement table metadata** – A `sqlalchemy.Table` object named `measurement` is instantiated with `autoload=True` and `autoload_with=self.engine`. This reflects the current state of the `measurement` table, including any automatically generated indexes.  
3. **Collect existing index names** – The list comprehension `[i.name for i in measurement.indexes]` extracts the names of all indexes currently attached to the `measurement` table.  
4. **Assert presence of expected non‑vocabulary indexes** – The loop `for idx in self.expected_measurement_index_names(): self.assertIn(idx, index_names)` checks that every index name defined in `expected_measurement_index_names` is present in the reflected list, confirming that the schema was created correctly.  
5. **Invoke drop operation** – `drop_indexes(self.conn_str, self.model_version)` is called with default arguments (`force=False`, `vocabulary=False`). This triggers the `drop_indexes` function to generate and execute `DROP INDEX` statements for all non‑vocabulary tables, including `measurement`.  
6. **Re‑reflect measurement table** – After the drop operation, a new `sqlalchemy.Table` instance for `measurement` is created to obtain the updated metadata.  
7. **Verify removal of all indexes** – `self.assertEqual(len(measurement.indexes), 0)` asserts that the `measurement` table now has zero indexes, confirming that the drop succeeded.  
8. **Check that vocabulary indexes remain** – A `sqlalchemy.Table` for the `concept` vocabulary table is reflected. Its index names are collected, and `self.assertNotEqual(self.expected_concept_index_names(), concept_index_names)` ensures that the set of concept indexes is **not** equal to the expected set, i.e., the vocabulary indexes were not dropped. (The test expects the actual list to contain the expected indexes, so inequality indicates they are still present.)  
9. **Validate error on double drop** – The `with self.assertRaises(psycopg2.ProgrammingError): drop_indexes(self.conn_str, self.model_version)` block calls `drop_indexes` a second time without `force=True`. Because the indexes have already been removed, the underlying PostgreSQL `DROP INDEX` statements raise a `ProgrammingError`, which the test expects and captures.  

**Relationship with callees**:  
- Calls `self.metadata.create_all` (SQLAlchemy) to build the schema.  
- Calls `sqlalchemy.Table` with `autoload=True` to reflect table definitions.  
- Calls `self.expected_measurement_index_names()` and `self.expected_concept_index_names()` to obtain the authoritative sets of index names for verification.  
- Calls the project function `pedsnetdcc.indexes.drop_indexes` to perform the actual index removal.  
- Relies on the `psycopg2` exception class `ProgrammingError` to detect the expected failure on a second drop attempt.  

**Note**:  
- The test uses a fresh in‑memory or temporary PostgreSQL database; any failure to create the schema will cause the test to abort before the drop verification.  
- `drop_indexes` is executed with its default `force=False`; therefore, any attempt to drop a non‑existent index results in a `ProgrammingError`. This behavior is intentionally tested in the final step.  
- The assertion `assertNotEqual` for concept indexes checks that the vocabulary indexes were **not** removed; the test does not explicitly compare the sets for equality because the order of reflection may differ, but inequality confirms that the expected indexes are still present.  
- Because the test re‑creates the `measurement` Table object after dropping indexes, it ensures that the metadata cache is refreshed; otherwise, stale index information could lead to a false positive.  
- The method does not return a value; its success is determined solely by the absence of assertion failures and the correct raising of the expected exception.
***
### FunctionDef test_add(self)
**test_add**: The function of test_add is to verify that index removal and creation work correctly for both non‑vocabulary and vocabulary tables in a transformed PEDSnet database.

**parameters**: This method does not accept any parameters.

**Code Description**:  
1. The test begins by creating all tables defined in the transformed PEDSnet metadata on the test database engine (`self.metadata.create_all(self.engine)`).  
2. It calls `drop_indexes` with the current connection string and model version to remove any existing indexes on the non‑vocabulary (site) tables.  
3. It calls `drop_indexes` again with `vocabulary=True` to remove indexes on the vocabulary tables. At this point both schema groups should have no indexes.  
4. The test loads the `measurement` table definition using SQLAlchemy’s reflection (`autoload=True`) and asserts that the `indexes` collection is empty, confirming that the previous drop operation succeeded for a non‑vocabulary table.  
5. It similarly reflects the `concept` table (a vocabulary table) and asserts that it also has zero indexes.  
6. The test invokes `add_indexes` (with default `vocabulary=False`) to create indexes on all non‑vocabulary tables.  
7. After index creation, the `measurement` table is reflected again and the test compares the set of actual index names (`i.name for i in measurement.indexes`) with the expected set returned by `self.expected_measurement_index_names()`. The assertion ensures that every expected non‑vocabulary index was created and that no unexpected indexes exist.  
8. The `concept` table is reflected once more and the test asserts that it still has zero indexes, confirming that `add_indexes` did not affect vocabulary tables when `vocabulary=False`.  
9. Finally, the test verifies that attempting to add the same indexes a second time raises a `psycopg2.ProgrammingError`. This is done by wrapping a second call to `add_indexes` in `self.assertRaises`. The exception is expected because the indexes already exist and `force` is not set to `True`, causing the underlying PostgreSQL error to propagate.

**Note**:  
- The test relies on the helper methods `drop_indexes` and `add_indexes` from `pedsnetdcc.indexes`. `drop_indexes` removes indexes, while `add_indexes` creates them; both operate on either the site or vocabulary schema based on the `vocabulary` flag.  
- The expected index names for the `measurement` table are provided by `expected_measurement_index_names`, a method that returns a hard‑coded set of index identifiers. If the CDM model version or transformation logic changes, this set must be updated to keep the test valid.  
- The use of SQLAlchemy reflection (`autoload=True`) ensures that the test inspects the actual database state rather than any in‑memory model.  
- The final `assertRaises` check confirms that `add_indexes` does not silently ignore duplicate index creation unless the `force` flag is used, which is important for detecting unintended re‑execution of index scripts.
***
### FunctionDef test_add_force(self)
**test_add_force**: The function of test_add_force is to verify that calling `add_indexes` with the `force=True` flag does not raise an exception when the indexes already exist.

**parameters**: This test method does not accept any external parameters.

**Code Description**:  
1. The test begins by creating all tables and indexes defined in the test metadata using `self.metadata.create_all(self.engine)`. This step materializes the transformed PEDSnet schema, including the index objects, in the temporary test database.  
2. After the schema is in place, the test invokes `add_indexes` with the following arguments:  
   - `self.conn_str`: a PostgreSQL connection string that points to the test database.  
   - `self.model_version`: the version identifier of the PEDSnet data model used for the test.  
   - `force=True`: instructs `add_indexes` to ignore benign errors such as “index already exists”.  
   The `vocabulary` argument is omitted, so it defaults to `False`, meaning the function will target the site (non‑vocabulary) schema.  
3. `add_indexes` internally calls `_process_indexes` with `drop=False`. `_process_indexes` generates the CREATE INDEX statements for the specified model version, wraps each statement in a `Statement` object, and executes them in parallel. Because the indexes already exist from step 1, each execution would normally raise a “object already exists” error. However, the `force=True` flag causes `_check_stmt_err` to treat these errors as benign, suppress them, and allow the process to continue.  
4. The test completes without asserting any return value; its success criterion is the absence of an exception. If `add_indexes` were to raise an error despite `force=True`, the test framework would mark the test as failed.

**Note**:  
- This test is specific to the non‑vocabulary schema because the default `vocabulary=False` is used. A separate test would be required for the vocabulary schema.  
- The `force` flag is essential for idempotent index creation; without it, attempting to create an existing index would raise a `DatabaseError` and cause the test to fail.  
- The test relies on the SQLAlchemy `metadata` object (`self.metadata`) being correctly configured with the index definitions for the given `model_version`.  
- No explicit assertions are made; the test’s purpose is to ensure that `add_indexes` handles the “already exists” condition gracefully when `force=True`.
***
### FunctionDef test_drop_force(self)
**test_drop_force**: The function of test_drop_force is to verify that calling drop_indexes with force=True does not raise an exception even when some indexes have already been removed manually.

**parameters**: This test method takes no explicit parameters; it operates on the instance attributes set up by the IndexesDatabaseTest test case (e.g., self.metadata, self.engine, self.conn_str, self.model_version, self.metadata).

**Code Description**:  
1. **Create the transformed PEDSnet schema** – `self.metadata.create_all(self.engine)` uses SQLAlchemy metadata to generate all tables and indexes defined for the test model version in the temporary test database. This ensures the database starts in a known state with every expected index present.  

2. **Manually drop a single index** – `Statement('DROP INDEX idx_measurement_concept_id').execute(self.conn_str)` creates a Statement object containing the raw SQL `DROP INDEX idx_measurement_concept_id` and immediately executes it against the test database. The Statement.execute method opens a new autocommit connection, runs the command, and returns the same Statement instance. This step simulates a situation where an index is missing before the bulk‑drop operation is invoked.  

3. **Confirm the index is absent** – A fresh SQLAlchemy Table object for `measurement` is reflected from the live database (`autoload=True`). The test then asserts that the name `idx_measurement_concept_id` does not appear in the list of index objects attached to the table. This guarantees that the manual drop succeeded.  

4. **Invoke the bulk‑drop routine with force** – `drop_indexes(self.conn_str, self.model_version, force=True)` calls the library function that iterates over all index definitions for the non‑vocabulary (site) tables and issues DROP INDEX statements for each. Because force=True, any “object does not exist” errors (such as the one for the already‑dropped idx_measurement_concept_id) are caught and ignored inside drop_indexes /_process_indexes_, allowing the function to complete without raising.  

5. **Implicit verification** – The test does not contain an explicit assertion after the call to drop_indexes; the absence of an exception is itself the success criterion. If drop_indexes were to raise, the test framework would mark the test as failed.  

**Relationship with callees**:  
- **Statement.execute** – Used to run the raw DROP INDEX SQL. The method handles connection creation, autocommit mode, error capture, and logging. In this test it is expected to succeed, so Statement.err remains None.  
- **drop_indexes** – The primary function under test. It forwards its arguments to the internal helper _process_indexes with drop=True. _process_indexes generates the appropriate DROP statements, wraps each in a Statement, executes them (potentially in parallel), and suppresses benign “does not exist” errors when force=True.  

**Note**:  
- The test relies on the test database being freshly created for each run; otherwise residual indexes could affect the outcome.  
- Because drop_indexes executes each DROP statement in its own autocommit connection, the manual drop performed earlier is immediately visible to the subsequent bulk operation.  
- The force=True flag is essential; without it the second DROP INDEX for idx_measurement_concept_id would raise a psycopg2.ProgrammingError and cause the test to fail.  
- No explicit cleanup of the manually dropped index is required; the subsequent call to drop_indexes covers all remaining indexes, leaving the database in a clean state for other tests.
***
