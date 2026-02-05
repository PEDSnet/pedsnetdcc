## ClassDef IDMappingTransformTest
**IDMappingTransformTest**: The function of IDMappingTransformTest is to verify that the IDMappingTransform utilities correctly modify SQLAlchemy select statements and metadata for PEDSnet data model transformations.  

**attributes**:  
· None – the test class does not define instance attributes beyond those created in `setUp`.  

**Code Description**:  
The class inherits from `unittest.TestCase` and contains a suite of unit tests for the `IDMappingTransform` component.  

- `setUp(self)`: Executed before each test method. It creates a metadata object for version 2.2.0 by calling `stock_metadata('2.2.0')` and stores it in `self.metadata`. This metadata represents the database schema used throughout the tests.  

- `test_modify_person_select(self)`:  
  1. Retrieves the `person` table from the metadata.  
  2. Builds a basic SQLAlchemy `select` object that selects all columns from `person`.  
  3. Calls `IDMappingTransform.modify_select` with the metadata, the table name `'person'`, the select object, and the initial join target (`person`). The method returns a possibly altered select object and a new join target.  
  4. Re‑associates the select with the returned join target via `s.select_from(j)`.  
  5. Compiles the statement to a PostgreSQL‑dialect SQL string.  
  6. Asserts that the generated SQL contains the expected column aliases (`person.person_id AS site_id`, `person_ids.dcc_id AS person_id`, etc.) and the required `JOIN` clauses linking the `person` table to the ID mapping tables (`person_ids`, `care_site_ids`, `provider_ids`, `location_ids`). The test confirms that the transformation adds both site‑id columns and the appropriate joins for foreign‑key references.  

- `test_modify_fact_relationship_select(self)`:  
  1. Retrieves the `fact_relationship` table from the metadata.  
  2. Constructs a select that pulls all columns from `fact_relationship`.  
  3. Invokes `IDMappingTransform.modify_select` with the table name `'fact_relationship'`.  
  4. Re‑binds the select to the returned join target and compiles it using the PostgreSQL dialect.  
  5. Formats the compiled statement with its bound parameters (`% stmt.params`).  
  6. Checks that the SQL includes the expected site‑id aliases for both fact identifiers (`fact_id_1 AS site_id_1`, `fact_id_2 AS site_id_2`).  
  7. Verifies that the transformation adds `CASE` expressions that map each `domain_concept_id` to the appropriate ID mapping table (`visit_occurrence_ids`, `observation_ids`, `measurement_ids`).  
  8. Confirms that `LEFT OUTER JOIN` clauses are generated for each possible domain concept, with correct join conditions linking the original fact IDs to the mapping tables based on the domain concept values (8, 27, 21).  

- `test_modify_metadata(self)`:  
  1. Calls `IDMappingTransform.modify_metadata` with the original metadata.  
  2. Asserts that the transformed metadata now contains a column named `site_id` in the `person` table (`metadata.tables['person'].c`).  
  3. Asserts that the `fact_relationship` table does **not** gain a `site_id` column, confirming that the transformation only adds the column where appropriate.  

- `test_pre_transform(self)`:  
  1. Checks for the presence of environment variables `PEDSNETDCC_TEST_DBURI` and `PEDSNETDCC_TEST_SEARCH_PATH`. If either is missing, the test is skipped with an explanatory message.  
  2. Constructs a connection string using `make_conn_str` with the provided URI and search path.  
  3. Executes `IDMappingTransform.pre_transform` with the connection string and the metadata. The test does not validate the outcome; it merely ensures that the method can be called without raising an exception when the required environment is configured.  

- `test_with_data(self)`:  
  This placeholder test is marked as skipped with the message “Not implemented yet”. It indicates a future intention to run the transformation against real test data and verify the results.  

**Note**:  
- The test class relies on external helper functions (`stock_metadata`, `make_conn_str`) and the `IDMappingTransform` implementation, which must be importable in the test environment.  
- The `test_pre_transform` method will be skipped unless both required environment variables are defined; ensure they are set when running the full test suite.  
- Assertions focus on the presence of specific SQL fragments; changes to the underlying SQL generation (e.g., alias naming conventions) may require updates to the expected strings.  
- No persistent database modifications are performed; the tests operate purely on compiled SQL strings and in‑memory metadata.  
### FunctionDef setUp(self)
**setUp**: The function of setUp is to create a fresh SQLAlchemy MetaData object representing the stock PEDSnet data model for version 2.2.0 and store it on the test instance.

**parameters**: This method takes no external parameters; it operates on the test instance (`self`).

**Code Description**:  
1. The method invokes `stock_metadata('2.2.0')`.  
2. `stock_metadata` (defined in *pedsnetdcc/utils.py*) constructs an empty `sqlalchemy.MetaData` instance and populates it by calling `dmsa.make_model_from_service` with the service name `'pedsnet'`, the supplied model version `'2.2.0'`, the constant `DATA_MODELS_SERVICE`, and the empty metadata object.  
3. The service call retrieves the canonical PEDSnet schema for version 2.2.0 and registers all tables, columns, primary‑key constraints, foreign‑key constraints, and other schema objects into the metadata.  
4. The populated `MetaData` instance is returned by `stock_metadata` and assigned to `self.metadata`. This makes the schema available to all subsequent test methods in the `IDMappingTransformTest` class.

**Relationship with Callees**:  
- The only direct callee is `stock_metadata`. By using this helper, `setUp` ensures that each test starts with a clean, version‑specific representation of the untransformed PEDSnet model. All test methods that need to inspect tables, columns, or constraints can rely on `self.metadata` without having to repeat the model‑loading logic.

**Note**:  
- `setUp` is executed automatically by the unittest framework before each test method, guaranteeing isolation between tests.  
- Because `stock_metadata` does not cache results, a network request to the data‑model service occurs for every test run; this ensures the metadata reflects the latest definition for version 2.2.0 but may add latency.  
- If additional transformations are required for a particular test, they should be applied to `self.metadata` after this initialization, preserving the original stock model for reference.
***
### FunctionDef test_modify_person_select(self)
**test_modify_person_select**: The function of test_modify_person_select is to verify that IDMappingTransform.modify_select correctly rewrites the SELECT clause and JOIN expression for the person table, producing the expected column aliases and JOIN statements for site‑to‑DCC ID mapping.

**parameters**:  
· self – the instance of the unittest.TestCase subclass IDMappingTransformTest that provides access to the test fixture, including the metadata object containing the source schema.

**Code Description**:  
1. The test retrieves the person Table object from the test metadata (`person = self.metadata.tables['person']`).  
2. It creates a basic SQLAlchemy Select object that selects all columns from person (`s = sqlalchemy.select([person])`). The initial join variable is set to the table itself (`j = person`).  
3. The core of the test calls the transformation under test:  
   `s, j = IDMappingTransform.modify_select(self.metadata, 'person', s, j)`  
   This method is expected to (a) replace the primary‑key column person_id with the DCC ID column from the mapping table person_ids, (b) add a site‑specific person_id column labeled site_id, and (c) add LEFT OUTER JOINs to the mapping tables for each foreign‑key column that references another data table (location_id, care_site_id, provider_id).  
4. After the transformation, the test re‑associates the SELECT with the updated FROM/JOIN clause (`s = s.select_from(j)`).  
5. The SELECT is compiled to a PostgreSQL‑dialect SQL string (`sql = str(s.compile(dialect=sqlalchemy.dialects.postgresql.dialect()))`).  
6. A series of assertions check that the generated SQL contains the exact fragments that the transformation should have produced:  
   - `person.person_id AS site_id` – the original site‑specific primary key is retained under the alias site_id.  
   - `person_ids.dcc_id AS person_id` – the DCC identifier column from the mapping table replaces the original primary‑key column.  
   - `location_ids.dcc_id AS location_id`, `care_site_ids.dcc_id AS care_site_id`, `provider_ids.dcc_id AS provider_id` – each foreign‑key column is replaced by the corresponding DCC ID column from its mapping table.  
   - `JOIN person_ids ON person.person_id = person_ids.site_id` – an inner join to the person_ids mapping table for the primary key.  
   - `JOIN care_site_ids ON person.care_site_id = care_site_ids.site_id` – an inner join for the non‑nullable care_site_id foreign key.  
   - `LEFT OUTER JOIN provider_ids ON person.provider_id = provider_ids.site_id` – a left outer join for the nullable provider_id foreign key.  
   - `LEFT OUTER JOIN location_ids ON person.location_id = location_ids.site_id` – a left outer join for the nullable location_id foreign key.  

The test does not modify any database objects; it only inspects the generated SQL string to ensure that IDMappingTransform.modify_select behaves as specified.

**Note**:  
- The test assumes that the metadata supplied to the test already contains the person table and that the foreign‑key columns location_id, care_site_id, and provider_id are defined with the appropriate nullability (nullable columns result in LEFT OUTER JOINs).  
- The transformation relies on naming conventions: mapping tables are named <source_table>_ids (e.g., person_ids) and contain columns dcc_id and site_id.  
- The test checks only the presence of expected SQL fragments; it does not validate the full correctness of the SELECT list ordering or other unrelated columns.  
- If the implementation of IDMappingTransform.modify_select changes its join type or alias naming, the assertions in this test will need to be updated accordingly.
***
### FunctionDef test_modify_fact_relationship_select(self)
**test_modify_fact_relationship_select**: The function of test_modify_fact_relationship_select is to verify that IDMappingTransform.modify_select correctly rewrites the SELECT statement for the fact_relationship table, adding the expected CASE expressions and LEFT OUTER JOINs for each domain‑specific mapping table.

**parameters**: The parameters of this Function.  
· self: the instance of the IDMappingTransformTest unittest class; provides access to the test fixture, including the metadata object that describes the source schema.

**Code Description**:  
1. Retrieve the fact_relationship Table object from the test metadata (`fact_rel = self.metadata.tables['fact_relationship']`).  
2. Build a basic SQLAlchemy Select that selects every column from fact_relationship (`s = sqlalchemy.select([fact_rel])`).  
3. Initialise the join clause with the table itself (`j = fact_rel`).  
4. Invoke the transformation under test:  
   `s, j = IDMappingTransform.modify_select(self.metadata, 'fact_relationship', s, j)`  
   This call returns a possibly‑modified Select object `s` and an updated Join object `j`.  
5. Attach the new FROM clause to the select (`s = s.select_from(j)`).  
6. Compile the statement using the PostgreSQL dialect (`stmt = s.compile(dialect=sqlalchemy.dialects.postgresql.dialect())`).  
7. Convert the compiled statement to a plain SQL string with bound parameters substituted (`sql = str(stmt) % stmt.params`).  
8. Execute a series of assertIn checks against the generated SQL string to confirm that the transformation produced:  
   - Aliased site‑ID columns (`fact_relationship.fact_id_1 AS site_id_1` and `fact_relationship.fact_id_2 AS site_id_2`).  
   - Two CASE expressions that map domain_concept_id_1 and domain_concept_id_2 to the appropriate DCC ID columns from the mapping tables (`visit_occurrence_ids`, `observation_ids`, `measurement_ids`).  
   - LEFT OUTER JOIN clauses for each mapping table, each joined on the original fact_id column and the corresponding domain_concept_id constant (8 for visit_occurrence, 27 for observation, 21 for measurement).  
   The assertions are written as literal string fragments, ensuring that the generated SQL contains the exact patterns required by the ID‑mapping logic.

**Relationship with Callers**:  
- This test method is part of the IDMappingTransformTest unittest suite.  
- It directly calls IDMappingTransform.modify_select, which is the core routine responsible for rewriting SELECT statements for all tables, including the special handling of fact_relationship.  
- The test does not invoke any higher‑level transformation runner; it isolates the modify_select method to focus on its output for a single table.  
- The compiled SQL string is later used by the transformation pipeline in pedsnetdcc/transform_runner.py to create the final CREATE TABLE AS SELECT statement for the target schema.

**Note**:  
- The test assumes that the metadata object contains a fully defined fact_relationship table and that the global constant FACT_RELATIONSHIP_DOMAINS maps domain concept IDs 8, 27, and 21 to the tables visit_occurrence, observation, and measurement respectively.  
- The SQL is compiled with the PostgreSQL dialect; therefore the expected fragments include PostgreSQL‑specific syntax such as LEFT OUTER JOIN and the use of CASE expressions.  
- If the implementation of IDMappingTransform.modify_select changes (e.g., different alias naming or join ordering), the literal string assertions may need to be updated accordingly.  
- The test does not verify column ordering or the presence of other columns; it only checks that the required transformation pieces appear somewhere in the generated SQL.
***
### FunctionDef test_modify_metadata(self)
**test_modify_metadata**: The function of test_modify_metadata is to verify that the IDMappingTransform correctly adds a `site_id` column to non‑vocabulary tables and does not add it to the `fact_relationship` table after the metadata transformation.

**parameters**: This test method takes no explicit parameters; it operates on the instance attributes set up by the test class (`self.metadata`).

**Code Description**:  
1. The test invokes `IDMappingTransform.modify_metadata(self.metadata)`.  
   - `modify_metadata` is defined in the abstract `Transform` base class and iterates over every table in the supplied `sqlalchemy.MetaData` object, skipping tables listed in the constant `VOCAB_TABLES`.  
   - For each remaining table it calls `IDMappingTransform.modify_table(metadata, table)`.  
   - `IDMappingTransform.modify_table` adds a `site_id` column to tables that have a single‑column primary key (except the special‑case `death` table) and adds `site_id_1` and `site_id_2` columns to the `fact_relationship` table.  

2. After the transformation, the test asserts two conditions:  
   - `self.assertTrue('site_id' in metadata.tables['person'].c)` checks that the `person` table now contains a column named `site_id`. This confirms that `modify_table` was applied to a regular non‑vocabulary table.  
   - `self.assertFalse('site_id' in metadata.tables['fact_relationship'].c)` checks that the `fact_relationship` table does **not** contain a column named `site_id`. The `fact_relationship` table receives `site_id_1` and `site_id_2` instead, so the presence of a plain `site_id` would be incorrect.  

The test therefore validates the selective column‑addition logic implemented in `IDMappingTransform.modify_table` via the generic `modify_metadata` driver.

**Note**:  
- The test relies on the `self.metadata` fixture being a complete `MetaData` instance that includes both the `person` and `fact_relationship` tables.  
- Because `modify_metadata` mutates the original `MetaData` object in place, the returned `metadata` reference is the same object that was passed in; the test works directly with this mutated object.  
- The assertion for `fact_relationship` specifically checks for the absence of a generic `site_id` column; the correct behavior is the presence of `site_id_1` and `site_id_2`, which are not examined in this test.  
- No database connection is required for this test; it operates purely on the in‑memory SQLAlchemy schema representation.
***
### FunctionDef test_pre_transform(self)
**test_pre_transform**: The function of test_pre_transform is to verify that the IDMappingTransform.pre_transform hook can be executed against a real database without raising errors.

**parameters**:  
· **self** – the instance of the IDMappingTransformTest unittest class; provides access to test utilities such as skipTest and the metadata attribute used by the transform.

**Code Description**:  
The method begins by defining the names of two environment variables that must be present for the test to run: `PEDSNETDCC_TEST_DBURI` (which holds a PostgreSQL URI) and `PEDSNETDCC_TEST_SEARCH_PATH` (which specifies the schema search_path to be used.  

It then checks whether **both** variables are defined in `os.environ`. If either variable is missing, the test is skipped by calling `self.skipTest` with a message that lists the missing variables and explains that they are required for testing `IDMappingTransform.pre_transform`.  

When the required environment variables are present, the test builds a libpq connection string by calling `make_conn_str`. The function receives the URI from `os.environ[dburi_var]` and the search_path from `os.environ[search_path_var]`. The resulting string (`conn_str`) is suitable for use with psycopg2 connections.  

Finally, the test invokes the class method `IDMappingTransform.pre_transform`, passing the constructed connection string and the test metadata (`self.metadata`). No assertions are made; the test succeeds if the call completes without raising an exception. Any exception propagates and causes the test to fail, indicating that the pre‑transform hook is not operational under the supplied database configuration.

**Note**:  
- The test does not create or verify any database objects; it only checks that the method can be called with valid arguments.  
- Because the test depends on external environment variables, it will be skipped in environments where those variables are not set, preventing false failures in CI pipelines that lack a test database.  
- The test relies on the helper `make_conn_str` to translate the URI and search_path into a libpq connection string; any change in that helper’s signature or behavior may require corresponding updates to this test.  
- The test uses the `self.metadata` fixture supplied by the test class; the metadata must describe the target model version so that `IDMappingTransform.pre_transform` can iterate over its tables.
***
### FunctionDef test_with_data(self)
**test_with_data**: The function of test_with_data is a placeholder unit test that is currently skipped because it has not been implemented.  

**parameters**: The parameters of this Function.  
· self: The instance of the IDMappingTransformTest class on which the test method is invoked.  

**Code Description**: The method defines a test case within the IDMappingTransformTest test suite. Inside the method, a comment indicates the intended future work: to use test data and verify the results of a transformation. Immediately after the comment, the method calls `self.skipTest('Not implemented yet')`, which tells the unittest framework to skip this test case at runtime, marking it as not executed and providing the reason “Not implemented yet”. No other logic, assertions, or data handling are present.  

**Note**: This test will not run any validation until the TODO is addressed. Developers should replace the skip call with actual test data setup, execution of the transformation logic, and appropriate assertions to verify expected outcomes. Until then, the test remains a reminder of pending implementation.
***
