## ClassDef ConceptNameTest
**ConceptNameTest**: The function of ConceptNameTest is to verify that the ConceptNameTransform utility correctly modifies SQL SELECT statements and metadata to include concept name columns and associated indexes.

**attributes**: The attributes of this Class.
· metadata – an instance of sqlalchemy.MetaData used to hold table definitions for the tests.  
· table1 – a sqlalchemy.Table representing a test table with columns `foo_concept_id` and `bar_concept_id`.  
· table2 – a sqlalchemy.Table representing a second test table with a single column `baz_concept_id`.  
· concept – a sqlalchemy.Table named `concept` containing `concept_id` (Integer) and `concept_name` (String(512)) columns, used as the lookup table for concept names.

**Code Description**:  
The class inherits from `unittest.TestCase`, providing two test methods that exercise the behavior of `ConceptNameTransform`.  

- **setUp**: Executed before each test, it creates a fresh `MetaData` object and defines three tables (`table1`, `table2`, and `concept`) with the appropriate columns. This isolated schema serves as the input for the transformation functions under test.  

- **test_modify_select**:  
  1. Constructs a simple SELECT object that selects all columns from `table1`.  
  2. Calls `ConceptNameTransform.modify_select`, passing the metadata, a placeholder schema name `'user'`, the SELECT object, and the original join object (`self.table1`).  
  3. The method returns a potentially altered SELECT object and a new join object that includes LEFT OUTER JOINs to the `concept` table, aliased as `concept_1` and `concept_2`.  
  4. The SELECT is re‑bound to the new join using `select_from`.  
  5. The resulting SQL string is compiled for the PostgreSQL dialect and compared against an expected multi‑line SQL string (normalized by the helper `clean`).  
  The test asserts that the transformation adds `concept_name` columns with appropriate aliases (`foo_concept_name`, `bar_concept_name`) and generates the correct LEFT OUTER JOIN clauses.

- **test_modify_metadata**:  
  1. Invokes `ConceptNameTransform.modify_metadata` with the original metadata.  
  2. Retrieves the indexes attached to `table1` after transformation.  
  3. Asserts that exactly two indexes have been created.  
  4. For each index, the test compiles the DDL using `CreateIndex` for the PostgreSQL dialect and compares it to the expected CREATE INDEX statements.  
  The expected indexes are named `tab_fcn_81387a6132eed7f69b5_ix` (on `foo_concept_name`) and `tab_bcn_7baa5e16ad1f8129b90_ix` (on `bar_concept_name`). If any other index appears, the test fails.

Both tests rely on helper functions (`clean`) to normalize whitespace and on the PostgreSQL dialect to ensure deterministic SQL rendering.

**Note**:  
- The tests assume that `ConceptNameTransform.modify_select` returns both a SELECT object and a join object; the join object must be supplied to `select_from` before compilation.  
- Index names are generated deterministically by the transformation logic; any change in the naming scheme will cause the test to fail.  
- The comparison of SQL strings is whitespace‑insensitive due to the `clean` helper, but the order of clauses and alias names must match exactly.  
- The PostgreSQL dialect is explicitly used; running the tests against a different dialect may produce different SQL syntax and cause failures.  
- The test suite does not interact with a live database; it operates purely on SQLAlchemy schema objects and compiled SQL strings.
### FunctionDef setUp(self)
**setUp**: The function of setUp is to create an in‑memory SQLAlchemy metadata schema with sample tables used by the test cases.  

**parameters**: The parameters of this Function.  
· self: the instance of the test case class (ConceptNameTest).  

**Code Description**:  
- Initializes a new `MetaData` object and assigns it to `self.metadata`. This object will hold the definitions of all tables created for the test.  
- Defines two integer columns, `foo_concept_id` and `bar_concept_id`, using `Column` with the `Integer` type.  
- Constructs a table named `table1` with the previously defined columns and registers it in `self.metadata`. The resulting `Table` object is stored in `self.table1`.  
- Defines a third integer column, `baz_concept_id`.  
- Constructs a second table named `table2` that contains only the `baz_concept_id` column, registers it in the same metadata, and stores the `Table` object in `self.table2`.  
- Finally, creates a `concept` table with two columns: `concept_id` (integer) and `concept_name` (string with a maximum length of 512 characters). This table is also added to `self.metadata` and the `Table` object is stored in `self.concept`.  

The method does not return a value; its purpose is to set up the schema objects that other test methods will reference.  

**Note**:  
- The tables are defined only in SQLAlchemy’s metadata; they are not automatically created in a database. Test code must bind the metadata to an engine and invoke `metadata.create_all()` if actual database tables are required.  
- All column names follow the convention `<entity>_concept_id`, matching the expected schema for concept‑related transformations in the test suite.  
- Because `setUp` is executed before each test method (as per unittest conventions), the metadata and table objects are freshly instantiated for every test, ensuring isolation between tests.
***
### FunctionDef test_modify_select(self)
**test_modify_select**: The function of test_modify_select is to verify that ConceptNameTransform.modify_select correctly adds *_concept_name* columns and the required LEFT OUTER JOINs for each *_concept_id* column in a table’s SELECT statement.

**parameters**:  
· **self** – the instance of the ConceptNameTest unittest class; it provides access to the test fixtures metadata, table1, and other helper attributes.

**Code Description**:  
1. **Create a base SELECT** – The test builds an initial SQLAlchemy Select object that selects all columns from self.table1 by calling `select([self.table1])`. At this point select_obj represents `SELECT * FROM table1`.  
2. **Initialize the FROM clause** – `join_obj` is set to `self.table1`, which will serve as the starting FROM expression for the query.  
3. **Apply the transformation** – The test invokes `ConceptNameTransform.modify_select` with the current metadata, a placeholder table name `'user'`, and the previously created select_obj and join_obj. The method returns a possibly‑modified pair (select_obj, join_obj) where:
   - New columns named `<original>_concept_name` have been added to the SELECT list for every column ending in `_concept_id`.  
   - Corresponding LEFT OUTER JOINs to distinct aliases of the concept table have been appended to the join expression.  
4. **Bind the FROM clause** – After the transformation, the test re‑binds the SELECT object to the updated join expression by calling `select_obj = select_obj.select_from(join_obj)`. This produces a complete SELECT statement that includes both the original columns and the newly added concept name columns.  
5. **Compile to SQL** – The SELECT object is compiled to a PostgreSQL‑dialect string with `select_obj.compile(dialect=postgresql.dialect())`. The resulting SQL string is stored in `new_sql`.  
6. **Prepare the expected SQL** – A multi‑line string containing the exact SQL that should be generated is defined inside a call to `clean(""" … """)`. The clean utility removes excess whitespace, collapses spaces, strips newlines, and expands the placeholder `{NL}` into real newline characters, yielding a normalized expected string.  
7. **Assert equality** – The test sets `self.maxDiff = None` to allow full diff output on failure, then asserts that the compiled SQL (`new_sql`) matches the expected normalized SQL (`expected`). If the transformation behaved correctly, the assertion passes; otherwise the test fails, indicating a regression in ConceptNameTransform.modify_select.

**Note**:  
- The placeholder table name `'user'` passed to modify_select is not used by the method; it exists only to satisfy the required signature.  
- The test relies on the clean function from pedsnetdcc/tests/transform_test_utils.py to make the expected SQL format‑independent, focusing the comparison on structural correctness rather than whitespace.  
- The test does not modify the original select_obj or join_obj outside of the transformation; any side effects are confined to the objects returned by ConceptNameTransform.modify_select.  
- Successful execution confirms that each *_concept_id* column results in a distinct alias (`concept_1`, `concept_2`, …) and that the generated LEFT OUTER JOIN clauses correctly reference the concept table’s concept_id column.
***
### FunctionDef test_modify_metadata(self)
**test_modify_metadata**: The function of test_modify_metadata is to verify that ConceptNameTransform correctly adds indexes for the newly created *_concept_name* columns in a transformed table’s metadata.  

**parameters**:  
· **self** – the instance of the unittest.TestCase subclass (ConceptNameTest) that provides access to the test fixture, including the original metadata object stored in `self.metadata`.  

**Code Description**:  
1. The test calls `ConceptNameTransform.modify_metadata(self.metadata)`.  
   - `modify_metadata` (defined in `pedsnetdcc/abstract_transform.py`) iterates over every non‑vocabulary table in the supplied `MetaData` object and invokes `ConceptNameTransform.modify_table` for each table.  
   - `ConceptNameTransform.modify_table` (implemented in `pedsnetdcc/concept_name_transform.py`) scans the table’s columns, adds a new column `<col>_concept_name` for each `<col>_concept_id`, and creates an `Index` object for the new column unless the column name appears in the class‑level `ignore_index_by_table` dictionary.  
   - The index name is generated by the helper `Transform.make_index_name`, which produces an Oracle‑compatible identifier.  

2. After the transformation, the test retrieves the collection of indexes attached to `metadata.tables['table1']` via `metadata.tables['table1'].indexes`.  

3. The test asserts that exactly two indexes exist (`self.assertEqual(len(indexes), 2, 'Indexes created')`). This matches the expectation that `table1` contains two `_concept_id` columns (`foo_concept_id` and `bar_concept_id`) and that neither is listed in `ignore_index_by_table`.  

4. The test iterates over each `Index` object:  
   - For each index, it constructs the DDL string by compiling the index with SQLAlchemy’s `CreateIndex` construct and the PostgreSQL dialect (`CreateIndex(index).compile(dialect=postgresql.dialect())`).  
   - If the index name equals `'tab_fcn_81387a6132eed7f69b5_ix'`, the expected DDL is:  
     ```
     CREATE INDEX tab_fcn_81387a6132eed7f69b5_ix
       ON table1 (foo_concept_name)
     ```  
     The test compares the generated DDL (`index_sql`) with the expected string after normalising it with the `clean` utility.  
   - If the index name equals `'tab_bcn_7baa5e16ad1f8129b90_ix'`, the expected DDL is:  
     ```
     CREATE INDEX tab_bcn_7baa5e16ad1f8129b90_ix
       ON table1 (bar_concept_name)
     ```  
     The same comparison is performed.  
   - If an index with any other name is encountered, the test fails with an informative message indicating an unexpected index.  

5. Successful execution confirms that `ConceptNameTransform.modify_metadata` correctly adds the two expected indexes to `table1` and that the generated index DDL matches the naming and column specifications.  

**Note**:  
- The test relies on the deterministic index‑name generation performed by `Transform.make_index_name`; any change to that algorithm will cause the expected index names in the test to become outdated.  
- The `clean` function (from `tests/transform_test_utils.py`) is used to normalise whitespace and line‑breaks in the expected DDL strings, ensuring that formatting differences do not cause false failures.  
- The test only examines `table1`; other tables are not validated here. Additional tests (e.g., `test_modify_select`) cover the SELECT‑side transformation.  
- Because `modify_metadata` mutates the original `MetaData` object in place, the test works directly on `self.metadata` without needing to copy it.  
- The test assumes that the fixture `self.metadata` already contains a `concept` table and that `table1` has exactly two `_concept_id` columns. If the fixture changes, the test expectations must be updated accordingly.
***
