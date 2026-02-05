## ClassDef SiteNameTest
**SiteNameTest**: The function of SiteNameTest is to verify that the SiteNameTransform correctly injects a site column into SQL SELECT statements and metadata definitions while leaving vocabulary tables untouched.

**attributes**: The attributes of this Class (created in `setUp`).
· `metadata`: a SQLAlchemy `MetaData` instance whose `info['site']` entry is set to the string `'test'`. It holds the table definitions used in the tests.  
· `table1`: a `Table` object named `'table1'` containing two integer columns, `foo` and `bar`.  
· `table2`: a `Table` object named `'table2'` containing a single integer column, `baz`.  
· `concept`: a `Table` object named `'concept'` representing a vocabulary table; it contains a single integer column, `concept_id`.  

**Code Description**:  
The `SiteNameTest` class inherits from `unittest.TestCase` and provides two unit‑test methods that exercise the behavior of the `SiteNameTransform` utility.

1. **setUp** – Executed before each test method. It creates a fresh `MetaData` container, assigns the site identifier `'test'` to `metadata.info['site']`, and defines three tables (`table1`, `table2`, and `concept`) using SQLAlchemy’s `Table` and `Column` constructs. The `concept` table is intentionally a vocabulary table; the transform logic is expected to ignore it.

2. **test_modify_select** –  
   * Constructs a simple `select` object that selects all columns from `table1`.  
   * Calls `SiteNameTransform.modify_select`, passing the metadata, the target table name `'table1'`, the original select object, and the join object (initially the same table). The method is expected to return a modified select object and a possibly altered join object that now includes the site column.  
   * Re‑binds the select to the returned join object via `select_from`.  
   * Compiles the resulting SQL using PostgreSQL dialect and converts it to a string.  
   * Asserts that the compiled SQL matches the expected string, which includes the original columns (`foo`, `bar`) and an additional expression `'test'::varchar(32) AS site` appended to the SELECT list, followed by a FROM clause referencing `table1`.

3. **test_modify_metadata** –  
   * Invokes `SiteNameTransform.modify_metadata` with the prepared metadata. The transform should add a `site` column to each non‑vocab table and create an index on that column.  
   * Retrieves the indexes defined on `table1` and asserts that exactly one index was created.  
   * For each index, compiles the `CreateIndex` DDL statement using PostgreSQL dialect and checks that it matches the expected `CREATE INDEX` statement, which names the index `tab_s_e8e4cc181f175c06e6d80_ix` and indexes the `site` column.  
   * Performs a second verification to ensure that the `concept` table (the vocab table) was not altered: it confirms that the set of column names on `concept` remains exactly `{'concept_id'}`.

Overall, the class validates both the SQL‑generation side (SELECT modification) and the schema‑modification side (metadata alteration) of the `SiteNameTransform`, confirming correct handling of regular tables and proper exclusion of vocabulary tables.

**Note**:  
- The tests rely on PostgreSQL‑specific SQL rendering; changing the dialect may affect the expected string representations.  
- The index name in the expected DDL (`tab_s_e8e4cc181f175c06e6d80_ix`) is derived from the internal naming logic of `SiteNameTransform`; if that logic changes, the test expectations must be updated accordingly.  
- The `clean` helper (used to normalize multiline strings) is assumed to strip leading/trailing whitespace and normalize line endings; it must be available in the test module for the assertions to work.  
- No external resources are required; all tables are in‑memory constructs, making the tests fast and isolated.
### FunctionDef setUp(self)
**setUp**: The function of setUp is to create a test metadata environment with sample tables for unit testing.  
**parameters**: This method takes no external parameters; it operates on the instance (`self`).  

**Code Description**:  
- Initializes a new `MetaData` object and assigns it to `self.metadata`. This object will hold schema information for the test tables.  
- Sets the `'site'` entry in the metadata’s `info` dictionary to the string `'test'`. This mimics a site identifier that the code under test may rely on.  
- Defines two `Column` objects, `foo_col` and `bar_col`, both of type `Integer`.  
- Creates a `Table` named `'table1'` attached to `self.metadata`, containing the previously defined `foo_col` and `bar_col`. The resulting `Table` object is stored in `self.table1`.  
- Defines a third `Column` object, `baz_col`, also of type `Integer`.  
- Creates a second `Table` named `'table2'` attached to `self.metadata`, containing only `baz_col`. This table is stored in `self.table2`.  
- Constructs a third `Table` named `'concept'` attached to `self.metadata`. This table includes a single column `'concept_id'` of type `Integer`. The comment indicates that this is a vocabulary table and should not be transformed by the code under test. The table is stored in `self.concept`.  

The method therefore prepares three tables (`table1`, `table2`, and `concept`) within a metadata container, providing a controlled schema for subsequent test cases.  

**Note**:  
- The method does not return any value; it solely sets up instance attributes for later use in test methods.  
- The `'concept'` table is intentionally left untouched by transformation logic; tests may verify that transformation functions correctly ignore such vocabulary tables.  
- All columns are defined as `Integer`; if other data types are needed for additional tests, they must be added explicitly.  
***
### FunctionDef test_modify_select(self)
**test_modify_select**: The function of test_modify_select is to verify that SiteNameTransform.modify_select correctly adds a constant site‑name column to a SELECT statement and that the resulting SQL matches the expected format.

**parameters**  
· self: instance of the unittest.TestCase subclass (SiteNameTest) that provides access to the test fixtures `metadata`, `table1`, and the helper method `clean`.

**Code Description**  
1. **Create a base SELECT** – `select_obj = select([self.table1])` builds a SQLAlchemy `Select` object that initially selects all columns from `self.table1` (the fixture table containing columns `foo` and `bar`).  
2. **Initialize the FROM clause** – `join_obj = self.table1` stores the table object that will later be used as the FROM/JOIN source.  
3. **Apply the SiteNameTransform** –  
   ```python
   select_obj, join_obj = SiteNameTransform.modify_select(
       self.metadata,
       'table1',
       select_obj,
       join_obj)
   ```  
   The test calls the class method `SiteNameTransform.modify_select`, passing the metadata (which must contain `metadata.info['site'] = 'test'`), the table name `'table1'`, and the previously created `select_obj` and `join_obj`. The method checks that `metadata.info` contains a `'site'` entry, creates a literal column `'<site>'::varchar(32)` labelled `site`, appends it to the SELECT clause, and returns the modified SELECT together with the unchanged join object.  
4. **Bind the SELECT to its FROM clause** – `select_obj = select_obj.select_from(join_obj)` attaches the SELECT object to the FROM source (`join_obj`). This step is necessary because `modify_select` only altered the column list; the FROM part remains the original table.  
5. **Compile to raw SQL** – `new_sql = str(select_obj.compile(dialect=postgresql.dialect()))` renders the SQLAlchemy expression into a PostgreSQL‑dialect string. The resulting SQL should contain the original columns (`table1.foo`, `table1.bar`) followed by the literal site column (`'test'::varchar(32) AS site`) and a `FROM table1` clause.  
6. **Prepare the expected SQL** – The multiline string passed to `clean` represents the ideal SQL output, using the placeholder `{NL}` to indicate a newline. `clean` normalises whitespace, removes stray newlines, and expands `{NL}` to an actual newline, producing a canonical string for comparison.  
7. **Assert equality** – `self.assertEqual(expected, new_sql)` checks that the compiled SQL exactly matches the expected string. `self.maxDiff = None` ensures that, if the assertion fails, the full diff is shown in the test output.

**Note**  
- The test relies on the fixture `self.metadata.info['site']` being set to `'test'` before execution; otherwise `SiteNameTransform.modify_select` raises a `RuntimeError`.  
- The `select` import used to build the initial SELECT comes from `sqlalchemy.sql`, and the `postgresql` dialect is imported from `sqlalchemy.dialects`.  
- The helper `clean` (defined in `pedsnetdcc/tests/transform_test_utils.py`) is essential for making the expected SQL string robust against formatting differences such as indentation or multiple spaces.  
- This test only exercises the column‑addition behavior; it does not verify index creation, which is handled by `SiteNameTransform.modify_table` in a separate test.  
- Because the test uses the actual SQLAlchemy compilation step, it validates both the logical transformation and the correct rendering of the literal column cast to `varchar(32)`.
***
### FunctionDef test_modify_metadata(self)
**test_modify_metadata**: The function of test_modify_metadata is to verify that SiteNameTransform.modify_metadata correctly adds a site‑index to non‑vocabulary tables and leaves vocabulary tables unchanged.  

**parameters**:  
· self – the instance of the SiteNameTest unittest.TestCase subclass; no other arguments are required.  

**Code Description**:  
1. The test invokes SiteNameTransform.modify_metadata with the metadata object that was prepared in the setUp method of the test case. The call returns a (mutated) MetaData instance.  
2. It retrieves the collection of Index objects attached to the table named `table1` via metadata.tables['table1'].indexes.  
3. An assertion checks that exactly one index exists, confirming that the transformation added an index for the newly created site column.  
4. For each index (there is only one), the test compiles the index into a PostgreSQL‑specific DDL string using CreateIndex(index).compile(dialect=postgresql.dialect()).  
5. The expected DDL string is built with a multiline literal and normalised by the helper clean function, which removes excess whitespace and expands the {NL} placeholder. The expected statement is:  

   ```
   CREATE INDEX tab_s_e8e4cc181f175c06e6d80_ix
     ON table1 (site)
   ```  

6. The compiled index SQL is compared to the expected string with assertEqual, ensuring that the index name and column list match the specification generated by SiteNameTransform.modify_table.  
7. The test then verifies the “vocab‑excluding” behaviour of modify_metadata. It defines the set expected_concept_columns containing only `concept_id`. It obtains the actual column names of the concept table from metadata.tables['concept'].c.keys() and asserts that the two sets are equal. This confirms that no additional columns (such as the site column) were added to the vocabulary table concept.  

**Note**:  
- The test relies on the clean utility to normalise the expected DDL string; any change to clean could affect the comparison.  
- The index name is generated by Transform.make_index_name, which produces an Oracle‑compatible identifier; the test does not hard‑code the name but checks the exact string produced for the current implementation.  
- Because modify_metadata operates in‑place, the original metadata object is mutated; the test works with the returned reference but could also inspect the original object.  
- The test only covers a single non‑vocabulary table (`table1`) and one vocabulary table (`concept`). Additional tables are exercised in other test modules.  
***
