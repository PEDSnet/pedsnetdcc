## ClassDef AgeTest
**AgeTest**: The function of AgeTest is to verify that the AgeTransform utility correctly modifies SQL SELECT statements, updates metadata with appropriate indexes, and performs any required pre‑transformation steps.

**attributes**: The attributes of this Class.  
· **metadata** – An instance of SQLAlchemy MetaData used to hold table definitions for the tests.  
· **table1** – A SQLAlchemy Table object representing a mock table with columns `foo_start_time`, `bar_start_time`, and `person_id`.  
· **table2** – A SQLAlchemy Table object representing a second mock table with columns `baz_start_time` and `person_id`.  
· **person** – A SQLAlchemy Table object representing a `person` table with columns `person_id` and `time_of_birth`.  
· **AgeTransform.columns_by_table** – A class‑level dictionary (set in `setUp`) that maps table names to the columns that should receive age‑in‑months calculations; for these tests it maps `'table1'` to `('foo_start_time', 'bar_start_time')`.

**Code Description**:  

- **setUp(self)** – Executed before each test method. It creates a fresh SQLAlchemy metadata container and defines three tables (`table1`, `table2`, and `person`) with the column structures required for the tests. It also configures the `AgeTransform.columns_by_table` mapping so that `AgeTransform` knows which columns of `table1` need age calculations. This method ensures that each test runs against an isolated, reproducible schema.

- **test_modify_select(self)** – Constructs a simple `select([self.table1])` object and passes it, together with the metadata and the table name `'table1'`, to `AgeTransform.modify_select`. The method expects `modify_select` to return a new SELECT object and a join object that includes a join to the `person` table. After re‑binding the SELECT to the returned join, the test compiles the statement for PostgreSQL and compares the generated SQL string against an expected query. The expected query contains the original columns plus two computed columns:
  * `months_in_interval(person.time_of_birth, table1.foo_start_time) AS foo_start_age_in_months`
  * `months_in_interval(person.time_of_birth, table1.bar_start_time) AS bar_start_age_in_months`
  It also expects an inner join on `person.person_id = table1.person_id`. The assertion verifies that `AgeTransform` correctly augments the SELECT with age‑in‑months expressions and the necessary join.

- **test_modify_select_negative(self)** – Performs the same workflow as the previous test but uses `table2`, which is **not** listed in `AgeTransform.columns_by_table`. The expectation is that `modify_select` will leave the SELECT unchanged and will not add any joins or computed columns. The resulting SQL should only list the original columns (`baz_start_time` and `person_id`) from `table2`. This test confirms that `AgeTransform` gracefully skips tables without configured age columns.

- **test_modify_metadata(self)** – Calls `AgeTransform.modify_metadata` with the metadata created in `setUp`. The method should add two indexes to `table1`, one for each newly created age column (`foo_start_age_in_months` and `bar_start_age_in_months`). The test retrieves the indexes from the modified metadata, asserts that exactly two indexes exist, and then checks the generated DDL for each index against expected CREATE INDEX statements. The index names are deterministic hashes (`tab_fsaim_107eee9e009461416_ix` and `tab_bsaim_ca07fdbcdf9bfef7a_ix`). This test validates that the metadata transformation correctly creates supporting indexes for the computed age columns.

- **test_pre_transform(self)** – Checks that the environment variables `PEDSNETDCC_TEST_DBURI` and `PEDSNETDCC_TEST_SEARCH_PATH` are defined; if not, the test is skipped. When the variables are present, it builds a connection string using `make_conn_str` and invokes `AgeTransform.pre_transform`. The test currently does not verify the side effects of the pre‑transform (e.g., creation of database functions) and includes a TODO comment indicating that future introspection should be added. This placeholder ensures that the pre‑transform entry point can be called without error.

- **test_with_data(self)** – A placeholder test that is deliberately skipped with the message “Not implemented yet”. It signals the intention to add integration‑style tests that load real data, run the transformation, and verify the results.

**Note**:  
- The tests rely on PostgreSQL‑specific SQL compilation; changing the target dialect may affect the generated strings.  
- Index names are generated from a hash of the table and column names; they must match the expected values in the test, otherwise the test will fail.  
- `AgeTransform.modify_select` must return both a SELECT object and a join object; the test re‑binds the SELECT using `select_obj.select_from(join_obj)`.  
- Environment variables are required for `test_pre_transform`; ensure they are set in the test environment or the test will be skipped.  
- The placeholder tests (`test_pre_transform` and `test_with_data`) are intentionally incomplete; they should be expanded with concrete assertions before being used in a production test suite.
### FunctionDef setUp(self)
**setUp**: The function of setUp is to create an in‑memory SQLAlchemy metadata environment with sample tables used by the AgeTransform unit tests.  

**parameters**:  
· self: the test case instance (inherits from unittest.TestCase).  

**Code Description**:  
The method begins by instantiating a fresh `MetaData` object and assigning it to `self.metadata`. This metadata container will hold all table definitions for the duration of each test, ensuring isolation between test runs.  

Three groups of tables are then defined:

1. **table1** – Constructed with three columns:  
   – `foo_start_time` (DateTime)  
   – `bar_start_time` (DateTime)  
   – `person_id` (Integer)  
   The table is added to `self.metadata` under the name `'table1'`.

2. **table2** – Constructed with two columns:  
   – `baz_start_time` (DateTime)  
   – `person_id` (Integer)  
   The table is added to `self.metadata` under the name `'table2'`.

3. **person** – Represents the required person reference table for the age transformation. It contains:  
   – `person_id` (Integer)  
   – `time_of_birth` (DateTime)  
   The table is added to `self.metadata` under the name `'person'`.

After the tables are defined, the method overrides the class‑level mapping used by `AgeTransform` by assigning a custom dictionary to `AgeTransform.columns_by_table`. In this test fixture the mapping contains a single entry:

```python
{
    'table1': ('foo_start_time', 'bar_start_time')
}
```

This limited mapping directs the `AgeTransform` logic to consider only the two datetime columns of `table1` when generating age‑in‑months columns. By redefining the mapping inside `setUp`, the test suite can verify the transformation behavior without relying on the full default mapping defined in the production code.

The objects created (`self.metadata`, `self.table1`, `self.table2`, `self.person`) are stored as instance attributes so that individual test methods can reference them directly when invoking `AgeTransform.modify_select`, `AgeTransform.modify_table`, or other helper functions.  

**Note**:  
- The `person` table in this test uses the column name `time_of_birth` instead of the production column name `birth_datetime`. The `AgeTransform` implementation expects a column named `birth_datetime`; however, the test suite may patch or reference the column accordingly, or the transformation code may rely only on the presence of a `person_id` column for join validation.  
- Overriding `AgeTransform.columns_by_table` in `setUp` affects all subsequent test methods in the class; therefore each test must either rely on this custom mapping or restore the original mapping if needed.  
- No database connection is opened in `setUp`; all tables exist only in the SQLAlchemy metadata object, which is sufficient for the unit tests that operate on the SQL expression layer.
***
### FunctionDef test_modify_select(self)
**test_modify_select**: The function of test_modify_select is to verify that AgeTransform.modify_select correctly adds age‑in‑months columns and the required join to the person table for a table that is configured for age transformation.  

**parameters**:  
· self – the instance of the unittest.TestCase subclass (AgeTest) that provides access to the test fixtures such as self.metadata, self.table1, and self.table2.  

**Code Description**:  
1. The test creates a basic SQLAlchemy Select object that selects all columns from self.table1 by calling select([self.table1]).  
2. It also creates a join_obj initially pointing to self.table1 (the FROM clause before any joins are added).  
3. The core of the test invokes AgeTransform.modify_select with the current metadata, the table name 'table1', and the previously created select_obj and join_obj.  
   - AgeTransform.modify_select examines the class‑level columns_by_table mapping, finds that table1 has datetime columns to be transformed, adds a join to the person table (if not already present), and appends two derived columns:  
     * months_in_interval(person.birth_datetime, table1.foo_start_time) AS foo_start_age_in_months  
     * months_in_interval(person.birth_datetime, table1.bar_start_time) AS bar_start_age_in_months  
   - The method returns the possibly‑augmented select_obj and join_obj.  
4. After the transform, the test re‑binds the SELECT to the (potentially) new FROM clause by calling select_obj = select_obj.select_from(join_obj). This produces a complete SELECT statement that includes the original columns, the new age columns, and the JOIN to person.  
5. The SELECT object is compiled to a PostgreSQL‑specific SQL string with select_obj.compile(dialect=postgresql.dialect()). The resulting string is stored in new_sql.  
6. An expected SQL string is built using the test helper clean (which normalises whitespace and expands the {NL} placeholder into real newlines). The expected statement contains:  
   - The original columns foo_start_time, bar_start_time, person_id from table1.  
   - The two age‑in‑months columns labelled foo_start_age_in_months and bar_start_age_in_months.  
   - A FROM clause that joins table1 to person on person.person_id = table1.person_id.  
7. The test sets self.maxDiff = None to allow full diff output on failure, then asserts that new_sql exactly matches expected. If the transformation behaves as specified, the assertion passes; otherwise the test fails, indicating a regression in AgeTransform.modify_select.  

**Relationship with Callees**:  
- Calls AgeTransform.modify_select, which is the method under test.  
- Uses select and join objects from SQLAlchemy to build the initial query.  
- Calls select_obj.select_from to attach the join clause returned by the transform.  
- Compiles the final query with postgresql.dialect() to produce PostgreSQL‑compatible SQL.  
- Utilises the helper function clean from pedsnetdcc/tests/transform_test_utils.py to normalise the expected SQL string before comparison.  

**Note**:  
- The test assumes that self.table1 contains the datetime columns foo_start_time and bar_start_time and a person_id column; otherwise AgeTransform.modify_select would raise a ValueError.  
- The test does not check the behaviour for tables that are not listed in AgeTransform.columns_by_table; that scenario is covered by a separate test (test_modify_select_negative).  
- The compiled SQL string includes PostgreSQL‑specific syntax (e.g., the function months_in_interval) and therefore the test uses the PostgreSQL dialect explicitly.  
- The use of self.maxDiff = None ensures that any mismatch between the generated and expected SQL is fully displayed, aiding debugging.
***
### FunctionDef test_modify_select_negative(self)
**test_modify_select_negative**: The function of **test_modify_select_negative** is to verify that `AgeTransform.modify_select` leaves a SELECT statement unchanged when the processed table is not listed in the age‑transform column mapping.

**parameters**  
· **self** – instance of the `AgeTest` unittest class; provides access to the test fixtures `self.metadata`, `self.table2`, and helper methods such as `self.maxDiff`.

**Code Description**  
1. **Create a baseline SELECT** – `select_obj = select([self.table2])` builds a SQLAlchemy `Select` object that initially selects all columns from the fixture table `table2`.  
2. **Initialize the FROM clause** – `join_obj = self.table2` stores the table object that will serve as the FROM part of the query.  
3. **Invoke the age transform** –  
   ```python
   select_obj, join_obj = AgeTransform.modify_select(
       self.metadata,
       'table2',
       select_obj,
       join_obj)
   ```  
   The call passes the metadata, the name `'table2'`, and the current SELECT/JOIN objects to `AgeTransform.modify_select`. Because `'table2'` is **not** a key in `AgeTransform.columns_by_table`, the method returns the original `select_obj` and `join_obj` unchanged (no age columns are added and no join to `person` is performed).  
4. **Attach the FROM clause** – `select_obj = select_obj.select_from(join_obj)` re‑binds the SELECT to the original table as its FROM source.  
5. **Compile to raw SQL** – `new_sql = str(select_obj.compile(dialect=postgresql.dialect()))` renders the SELECT statement using the PostgreSQL dialect, producing a string representation of the query.  
6. **Define the expected SQL** – The `expected` variable holds a cleaned multi‑line string that represents the exact SQL the test expects:
   ```
   SELECT table2.baz_start_time,
          table2.person_id
   FROM table2
   ```  
   The `clean` utility removes indentation and normalises whitespace.  
7. **Assert equality** – `self.assertEqual(new_sql, expected)` confirms that the compiled SQL matches the expected string, proving that `AgeTransform.modify_select` performed no modifications for `table2`.  

**Relationship with Callees**  
- **AgeTransform.modify_select** – The only external method called. Its contract is to return the incoming SELECT and JOIN objects unchanged when the table name is absent from `columns_by_table`. This test validates that contract for a negative case.  
- **SQLAlchemy `select` and `select_from`** – Used to construct and bind the query; the compiled output is compared against the expected string.  
- **`clean` (from `tests/transform_test_utils.py`)** – Normalises the expected SQL string before comparison, ensuring whitespace differences do not affect the assertion.  

**Note**  
- The test relies on the fixture `self.table2` having at least the columns `baz_start_time` and `person_id`; those columns are selected automatically by `select([self.table2])`.  
- If `AgeTransform.columns_by_table` were modified to include `'table2'`, this test would fail because the transform would then add age columns and a join, changing the compiled SQL.  
- The test sets `self.maxDiff = None` to allow full diff output from `unittest` in case of mismatch, aiding debugging.  
- The use of the PostgreSQL dialect is intentional because the age‑transform SQL expression (`months_in_interval`) is PostgreSQL‑specific; however, in this negative test no such expression is generated.
***
### FunctionDef test_modify_metadata(self)
**test_modify_metadata**: The function of test_modify_metadata is to verify that AgeTransform.modify_metadata correctly adds the expected index objects to a non‑vocabulary table in the transformed SQLAlchemy MetaData.

**parameters**:  
· self – the instance of the AgeTest unittest.TestCase subclass; provides access to the fixture metadata prepared in the test’s setUp method.

**Code Description**:  
1. The test invokes the class method AgeTransform.modify_metadata with the test fixture self.metadata. modify_metadata iterates over every non‑vocabulary table in the supplied MetaData object and calls AgeTransform.modify_table for each table. modify_table adds new age‑in‑months columns and creates an Index object for each column unless the column appears in ignore_index_by_table.  
2. After the transformation, the test retrieves the collection of Index objects attached to metadata.tables['table1'].indexes. It asserts that exactly two indexes exist, confirming that modify_table added one index per newly created age column.  
3. The test iterates over the two Index instances. For each index it:  
   - Compiles the index to a PostgreSQL‑dialect SQL string using CreateIndex(index).compile(dialect=postgresql.dialect()).  
   - Checks the index name. If the name matches 'tab_fsaim_107eee9e009461416_ix', the expected SQL is built as a multi‑line string that creates an index on the column foo_start_age_in_months.  
   - If the name matches 'tab_bsaim_ca07fdbcdf9bfef7a_ix', the expected SQL is built for an index on bar_start_age_in_months.  
   - The expected string is passed through the helper clean (to normalise whitespace and line‑breaks) and then compared with the compiled index_sql using self.assertEqual.  
4. If an index with an unexpected name is encountered, the test fails with a descriptive message indicating the unknown index name.

The test therefore confirms three concrete behaviours of AgeTransform.modify_metadata (and by extension modify_table):  
* the correct number of indexes is created,  
* each index is named deterministically using Transform.make_index_name,  
* each index references the appropriate newly added age‑in‑months column.

**Note**:  
- The test relies on the fixture self.metadata containing a table named 'table1' with columns 'foo_start_age_in_months' and 'bar_start_age_in_months' already defined by a prior call to modify_table.  
- The comparison uses the clean function to ignore differences in indentation, extra spaces, or newline characters, ensuring that only the structural SQL content is validated.  
- The test does not interact with a live database; it operates purely on the in‑memory SQLAlchemy MetaData objects.  
- If the implementation of AgeTransform.modify_table or the naming convention in Transform.make_index_name changes, the expected index names and SQL strings in this test must be updated accordingly.
***
### FunctionDef test_pre_transform(self)
**test_pre_transform**: The function of test_pre_transform is to verify that the AgeTransform.pre_transform hook can be executed without raising an exception when a valid database connection string is supplied.

**parameters**:  
· self: instance of the unittest.TestCase subclass (AgeTest) that provides the testing context and utility methods such as skipTest.

**Code Description**:  
The method begins by defining the names of two environment variables that must be present for the test to run:  

* `PEDSNETDCC_TEST_DBURI` – a PostgreSQL URI that points to a test database.  
* `PEDSNETDCC_TEST_SEARCH_PATH` – the schema search_path that should be used for the test.

It then checks whether **both** variables are defined in the process environment. If either variable is missing, the test is skipped by calling `self.skipTest` with a message that lists the missing variables. This prevents false failures when the test suite is executed in an environment that does not provide a test database.

When the required variables are present, the method builds a libpq‑compatible connection string by invoking `make_conn_str` (from `pedsnetdcc.utils`). The call passes the URI taken from `os.environ[dburi_var]` and the search_path taken from `os.environ[search_path_var]`. `make_conn_str` parses the URI, incorporates the supplied search_path into the `options` parameter, and returns a string suitable for `psycopg2.connect`.

Finally, the method calls `AgeTransform.pre_transform(conn_str)`. `AgeTransform.pre_transform` is a class method that opens a PostgreSQL connection using the supplied `conn_str`, iterates over the SQL statements defined in the module‑level list `AGE_FUNCTIONS_SQL`, and executes each statement to create the PL/pgSQL functions required for the age transformation. The test does not inspect the database state; it simply ensures that the call completes without raising an exception. If any exception propagates (e.g., connection failure, SQL syntax error), the test will fail, indicating that the pre‑transform hook is not functional in the current environment.

**Note**:  
* The test requires a reachable PostgreSQL instance and appropriate privileges to create functions; otherwise it will be skipped or fail.  
* Only the presence of the two environment variables is checked; the test does not validate their contents.  
* `make_conn_str` may raise `ValueError` if the supplied URI contains duplicate query parameters; such an error will cause the test to fail.  
* `AgeTransform.pre_transform` does not use the `metadata`, `id_name`, or `id_type` arguments, so they are omitted in this call.  
* Because the test does not clean up the created functions, repeated runs against the same database may encounter “function already exists” errors unless the underlying SQL uses `CREATE OR REPLACE`. This behavior is intentional for a simple sanity‑check test.
***
### FunctionDef test_with_data(self)
**test_with_data**: The function of test_with_data is a placeholder test method intended to verify transformation results using test data.  

**parameters**:  
· self: the instance of the AgeTest class (inherits from unittest.TestCase).  

**Code Description**:  
The method begins with a comment indicating a future implementation step: “# TODO: use test data and verify transformation results”. Immediately after the comment, the method calls `self.skipTest('Not implemented yet')`. This call tells the unittest framework to skip the execution of this test case, marking it as intentionally omitted because the test logic has not been written. No other operations, assertions, or data handling are performed within the method.  

**Note**:  
- The test is currently inactive; it will not run any assertions until the `skipTest` call is removed and concrete test logic is added.  
- When implementing the test, replace the `skipTest` line with code that loads appropriate test data, applies the transformation under test, and asserts expected outcomes.  
- Ensure that any required fixtures or test resources are set up before the test runs to avoid false failures.
***
