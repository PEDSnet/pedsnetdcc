## ClassDef IDMappingTransform
**IDMappingTransform**: The function of IDMappingTransform is to generate and apply site‑specific identifier mappings for all non‑vocabulary tables in a PEDSNet CDM, creating mapping tables, populating them with new IDs, and rewriting SELECT statements so that primary‑key and foreign‑key columns reference the generated DCC IDs instead of the original site IDs.

**attributes**  
· **pre_transform(cls, conn_str, metadata, id_name='dcc', id_type='BigInteger')** – class method that creates the mapping infrastructure (mapping tables, last‑ID tracking tables, and the rows that map each site‑specific primary key to a new DCC ID).  
· **modify_select(cls, metadata, table_name, select, join, id_name='dcc', id_type='BigInteger')** – class method that rewrites a SQLAlchemy `Select` and its accompanying `Join` so that every primary‑key and foreign‑key column that points to a data table is replaced by the corresponding DCC ID from the mapping table.  
· **modify_table(cls, metadata, table, id_type='BigInteger')** – class method that mutates the in‑memory `MetaData` definition of a table by adding a `site_id` column (for tables with a single‑column primary key) and, for the `fact_relationship` table, adds `site_id_1` and `site_id_2` columns.  

**Code Description**  

1. **pre_transform**  
   * Logs the start of the ID‑mapping pre‑transform and records the start time.  
   * Iterates over every table in `metadata` that is not a vocabulary table (`VOCAB_TABLES`).  
   * Skips tables that have no primary key to map (`fact_relationship`, `lab_site_mapping`), tables that belong in the vocabulary (`drug_strength`), and the `cohort` table (currently has a composite primary key).  
   * For each remaining table it validates the primary‑key definition:  
     – Raises `ValueError` if the table has a composite primary key (except the special‑case `cohort`).  
     – Raises `ValueError` if the table has no primary key (except the `death` table, which may lack a PK in some versions).  
   * Extracts the primary‑key column name (`pkey_name`) and builds the names of the mapping table and the “last‑ID” tracking table using the templates `map_table_name_tmpl` and `last_id_table_name_tmpl`.  
   * Executes a SQL statement (`new_id_count_sql`) that counts how many new ID mappings are required for the current table. The count is stored in `tpl_vars['new_id_count']`.  
   * Executes two statements inside a single transaction to lock and then update the last‑ID tracking table (`lock_last_id_sql` and `update_last_id_sql`). The result provides `old_last_id` and `new_last_id`.  
   * Inserts the required number of new rows into the mapping table (`insert_new_maps_sql`). The number of rows inserted is logged.  
   * Creates an index on the mapping table (`CREATE_ID_MAP_INDEX_SQL`). Errors from any statement are captured and re‑raised with a descriptive message.  

2. **modify_select**  
   * Retrieves the `Table` object for `table_name`.  
   * Determines the SQLAlchemy type for the site identifier (`site_id_type`), using `String(256)` when `id_type` is `'String'`, otherwise `BigInteger`.  
   * Raises `ValueError` for tables with a composite primary key, except the `cohort` table.  
   * **Primary‑key handling** – If the table has a single primary‑key column (and is not the special `death` case), the method:  
     – Constructs or fetches the mapping table (`<table_name>_ids`).  
     – Adds an inner join from the data table to the mapping table on `table.pkey = map_table.site_id`.  
     – Builds a new `Select` that:  
       * Removes the original primary‑key column.  
       * Adds the mapping table’s DCC ID column (`dcc_id`) labelled with the original primary‑key name.  
       * Adds the original site‑specific primary‑key column labelled `site_id`.  
   * **Foreign‑key handling** – For each foreign‑key constraint whose referenced table is not a vocabulary table:  
     – Determines the referenced table name and builds the corresponding mapping table name.  
     – Creates (or reuses) the mapping table object.  
     – Joins the mapping table to the current join, using an outer join when the foreign‑key column is nullable.  
     – Replaces the foreign‑key column in the `Select` with the mapping table’s DCC ID column, preserving the original column name.  
   * **Special case – `fact_relationship`** – Because this table stores two generic “fact” identifiers together with domain concept IDs, the method:  
     – For each domain concept defined in `FACT_RELATIONSHIP_DOMAINS`, creates two aliases of the appropriate mapping table.  
     – Joins each alias with a condition that matches both the site‑specific fact ID and the domain concept ID.  
     – Builds two `CASE` expressions that select the correct DCC ID based on the domain concept ID, labeling the results `fact_id_1` and `fact_id_2`.  
     – Adds the original site‑specific fact IDs as `site_id_1` and `site_id_2`.  
   * Returns the transformed `(select, join)` pair.  

3. **modify_table**  
   * Determines the appropriate SQLAlchemy type for `site_id`.  
   * If the table has a single primary‑key column (and is not the `death` table with a PK on `person_id`), appends a new column named `site_id` of the determined type.  
   * If the table is `fact_relationship`, appends two columns, `site_id_1` and `site_id_2`, of the same type.  
   * These columns are used later by `modify_select` to expose the original site‑specific identifiers in the final SELECT output.  

**Reference Relationships**  

* **Callers**  
  – `pedsnetdcc/transform_runner.py::_transform_id` invokes `IDMappingTransform.pre_transform` before any SELECT manipulation and then calls `_transform_id_select_sql` to build transformed CREATE‑TABLE statements.  
  – `_transform_id_select_sql` creates a base `select([table])` and `join = table`, then passes them to `IDMappingTransform.modify_select`. The returned `(select, join)` are combined into a final `CREATE UNLOGGED TABLE … AS SELECT …` statement.  
  – `Transform.modify_metadata` (inherited from the abstract base) calls `IDMappingTransform.modify_table` for each non‑vocabulary table, ensuring the in‑memory `MetaData` reflects the added `site_id` columns.  

* **Callees / Dependencies**  
  – Uses global constants `VOCAB_TABLES`, `map_table_name_tmpl`, `last_id_table_name_tmpl`, and SQL template strings (`new_id_count_sql`, `lock_last_id_sql`, `update_last_id_sql`, `insert_new_maps_sql`, `CREATE_ID_MAP_INDEX_SQL`).  
  – Executes statements through the helper classes `Statement`, `StatementList`, and utility functions `check_stmt_err`, `check_stmt_data`, `secs_since`.  
  – Relies on SQLAlchemy for table introspection, column creation, aliasing, and expression building (`sqlalchemy.Table`, `sqlalchemy.Column`, `sqlalchemy.select`, `sqlalchemy.case`, `sqlalchemy.and_`, `sqlalchemy.cast`).  

* **Tests**  
  – `pedsnetdcc/tests/id_mapping_transform_test.py` verifies that `modify_select` correctly rewrites the `person` table SELECT (primary‑key and foreign‑key joins, column aliases) and that the special handling for `fact_relationship` produces the expected CASE expressions and outer joins.  
  – The same test suite checks that `modify_table` adds a `site_id` column to `person` but does not add it to `fact_relationship`.  
  – `test_pre_transform` ensures that the pre‑transform can be executed against a real database when the required environment variables are set.  

**Note**  
* The transformation assumes a naming convention where each data table `<tbl>` has a corresponding mapping table `<tbl>_ids` with columns `<id_name>_id` (the new DCC ID) and `site_id` (the original site‑specific identifier).  
* Composite primary keys are not supported except for the explicit `cohort` exception; attempting to run the transform on such a table will raise `ValueError`.  
* The `death` table is treated specially because some model versions define a primary‑key constraint on `person_id`; the code skips ID generation for that column.  
* All SQL statements are executed inside a transaction when updating the last‑ID tracking table to avoid race conditions in concurrent runs.  
* The `id_type` parameter allows the same logic to be used with string‑based identifiers; the code automatically switches the column type for both mapping tables and the added `site_id` columns.  

**Output Example**  

When `IDMappingTransform.modify_select` processes the `person` table, the resulting SQL (PostgreSQL dialect) resembles:

```
SELECT
    person_ids.dcc_id AS person_id,
    person.person_id AS site_id,
    location_ids.dcc_id AS location_id,
    care_site_ids.dcc_id AS care_site_id,
    provider_ids.dcc_id AS provider_id,
    person.*  -- all other original columns
FROM person
JOIN person_ids ON person.person_id = person_ids.site_id
LEFT OUTER JOIN location_ids ON person.location_id = location_ids.site_id
LEFT OUTER JOIN care_site_ids ON person.care_site_id = care_site_ids.site_id
LEFT OUTER JOIN provider_ids ON person.provider_id = provider_ids.site_id
```

For the `fact_relationship` table, the generated SQL includes CASE expressions and two sets of outer joins, for example:

```
SELECT
    CASE fact_relationship.domain_concept_id_1
        WHEN 8  THEN visit_occurrence_ids_1.dcc_id
        WHEN 27 THEN observation_ids_1.dcc_id
        WHEN 21 THEN measurement_ids_1.dcc_id
    END AS fact_id_1,
    CASE fact_relationship.domain_concept_id_2
        WHEN 8  THEN visit_occurrence_ids_2.dcc_id
        WHEN 27 THEN observation_ids_2.dcc_id
        WHEN 21 THEN measurement_ids_2.dcc_id
    END AS fact_id_2,
    fact_relationship.fact_id_1 AS site_id_1,
    fact_relationship.fact_id_2 AS site_id_2,
    fact_relationship.*
FROM fact_relationship
LEFT OUTER JOIN visit_occurrence_ids AS visit_occurrence_ids_1
    ON fact_relationship.fact_id_1 = visit_occurrence_ids_1.site_id
   AND fact_relationship.domain_concept_id_1 = 8
LEFT OUTER JOIN visit_occurrence_ids AS visit_occurrence_ids_2
    ON fact_relationship.fact_id_2 = visit_occurrence_ids_2.site_id
   AND fact_relationship.domain_concept_id_2 = 8
... (additional joins for observation and measurement) ...
```
### FunctionDef pre_transform(cls, conn_str, metadata, id_name, id_type)
**pre_transform**: The function of pre_transform is to generate and reserve new DCC IDs for every non‑vocabulary table in a PEDSnet database, creating the necessary mapping and last‑ID tracking rows.  

**parameters**:  
· cls: the IDMappingTransform class (used implicitly as a class method).  
· conn_str (str): PostgreSQL libpq‑style connection string that identifies the target database.  
· metadata (sqlalchemy.MetaData): SQLAlchemy metadata object containing Table objects for all tables in the model version.  
· id_name (str, optional, default 'dcc'): the column name that will hold the generated DCC identifier in the mapping tables.  
· id_type (str, optional, default 'BigInteger'): the PostgreSQL data type of the generated identifier column.  

**Code Description**:  

1. **Logging start and timing** – The function logs a structured message “starting ID mapping pre-transform” and records the current epoch time in `starttime` for later elapsed‑time reporting.  

2. **Iterate over tables** – It loops over every table name present in `metadata.tables` except those listed in `VOCAB_TABLES`. For each table it skips a small set of known exceptions that either lack a primary key or have composite primary keys (`fact_relationship`, `lab_site_mapping`, `drug_strength`, `cohort`).  

3. **Retrieve Table object** – For each remaining table it obtains the SQLAlchemy Table object (`table = metadata.tables[table_name]`) and builds a dictionary `tpl_vars` that will be used to format the various SQL templates later. The dictionary is populated with:  
   * `table_name` – current table name  
   * `id_name` – the identifier column name supplied by the caller  
   * `id_type` – the identifier column type supplied by the caller  

4. **Primary‑key validation** –  
   * If the table is `death` and its primary key consists of the column `person_id`, the table is skipped because some model versions place a PK constraint on that column.  
   * If the table has more than one primary‑key column, a `ValueError` is raised with a clear message; the error is logged before raising.  
   * If the table has no primary‑key column (and it is not `death`), a `ValueError` is raised and logged.  

5. **Derive auxiliary table names** – Using the convention‑based templates `map_table_name_tmpl` and `last_id_table_name_tmpl`, the function computes:  
   * `pkey_name` – the single primary‑key column name (`list(table.primary_key.columns.keys())[0]`)  
   * `map_table_name` – name of the mapping table that will store the old‑PK → new‑ID pairs  
   * `last_id_table_name` – name of the tracking table that holds the last allocated ID for this table  

6. **Count needed new ID mappings** – A `Statement` object (`new_id_count_stmt`) is created with the SQL `new_id_count_sql` (formatted with `tpl_vars`) and a descriptive message. The statement is executed (`execute(conn_str)`). Helper `check_stmt_err` validates that no execution error occurred, and `check_stmt_data` ensures a result row was returned. The count (`new_id_count`) is extracted from the first row of the result set and stored back into `tpl_vars`. The count is logged.  

7. **Reserve a block of IDs atomically** – Two statements are assembled in a `StatementList` (`update_last_id_stmts`):  
   * A `LOCK` statement (`lock_last_id_sql`) that acquires an exclusive lock on the last‑ID tracking row.  
   * An `UPDATE` statement (`update_last_id_sql`) that increments the last‑ID value by the previously counted amount and returns the old and new last IDs.  
   The list is executed with `serial_execute(conn_str, transaction=True)`, guaranteeing that the lock and update occur in a single transaction. Each statement is checked for errors (`check_stmt_err`) and the second statement is checked for returned data (`check_stmt_data`). The old and new last IDs are stored in `tpl_vars` and logged.  

8. **Insert new mapping rows** – A `Statement` (`insert_new_maps_stmt`) is built from `insert_new_maps_sql` (formatted with `tpl_vars`). Executing this statement inserts one row per source primary‑key value, assigning a new DCC ID from the reserved range. After execution, `check_stmt_err` validates success, and the number of rows inserted (`rowcount`) is logged together with the elapsed time since the start of the whole pre‑transform.  

9. **Create an index on the mapping table** – A final `Statement` (`index_stmt`) is created with the DDL `CREATE_ID_MAP_INDEX_SQL` (formatted with the Table object) and executed. Errors are checked, and successful index creation is logged with elapsed time.  

10. **Loop continues** – The process repeats for each eligible table, thereby ensuring that every non‑vocabulary table has a fresh mapping table and a contiguous block of DCC IDs reserved.  

**Reference relationships**:  

* **Callers** – `pre_transform` is invoked by the transformation runner functions in `pedsnetdcc/transform_runner.py` (`_transform_id`, `_transform`, `_transform_target`, etc.) before any SELECT‑based transformation statements are executed. It is also called directly in unit tests (`tests/id_mapping_transform_test.py`, `tests/age_transform_test.py`).  

* **Callees / helpers** – Within `pre_transform` the following utilities are used:  
  * `Statement` (from `pedsnetdcc/db.py`) to encapsulate each SQL command.  
  * `StatementList.serial_execute` to run the lock‑and‑update pair atomically.  
  * `check_stmt_err` (from `pedsnetdcc/utils.py`) to raise a `DatabaseError` if any statement failed.  
  * `check_stmt_data` (from `pedsnetdcc/utils.py`) to raise `MissingDataError` when a SELECT did not return a row.  
  * `secs_since` (from `pedsnetdcc/dict_logging.py`) to compute elapsed seconds for log messages.  

* **SQL templates** – The function relies on several module‑level string templates (`new_id_count_sql`, `lock_last_id_sql`, `update_last_id_sql`, `insert_new_maps_sql`, `CREATE_ID_MAP_INDEX_SQL`) and naming conventions (`map_table_name_tmpl`, `last_id_table_name_tmpl`). These templates are defined elsewhere in the same module and are formatted with the `tpl_vars` dictionary built inside the loop.  

**Note**:  
* The function aborts immediately with a `ValueError` if any non‑vocabulary table has a composite primary key or lacks a primary key (except the special‑case `death`). This behavior enforces the assumption that each table can be mapped with a single column.  
* All DDL statements (creation of mapping tables and indexes) are executed with autocommit semantics because each `Statement.execute` opens its own connection with isolation level 0. The lock‑and‑update pair, however, runs inside a single transaction to avoid race conditions when multiple processes allocate IDs concurrently.  
* Logging is performed at INFO level for normal progress and at ERROR level for any failure; the elapsed time is reported using the zero‑padded format from `secs_since`.  
* The function does not return a value; its effect is side‑effects in the database (creation of mapping tables, population of ID ranges, and index creation).  

**Output Example**:  
`pre_transform` does not produce a Python return value. After successful execution, the database will contain, for each processed table (e.g., `person`), a mapping table named `person_dcc_map` (or similar) with columns `person_id` (original PK) and `dcc` (new ID), populated with a contiguous block of IDs, and an index on the `dcc` column. Log output will include entries such as:  

```
{'msg': 'starting ID mapping pre-transform'}
{'msg': 'counted new IDs needed', 'table': 'person', 'count': 12500}
{'msg': 'last ID tracking table updated', 'table': 'person',
 'old_last_id': 500000, 'new_last_id': 512500}
{'msg': 'generated new ID mappings', 'table': 'person',
 'count': 12500, 'elapsed': '0012'}
{'msg': 'created ID mapping indexes', 'table': 'person', 'elapsed': '0013'}
```
***
### FunctionDef modify_select(cls, metadata, table_name, select, join, id_name, id_type)
**modify_select**: The function of modify_select is to rewrite a SQLAlchemy SELECT statement and its associated JOIN clause so that primary‑key and foreign‑key columns referencing data tables are replaced with the corresponding DCC (central) IDs obtained from mapping tables.

**parameters**:  
· **cls** – the class on which the method is invoked (not used inside the method).  
· **metadata** – a SQLAlchemy MetaData object containing Table objects for the source schema.  
· **table_name** – the name of the table being transformed.  
· **select** – a SQLAlchemy Select object that initially selects all columns from *table_name*.  
· **join** – a SQLAlchemy FromClause (initially the Table object itself) that will be extended with additional joins required for ID mapping.  
· **id_name** – base name of the identifier set (default `'dcc'`); used to build mapping‑table column names such as `dcc_id`.  
· **id_type** – the SQL type of the site‑specific IDs (`'BigInteger'` or `'String'`); determines the type of the `site_id` column in mapping tables.

**Code Description**:  

1. **Table lookup** – Retrieves the Table object for *table_name* from *metadata*.  

2. **Site‑ID type handling** – Sets `site_id_type` to `sqlalchemy.BigInteger`. If *id_type* is `'String'`, it is changed to `sqlalchemy.String(256)`. This type is used for the `site_id` column in any mapping table that must be created on‑the‑fly.  

3. **Multi‑column primary‑key guard** – Raises a `ValueError` when the target table has a composite primary key, except for the special case of the `cohort` table.  

4. **Primary‑key mapping (when a PK exists)**  
   * Skips mapping for tables without a primary key, for the `cohort` table, and for the `death` table when its PK is `person_id`.  
   * Determines the primary‑key column name (`pkey_name`).  
   * Constructs the mapping‑table name using the global template `map_table_name_tmpl` (e.g., `person_ids` for table `person`).  
   * If the mapping table is not already present in *metadata*, it is created with two columns: `<id_name>_id` (BigInteger) and `site_id` (type determined in step 2).  
   * Adds an inner join between the source table and the mapping table on `table.pkey_name = map_table.site_id`.  
   * Builds a new SELECT object that:  
     – copies all original columns except the original primary‑key column,  
     – adds the mapped DCC ID column (`map_table.<id_name>_id`) labelled with the original primary‑key name,  
     – adds the original site‑specific primary‑key column labelled `site_id`.  
   * Replaces the original *select* with this new SELECT.  

5. **Foreign‑key mapping loop** – Iterates over each foreign‑key constraint of the table.  
   * Skips foreign keys that reference vocabulary tables (listed in `VOCAB_TABLES`).  
   * For each remaining foreign key:  
     – Retrieves the referenced table name and builds its mapping‑table name.  
     – Creates the mapping table if absent, using the same two‑column definition as in step 4.  
     – Determines whether the join should be outer (`isouter=True`) based on the nullability of the foreign‑key column.  
     – Adds a join between the source table and the mapping table, casting the foreign‑key column to `site_id_type` and matching it to `map_table.site_id`.  
     – Constructs a new SELECT that replaces the original foreign‑key column with the mapped DCC ID column (`map_table.<id_name>_id`) labelled with the original foreign‑key name.  
     – Updates *select* with this new SELECT for subsequent processing.  

6. **Special handling for `fact_relationship`** – Because this table stores two generic “fact” IDs together with domain concept IDs, a more complex transformation is required:  
   * For each domain concept defined in `FACT_RELATIONSHIP_DOMAINS`, the corresponding mapping table is obtained/created.  
   * Two aliases of the mapping table are made (one for `fact_id_1`, one for `fact_id_2`).  
   * Outer joins are added that also match the domain concept ID column to the appropriate constant.  
   * Two CASE expressions (`case_1` and `case_2`) are built mapping each possible domain concept ID to the appropriate mapping‑table DCC ID column.  
   * A new SELECT is assembled that:  
     – retains all original columns except `fact_id_1` and `fact_id_2`,  
     – adds the CASE expressions labelled `fact_id_1` and `fact_id_2`,  
     – adds the original site‑specific fact IDs labelled `site_id_1` and `site_id_2`.  
   * The SELECT is stored back into *select*.  

7. **Return** – The function returns the possibly modified *select* object and the accumulated *join* clause. The caller will later invoke `select.select_from(join)` to produce the final SQL statement.

**Relationship with callers**:  
`modify_select` is invoked by the transformation pipeline in `pedsnetdcc/transform_runner.py`. Functions such as `_transform_id_select_sql` iterate over every non‑vocabulary table, create a base SELECT and JOIN, and then call `IDMappingTransform.modify_select` (the class that defines this method) to apply the ID‑mapping logic. The returned SELECT and JOIN are then combined (`select.select_from(join)`) and compiled into a CREATE TABLE AS statement that creates a transformed copy of the source table in the target schema. The same method is also exercised directly in unit tests (`test_modify_person_select`, `test_modify_fact_relationship_select`) to verify that primary‑key, foreign‑key, and fact‑relationship transformations produce the expected SQL fragments.

**Note**:  
- The method assumes a naming convention for mapping tables (`<source_table>_ids`) and for the DCC ID column (`<id_name>_id`).  
- Mapping tables are created lazily only if they are missing from the supplied *metadata*.  
- For nullable foreign keys, outer joins are used to preserve rows that lack a reference.  
- The function does not modify vocabulary‑related foreign keys; those are left untouched.  
- Composite primary keys are not supported except for the `cohort` table, which is explicitly excluded from PK mapping.  
- The `cast` operation is applied only to foreign‑key columns when the site ID type is a string, ensuring type compatibility with the mapping table’s `site_id`.  

**Output Example**:  
When called for the `person` table with default arguments, the function returns a SELECT that selects:

```
person_ids.dcc_id AS person_id,
person.person_id AS site_id,
location_ids.dcc_id AS location_id,
care_site_ids.dcc_id AS care_site_id,
provider_ids.dcc_id AS provider_id,
...
```

and a JOIN clause equivalent to:

```
person
JOIN person_ids ON person.person_id = person_ids.site_id
LEFT OUTER JOIN location_ids ON person.location_id = location_ids.site_id
LEFT OUTER JOIN care_site_ids ON person.care_site_id = care_site_ids.site_id
LEFT OUTER JOIN provider_ids ON person.provider_id = provider_ids.site_id
```

The final SQL generated by the runner would be:

```
CREATE UNLOGGED TABLE target_schema.person AS
SELECT person_ids.dcc_id AS person_id,
       person.person_id AS site_id,
       location_ids.dcc_id AS location_id,
       care_site_ids.dcc_id AS care_site_id,
       provider_ids.dcc_id AS provider_id,
       ...
FROM person
JOIN person_ids ON person.person_id = person_ids.site_id
LEFT OUTER JOIN location_ids ON person.location_id = location_ids.site_id
LEFT OUTER JOIN care_site_ids ON person.care_site_id = care_site_ids.site_id
LEFT OUTER JOIN provider_ids ON person.provider_id = provider_ids.site_id;
```
***
### FunctionDef modify_table(cls, metadata, table, id_type)
**modify_table**: The function of modify_table is to add site‑identification columns to a table definition in place, according to the table’s primary‑key structure and the requested identifier type.

**parameters**:  
· cls: the class object on which the method is called (typically a subclass of Transform).  
· metadata: SQLAlchemy MetaData object that holds the collection of Table objects (not directly used inside the method but required by the Transform interface).  
· table: a SQLAlchemy Table instance that will be modified.  
· id_type (optional, default 'BigInteger'): a string indicating the data type to use for the new site‑id columns; accepted values are `'BigInteger'` (default) or `'String'`.

**Code Description**:  
1. The method determines the SQLAlchemy column type to use for the new site‑id columns.  
   - By default it selects `sqlalchemy.BigInteger`.  
   - If `id_type` equals `'String'`, it switches to `sqlalchemy.String(256)`.  
2. It checks whether the target table has exactly one primary‑key column **and** is not the special case of the `death` table whose primary key includes `person_id`.  
   - If the condition is satisfied, a new column named `site_id` with the previously chosen type is created and appended to the table via `table.append_column(new_col)`.  
3. It then checks whether the table name is exactly `'fact_relationship'`.  
   - For this table, two additional columns are required: `site_id_1` and `site_id_2`. Both are created with the same type as determined in step 1 and appended to the table.  
4. No return value is provided; the modifications are performed directly on the passed `table` object, which is therefore altered in place.

**Note**:  
- The function assumes that `id_type` will be either `'BigInteger'` or `'String'`; any other value will keep the default `BigInteger` type.  
- The special handling for the `death` table prevents adding a `site_id` column when the primary key consists of `person_id`, preserving the original schema semantics.  
- The method does not interact with the `metadata` argument; it is present only to satisfy the signature required by the parent `Transform` class.  
- Because columns are appended directly, the table must not have been reflected from an existing database schema that disallows further column additions without a migration step.
***
