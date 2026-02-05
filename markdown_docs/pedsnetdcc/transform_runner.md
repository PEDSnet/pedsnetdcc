## FunctionDef _transform_select_sql(model_version, site, target_schema, id_name, id_type, logged)
**_transform_select_sql**: The function of _transform_select_sql is to generate a set of SQL CREATE TABLE statements (logged or unlogged) that materialize transformed copies of each source table according to the registered transformation pipeline.

**parameters**:  
· model_version: string indicating the PEDSnet model version (e.g., “2.3.0”).  
· site: string name of the site (e.g., “stlouis”).  
· target_schema: string name of the schema where the transformed tables will be created.  
· id_name: string identifier for the site‑specific ID set (default “dcc” in callers).  
· id_type: string representing the SQL type of the site ID set (“BigInteger” or “String”).  
· logged: boolean flag; if True the created tables are logged, otherwise they are created as UNLOGGED tables.

**Code Description**:  
1. **Metadata acquisition** – Calls `stock_metadata(model_version)` to obtain a fresh SQLAlchemy `MetaData` object that contains the canonical PEDSnet schema for the supplied model version. The metadata is then annotated with the site name (`metadata.info['site'] = site`).  

2. **Result container** – Initializes an empty Python `set` named `stmt_pairs`. Each element of the set will be a tuple `(sql_string, msg)`.  

3. **Iterate over source tables** – Loops through `metadata.tables.items()`. For each `table_name` and its corresponding `Table` object:  
   - Skips any table listed in the global constant `VOCAB_TABLES` because vocabulary tables are not transformed.  

4. **Base SELECT and JOIN objects** – Creates a SQLAlchemy `Select` object that selects all columns from the current table (`select([table])`). The variable `join_obj` is initially set to the table itself; it will be extended by the transformation pipeline.  

5. **Apply transformation pipeline** – For every class in the global list `TRANSFORMS`, the function invokes the class method `modify_select`:

   ```python
   select_obj, join_obj = transform.modify_select(
       metadata,
       table_name,
       select_obj,
       join_obj,
       id_name,
       id_type)
   ```

   Each `modify_select` may add derived columns, replace ID columns, add joins, or otherwise mutate the SELECT and JOIN objects. The pipeline includes `AddIndexTransform`, `AgeTransform`, `ConceptNameTransform`, `IDMappingTransform`, and `SiteNameTransform`. The order of execution is the order of `TRANSFORMS`.  

6. **Finalize SELECT** – After all transforms have been applied, the SELECT is bound to its FROM clause by calling `select_obj.select_from(join_obj)`.  

7. **Compile to raw SQL** – The combined SELECT object is compiled for the PostgreSQL dialect:

   ```python
   table_sql_obj = final_select_obj.compile(
       dialect=sqlalchemy.dialects.postgresql.dialect())
   ```

   The compiled object provides a SQL string with placeholders (`%s`). The code substitutes the actual parameter values with `str(table_sql_obj) % table_sql_obj.params`. It also removes the internal alias prefix `anon_1.` that SQLAlchemy adds for the base table, yielding a clean statement stored in `table_sql`.  

8. **Wrap in CREATE TABLE** – Depending on the `logged` flag, the function builds either:

   - `CREATE TABLE {target_schema}.{table_name} AS {table_sql}`  (logged)  
   - `CREATE UNLOGGED TABLE {target_schema}.{table_name} AS {table_sql}` (unlogged)

   The message describing the operation is set to `"creating transformed copy of table {table_name}"`.  

9. **Collect result** – The tuple `(final_sql, msg)` is added to `stmt_pairs`. Because a set is used, duplicate statements for the same table cannot occur.  

10. **Return** – After processing all tables, the function returns `stmt_pairs`, a set of `(SQL string, description)` tuples ready for execution by the caller.

**Reference Relationships**:  
- **Callers**:  
  * `pedsnetdcc/main.py.generate_transform_statements` calls `_transform_select_sql` to obtain the SQL statements that are printed to the console.  
  * `pedsnetdcc/transform_runner.py._transform` invokes `_transform_select_sql` after running each transform’s `pre_transform`. The returned statements are wrapped in `Statement` objects and executed in parallel.  

- **Callees**:  
  * `stock_metadata` (from `pedsnetdcc.utils`) provides the base metadata.  
  * Each class listed in the global `TRANSFORMS` (e.g., `AddIndexTransform`, `AgeTransform`, `ConceptNameTransform`, `IDMappingTransform`, `SiteNameTransform`) supplies a `modify_select` method that is called inside the loop.  
  * SQLAlchemy functions (`select`, `compile`, `dialects.postgresql.dialect`) are used to build and render the SQL.  

**Note**:  
- The function does **not** execute any SQL; it only produces the statements. The caller must ensure that any prerequisite steps (e.g., `pre_transform` methods of each transform) have been run, because those methods may create auxiliary tables or functions required by the SELECT statements.  
- The `search_path` used by the database connection must contain the source schema; the target schema is explicitly embedded in each CREATE TABLE statement, so it does not need to be in the search path.  
- Vocabulary tables listed in `VOCAB_TABLES` are deliberately omitted because they are not subject to transformation.  
- The use of a Python `set` guarantees uniqueness but does not preserve order; if deterministic execution order is required, callers should sort the returned set.  
- The replacement of the `anon_1.` alias assumes that the compiled SQL contains exactly that prefix; changes in SQLAlchemy’s alias naming could require adjustment.  

**Output Example**:  
A possible element of the returned set when processing a table named `person` in schema `transformed` might look like:

```
(
    "CREATE UNLOGGED TABLE transformed.person AS SELECT person.person_id, person.gender_concept_id, person.year_of_birth, 'stlouis'::varchar(32) AS site FROM person LEFT OUTER JOIN concept AS concept_1 ON concept_1.concept_id = person.gender_concept_id",
    "creating transformed copy of table person"
)
```

The full return value is a set containing one such tuple for each non‑vocabulary table in the source model.
## FunctionDef _transform_target_select_sql(model_version, site, target_schema, id_name, id_type, target_table)
**_transform_target_select_sql**: The function of _transform_target_select_sql is to generate a set of SQL CREATE TABLE statements (with accompanying descriptive messages) that materialize transformed copies of the specified target tables in a given schema.

**parameters**:  
· model_version: string indicating the PEDSnet model version (e.g., “2.3.0”).  
· site: string name of the site (e.g., “stlouis”).  
· target_schema: name of the schema where the transformed tables will be created.  
· id_name: identifier set name used by ID‑mapping transforms (default “dcc” in callers).  
· id_type: SQL type of the site‑specific identifier set (“BigInteger” or “String”).  
· target_table: a comma‑separated string listing the tables that should be transformed (e.g., “person,measurement”).  

**Code Description**:  
1. **Parse target_table** – The incoming `target_table` string is split on commas, producing a list of table names that the function will process.  
2. **Load model metadata** – Calls `stock_metadata(model_version)` to obtain a fresh SQLAlchemy `MetaData` object that contains the complete, unmodified PEDSnet schema for the requested version.  
3. **Inject site information** – The `site` argument is stored in `metadata.info['site']`. This value is later used by transforms such as `SiteNameTransform`.  
4. **Prepare result container** – An empty Python `set` named `stmt_pairs` is created. Each element of the set will be a tuple `(sql_string, msg)`.  
5. **Iterate over all tables in the metadata** – For each `(table_name, table)` pair in `metadata.tables.items()`:  
   - **Skip vocabulary tables** – If `table_name` is listed in the global `VOCAB_TABLES` constant, the loop continues to the next table because vocabulary tables are not transformed.  
   - **Process only requested tables** – If `table_name` is not present in the parsed `target_table` list, the loop continues; only tables explicitly requested are transformed.  
6. **Create initial SELECT and JOIN objects** –  
   - `select_obj = sqlalchemy.select([table])` builds a SELECT that initially retrieves all columns from the current table.  
   - `join_obj = table` sets the FROM clause to the table itself.  
7. **Apply each registered transform** – The global list `TRANSFORMS` contains transform classes (e.g., `AddIndexTransform`, `AgeTransform`, `ConceptNameTransform`, `IDMappingTransform`, `SiteNameTransform`). For each `transform` in this list, the function calls:  

   ```python
   select_obj, join_obj = transform.modify_select(
       metadata,
       table_name,
       select_obj,
       join_obj,
       id_name,
       id_type)
   ```

   The `modify_select` method may augment the SELECT clause (adding derived columns, literals, or replacing columns) and/or extend the JOIN clause (adding joins to lookup or mapping tables). The returned objects replace the current `select_obj` and `join_obj` for the next iteration.  
8. **Finalize the SELECT statement** – After all transforms have been applied, `final_select_obj = select_obj.select_from(join_obj)` binds the accumulated FROM/JOIN expression to the SELECT.  
9. **Compile to PostgreSQL dialect** –  

   ```python
   table_sql_obj = final_select_obj.compile(
       dialect=sqlalchemy.dialects.postgresql.dialect())
   table_sql = str(table_sql_obj) % table_sql_obj.params
   ```

   This produces a raw SQL string with all parameters rendered inline, suitable for execution on PostgreSQL.  
10. **Wrap in CREATE TABLE** – The function builds a DDL statement that creates an **UNLOGGED** table (fast, non‑WAL) in the target schema:  

    ```python
    final_sql = 'CREATE UNLOGGED TABLE {0}.{1} AS {2}'.format(
        target_schema, table_name, table_sql)
    ```

    The table name is the same as the source table name; the schema is the user‑provided `target_schema`.  
11. **Create descriptive message** –  

    ```python
    msg = 'creating transformed copy of table {}'.format(table_name)
    ```

12. **Add to result set** – The tuple `(final_sql, msg)` is added to `stmt_pairs`. Because a set is used, duplicate statements are automatically eliminated (though duplicates are unlikely in normal operation).  
13. **Return** – After processing all eligible tables, the function returns `stmt_pairs`. The caller (`_transform_target`) iterates over this set, wraps each tuple in a `Statement` object, and executes them in parallel.

**Reference relationships**:  
- **Caller**: `_transform_target` (in the same module) invokes `_transform_target_select_sql` to obtain the DDL statements after having run each transform’s `pre_transform` method. `_transform_target` then executes the statements against the database.  
- **Callees / dependencies**:  
  * `stock_metadata` (pedsnetdcc.utils) – provides the base schema metadata.  
  * Global constants `VOCAB_TABLES` and `TRANSFORMS` – control which tables are excluded and which transform classes are applied.  
  * Each transform class’s `modify_select` method – the core of the pipeline; examples include `AddIndexTransform.modify_select`, `AgeTransform.modify_select`, `ConceptNameTransform.modify_select`, `IDMappingTransform.modify_select`, and `SiteNameTransform.modify_select`. These methods may raise `psycopg2.ProgrammingError` if they generate invalid SQL, which propagates to the caller.  
  * SQLAlchemy library – used for constructing and compiling the SELECT statements.  

**Note**:  
- The function does **not** execute any SQL; it only returns the statements. Execution must be performed by the caller after any required `pre_transform` steps.  
- The target schema is embedded directly in each CREATE TABLE statement; therefore the caller’s search_path does not need to include the target schema.  
- Because the function uses an UNLOGGED table, the created tables are faster to write but are not crash‑recoverable; they are intended for temporary transformation results.  
- The returned set contains tuples, not `Statement` objects; callers must wrap them as needed.  
- If a table name appears both in `VOCAB_TABLES` and in `target_table`, it will be skipped because vocabulary tables are never transformed.  

**Output Example**:  
Assume `model_version = "2.3.0"`, `site = "stlouis"`, `target_schema = "tmp_transform"`, `id_name = "dcc"`, `id_type = "BigInteger"`, and `target_table = "person,measurement"`. After processing, a possible element of the returned set could be:

```
(
    "CREATE UNLOGGED TABLE tmp_transform.person AS SELECT person.person_id, person.location_id, person.care_site_id, person.provider_id, person.gender_concept_id, person.year_of_birth, person.month_of_birth, person.day_of_birth, person.birth_datetime, person.race_concept_id, person.ethnicity_concept_id, person.death_datetime, person.death_type_concept_id, person.death_source_value, person.death_source_concept_id, person.age_in_months, 'stlouis'::varchar(32) AS site FROM person LEFT OUTER JOIN concept AS concept_1 ON concept_1.concept_id = person.gender_concept_id LEFT OUTER JOIN concept AS concept_2 ON concept_2.concept_id = person.race_concept_id ...",
    "creating transformed copy of table person"
)
```

A second tuple would be similar for the `measurement` table. The set would contain one tuple per requested, non‑vocabulary table.
## FunctionDef _transform_age_select_sql(model_version, site, target_schema, target_table)
**_transform_age_select_sql**: The function of _transform_age_select_sql is to generate the SQL statements that create transformed copies of selected tables with age‑in‑months columns added.

**parameters**:  
· model_version: string indicating the PEDSnet model version (e.g., “2.3.0”).  
· site: string containing the site identifier (e.g., “stlouis”).  
· target_schema: name of the schema where the transformed tables will be created.  
· target_table: a comma‑separated string of table names that should be transformed.

**Code Description**:  
1. The function obtains a read‑only SQLAlchemy MetaData object for the requested model version by calling `stock_metadata(model_version)`. This metadata contains the full definition of all source tables.  
2. The `site` value is stored in the metadata’s `info` dictionary (`metadata.info['site'] = site`). This information is used by downstream transforms (e.g., SiteNameTransform) that rely on the site name.  
3. An empty Python `set` named `stmt_pairs` is created to hold the resulting `(sql_string, msg)` tuples.  
4. The `target_table` argument is split on commas, producing a list of individual table names that the caller wants to process.  
5. The function iterates over every table defined in `metadata.tables`. For each `(table_name, table)` pair:  
   * If `table_name` is **not** present in the list derived from `target_table`, the loop skips to the next table.  
   * A basic SELECT object that selects all columns from the current table is created with `sqlalchemy.select([table])`. The variable `join_obj` is initially set to the table itself (the FROM clause).  
   * The SELECT and join objects are passed to `AgeTransform.modify_select`. This class method augments the SELECT by adding derived age‑in‑months columns for the datetime columns listed in `AgeTransform.columns_by_table` and, when needed, adds an inner join to the `person` table. The method returns the possibly‑modified SELECT and join objects.  
   * The final SELECT object is bound to the (potentially extended) join clause via `select_obj.select_from(join_obj)`.  
   * The combined SELECT is compiled to a PostgreSQL‑compatible SQL string using `compile(dialect=sqlalchemy.dialects.postgresql.dialect())`. The compiled object’s `params` dictionary is interpolated into the string (`str(table_sql_obj) % table_sql_obj.params`) to produce a plain SQL statement.  
   * A `CREATE UNLOGGED TABLE` statement is assembled:  
     `CREATE UNLOGGED TABLE {target_schema}.{table_name} AS {table_sql}`.  
     The use of `UNLOGGED` makes the table creation faster because WAL logging is omitted.  
   * A human‑readable message describing the operation (`'creating transformed copy of table {table_name}'`) is paired with the SQL string, and the tuple is added to `stmt_pairs`.  
6. After all relevant tables have been processed, the set `stmt_pairs` is returned. Each element of the set is a tuple `(sql_string, msg)` ready for execution by the caller.

**Reference Relationships**:  
*Caller*: The function is invoked by `_transform_age` (in the same module). `_transform_age` first runs `AgeTransform.pre_transform` to install required PL/pgSQL functions, then iterates over the tuples returned by `_transform_age_select_sql`, wraps each SQL string in a `Statement` object, and executes them in parallel.  
*Callees*:  
- `stock_metadata` – provides the source schema metadata.  
- `AgeTransform.modify_select` – performs the column‑level age transformation and join augmentation.  
- SQLAlchemy utilities (`select`, `compile`, `dialects.postgresql.dialect`) – used to build and render the SELECT statement.  

**Note**:  
- The function does **not** execute any SQL; it only builds the statements. Execution must be performed after a pre‑transform step (`AgeTransform.pre_transform`) because the generated SELECT relies on the PL/pgSQL function `months_in_interval`.  
- `target_table` may contain multiple table names separated by commas; whitespace is not stripped, so callers should provide a clean list (e.g., `"person,measurement"`).  
- The returned set is unordered; if deterministic ordering is required, the caller should sort the set after retrieval.  
- Because the function uses `CREATE UNLOGGED TABLE`, the resulting tables are not crash‑recoverable and will be lost if the database crashes before they are persisted elsewhere. This is intentional for temporary transformation workspaces.  
- Any exception raised inside `AgeTransform.modify_select` (e.g., `ValueError` for missing columns) propagates as a `psycopg2.ProgrammingError` when the SQL is later executed, matching the function’s documented exception type.

**Output Example**:  
Assuming `target_schema = "tmp_schema"` and `target_table = "person,measurement"` for model version “2.3.0”, a possible return value could be:

```
{
    (
        "CREATE UNLOGGED TABLE tmp_schema.person AS SELECT person.*, "
        "months_in_interval(person.birth_datetime, person.death_datetime) "
        "AS death_age_in_months FROM person JOIN person AS person "
        "ON person.person_id = person.person_id",
        "creating transformed copy of table person"
    ),
    (
        "CREATE UNLOGGED TABLE tmp_schema.measurement AS SELECT measurement.*, "
        "months_in_interval(person.birth_datetime, measurement.measurement_datetime) "
        "AS measurement_age_in_months, "
        "months_in_interval(person.birth_datetime, measurement.measurement_result_datetime) "
        "AS measurement_result_age_in_months FROM measurement "
        "JOIN person ON person.person_id = measurement.person_id",
        "creating transformed copy of table measurement"
    )
}
```
## FunctionDef _transform_concept_select_sql(model_version, site, target_schema, target_table)
**_transform_concept_select_sql**: The function of _transform_concept_select_sql is to generate SQL statements that create transformed copies of selected tables by applying the ConceptNameTransform to the SELECT clause.

**parameters**:  
· model_version: string representing the PEDSnet model version (e.g., “2.3.0”).  
· site: string containing the site identifier (e.g., “stlouis”).  
· target_schema: string name of the schema where the transformed tables will be created.  
· target_table: string containing one or more comma‑separated table names to be transformed.

**Code Description**:  
1. The function obtains a read‑only MetaData object for the requested model version by calling `stock_metadata(model_version)`. This MetaData contains the full PEDSnet schema (tables, columns, constraints).  
2. It stores the site name in the MetaData’s `info` dictionary (`metadata.info['site'] = site`). This value is later used by transforms that need the site name (e.g., SiteNameTransform).  
3. An empty set `stmt_pairs` is created to collect the generated SQL‑message tuples.  
4. The `target_table` argument is split on commas, producing a list of individual table names that the caller wants to transform.  
5. The function iterates over every table defined in `metadata.tables`. For each table whose name appears in the `target_table` list:  
   a. A basic SELECT object that selects all columns from the table is created: `select_obj = sqlalchemy.select([table])`.  
   b. The initial FROM clause (`join_obj`) is set to the table itself.  
   c. The SELECT and JOIN objects are passed to `ConceptNameTransform.modify_select`. This class method inspects the SELECT’s column list, adds a `_concept_name` column for each `_concept_id` column, and extends the join tree with LEFT OUTER JOINs to the `concept` lookup table. The method returns possibly‑modified `select_obj` and `join_obj`.  
   d. The final SELECT statement is bound to the (potentially extended) FROM clause by calling `select_obj.select_from(join_obj)`.  
   e. The statement is compiled to PostgreSQL dialect SQL: `table_sql_obj = final_select_obj.compile(dialect=sqlalchemy.dialects.postgresql.dialect())`. The compiled object’s parameter placeholders are substituted with actual values using `str(table_sql_obj) % table_sql_obj.params`, yielding a plain SQL string (`table_sql`).  
   f. A CREATE UNLOGGED TABLE statement is assembled, embedding the target schema and table name:  
      `final_sql = 'CREATE UNLOGGED TABLE {0}.{1} AS {2}'.format(target_schema, table_name, table_sql)`  
   g. A human‑readable message describing the operation is built: `msg = 'creating transformed copy of table {}'.format(table_name)`.  
   h. The tuple `(final_sql, msg)` is added to `stmt_pairs`.  
6. After processing all requested tables, the set `stmt_pairs` is returned. Each element of the set is a two‑item tuple containing the full CREATE TABLE … AS SELECT statement and a short description of the action.

**Reference Relationships**:  
- **Caller**: `_transform_concept` (in the same module) invokes `_transform_concept_select_sql` to obtain the SQL statements, wraps each in a `Statement` object, and executes them in parallel. `_transform_concept` also runs `AgeTransform.pre_transform` before calling this function, because the generated SELECT statements assume that any prerequisite transformations (e.g., age column creation) have already been applied.  
- **Callee**: `stock_metadata` supplies the base schema metadata. `ConceptNameTransform.modify_select` is the only transform applied within this function; it enriches the SELECT clause with concept name columns and adds the necessary joins. No other transforms are invoked here.  
- The returned SQL strings are later executed by the `StatementSet.parallel_execute` method in `_transform_concept`. Errors are captured and, if `force` is true, duplicate‑table errors may be ignored.

**Note**:  
- The function only builds the SELECT‑based transformation; any required pre‑transform steps (e.g., age column creation, site name addition) must be executed beforehand, otherwise the generated SQL may reference columns that do not exist.  
- The target schema is hard‑coded into each CREATE TABLE statement; the caller’s search_path does not need to include the target schema.  
- Because a set is used to collect statements, duplicate table names will be eliminated automatically, but the order of execution is nondeterministic. If deterministic ordering is required, the caller should sort the set before execution.  
- The function raises `psycopg2.ProgrammingError` indirectly if `ConceptNameTransform.modify_select` encounters an unexpected schema condition (e.g., missing `concept` table).

**Output Example**:  
Assume `model_version = "2.3.0"`, `site = "stlouis"`, `target_schema = "tmp_schema"`, and `target_table = "condition_occurrence,drug_exposure"`. After processing, the function might return a set similar to:

{
    (
        "CREATE UNLOGGED TABLE tmp_schema.condition_occurrence AS SELECT condition_occurrence.*, concept_1.concept_name AS condition_concept_name FROM condition_occurrence LEFT OUTER JOIN concept AS concept_1 ON concept_1.concept_id = condition_occurrence.condition_concept_id",
        "creating transformed copy of table condition_occurrence"
    ),
    (
        "CREATE UNLOGGED TABLE tmp_schema.drug_exposure AS SELECT drug_exposure.*, concept_1.concept_name AS drug_concept_name, concept_2.concept_name AS route_concept_name FROM drug_exposure LEFT OUTER JOIN concept AS concept_1 ON concept_1.concept_id = drug_exposure.drug_concept_id LEFT OUTER JOIN concept AS concept_2 ON concept_2.concept_id = drug_exposure.route_concept_id",
        "creating transformed copy of table drug_exposure"
    )
}
## FunctionDef _transform_site_select_sql(model_version, site, target_schema, target_table)
**_transform_site_select_sql**: The function of _transform_site_select_sql is to generate a set of SQL `CREATE UNLOGGED TABLE … AS SELECT …` statements that add a constant site‑name column to each requested table while preserving all original columns.

**parameters**:  
· **model_version** – string identifying the PEDSnet model version (e.g., “2.3.0”).  
· **site** – string containing the site identifier that will be written into the new *site* column (e.g., “stlouis”).  
· **target_schema** – name of the schema where the transformed tables will be created.  
· **target_table** – comma‑separated list of table names that should be transformed; only tables appearing in this list are processed.

**Code Description**:  
1. **Metadata preparation** – The function calls `stock_metadata(model_version)` to obtain a fresh SQLAlchemy `MetaData` object that describes the unmodified PEDSnet model for the supplied version.  
2. **Result container** – An empty Python `set` named `stmt_pairs` is created to hold the `(sql_string, msg)` tuples that will be returned.  
3. **Target table parsing** – The `target_table` argument is split on commas, producing a list of individual table names that the caller wants to transform.  
4. **Iterate over model tables** – The function loops over `metadata.tables.items()`. For each `(table_name, table)` pair:  
   - If `table_name` is not present in the parsed `target_table` list, the iteration skips to the next table.  
   - A base `SELECT` object is created that selects all columns from the current table: `select_obj = sqlalchemy.select([table])`.  
   - The variable `join_obj` is initially set to the table itself; this object will be passed through any transform that needs to add joins.  
5. **Apply SiteNameTransform** – The `SiteNameTransform.modify_select` class method is invoked with the current metadata, table name, the `select_obj`, and the `join_obj`.  
   - Inside `modify_select`, a literal column containing the site name (taken from `metadata.info['site']`) is added to the SELECT clause.  
   - The method returns the possibly‑modified `select_obj` (now containing the extra *site* column) and the unchanged `join_obj`.  
6. **Finalize SELECT** – The returned `select_obj` is bound to the final FROM clause by calling `select_obj.select_from(join_obj)`. This produces a complete SELECT statement that includes all original columns plus the constant *site* column.  
7. **SQL compilation** – The SELECT object is compiled to PostgreSQL dialect SQL:  
   ```python
   table_sql_obj = final_select_obj.compile(
       dialect=sqlalchemy.dialects.postgresql.dialect())
   ```  
   The compiled object’s string representation is interpolated with its parameters (`% table_sql_obj.params`) to obtain a plain SQL string (`table_sql`).  
8. **Create table statement** – The function builds a `CREATE UNLOGGED TABLE` statement that materialises the SELECT result in the target schema:  
   ```python
   final_sql = 'CREATE UNLOGGED TABLE {0}.{1} AS {2}'.format(
       target_schema, table_name, table_sql)
   ```  
   The statement is unlogged to improve write performance during bulk transformation.  
9. **Message construction** – A human‑readable description is assembled:  
   ```python
   msg = 'creating transformed copy of table {}'.format(table_name)
   ```  
10. **Collect result** – The tuple `(final_sql, msg)` is added to `stmt_pairs`. Because `stmt_pairs` is a set, duplicate statements for the same table cannot be added.  
11. **Return** – After all tables have been processed, the function returns `stmt_pairs`, which contains one entry per requested table.

**Reference relationships**:  
- **Caller**: `_transform_site` (in the same module) invokes `_transform_site_select_sql` to obtain the SQL statements that will be executed in parallel. `_transform_site` first runs `SiteNameTransform.pre_transform` to ensure the metadata contains the required `site` entry, then iterates over the returned statement set, adds each to a `StatementSet`, and executes them.  
- **Callees**:  
  * `stock_metadata` – provides the source schema definition.  
  * `SiteNameTransform.modify_select` – injects the constant site column into the SELECT clause.  
  * SQLAlchemy functions (`select`, `compile`, `dialects.postgresql.dialect`) – used to build and render the SQL.  

**Note**:  
- The function assumes that `metadata.info['site']` has been populated before it is called; this is done by `SiteNameTransform.pre_transform` in the caller. If the key is missing, `SiteNameTransform.modify_select` will raise a `RuntimeError`.  
- The generated tables are created as **UNLOGGED**, which means they are not crash‑recoverable and are faster for bulk inserts, but they will be lost if the database crashes before they are committed.  
- The function does not execute any SQL; it only returns the statements. Execution, error handling, and optional forced‑overwrite logic are performed by the caller (`_transform_site`).  
- Because the result container is a set, the order of returned statements is nondeterministic; callers should not rely on ordering.  

**Output Example**:  
If `model_version` is “2.3.0”, `site` is “stlouis”, `target_schema` is “tmp_schema”, and `target_table` is “person,measurement”, a possible element of the returned set is:

```
(
    "CREATE UNLOGGED TABLE tmp_schema.person AS SELECT person.person_id, person.gender_concept_id, person.birth_datetime, 'stlouis'::varchar(32) AS site FROM person",
    "creating transformed copy of table person"
)
```

A second element for the *measurement* table would look similar, with all measurement columns followed by the same literal site column. The full return value is a set containing one such tuple for each table listed in `target_table`.
## FunctionDef _transform_id_select_sql(model_version, site, target_schema, target_table, id_name, id_type)
**_transform_id_select_sql**: The function of _transform_id_select_sql is to generate the SQL statements that create transformed copies of the specified tables, applying the ID‑mapping transformation to each table.

**parameters**:  
· model_version: string indicating the PEDSnet model version (e.g., “2.3.0”).  
· site: string name of the site (e.g., “stlouis”).  
· target_schema: name of the schema where the transformed tables will be created.  
· target_table: a comma‑separated list of table names that should be transformed.  
· id_name: base name of the identifier set (default “dcc”). Used by IDMappingTransform to build mapping‑table column names.  
· id_type: SQL type of the site identifier (“BigInteger” or “String”). Determines the type of the `site_id` column in mapping tables.

**Code Description**:  
1. The function obtains a read‑only MetaData object that describes the stock PEDSnet model for the supplied `model_version` by calling `stock_metadata(model_version)`.  
2. It stores the site name in the MetaData’s `info` dictionary (`metadata.info['site'] = site`). This value is required by transforms that need the site name (e.g., SiteNameTransform).  
3. An empty set `stmt_pairs` is created to collect the generated `(sql_string, msg)` tuples.  
4. The `target_table` argument is split on commas, producing a list of individual table names that the caller wants to transform.  
5. The function iterates over every table defined in the MetaData (`for table_name, table in metadata.tables.items():`). For each table whose name appears in the `target_table` list:  
   a. A basic SELECT object that selects all columns from the table is created: `select_obj = sqlalchemy.select([table])`.  
   b. The initial FROM clause (`join_obj`) is set to the table itself.  
   c. The SELECT and JOIN objects are passed to `IDMappingTransform.modify_select`. This method rewrites the SELECT so that primary‑key and foreign‑key columns are replaced with the corresponding DCC IDs from mapping tables, and it augments the JOIN clause with the necessary mapping‑table joins. The method returns potentially new `select_obj` and `join_obj`.  
   d. The SELECT is bound to the (possibly extended) FROM clause by calling `select_obj.select_from(join_obj)`, producing `final_select_obj`.  
   e. The final SELECT object is compiled to PostgreSQL dialect SQL: `table_sql_obj = final_select_obj.compile(dialect=sqlalchemy.dialects.postgresql.dialect())`. The compiled object contains a parameterized SQL string and a dictionary of bound parameters. The parameters are interpolated with `str(table_sql_obj) % table_sql_obj.params` to obtain a plain SQL string (`table_sql`).  
   f. A CREATE TABLE statement is assembled: `final_sql = 'CREATE UNLOGGED TABLE {0}.{1} AS {2}'.format(target_schema, table_name, table_sql)`. The statement creates an unlogged table in `target_schema` with the same name as the source table, populated by the transformed SELECT.  
   g. A human‑readable message describing the operation (`msg = 'creating transformed copy of table {}'.format(table_name)`) is paired with the SQL string and added to `stmt_pairs`.  
6. After processing all requested tables, the set `stmt_pairs` is returned. Each element of the set is a tuple `(sql_string, msg)` ready for execution by the caller.

**Reference Relationships**:  
- Called exclusively by `_transform_id` in the same module. `_transform_id` first runs `IDMappingTransform.pre_transform` to create the mapping tables, then iterates over the tuples returned by `_transform_id_select_sql`, wrapping each in a `Statement` object and executing them in parallel.  
- Relies on `stock_metadata` to obtain the source schema definition and on `IDMappingTransform.modify_select` to apply the ID‑mapping logic. No other transforms are invoked in this path; the ID‑mapping transform is the only one that modifies the SELECT for this runner.  
- The generated SQL strings are later executed by the `StatementSet.parallel_execute` method, and any errors are handled by `_transform_id`.

**Note**:  
- The function does not perform the pre‑transformation step; the caller must ensure that `IDMappingTransform.pre_transform` has been executed beforehand, otherwise the required mapping tables will not exist.  
- The `target_schema` is embedded directly in each CREATE TABLE statement; the caller’s search_path does not need to include the target schema.  
- Because the function returns a **set**, duplicate statements are automatically eliminated, but the order of execution is nondeterministic. If a deterministic order is required, the caller should convert the set to a list and sort it.  
- The function assumes that all tables listed in `target_table` exist in the stock metadata; if a name is misspelled, the table will be silently ignored (no statement is generated for it).  
- The generated tables are created as **UNLOGGED**, which improves write performance but means the tables are not crash‑recoverable; they are intended for temporary transformation results.

**Output Example**:  
If `model_version = "2.3.0"`, `site = "stlouis"`, `target_schema = "tmp_schema"`, and `target_table = "person,visit_occurrence"`, a possible element of the returned set could be:

```
(
    "CREATE UNLOGGED TABLE tmp_schema.person AS SELECT person_ids.dcc_id AS person_id, person.person_id AS site_id, location_ids.dcc_id AS location_id, care_site_ids.dcc_id AS care_site_id, provider_ids.dcc_id AS provider_id, person.birth_datetime, person.gender_concept_id, ... FROM person JOIN person_ids ON person.person_id = person_ids.site_id LEFT OUTER JOIN location_ids ON person.location_id = location_ids.site_id LEFT OUTER JOIN care_site_ids ON person.care_site_id = care_site_ids.site_id LEFT OUTER JOIN provider_ids ON person.provider_id = provider_ids.site_id",
    "creating transformed copy of table person"
)
```

A second tuple would be produced for `visit_occurrence` with its own transformed SELECT and a similar message.
## FunctionDef _transform_index_select_sql(model_version, site, target_schema, target_table)
**_transform_index_select_sql**: The function of _transform_index_select_sql is to generate the SQL statements that create transformed copies of specified tables by applying the AddIndexTransform (which does not modify the SELECT clause) and returning them together with descriptive messages.

**parameters**:  
· model_version: string indicating the PEDSnet model version (e.g., “2.3.0”).  
· site: string containing the site identifier (e.g., “stlouis”).  
· target_schema: string name of the schema where the transformed tables will be created.  
· target_table: string containing a comma‑separated list of table names that should be transformed.

**Code Description**:  
1. **Metadata acquisition** – The function calls `stock_metadata(model_version)` to obtain a fresh SQLAlchemy `MetaData` object that represents the unmodified PEDSnet model for the requested version.  
2. **Site information injection** – The site name is stored in the metadata’s `info` dictionary under the key `'site'`. This value is later used by transforms that need the site name (e.g., `SiteNameTransform`).  
3. **Result container** – An empty Python `set` named `stmt_pairs` is created. Each element of the set will be a tuple `(sql_string, msg)`. Using a set guarantees that duplicate statements are automatically eliminated.  
4. **Target table parsing** – The `target_table` argument is split on commas, producing a list of individual table names that the caller wants to materialise.  
5. **Iterate over model tables** – The function loops over every table defined in the metadata (`metadata.tables.items()`). For each `(table_name, table)` pair:  
   - If `table_name` is not present in the parsed `target_table` list, the loop continues to the next table.  
   - A basic SQLAlchemy `select` object is created that selects all columns from the current table: `select_obj = sqlalchemy.select([table])`.  
   - The variable `join_obj` is initially set to the table itself; this represents the FROM clause before any joins are added.  
6. **Apply AddIndexTransform** – The `modify_select` class method of `AddIndexTransform` is invoked:  

   ```python
   select_obj, join_obj = AddIndexTransform.modify_select(
       metadata, table_name, select_obj, join_obj)
   ```

   For the index‑only transformation this method returns the original `select_obj` and `join_obj` unchanged, satisfying the transform interface without altering the SELECT clause.  
7. **Finalize SELECT statement** – The SELECT object is bound to its FROM clause by calling `select_obj.select_from(join_obj)`, producing `final_select_obj`.  
8. **SQL compilation** – `final_select_obj` is compiled to a PostgreSQL‑dialect SQL string:

   ```python
   table_sql_obj = final_select_obj.compile(
       dialect=sqlalchemy.dialects.postgresql.dialect())
   table_sql = str(table_sql_obj) % table_sql_obj.params
   ```

   The compilation step substitutes any bound parameters, yielding a plain SQL SELECT statement.  
9. **Wrap in CREATE TABLE** – The SELECT is embedded in a `CREATE UNLOGGED TABLE` statement that creates the transformed copy in the target schema:

   ```python
   final_sql = 'CREATE UNLOGGED TABLE {0}.{1} AS {2}'.format(
       target_schema, table_name, table_sql)
   ```

   The table is created as *UNLOGGED* to avoid WAL logging, which improves performance for temporary transformation tables.  
10. **Message construction** – A human‑readable description is built:  

    ```python
    msg = 'creating transformed copy of table {}'.format(table_name)
    ```  

11. **Collect result** – The tuple `(final_sql, msg)` is added to `stmt_pairs`. Because `stmt_pairs` is a set, each table contributes at most one entry.  
12. **Return** – After processing all tables, the function returns `stmt_pairs`, a set of `(SQL string, message)` tuples ready for execution by the caller.

**Reference relationships**:  
- **Caller** – `_transform_index` (in the same module) invokes `_transform_index_select_sql` to obtain the SQL statements that will be executed after the pre‑transform step (`AddIndexTransform.pre_transform`).  
- **Callee** – `stock_metadata` supplies the base model metadata; `AddIndexTransform.modify_select` is called to satisfy the transform contract; SQLAlchemy’s `select`, `compile`, and PostgreSQL dialect objects are used to build and render the SQL.  
- The generated statements are later wrapped in `Statement` objects and executed in parallel by `StatementSet.parallel_execute` inside `_transform_index`.  

**Note**:  
- The function does **not** execute any SQL; it only builds the statements. Execution is the responsibility of the caller.  
- The `pre_transform` step must be run before this function (as done in `_transform_index`) because `AddIndexTransform.modify_table` may have added index metadata that later DDL generation relies on.  
- The target schema is embedded directly in the `CREATE UNLOGGED TABLE` command; therefore the caller does not need to adjust the search_path for the target schema.  
- If `target_table` contains a name that does not exist in the model metadata, the function simply skips it without raising an error.  
- Because the function returns a set, the order of statements is nondeterministic; callers should not rely on any particular ordering.  

**Output Example**:  

Assuming `model_version = "2.3.0"`, `site = "stlouis"`, `target_schema = "tmp_schema"` and `target_table = "person,measurement"`, a possible element of the returned set is:

```
(
    "CREATE UNLOGGED TABLE tmp_schema.person AS SELECT person.person_id, person.birth_datetime, person.gender_concept_id, person.race_concept_id, person.ethnicity_concept_id FROM person",
    "creating transformed copy of table person"
)
```

Another element for the `measurement` table might be:

```
(
    "CREATE UNLOGGED TABLE tmp_schema.measurement AS SELECT measurement.measurement_id, measurement.person_id, measurement.measurement_concept_id, measurement.measurement_date, measurement.value_as_number FROM measurement",
    "creating transformed copy of table measurement"
)
```
## FunctionDef _transform(conn_str, model_version, site, target_schema, id_name, id_type, logged, pool_limit, force)
**_transform**: The function of _transform is to execute the full data‑transformation pipeline for a given PEDSnet model version, creating transformed tables in a temporary schema.

**parameters**  
· conn_str (str): libpq connection string used to open a PostgreSQL session.  
· model_version (str): PEDSnet model version identifier (e.g., “2.3.0”).  
· site (str): name of the site for which the transformation is being run (e.g., “stlouis”).  
· target_schema (str): name of the temporary schema that will receive the transformed tables.  
· id_name (str): column name that will hold the site‑specific identifier in transformed tables.  
· id_type (str): PostgreSQL data type of the identifier column (e.g., “BigInteger”).  
· logged (bool): if True the created tables are logged; otherwise they are created as UNLOGGED tables.  
· pool_limit (bool): if True the parallel worker pool is limited to a single process; otherwise up to 25 workers are used.  
· force (bool, optional, default False): when True, benign errors such as “duplicate table” are ignored and the transformation continues.

**Code Description**  
1. **Run prerequisite steps** – The function iterates over the global list `TRANSFORMS`. For each transform class it calls the class method `pre_transform(conn_str, stock_metadata(model_version), id_name, id_type)`. This gives every registered transform a chance to create required database objects (functions, temporary tables, etc.) before the main transformation SQL is generated. The `pre_transform` calls may raise `psycopg2.ProgrammingError`; such exceptions are propagated to the caller.  

2. **Generate main transformation statements** – `_transform_select_sql` is invoked with the supplied arguments. It returns a set of `(sql_string, message)` tuples, each representing a `CREATE [UN]LOGGED TABLE … AS SELECT …` statement that materialises a transformed copy of a source table. The function respects the `logged` flag when choosing between `CREATE TABLE` and `CREATE UNLOGGED TABLE`.  

3. **Collect statements in a StatementSet** – An empty `StatementSet` instance (`stmts`) is created. For every `(sql, msg)` pair produced in step 2, a `Statement` object is instantiated and added to the set via `stmts.add(Statement(sql, msg))`.  

4. **Determine worker pool size** – If `pool_limit` is True the pool size is forced to 1; otherwise the default pool size (up to 25 workers) will be chosen later by `StatementSet.parallel_execute`.  

5. **Execute statements in parallel** – `stmts.parallel_execute(conn_str, pool_size)` launches a pool of worker processes that each call the internal `_worker_process` function. Every `Statement` is executed against the same connection string; results, errors and log records are collected via multiprocessing queues. After all workers finish, the `StatementSet` is repopulated with the executed `Statement` objects, now containing any result data or error information.  

6. **Error handling** – The function iterates over the executed statements. For each statement that has a non‑null `err` attribute:  
   * If `force` is True and `pg_error(stmt)` resolves to the PostgreSQL condition name `'DUPLICATE_TABLE'`, the function returns immediately, effectively ignoring the duplicate‑table error.  
   * Otherwise a `DatabaseError` is raised with a message that combines the statement’s descriptive `msg` and the underlying error text `err`.  

7. **Return value** – The function does not contain an explicit `return` statement; therefore it returns `None` on successful completion.  

**Reference Relationships**  
* **Callers** – The primary caller is `run_transformation` in the same module. `run_transformation` creates the temporary schema, then invokes `_transform` to populate it with transformed tables before proceeding with post‑transformation steps (setting logged status, adding primary keys, indexes, foreign keys, moving tables, etc.).  
* **Callees** –  
  * `Transform.pre_transform` (via each class in `TRANSFORMS`) – prepares database objects required by individual transforms.  
  * `stock_metadata` (indirectly through `_transform_select_sql`) – provides the canonical model metadata for the given `model_version`.  
  * `_transform_select_sql` – builds the set of CREATE‑TABLE‑AS‑SELECT statements that implement the transformation logic.  
  * `StatementSet` and its methods `add` and `parallel_execute` – manage the collection of `Statement` objects and run them concurrently.  
  * `pg_error` – translates PostgreSQL error codes to symbolic names for the `force`‑duplicate‑table check.  
  * `DatabaseError` – custom exception raised when a statement fails and the error is not suppressed.  

**Note**  
* The function assumes that every transform listed in `TRANSFORMS` implements a `pre_transform` method compatible with the abstract base class signature.  
* The `force` flag only suppresses duplicate‑table errors; any other error (including syntax errors, permission problems, or unexpected PostgreSQL conditions) will cause a `DatabaseError` to be raised.  
* Because the target schema name is embedded directly in each generated CREATE statement, the caller must ensure that the schema exists (created earlier by `run_transformation`).  
* Parallel execution uses separate processes; all objects placed on the multiprocessing queues must be pickleable. The `Statement` class satisfies this requirement.  
* The function does not return the list of SQL strings; callers that need the raw statements must invoke `_transform_select_sql` directly.  

**Output Example**  
When the transformation completes without error, the function returns `None`:

```
result = _transform(conn_str, '2.3.0', 'stlouis',
                    'mydb_transformed', 'dcc', 'BigInteger',
                    logged=False, pool_limit=False, force=False)
print(result)   # Output: None
```
## FunctionDef _transform_target(conn_str, model_version, site, target_schema, id_name, id_type, target_table, force)
**_transform_target**: The function of _transform_target is to execute all pre‑transform hooks, generate the CREATE‑TABLE statements for the requested target tables, run those statements in parallel, and raise an error if any statement fails (unless the error is a duplicate‑table condition and the *force* flag is set).

**parameters**:  
· conn_str (str): libpq connection string used to connect to the PostgreSQL database.  
· model_version (str): PEDSnet model version (e.g., “2.3.0”).  
· site (str): site identifier (e.g., “stlouis”).  
· target_schema (str): name of the temporary schema that will hold the transformed tables.  
· id_name (str): name of the identifier column that transforms may add (default “dcc”).  
· id_type (str): SQL type of the identifier column (e.g., “BigInteger”).  
· target_table (str): comma‑separated list of tables to be transformed.  
· force (bool, optional): when True, duplicate‑table errors are ignored; default is False.

**Code Description**:  
1. **Run pre‑transform hooks** – The function iterates over the global list `TRANSFORMS`. For each transform class it calls the class method `pre_transform(conn_str, stock_metadata(model_version), id_name, id_type)`. This gives every registered transform a chance to create database objects (functions, temporary tables, etc.) that are required before the main transformation SQL is generated. The call uses the stock (unmodified) model metadata obtained from `stock_metadata`.  

2. **Collect transformation statements** – An empty `StatementSet` instance (`stmts`) is created. The helper `_transform_target_select_sql` is invoked with the same arguments (except *force*). That helper returns a set of `(sql, msg)` tuples, each representing a `CREATE UNLOGGED TABLE … AS SELECT …` statement that materialises a transformed copy of one of the requested tables. For every tuple the function creates a `Statement(sql, msg)` object and adds it to the `StatementSet` via `stmts.add`.  

3. **Parallel execution** – The populated `StatementSet` is executed with `stmts.parallel_execute(conn_str)`. The `parallel_execute` method spawns a pool of worker processes (up to 25, or one per statement) that each call the statement’s `execute` method. All statements run concurrently, and each statement records any error in its `err` attribute.  

4. **Error handling** – After parallel execution the function iterates over the statements in `stmts`. For any statement whose `err` attribute is not `None` it determines the PostgreSQL error condition name by calling `pg_error(stmt)`.  
   * If *force* is True **and** the error condition is `'DUPLICATE_TABLE'`, the function returns immediately, effectively ignoring the duplicate‑table error.  
   * Otherwise a `DatabaseError` is raised with a message that combines the statement’s descriptive message (`stmt.msg`) and the error text (`stmt.err`).  

5. **Return value** – The function does not return any explicit value; successful completion results in an implicit `None`. All error paths raise `DatabaseError` (or propagate other exceptions such as `psycopg2.ProgrammingError` that may arise from `pre_transform` or the SQL generation step).

**Reference relationships**:  
* **Caller** – `run_target_transformation` (in the same module) invokes `_transform_target` after creating the temporary transformed schema. The caller relies on `_transform_target` to populate that schema with transformed tables and to surface any fatal errors.  
* **Callees** –  
  * `TRANSFORMS` list elements’ `pre_transform` methods (e.g., `AgeTransform.pre_transform`, `IDMappingTransform.pre_transform`).  
  * `stock_metadata` – provides the base model metadata passed to each `pre_transform`.  
  * `_transform_target_select_sql` – generates the set of CREATE‑TABLE statements that are executed.  
  * `StatementSet` – collects `Statement` objects and runs them in parallel.  
  * `pg_error` – translates PostgreSQL error codes to symbolic names for the *force*‑duplicate‑table check.  
  * `DatabaseError` – the exception type raised for any non‑ignorable statement error.  

**Note**:  
* The *force* flag only suppresses duplicate‑table errors; all other errors (syntax errors, permission problems, etc.) will still raise `DatabaseError`.  
* `pre_transform` may raise `psycopg2.ProgrammingError`; the docstring of `_transform_target` mentions that such exceptions can propagate to the caller.  
* The target schema name is embedded directly in each generated CREATE statement, so the caller’s `search_path` does not need to include the temporary schema.  
* Because the statements are executed in parallel, the order of execution is nondeterministic; however, each statement is independent (each creates a separate table), so this does not affect correctness.  
* The function assumes that every element in `TRANSFORMS` implements a compatible `pre_transform` class method; missing implementations will cause an `AttributeError`.  

**Output Example**:  
```python
result = _transform_target(conn_str, '2.3.0', 'stlouis',
                           'tmp_transformed', 'dcc', 'BigInteger',
                           'person,measurement', force=False)
print(result)   # Output: None (successful execution)
```
## FunctionDef _transform_age(conn_str, model_version, site, target_schema, target_table, force)
**_transform_age**: The function of _transform_age is to execute the age‑in‑months transformation for a set of tables, creating the transformed tables in a temporary schema and handling any execution errors.

**parameters**:  
· **conn_str** (str): PostgreSQL libpq connection string used for all database operations.  
· **model_version** (str): PEDSnet model version (e.g., “2.3.0”) that determines the source schema metadata.  
· **site** (str): Site identifier that is stored in the metadata info dictionary for downstream transforms.  
· **target_schema** (str): Name of the temporary schema in which the transformed tables will be created.  
· **target_table** (str): Comma‑separated list of table names that should be transformed.  
· **force** (bool, optional, default False): When True, duplicate‑table errors are ignored and the function returns silently.

**Code Description**:  
1. **Prerequisite PL/pgSQL functions** – The function begins by invoking `AgeTransform.pre_transform(conn_str, stock_metadata(model_version))`. This call installs the database functions (e.g., `months_in_interval`) required by the age transformation. The `stock_metadata` call supplies a read‑only `MetaData` object for the requested model version; the metadata itself is not otherwise used in this step.  

2. **Collect transformation statements** – An empty `StatementSet` instance (`stmts`) is created. The helper `_transform_age_select_sql` is called with the same arguments (`model_version, site, target_schema, target_table`). This helper returns a set of `(sql, msg)` tuples, each representing a `CREATE UNLOGGED TABLE … AS SELECT …` statement that adds the age‑in‑months columns to one target table. For every tuple returned, a `Statement` object is instantiated and added to `stmts` via `stmts.add(Statement(sql, msg))`.  

3. **Parallel execution** – `stmts.parallel_execute(conn_str)` is called. `StatementSet.parallel_execute` spawns a pool of worker processes (up to 25, or one per statement) that each execute the `execute` method of the contained `Statement` objects against the supplied connection string. The statements are run concurrently, and each `Statement` is updated with the result data or error information.  

4. **Error handling** – After parallel execution finishes, the function iterates over the statements in `stmts`. For any statement whose `err` attribute is set:  
   - If `force` is True **and** `pg_error(stmt)` evaluates to `'DUPLICATE_TABLE'`, the function returns immediately, treating the duplicate‑table condition as benign.  
   - Otherwise a `DatabaseError` is raised with a message that combines the statement’s human‑readable description (`stmt.msg`) and the underlying error (`stmt.err`). The loop stops at the first non‑ignored error.  

5. **Return value** – The function does not return an explicit value; on successful completion it returns `None`. Any failure that is not suppressed by the `force` flag results in a `DatabaseError` exception being propagated to the caller.

**Reference relationships**:  
- **Caller**: `run_age_transformation` (in the same module) invokes `_transform_age` after creating the temporary transformed schema. The caller relies on `_transform_age` to populate that schema with the transformed tables.  
- **Callees**:  
  * `AgeTransform.pre_transform` – installs required PL/pgSQL functions.  
  * `_transform_age_select_sql` – builds the set of SQL statements for each target table.  
  * `StatementSet` – aggregates the statements and provides the `parallel_execute` method.  
  * `pg_error` – extracts the PostgreSQL error condition name from a failed `Statement`.  
  * `DatabaseError` – the exception type raised when a non‑ignorable error occurs.  
- **Indirect dependencies**: `stock_metadata` (used inside `AgeTransform.pre_transform`), SQLAlchemy utilities for statement compilation, and the `Statement` class (which encapsulates each SQL command).  

**Note**:  
- The `force` flag only suppresses duplicate‑table errors (`pg_error` == `'DUPLICATE_TABLE'`). All other errors, including syntax errors in the generated SELECT or failures in the PL/pgSQL functions, will raise `DatabaseError`.  
- The target schema name is embedded directly in each generated `CREATE UNLOGGED TABLE` statement; therefore the schema must exist before `_transform_age` is called (it is created by `run_age_transformation`).  
- Because the statements are executed in parallel, the order of execution is nondeterministic; however, each statement is independent, so ordering does not affect correctness.  
- The function relies on the side‑effect of `AgeTransform.pre_transform`; if that step fails, the subsequent SELECT statements will raise a `psycopg2.ProgrammingError` when they reference the missing functions.  
- No value is returned; callers should treat the absence of an exception as success.

**Output Example**:  
```python
# Successful run – no exception raised, function returns None
_transform_age(conn_str, "2.3.0", "stlouis", "tmp_schema", "person,measurement")
# => None
```
## FunctionDef _transform_concept(conn_str, model_version, site, target_schema, target_table, force)
**_transform_concept**: The function of _transform_concept is to execute the concept‑name transformation pipeline for a set of tables, creating transformed copies in a temporary schema.

**parameters**:  
· **conn_str** (str): libpq connection string used to open a PostgreSQL session.  
· **model_version** (str): PEDSnet model version identifier (e.g., “2.3.0”).  
· **site** (str): Site label that may be required by transforms (e.g., “stlouis”).  
· **target_schema** (str): Name of the temporary schema in which the transformed tables will be created.  
· **target_table** (str): Comma‑separated list of table names to be transformed.  
· **force** (bool, optional, default False): When True, duplicate‑table errors are ignored; otherwise any error aborts the transformation.

**Code Description**:  
1. **Pre‑transform hook** – The function begins by invoking `AgeTransform.pre_transform`. This call installs the PL/pgSQL functions required by the age‑column transform (e.g., `months_in_interval`). The call uses the same `conn_str` and the model metadata obtained from `stock_metadata(model_version)`. Although the age transform is not directly applied here, its prerequisite functions must exist because the SQL generated later may reference them.  
2. **Statement collection** – An empty `StatementSet` instance (`stmts`) is created.  
3. **SQL generation** – The helper `_transform_concept_select_sql` is called with `model_version`, `site`, `target_schema`, and `target_table`. This helper:  
   - Retrieves the stock metadata for the given model version,  
   - Stores the site name in `metadata.info['site']`,  
   - For each table listed in `target_table`, builds a `SELECT` statement that selects all columns and then passes it to `ConceptNameTransform.modify_select`.  
   - The transform adds `_concept_name` columns and the necessary LEFT OUTER JOINs to the `concept` lookup table.  
   - The final SELECT is compiled to a PostgreSQL‑compatible SQL string and wrapped in a `CREATE UNLOGGED TABLE {target_schema}.{table} AS …` statement.  
   - Each generated pair `(sql, msg)` is returned as a set.  
4. **Statement wrapping** – For every `(sql, msg)` tuple returned, a `Statement` object is instantiated and added to the `StatementSet` via `stmts.add(Statement(sql, msg))`.  
5. **Parallel execution** – `stmts.parallel_execute(conn_str)` runs all CREATE‑TABLE statements concurrently in separate worker processes. The target schema name is already embedded in each SQL string, so the workers do not need to modify the search path.  
6. **Error handling** – After execution, the function iterates over each `Statement` in `stmts`. If a statement has an error (`stmt.err` is truthy):  
   - If `force` is True and the PostgreSQL error condition (obtained via `pg_error(stmt)`) equals `'DUPLICATE_TABLE'`, the function returns early, effectively ignoring the duplicate‑table error.  
   - Otherwise a `DatabaseError` is raised with a message that combines the statement’s descriptive message (`stmt.msg`) and the underlying error (`stmt.err`). This aborts the transformation and propagates the failure to the caller.  

**Reference Relationships**:  
- **Callers**: `_transform_concept` is invoked by the high‑level runners `run_concept_transformation` and `run_concept_age_transformation`. Both runners create a temporary “*_transformed*” schema, call `_transform_concept` to populate it, and then perform post‑processing steps such as setting tables to logged, moving tables between schemas, and cleaning up.  
- **Callees**:  
  * `AgeTransform.pre_transform` – prepares age‑related PL/pgSQL functions.  
  * `_transform_concept_select_sql` – generates the CREATE‑TABLE … AS SELECT statements for each target table, applying `ConceptNameTransform.modify_select`.  
  * `StatementSet.add` – adds each `Statement` to the mutable set.  
  * `StatementSet.parallel_execute` – runs the statements in parallel.  
  * `pg_error` – translates a PostgreSQL error code to a symbolic name for the duplicate‑table check.  
  * `DatabaseError` – exception type raised when an unrecoverable error occurs.  

**Note**:  
- The function assumes that the target schema does not already contain tables with the same names unless `force=True`. Duplicate‑table errors are the only condition suppressed by the `force` flag; all other errors raise `DatabaseError`.  
- The pre‑transform step currently only runs `AgeTransform.pre_transform`. If additional transforms require pre‑execution hooks, they must be called explicitly before `_transform_concept_select_sql`.  
- The generated SQL strings embed the target schema; therefore the database connection’s `search_path` does not need to include the temporary schema.  
- Because `StatementSet.parallel_execute` uses multiprocessing, all objects placed in the set (the `Statement` instances) must be pickleable. The function does not perform any explicit cleanup of the worker processes; that is handled internally by `StatementSet`.  

**Output Example**:  
`_transform_concept` does not return a value; its effect is the creation of transformed tables. A successful run results in statements similar to the following being executed:

```
CREATE UNLOGGED TABLE tmp_schema.condition_occurrence AS
SELECT condition_occurrence.*,
       concept_1.concept_name AS condition_concept_name
FROM condition_occurrence
LEFT OUTER JOIN concept AS concept_1
       ON concept_1.concept_id = condition_occurrence.condition_concept_id;
```

If `force=True` and the table `tmp_schema.condition_occurrence` already exists, the function returns early without raising an exception. Otherwise, any error (e.g., syntax error, missing column) would cause a `DatabaseError` such as:

```
DatabaseError: creating transformed copy of table condition_occurrence: duplicate key value violates unique constraint "condition_occurrence_pkey"
```
## FunctionDef _transform_site(conn_str, model_version, site, target_schema, target_table, force)
**_transform_site**: The function of _transform_site is to execute the site‑specific transformation pipeline, creating transformed tables that contain a constant site‑name column.

**parameters**:  
· **conn_str** (str): libpq connection string used to open a PostgreSQL session.  
· **model_version** (str): PEDSnet data‑model version (e.g., “2.3.0”).  
· **site** (str): Site identifier that will be written into the new *site* column (e.g., “stlouis”).  
· **target_schema** (str): Name of the temporary schema in which the transformed tables will be created.  
· **target_table** (str): Comma‑separated list of table names that should be transformed.  
· **force** (bool, optional, default False): When True, benign errors such as “duplicate table” are ignored; otherwise any error aborts the transformation.

**Code Description**:  
1. **Pre‑transform hook** – The function begins by invoking `SiteNameTransform.pre_transform(conn_str, stock_metadata(model_version))`. This call gives the `SiteNameTransform` class a chance to perform any prerequisite work (e.g., ensuring that the metadata contains the required `site` entry). The base `pre_transform` does nothing, but concrete subclasses may execute SQL statements; any `psycopg2.ProgrammingError` raised here propagates to the caller.  

2. **Generate transformation statements** – An empty `StatementSet` (`stmts`) is created to hold the SQL statements that will materialise the transformed tables. The helper `_transform_site_select_sql(model_version, site, target_schema, target_table)` is called; it returns an iterable of `(sql, msg)` tuples, each representing a `CREATE UNLOGGED TABLE … AS SELECT …` statement that adds the constant site column to one target table. For every tuple, a `Statement` object is instantiated and added to the `StatementSet` via `stmts.add(Statement(sql, msg))`.  

3. **Parallel execution** – The `StatementSet.parallel_execute(conn_str)` method is invoked. This method launches a pool of worker processes (up to 25, or one per statement) that each call the `execute` method of the contained `Statement` objects. The workers run the DDL statements concurrently, collect any result or error information, and place the completed `Statement` objects back into the set.  

4. **Error handling after execution** – After the parallel run finishes, the function iterates over the `StatementSet`. For each `stmt`:
   - If `stmt.err` is truthy, the error is examined.  
   - When `force` is True and the PostgreSQL error condition (obtained via `pg_error(stmt)`) equals `'DUPLICATE_TABLE'`, the function returns immediately, effectively treating the duplicate‑table situation as non‑fatal.  
   - For any other error, a `DatabaseError` is raised with a message that combines the statement’s descriptive `msg` and the underlying error text `stmt.err`. This aborts the transformation and propagates the failure to the caller.  

5. **Return value** – The function does not explicitly return a value; successful completion results in an implicit `None`. All side‑effects (creation of transformed tables) are performed in the database.

**Reference relationships**:  
- **Caller** – `run_site_transformation` (in the same module) prepares the target schema, calls `_transform_site` to build the transformed tables, and then proceeds with logging, setting tables to logged, and moving the tables into the final schema.  
- **Callees** –  
  * `SiteNameTransform.pre_transform` (abstract hook) – prepares metadata for the site transformation.  
  * `_transform_site_select_sql` – builds the set of `CREATE UNLOGGED TABLE … AS SELECT …` statements that implement the transformation.  
  * `stock_metadata` – provides the unmodified model metadata used by the pre‑transform hook.  
  * `StatementSet` and its `add` and `parallel_execute` methods – manage the collection of statements and run them in parallel.  
  * `pg_error` – translates PostgreSQL error codes to symbolic names for the `force`‑duplicate‑table check.  
  * `DatabaseError` – the exception type raised when a non‑ignorable error occurs.  

**Note**:  
- The function assumes that `target_table` is a comma‑separated string; the helper `_transform_site_select_sql` splits it internally.  
- The transformation creates **UNLOGGED** tables for performance; these tables are not crash‑recoverable and should be moved to a permanent schema before the transaction is committed (handled by the caller).  
- When `force` is True, only the specific `'DUPLICATE_TABLE'` condition is suppressed; all other errors still raise `DatabaseError`.  
- Because `StatementSet.parallel_execute` clears the original set and repopulates it with the executed statements, the iteration after execution reflects the final state (including any error information).  
- The function does not return the list of SQL strings; callers that need that information must capture the output of `_transform_site_select_sql` directly.  

**Output Example**:  
```python
result = _transform_site(conn_str, "2.3.0", "stlouis",
                         "tmp_schema", "person,measurement", force=False)
print(result)   # Output: None  (tables are created in the database)
```
## FunctionDef _transform_id(conn_str, model_version, site, target_schema, target_table, id_name, id_type, force)
**_transform_id**: The function of _transform_id is to execute the ID‑mapping transformation for a set of tables, creating transformed copies in a target schema.

**parameters**:  
· conn_str (str): PostgreSQL libpq connection string used for all database operations.  
· model_version (str): PEDSnet model version (e.g., “2.3.0”) that determines the source metadata.  
· site (str): Site label (e.g., “stlouis”) required by some transforms.  
· target_schema (str): Name of the temporary schema where transformed tables will be created.  
· target_table (str): Comma‑separated list of table names to be transformed.  
· id_name (str): Base name of the identifier set (default “dcc”).  
· id_type (str): Data type of the site identifier (“BigInteger” or “String”).  
· force (bool, optional, default False): When True, duplicate‑table errors are ignored.

**Code Description**:  
1. The function begins by invoking the pre‑transform hook of the ID‑mapping transform:  
   `IDMappingTransform.pre_transform(conn_str, stock_metadata(model_version), id_name, id_type)`.  
   This step creates the mapping tables, reserves new DCC IDs, and builds required indexes. Any exception raised here (e.g., `psycopg2.ProgrammingError`) propagates to the caller.  

2. A `StatementSet` instance named `stmts` is created to hold the SQL statements that will create the transformed tables.  

3. The helper `_transform_id_select_sql` is called with the supplied arguments. It returns a set of `(sql, msg)` tuples, each representing a `CREATE UNLOGGED TABLE … AS SELECT …` statement that applies the ID‑mapping logic to one source table. For every tuple returned, the function creates a `Statement` object (`Statement(sql, msg)`) and adds it to `stmts` via `stmts.add`.  

4. All statements in `stmts` are executed in parallel by calling `stmts.parallel_execute(conn_str)`. The target schema name is already embedded in each SQL string, so the workers do not need to modify the search path.  

5. After parallel execution, the function iterates over each `Statement` in `stmts`. If a statement has an error (`stmt.err` is not None), the following logic is applied:  
   – If `force` is True **and** the PostgreSQL error condition obtained by `pg_error(stmt)` equals `'DUPLICATE_TABLE'`, the function returns early, effectively ignoring the duplicate‑table error.  
   – Otherwise, a `DatabaseError` is raised with a message that includes the statement’s human‑readable description (`stmt.msg`) and the underlying error (`stmt.err`). This aborts the transformation and propagates the failure to the caller.  

6. If no statements produced an error (or all duplicate‑table errors were suppressed), the function completes silently, returning `None` (the implicit return value of a Python function without an explicit `return`).

**Reference Relationships**:  
- **Callers**: The only public entry point that invokes `_transform_id` is `run_id_transformation` in the same module. `run_id_transformation` prepares the temporary transformed schema, calls `_transform_id` to populate it, and then proceeds with logging, schema manipulation, and cleanup.  
- **Callees**:  
  * `IDMappingTransform.pre_transform` – prepares the mapping infrastructure before any SELECT statements are generated.  
  * `_transform_id_select_sql` – generates the set of CREATE‑TABLE‑AS‑SELECT statements that incorporate the ID‑mapping logic.  
  * `StatementSet` (methods `add` and `parallel_execute`) – collects the statements and runs them concurrently.  
  * `pg_error` – extracts the PostgreSQL error condition name from a failed `Statement` to decide whether it can be ignored when `force` is True.  
  * `DatabaseError` – the exception type raised for any non‑ignorable statement error.  

**Note**:  
- The function does not return a value; its effect is the creation of transformed tables in `target_schema`.  
- Errors other than duplicate‑table errors are never suppressed, even when `force` is True.  
- The parallel execution uses a worker pool sized automatically by `StatementSet.parallel_execute`; the maximum number of workers is 25.  
- The caller must ensure that the target schema exists before invoking this function (normally created by `run_id_transformation`).  
- The pre‑transform step must complete successfully; otherwise the subsequent SELECT generation will fail because the required mapping tables do not exist.  

**Output Example**:  
Because `_transform_id` returns `None` on success, a typical usage pattern looks like:

```python
_transform_id(conn_str, '2.3.0', 'stlouis',
              'tmp_transformed', 'person,visit_occurrence',
              'dcc', 'BigInteger', force=False)
# No exception raised → transformed tables now exist in schema tmp_transformed
```
## FunctionDef _transform_index(conn_str, model_version, site, target_schema, target_table, force)
**_transform_index**: The function of _transform_index is to execute the index‑only transformation pipeline – it runs any required pre‑transform SQL, generates the CREATE‑TABLE‑AS‑SELECT statements for the requested tables, executes those statements in parallel, and raises an error if any statement fails (unless the error is a duplicate‑table error and the *force* flag is set).

**parameters**:  
· **conn_str** (str): libpq connection string used to open a PostgreSQL session.  
· **model_version** (str): PEDSnet model version (e.g., “2.3.0”).  
· **site** (str): Site identifier (e.g., “stlouis”).  
· **target_schema** (str): Name of the temporary schema in which the transformed tables will be created.  
· **target_table** (str): Comma‑separated list of table names that should be materialised by the transformation.  
· **force** (bool, optional, default False): When True, a duplicate‑table error is ignored and the function returns early.

**Code Description**:  

1. **Pre‑transform hook** – The function calls `AddIndexTransform.pre_transform(conn_str, stock_metadata(model_version))`.  
   * `stock_metadata` builds a fresh SQLAlchemy `MetaData` object for the supplied *model_version*.  
   * `AddIndexTransform.pre_transform` inherits the base implementation from `Transform`, which is a no‑op; the call exists for symmetry with other transforms that may need to create database objects before the main SQL runs.

2. **Statement generation** – An empty `StatementSet` instance (`stmts`) is created.  
   * The helper `_transform_index_select_sql(model_version, site, target_schema, target_table)` is invoked.  
   * That helper returns a set of `(sql, msg)` tuples, each representing a `CREATE UNLOGGED TABLE … AS SELECT …` statement for one of the tables listed in *target_table*.  
   * For each tuple, a `Statement` object is instantiated and added to `stmts` via `stmts.add(Statement(sql, msg))`.

3. **Parallel execution** – `stmts.parallel_execute(conn_str)` runs all collected statements concurrently using the `StatementSet.parallel_execute` method.  
   * The method spawns a pool of worker processes (up to 25) that each call the statement’s `execute` method.  
   * Results, including any error information, are collected back into the same `StatementSet` instance.

4. **Error handling** – After execution, the function iterates over each `Statement` in `stmts`.  
   * If a statement has a non‑null `err` attribute, the error is examined with `pg_error(stmt)`.  
   * If *force* is True **and** the PostgreSQL error condition is `'DUPLICATE_TABLE'`, the function returns immediately, silently ignoring the duplicate‑table situation.  
   * For any other error, a `DatabaseError` is raised with a message that combines the statement’s descriptive `msg` and the underlying error text.

5. **Return value** – The function does not return any explicit value; it either completes silently (returning `None`) or raises an exception on failure.

**Reference relationships**:  

*Callers* – The only public entry point that invokes `_transform_index` is `run_index_transformation` in the same module. `run_index_transformation` prepares the temporary transformed schema, calls `_transform_index` to materialise the tables, and then proceeds with post‑transformation steps such as setting tables to logged, moving tables between schemas, and cleaning up.

*Callees* –  
- `AddIndexTransform.pre_transform` (inherited from `Transform`) – provides a hook for any prerequisite SQL; for the index transform it does nothing.  
- `stock_metadata` – supplies the unmodified model metadata required by the pre‑transform hook.  
- `_transform_index_select_sql` – builds the actual CREATE‑TABLE‑AS‑SELECT statements for each target table.  
- `StatementSet` and its `add` and `parallel_execute` methods – manage the collection of `Statement` objects and run them in parallel.  
- `pg_error` – translates a PostgreSQL error code into a symbolic name used for the *force* duplicate‑table check.  
- `DatabaseError` – the exception type raised when a statement fails and the error is not suppressed.

**Note**:  
- The function assumes that the target schema already exists; `run_index_transformation` creates it beforehand.  
- Because the CREATE statements use the *UNLOGGED* option, the resulting tables are not WAL‑logged, which improves performance but means they are not crash‑recoverable.  
- The *force* flag only suppresses duplicate‑table errors; any other error (syntax, permission, etc.) will still cause a `DatabaseError`.  
- The order of execution is nondeterministic because the statements are stored in a set; however, each statement is independent, so ordering does not affect correctness.  
- If `AddIndexTransform` were to override `pre_transform` in the future, the call at the start of `_transform_index` will automatically execute any required preparatory SQL.

**Output Example**:  

```python
result = _transform_index(conn_str,
                          model_version='2.3.0',
                          site='stlouis',
                          target_schema='tmp_transformed',
                          target_table='person,measurement',
                          force=False)
print(result)   # Output: None  (function completes without raising)
```
## FunctionDef _move_tables_statements(model_version, from_schema, to_schema)
**_move_tables_statements**: The function of _move_tables_statements is to generate a StatementList containing SQL ALTER TABLE commands that move all core PEDSnet tables (excluding vocabulary tables) from one schema to another.

**parameters**  
· model_version (str): the PEDSnet data‑model version whose table list is used.  
· from_schema (str): the source PostgreSQL schema that currently holds the tables.  
· to_schema (str): the destination PostgreSQL schema to which the tables will be moved.

**Code Description**  
1. **Create a container for statements** – An empty StatementList object (`stmts`) is instantiated. StatementList is a mutable‑sequence that stores Statement objects and later can execute them in order, optionally inside a transaction.  

2. **Obtain the canonical metadata** – The helper stock_metadata(model_version) is called. This function contacts the PEDSnet data‑model service and returns a SQLAlchemy MetaData object that contains a dictionary metadata.tables mapping every table name (including vocabulary tables) to a Table definition for the requested model version.  

3. **Prepare SQL templates** – Two format strings are defined:  
   * `move_tpl = 'ALTER TABLE {from_sch}.{tbl} SET SCHEMA {to_sch}'` – the actual DDL that moves a table.  
   * `msg_tpl = 'moving {tbl} from {from_sch} to {to_sch}'` – a human‑readable log message attached to each Statement.  

4. **Iterate over core tables** – The function builds a set of table names to process by taking all keys from metadata.tables and subtracting the constant VOCAB_TABLES (which lists the vocabulary tables that must remain untouched). For each remaining table_name:  
   * A dictionary tpl_vals is populated with the current from_schema, to_schema, and the table name.  
   * A Statement object is created with the formatted SQL (`move_tpl.format(**tpl_vals)`) and the formatted message (`msg_tpl.format(**tpl_vals)`).  
   * The statement is appended to stmts.  

5. **Return the assembled list** – After all core tables have been processed, the populated StatementList (`stmts`) is returned to the caller.

**Reference Relationships**  
* **Callers** – The function is used by two transformation utilities:  
  * `run_transformation` (builds the list of statements that first move the original tables into a backup schema and then move the newly transformed tables into the production schema).  
  * `undo_transformation` (builds the list of statements that move tables from the backup schema back to the original schema when a transformation is being reverted.  
  In both cases the returned StatementList is extended into a larger list of DDL statements and finally executed with StatementList.serial_execute(conn_str, transaction=True).  

* **Callees** – Internally the function relies on:  
  * `stock_metadata` to obtain the authoritative list of tables for the supplied model version.  
  * `StatementList` to collect the generated Statement objects.  
  * `VOCAB_TABLES` (a module‑level constant that enumerates the vocabulary tables that must not be moved).  

**Note**  
* The function does **not** execute any SQL; it only creates the statements. Execution is delegated to the caller’s StatementList.serial_execute call, which can run the whole move inside a single transaction.  
* Vocabulary tables are deliberately excluded because they are shared across sites and should remain in the vocabulary schema. Changing their schema would break other transformations.  
* The generated Statement objects contain both the SQL command and a descriptive message; these messages are used by the calling code to log detailed error information if a statement fails.  
* If the supplied model_version does not exist or the metadata service is unreachable, stock_metadata will raise an exception before any statements are created. Callers should ensure the model version is valid.  
* The function assumes that both from_schema and to_schema already exist; it does not create or drop schemas.

**Output Example**  
```python
>>> stmts = _move_tables_statements('2.2.0', 'stlouis', 'stlouis_backup')
>>> type(stmts)
pedsnetdcc.db.StatementList
>>> for s in stmts[:3]:   # show first three statements
...     print(s.sql, '|', s.msg)
ALTER TABLE stlouis.person SET SCHEMA stlouis_backup | moving person from stlouis to stlouis_backup
ALTER TABLE stlouis.observation SET SCHEMA stlouis_backup | moving observation from stlouis to stlouis_backup
ALTER TABLE stlouis.measurement SET SCHEMA stlouis_backup | moving measurement from stlouis to stlouis_backup
```
## FunctionDef _move_target_tables_statements(model_version, from_schema, to_schema, target_table)
**_move_target_tables_statements**: The function of _move_target_tables_statements is to generate a StatementList containing SQL statements that move selected PEDSnet tables from one schema to another, while skipping vocabulary tables.

**parameters**  
· model_version: string identifying the PEDSnet model version (e.g., "2.2.0").  
· from_schema: string name of the source PostgreSQL schema that currently holds the tables.  
· to_schema: string name of the destination PostgreSQL schema to which the tables will be moved.  
· target_table: string containing one or more comma‑separated table names that are subject to the move operation.

**Code Description**  
1. The incoming *target_table* argument is split on commas, producing a Python list of individual table names that the caller wishes to move.  
2. An empty **StatementList** instance (`stmts`) is created. This object will collect the generated ALTER TABLE statements in the order they are added.  
3. `stock_metadata(model_version)` is called to obtain a fresh SQLAlchemy **MetaData** object that describes the complete, untransformed PEDSnet data model for the supplied version. The metadata’s `tables` dictionary maps each table name to a SQLAlchemy Table object.  
4. Two template strings are defined:  
   * `move_tpl = 'ALTER TABLE {from_sch}.{tbl} SET SCHEMA {to_sch}'` – the actual DDL command that moves a table between schemas.  
   * `msg_tpl = 'moving {tbl} from {from_sch} to {to_sch}'` – a human‑readable log message that will be attached to the corresponding Statement object.  
5. The function iterates over the set difference `set(metadata.tables.keys()) - set(VOCAB_TABLES)`. This yields every table defined in the model **except** the tables listed in the constant `VOCAB_TABLES` (the vocabulary tables that must never be moved).  
6. For each *table_name* in that filtered set, the code checks whether the name appears in the *target_table* list supplied by the caller. If it does, a dictionary `tpl_vals` is built containing the concrete values for `{from_sch}`, `{to_sch}`, and `{tbl}`.  
7. A **Statement** object (created implicitly by the `StatementList.append` call) is instantiated with two arguments: the formatted ALTER TABLE command (`move_tpl.format(**tpl_vals)`) and the formatted log message (`msg_tpl.format(**tpl_vals)`). The statement is appended to `stmts`.  
8. After processing all relevant tables, the populated `StatementList` is returned to the caller.

**Reference Relationships**  
* **Callers** – The function is invoked by several transformation runner helpers (`run_target_transformation`, `run_age_transformation`, `run_concept_transformation`, `run_concept_age_transformation`, `run_site_transformation`, `run_id_transformation`, `run_index_transformation`). In each case it is used twice: first to generate statements that move the original tables into a backup schema, and second to move the newly transformed tables from a temporary schema into the original schema. The returned `StatementList` is then combined with other schema‑creation and drop statements and executed transactionally via `StatementList.serial_execute`.  
* **Callees** – It calls `stock_metadata` to retrieve the canonical table list and relies on the constant `VOCAB_TABLES` (defined elsewhere in the package) to filter out vocabulary tables. The generated statements are instances compatible with the `Statement` API expected by `StatementList`.  

**Note**  
* The function only creates statements for tables that are both (a) present in the stock model and (b) explicitly listed in the *target_table* argument. Tables omitted from *target_table* are ignored, even if they exist in the schema.  
* Vocabulary tables are never moved; they are excluded by the set subtraction with `VOCAB_TABLES`. Attempting to move a vocabulary table will result in no statement being generated for it.  
* The returned `StatementList` does not execute the statements; execution is the responsibility of the caller, typically inside a transaction to guarantee atomic schema changes.  
* Because the function uses `target_table.split(",")`, whitespace around commas is not stripped. Callers should provide a clean comma‑separated list (e.g., `"person,measurement"`).  

**Output Example**  
```python
stmts = _move_target_tables_statements(
    model_version='2.2.0',
    from_schema='raw',
    to_schema='raw_backup',
    target_table='person,measurement'
)

# The resulting StatementList contains two Statement objects:
# 1. ALTER TABLE raw.person SET SCHEMA raw_backup   (msg: moving person from raw to raw_backup)
# 2. ALTER TABLE raw.measurement SET SCHEMA raw_backup   (msg: moving measurement from raw to raw_backup)
```
## FunctionDef _drop_tables_statements(model_version, schema, if_exists)
**_drop_tables_statements**: The function of _drop_tables_statements is to generate a **StatementList** containing SQL DROP TABLE commands for all non‑vocabulary PEDSnet tables in a given schema.

**parameters**  
· **model_version** (str): the PEDSnet data‑model version (e.g., "2.2.0") used to retrieve the canonical table list.  
· **schema** (str): the target PostgreSQL schema that holds the tables to be dropped.  
· **if_exists** (bool, default False): when True, the generated DROP statements include the `IF EXISTS` clause to suppress errors for missing tables.

**Code Description**  
1. **Create container** – Instantiates an empty `StatementList` (`stmts`). This object will collect the individual `Statement` objects in the order they are added.  
2. **Obtain model metadata** – Calls `stock_metadata(model_version)` which returns a SQLAlchemy `MetaData` object populated with the stock PEDSnet schema for the supplied version. The `metadata.tables` dictionary maps each table name to its `Table` definition.  
3. **Prepare templates** –  
   * `move_tpl = 'DROP TABLE {if_exists} {sch}.{tbl}'` – the SQL template for each DROP command.  
   * `msg_tpl = 'dropping {tbl} in {sch}'` – a human‑readable log message attached to each `Statement`.  
4. **Determine IF EXISTS clause** – Sets `if_exists_clause` to the literal string `'IF EXISTS'` when `if_exists` is True; otherwise it is an empty string.  
5. **Iterate over tables** –  
   * Constructs the set of tables to drop: `set(metadata.tables.keys()) - set(VOCAB_TABLES)`. `VOCAB_TABLES` is a project‑wide constant that lists vocabulary tables (e.g., concept, concept_relationship) which must be preserved.  
   * For each `table_name` in that set, builds a dictionary `tpl_vals` containing the schema name, table name, and the conditional clause.  
   * Formats the SQL string with `move_tpl.format(**tpl_vals)` and the log message with `msg_tpl.format(**tpl_vals)`.  
   * Wraps both strings in a `Statement` object and appends it to `stmts`.  
6. **Return** – The fully populated `StatementList` is returned to the caller.

**Reference Relationships**  
* **Callees** –  
  * `stock_metadata` (pedsnetdcc/utils.py) – supplies the authoritative list of tables for the requested model version.  
  * `StatementList` (pedsnetdcc/db.py) – the container that holds the generated `Statement` objects and later provides the `serial_execute` method.  
* **Callers** –  
  * `undo_transformation` (pedsnetdcc/transform_runner.py) – uses `_drop_tables_statements` to create the DROP statements that are later executed inside a transaction when rolling back a schema transformation.  
  * Any future utility that needs to purge a transformed schema can also invoke this helper, provided foreign‑key constraints have already been removed (the function does not attempt to drop them).  

**Note**  
* Vocabulary tables listed in `VOCAB_TABLES` are deliberately excluded; dropping them would break the shared terminology layer.  
* The function assumes that all foreign‑key constraints referencing the target tables have been removed beforehand; otherwise PostgreSQL will raise an error during execution.  
* The order of statements follows the iteration order of a Python `set`, which is not guaranteed to be alphabetical. If a deterministic drop order is required, callers should sort the table names before invoking the helper.  
* When `if_exists=False` and a table is absent, the generated DROP statement will cause the transaction to abort unless the caller catches the error. Setting `if_exists=True` makes the operation idempotent.  
* The returned `StatementList` contains `Statement` objects whose `sql` attribute holds the final DROP command and whose `msg` attribute holds the descriptive log message. These statements are typically executed with `StatementList.serial_execute(conn_str, transaction=True)`.

**Output Example**  
```python
>>> stmts = _drop_tables_statements('2.2.0', 'clinical', if_exists=True)
>>> for stmt in stmts:
...     print(stmt.sql, '|', stmt.msg)
DROP TABLE IF EXISTS clinical.measurement | dropping measurement in clinical
DROP TABLE IF EXISTS clinical.observation | dropping observation in clinical
DROP TABLE IF EXISTS clinical.condition_occurrence | dropping condition_occurrence in clinical
# ... one line per non‑vocabulary table
```
## FunctionDef _drop_target_tables_statements(model_version, schema, target_table, if_exists)
**_drop_target_tables_statements**: The function of _drop_target_tables_statements is to generate a StatementList containing SQL DROP TABLE commands for all non‑vocabulary tables that belong to a given transformation target.

**parameters**  
· **model_version** (str): the PEDSnet model version (e.g., "2.2.0") used to retrieve the stock metadata for the schema.  
· **schema** (str): the name of the database schema in which the tables reside.  
· **target_table** (str): a comma‑separated list of table names that are part of the current transformation; only these tables will be considered for dropping.  
· **if_exists** (bool, optional, default False): when True the generated DROP statements include the IF EXISTS clause to suppress errors if a table is missing.

**Code Description**  
1. **Parse target list** – The incoming target_table string is split on commas, producing a Python list of individual table names that the caller wishes to drop.  
2. **Create container** – An empty StatementList instance (`stmts`) is instantiated. This object will hold the ordered Statement objects that represent each DROP command.  
3. **Obtain model metadata** – The helper stock_metadata(model_version) is called. It returns a SQLAlchemy MetaData object that contains a dictionary metadata.tables mapping every table name defined in the specified PEDSnet model version to its Table object. This metadata represents the *stock* (untransformed) schema.  
4. **Prepare templates** – Two format strings are defined:  
   * `move_tpl = 'DROP TABLE {if_exists} {sch}.{tbl}'` – the actual SQL command.  
   * `msg_tpl = 'dropping {tbl} in {sch}'` – a human‑readable log/message that will be attached to the Statement object.  
5. **Determine IF EXISTS clause** – If the caller set if_exists to True, the variable if_exists_clause receives the literal string 'IF EXISTS'; otherwise it is an empty string.  
6. **Iterate over model tables** – The function computes the set difference between all tables in the metadata (`metadata.tables.keys()`) and the constant VOCAB_TABLES (which enumerates vocabulary tables that must never be dropped). For each remaining table_name:**  
   * It checks whether table_name appears in the parsed target_table list.  
   * If the check passes, a dictionary tpl_vals is built with keys sch, tbl, if_exists containing the schema name, the table name, and the optional IF EXISTS clause.  
   * A new Statement object is created by formatting move_tpl with tpl_vals (the SQL) and msg_tpl with tpl_vals (the log message).  
   * The statement is appended to the StatementList (`stmts.append(...)`).  
7. **Return** – After processing all applicable tables, the populated StatementList is returned to the caller.

**Relationship with callees**  
* **stock_metadata** – Provides the authoritative list of tables for the requested model version. The function relies on this metadata to know which tables exist in the base schema and to exclude vocabulary tables.  
* **StatementList** – Serves as the container for the generated Statement objects. The caller will typically invoke stmts.serial_execute(conn_str, transaction=True) to run the DROP commands atomically.  
* **VOCAB_TABLES** – A project‑wide constant (imported elsewhere) that enumerates tables that are part of the controlled vocabulary; these are deliberately omitted from the drop list to preserve reference data.  

**Note**  
* The function does **not** perform any safety checks beyond excluding vocabulary tables; it assumes that foreign‑key constraints have already been removed. Attempting to drop a table that is still referenced will cause a database error.  
* The target_table parameter must be a comma‑separated string without surrounding whitespace; otherwise the membership test may fail.  
* When if_exists is False and a table does not exist, the generated DROP statement will raise an error and abort execution unless the caller catches it.  
* The order of statements follows the order of iteration over the set difference, which is nondeterministic in older Python versions; however, because all statements are independent DROP commands, execution order does not affect the outcome.  

**Output Example**  

```python
# Example call
stmts = _drop_target_tables_statements(
    model_version='2.2.0',
    schema='public',
    target_table='measurement,observation,visit_occurrence',
    if_exists=True
)

# The returned StatementList might contain three Statement objects:
# 1. SQL: "DROP TABLE IF EXISTS public.measurement"
#    Message: "dropping measurement in public"
# 2. SQL: "DROP TABLE IF EXISTS public.observation"
#    Message: "dropping observation in public"
# 3. SQL: "DROP TABLE IF EXISTS public.visit_occurrence"
#    Message: "dropping visit_occurrence in public"
```
## FunctionDef _adjust_specialty_entity_ids(conn_str, schema)
**_adjust_specialty_entity_ids**: The function of _adjust_specialty_entity_ids is to update the `entity_id` column in the `specialty` table so that it correctly references the corresponding `care_site` or `provider` entity based on the `site_id` and `domain_id` values.  

**parameters**:  
· **conn_str** (str): PostgreSQL libpq‑style connection string used to open a new database connection.  
· **schema** (str): Name of the schema that contains the `specialty`, `care_site`, and `provider` tables to be updated.  

**Code Description**:  
1. **SQL statement preparation** – A multi‑line SQL string (`update_specialty_sql`) is defined. The statement performs an `UPDATE` on `{schema}.specialty` (`s`) and sets `s.entity_id` to the matching `entity_id` derived from a sub‑query (`et`).  
   * The sub‑query builds a temporary set that unions two SELECTs:  
     – From `{schema}.care_site`, it selects `site_id`, `care_site_id` as `entity_id`, and the literal `'CARE_SITE'` as `entity_type`.  
     – From `{schema}.provider`, it selects `site_id`, `provider_id` as `entity_id`, and the literal `'PROVIDER'` as `entity_type`.  
   * The `WHERE` clause matches rows where `s.entity_id::text = et.site_id::text` **and** `s.domain_id = et.entity_type`. This ensures that a specialty row is linked to the correct entity type (care site or provider) based on its domain.  

2. **Log message preparation** – A human‑readable message template (`update_specialty_msg`) is defined: `"updating {0}.specialty entity_id"`.  

3. **Statement object creation** – An instance of `Statement` is created with the formatted SQL (`update_specialty_sql.format(schema)`) and the formatted log message (`update_specialty_msg.format(schema)`). This object encapsulates the SQL, a descriptive message, and later holds execution results or errors.  

4. **Execution of the statement** – The `execute` method of the `Statement` instance is called with `conn_str`. `Statement.execute` opens a new connection, runs the SQL in autocommit mode, captures any database‑level error in `stmt.err`, and closes the connection.  

5. **Error checking** – Immediately after execution, `check_stmt_err` is invoked with the statement object and the caller name `'update specialty entity_id'`. `check_stmt_err` inspects `stmt.err`; if an error is present it logs a detailed error message and raises a `DatabaseError`. This guarantees that any failure aborts the transformation process.  

6. **Successful completion** – If no exception is raised, the function returns `True` to signal that the update succeeded.  

**Reference Relationships**:  
* **Caller** – `_adjust_specialty_entity_ids` is invoked from `run_transformation` (in `pedsnetdcc/transform_runner.py`). It is called after primary keys have been added to the transformed tables and before NOT NULL constraints, indexes, and foreign keys are applied. Its purpose within the transformation pipeline is to ensure that the `specialty` table’s foreign‑key reference (`entity_id`) points to the correct entity after the schema has been recreated.  
* **Callees** –  
  – `Statement` (from `pedsnetdcc/db.py`): used to encapsulate the SQL and message.  
  – `Statement.execute` (also from `pedsnetdcc/db.py`): runs the SQL against the database.  
  – `check_stmt_err` (from `pedsnetdcc/utils.py`): validates the execution result and raises an exception on error.  

**Note**:  
- The function assumes that the target schema contains the three tables (`specialty`, `care_site`, `provider`) with the columns `site_id`, `care_site_id`, `provider_id`, `entity_id`, and `domain_id`. If any of these columns are missing or have different data types, the UPDATE will fail.  
- The comparison casts both `entity_id` and `site_id` to text to avoid type‑mismatch errors; this is safe because the values are expected to be comparable identifiers.  
- The function runs in autocommit mode; therefore the update is permanent as soon as the statement executes. No explicit transaction handling is required here.  
- Any database error (e.g., permission issues, missing tables, or constraint violations) will be caught by `check_stmt_err`, logged, and propagated as a `DatabaseError`. Callers must be prepared to handle this exception.  

**Output Example**:  
```python
result = _adjust_specialty_entity_ids(conn_str, 'my_schema')
print(result)   # => True  (indicates the UPDATE completed without error)
```
## FunctionDef run_transformation(conn_str, model_version, site, search_path, id_name, id_type, logged, post_only, pool_limit, nopk, nonull, noidx, nodrop, nofk, limit, owner, force)
**run_transformation**: The function of run_transformation is to execute the complete PEDSnet data‑transformation pipeline, moving existing core tables to a backup schema, creating transformed tables in a temporary schema, applying post‑transform steps (logged status, primary keys, NOT NULL constraints, indexes, foreign keys), and finally swapping the transformed tables into the original schema.

**parameters**:  
· conn_str (str): PostgreSQL libpq connection string that identifies the target database.  
· model_version (str): PEDSnet data‑model version (e.g., “2.3.0”).  
· site (str): Site label used for site‑specific transformations (e.g., “stlouis”).  
· search_path (str): PostgreSQL schema search_path; the first schema in the list is treated as the primary schema.  
· id_name (str): Name of the identifier column to be created (e.g., “dcc” or “onco”).  
· id_type (str): PostgreSQL data type of the identifier column (e.g., “BigInteger” or “String”).  
· logged (bool): If True, transformed tables are created as LOGGED; otherwise they are created as UNLOGGED.  
· post_only (bool): If True, only the post‑transform steps are performed (schema creation and table transformation are skipped).  
· pool_limit (bool): If True, the parallel worker pool used for table creation is limited to a single process; otherwise up to 25 workers may be used.  
· nopk (bool, optional, default False): When True, primary‑key creation is skipped.  
· nonull (bool, optional, default False): When True, NOT NULL constraints are not added.  
· noidx (bool, optional, default False): When True, index creation is skipped.  
· nodrop (bool, optional, default False): When True, dropping of unneeded indexes is skipped.  
· nofk (bool, optional, default False): When True, foreign‑key creation is skipped.  
· limit (bool, optional, default False): When True, schema permissions are granted only to a specific owner role.  
· owner (str, optional, default 'loading_user'): Database role that receives limited permissions when *limit* is True.  
· force (bool, optional, default False): When True, benign errors such as duplicate objects are ignored during transformation steps.

**Code Description**:  
1. **Logging preparation** – A dictionary containing *model_version*, *search_path*, and *force* is merged with connection‑info extracted by `get_conn_info_dict(conn_str)` using `combine_dicts`. This *log_dict* is used for all subsequent structured log messages.  
2. **Task start logging** – The function records the start time (`start_time = time.time()`) and logs an INFO entry “started running transformation”.  
3. **Primary schema resolution** – `primary_schema(search_path)` extracts the first schema name from the supplied *search_path*; this is the schema that holds the current core tables.  
4. **Create temporary transformed schema** – If *post_only* is False, a new schema named `<primary>_transformed` is created with `create_schema(conn_str, tmp_schema, force)`.  
5. **Run the core transformation** – Still under the *post_only* guard, `_transform` is called with the original connection string, model version, site, the temporary schema, identifier name/type, *logged*, *pool_limit*, and *force*. `_transform` creates transformed tables in the temporary schema by executing a set of `CREATE TABLE AS SELECT` statements in parallel. Errors for duplicate tables are suppressed when *force* is True.  
6. **Build a new connection string for post‑transform work** – The search_path is extended to include the temporary schema and the vocabulary schema: `new_search_path = ','.join((tmp_schema, schema, 'vocabulary'))`. `conn_str_with_search_path` injects this path into a copy of the original connection string, producing *new_conn_str*.  
7. **Set tables to LOGGED** – If *logged* is False, `set_logged(new_conn_str, model_version)` changes all transformed tables from UNLOGGED to LOGGED.  
8. **Add primary keys** – Unless *nopk* is True, `add_primary_keys(new_conn_str, model_version, force)` creates primary‑key constraints on the transformed tables.  
9. **Adjust specialty entity IDs** – `_adjust_specialty_entity_ids(new_conn_str, tmp_schema)` updates the `entity_id` column in the `specialty` table so that it correctly references `care_site` or `provider` rows after the schema move.  
10. **Add NOT NULL constraints** – If *nonull* is False, `set_not_nulls(new_conn_str, model_version)` adds NOT NULL constraints defined by the model metadata.  
11. **Create indexes** – If *noidx* is False, `add_indexes(new_conn_str, model_version, force)` creates all required indexes on the transformed tables.  
12. **Drop unneeded indexes** – If *nodrop* is False, `drop_unneeded_indexes(new_conn_str, model_version, force)` removes indexes that are no longer required after transformation.  
13. **Add foreign keys** – If *nofk* is False, `add_foreign_keys(new_conn_str, model_version, force)` creates foreign‑key constraints for the transformed tables.  
14. **Prepare schema‑swap statements** – The backup schema name is derived as `<primary>_backup`. A `StatementList` named *stmts* is built containing:  
    - `DROP SCHEMA <backup> IF EXISTS CASCADE` (to remove any previous backup).  
    - `CREATE SCHEMA <backup>` (to hold the old core tables).  
    - Statements generated by `_move_tables_statements(model_version, schema, backup_schema)` to move current core tables into the backup schema.  
    - Statements generated by `_move_tables_statements(model_version, tmp_schema, schema)` to move the newly transformed tables from the temporary schema into the original schema.  
    - `DROP SCHEMA <tmp_schema> CASCADE` to remove the now‑empty temporary schema.  
15. **Execute schema‑swap transactionally** – `stmts.serial_execute(conn_str, transaction=True)` runs all statements inside a single PostgreSQL transaction. If any statement fails, the transaction is rolled back. After execution, each statement is inspected; any error that is not the “IN_FAILED_SQL_TRANSACTION” condition is logged and re‑raised as a `DatabaseError`.  
16. **Re‑grant permissions** –  
    - If *limit* is True, `grant_schema_permissions_limited(new_conn_str, False, owner, id_name, (site,))` grants only the minimal permissions required for the specified *owner* on the transformed schema, and `grant_vocabulary_only_permissions_limited(new_conn_str, owner)` does the same for the vocabulary schema.  
    - If *limit* is False, `grant_schema_permissions(new_conn_str)` and `grant_vocabulary_permissions(new_conn_str)` grant the full set of standard PEDSnet permissions.  
17. **Task completion logging** – An INFO entry records “finished running transformation” together with the elapsed time (`secs_since(start_time)`).  
18. **Return value** – The function returns `True` to indicate successful completion; any exception aborts the process before this point.

**Reference relationships**:  
*Callers* – `run_transformation` is invoked by the CLI entry points `post_load` and `transform` in `pedsnetdcc/main.py`. It is also used directly in the test suite `TransformRunnerTest.test_without_data`.  
*Callees* – The function relies on a large set of utilities:  
- `combine_dicts`, `get_conn_info_dict`, `secs_since` for logging.  
- `primary_schema` to determine the target schema.  
- `create_schema` to provision the temporary schema.  
- `_transform` to generate transformed tables.  
- `conn_str_with_search_path` to build a connection string that includes the temporary and vocabulary schemas.  
- `set_logged`, `add_primary_keys`, `_adjust_specialty_entity_ids`, `set_not_nulls`, `add_indexes`, `drop_unneeded_indexes`, `add_foreign_keys` for post‑transform DDL.  
- `_move_tables_statements` to generate ALTER TABLE … SET SCHEMA commands.  
- `StatementList`, `Statement`, `StatementSet` for ordered execution and transactional control.  
- `pg_error` and `DatabaseError` for error classification and propagation.  
- Permission‑granting functions `grant_schema_permissions`, `grant_schema_permissions_limited`, `grant_vocabulary_permissions`, `grant_vocabulary_only_permissions_limited`.  

**Note**:  
- The function assumes that the supplied *search_path* contains at least one schema; otherwise `primary_schema` raises `ValueError`.  
- When *post_only* is True, the transformation step that creates the temporary schema and populates it is skipped; the function only performs the post‑transform DDL and schema swap on an already‑existing transformed schema.  
- The *force* flag only suppresses duplicate‑object errors for primary‑key, index, foreign‑key, and table creation; any other error will abort the run.  
- All permission‑granting calls are executed without a transaction because PostgreSQL does not allow GRANT statements inside a transaction block.  
- The schema‑swap is performed inside a single transaction to guarantee atomicity; if any move fails, the database is rolled back to its original state.  
- The function does not return any data other than a boolean; callers must rely on exceptions for error handling.

**Output Example**:  
```python
result = run_transformation(
    conn_str="host=pg host=5432 dbname=pedsnet user=loader password=secret",
    model_version="2.3.0",
    site="stlouis",
    search_path="stlouis",
    id_name="dcc",
    id_type="BigInteger",
    logged=False,
    post_only=False,
    pool_limit=False,
    nopk=False,
    nonull=False,
    noidx=False,
    nodrop=False,
    nofk=False,
    limit=False,
    owner="loading_user",
    force=False
)
print(result)   # -> True
```
## FunctionDef run_target_transformation(conn_str, model_version, site, search_path, target_table, id_name, id_type, force)
**run_target_transformation**: The function of run_target_transformation is to execute a complete transformation workflow for a specified target table, creating a temporary transformed schema, populating it, applying logging and primary‑key constraints, and finally swapping the transformed tables into the original schema while backing up the old tables.

**parameters**:  
· **conn_str** (str): PostgreSQL libpq connection string that identifies the database to operate on.  
· **model_version** (str): PEDSnet data‑model version (e.g., “2.3.0”).  
· **site** (str): Site identifier used by the transformation logic (e.g., “stlouis”).  
· **search_path** (str): The current PostgreSQL schema search_path; the first entry is treated as the primary schema.  
· **target_table** (str): Comma‑separated list of tables that should be transformed.  
· **id_name** (str): Name of the identifier column to be added or used by the transformation (e.g., “dcc”).  
· **id_type** (str): SQL type of the identifier column (e.g., “BigInteger” or “String”).  
· **force** (bool, optional, default False): When True, benign errors such as duplicate‑table or duplicate‑schema are ignored; otherwise any error aborts the process.

**Code Description**:  

1. **Logging preparation** – The function builds a dictionary (`log_dict`) that merges the supplied `model_version`, `search_path`, and `force` flag with connection‑metadata extracted by `get_conn_info_dict(conn_str)`. This dictionary is used for all subsequent structured log entries.

2. **Task start logging** – A log entry with the message “started running transformation” is emitted, and the current time is stored in `start_time` for elapsed‑time calculations.

3. **Primary schema extraction** – `primary_schema(search_path)` is called to obtain the first schema name from the supplied `search_path`. This schema is the one that currently holds the target tables.

4. **Create temporary transformed schema** – A new schema name is constructed by appending “_transformed” to the primary schema (`tmp_schema`). `create_schema(conn_str, tmp_schema, force)` creates this schema, optionally suppressing “duplicate schema” errors when `force=True`.

5. **Run the actual transformation** – `_transform_target` is invoked with the temporary schema, site, identifier information, and the list of target tables. This helper runs any pre‑transform hooks, generates `CREATE UNLOGGED TABLE … AS SELECT …` statements for each target table, and executes them in parallel. Errors other than duplicate‑table (when `force=True`) raise `DatabaseError`.

6. **Log transformation completion** – After `_transform_target` returns, a log entry records the finished state and the elapsed seconds (formatted by `secs_since`).

7. **Adjust connection string for post‑transform operations** – A new `search_path` is built that places the temporary schema first, followed by the original schema and the `vocabulary` schema. `conn_str_with_search_path` creates a new connection string (`new_conn_str`) that includes this search path.

8. **Set tables to LOGGED** – `set_logged(new_conn_str, model_version, False, target_table.split(","))` changes the newly created unlogged tables to the default logged state.

9. **Add primary‑key constraints** – `add_primary_keys(new_conn_str, model_version, True)` adds primary‑key constraints to all non‑vocabulary tables in the temporary schema, forcing duplicate‑key errors to be ignored.

10. **Prepare schema‑swap statements** –  
    * `backup_schema` is defined as `<primary>_backup`.  
    * A `StatementList` (`stmts`) is created.  
    * Statements are appended to drop any existing backup schema (`drop_schema_statement(..., if_exists=True, cascade=True)`), create a fresh backup schema, move the original tables into the backup schema, move the transformed tables from the temporary schema into the original schema, and finally drop the temporary schema. The move operations are generated by `_move_target_tables_statements` for both directions (original→backup and temporary→original).

11. **Execute schema‑swap transactionally** – `stmts.serial_execute(conn_str, transaction=True)` runs all statements inside a single PostgreSQL transaction. If any statement fails, the transaction is rolled back.

12. **Error handling for the transaction** – After execution, each statement in `stmts` is inspected. If a statement has an error (`stmt.err`) that is not the “IN_FAILED_SQL_TRANSACTION” condition, an error log entry is written and a `DatabaseError` is raised with a message that includes the offending SQL and error text. The loop ensures the first real error is reported.

13. **Final logging** – Upon successful completion of the schema swap, a log entry records the finished state and total elapsed time.

14. **Return value** – The function returns `True` to indicate that no exception was raised during the entire workflow.

**Reference relationships**  

*Callers* – The function is invoked by the CLI entry point `run_target_transform` in `pedsnetdcc/main.py`. That command‑line wrapper parses user options, builds a connection string, and calls `run_target_transformation`. The wrapper expects a boolean return (`True` on success) and exits with a non‑zero status if an exception propagates.

*Callees* –  
- `combine_dicts`, `get_conn_info_dict` – used for structured logging.  
- `primary_schema` – extracts the primary schema from `search_path`.  
- `create_schema` – creates the temporary transformed schema.  
- `_transform_target` – performs the core table‑creation transformation.  
- `secs_since` – formats elapsed time for log messages.  
- `conn_str_with_search_path` – builds a connection string that includes the temporary schema in the search path.  
- `set_logged` – converts unlogged tables to logged.  
- `add_primary_keys` – adds primary‑key constraints to the transformed tables.  
- `_move_target_tables_statements` – generates `ALTER TABLE … SET SCHEMA` statements for moving tables.  
- `drop_schema_statement`, `create_schema_statement` – produce DDL statements for dropping/creating schemas.  
- `StatementList.serial_execute` – executes the schema‑swap statements atomically.  
- `pg_error` – translates PostgreSQL error codes for selective error handling.  
- `DatabaseError` – raised for any unrecoverable database failure.

**Note**:  
- The function assumes that `search_path` contains at least one schema; otherwise `primary_schema` raises `ValueError`.  
- All DDL statements that modify schemas are executed inside a single transaction (`serial_execute` with `transaction=True`). If any statement fails, the entire transaction is rolled back, and the original schema remains unchanged.  
- The `force` flag only suppresses duplicate‑schema, duplicate‑table, and duplicate‑primary‑key errors where the helper functions explicitly check `pg_error`. Other errors (permission issues, syntax errors, missing tables) will abort the process.  
- `target_table.split(",")` does not strip whitespace; callers should provide a clean comma‑separated list.  
- After the schema swap, the temporary schema is dropped, and the original schema name now points to the transformed tables. The previous version of those tables resides in `<primary>_backup`.  
- Logging is performed with the project’s structured logger; each log entry merges the base `log_dict` with a message‑specific dictionary.

**Output Example**:  

```python
>>> success = run_target_transformation(
...     conn_str="host=pg host port=5432 dbname=demo user=loader",
...     model_version="2.3.0",
...     site="stlouis",
...     search_path="dcc_pedsnet,public,vocabulary",
...     target_table="person,measurement",
...     id_name="dcc",
...     id_type="BigInteger",
...     force=False)
>>> print(success)
True
```
## FunctionDef run_age_transformation(conn_str, model_version, site, search_path, target_table, force)
**run_age_transformation**: The function of run_age_transformation is to orchestrate the complete age‑in‑months transformation for a specified set of tables, handling schema creation, execution of the transformation, logging, and final schema swapping.

**parameters**:  
· **conn_str** (str): PostgreSQL libpq connection string used for all database operations.  
· **model_version** (str): PEDSnet model version (e.g., “2.3.0”) that determines the source metadata.  
· **site** (str): Site identifier that is stored in the transformation metadata.  
· **search_path** (str): PostgreSQL schema search_path string (comma‑separated list). The first schema in this list is treated as the primary schema.  
· **target_table** (str): Comma‑separated list of table names that should be transformed.  
· **force** (bool, optional, default False): When True, benign errors such as duplicate‑table creation are ignored.

**Code Description**:  

1. **Logging preparation** – A dictionary `log_dict` is built by merging the connection information (extracted with `get_conn_info_dict`) with the transformation‑specific values (`model_version`, `search_path`, `force`). This dictionary is used for structured log messages throughout the function.  

2. **Task start logging** – The variable `task` is set to “running age transformation”. The start time is recorded with `time.time()`. An INFO log entry “started running age transformation” is emitted using `logger.info(combine_dicts({'msg': ...}, log_dict))`.  

3. **Determine primary schema** – `primary_schema(search_path)` extracts the first schema name from the supplied `search_path`. This schema is the one that currently holds the original tables.  

4. **Create temporary transformed schema** – A new schema name `tmp_schema` is formed by appending “_transformed” to the primary schema name. `create_schema(conn_str, tmp_schema, force)` creates this schema; if `force` is True, an existing schema is ignored.  

5. **Execute the age transformation** – `_transform_age(conn_str, model_version, site, tmp_schema, target_table, force)` is called. This helper installs required PL/pgSQL functions, builds a set of `CREATE UNLOGGED TABLE … AS SELECT …` statements for each target table, and runs them in parallel. Any non‑ignorable error raises `DatabaseError`.  

6. **Log task completion** – After `_transform_age` returns, an INFO log entry records the elapsed time (`secs_since(start_time)`).  

7. **Adjust connection for post‑transformation operations** – A new search path is constructed as `tmp_schema, schema, vocabulary`. `conn_str_with_search_path(conn_str, new_search_path)` produces a connection string that sets this search_path via the `options` parameter.  

8. **Set transformed tables to LOGGED** – `set_logged(new_conn_str, model_version, False, target_table.split(","))` changes the newly created unlogged tables to the default logged state.  

9. **Prepare schema‑swap statements** –  
   * `backup_schema` is defined as `<primary_schema>_backup`.  
   * A `StatementList` named `stmts` is created.  
   * Statements are appended to `stmts` in the following order:  
     - Drop the backup schema if it exists (`drop_schema_statement(..., if_exists=True, cascade=True)`).  
     - Create a fresh backup schema (`create_schema_statement(backup_schema)`).  
     - Move the original target tables from the primary schema to the backup schema (`_move_target_tables_statements(..., schema, backup_schema, target_table)`).  
     - Move the transformed tables from the temporary schema into the primary schema (`_move_target_tables_statements(..., tmp_schema, schema, target_table)`).  
     - Drop the temporary transformed schema (`drop_schema_statement(tmp_schema, if_exists=False, cascade=True)`).  

10. **Execute schema‑swap transactionally** – `stmts.serial_execute(conn_str, transaction=True)` runs all statements inside a single PostgreSQL transaction, guaranteeing atomicity.  

11. **Error handling for the transaction** – After execution, each statement in `stmts` is inspected. If a statement has an error (`stmt.err`):  
    * Errors with PostgreSQL condition `IN_FAILED_SQL_TRANSACTION` are ignored because they are side‑effects of a prior failure.  
    * All other errors are logged at ERROR level with details (`msg`, `submsg`, `err`, `sql`). An INFO log records the aborted task and elapsed time. A `DatabaseError` is raised with a formatted message containing the offending SQL and error text.  

12. **Final success logging** – If no unhandled errors occurred, a final INFO log entry records the successful completion of the task and the total elapsed time.  

13. **Return value** – The function returns `True` to indicate that the transformation completed without raising an exception.

**Reference relationships**:  

*Callers* – The function is invoked by the CLI command `age_transform` defined in `pedsnetdcc/main.py`. The CLI parses command‑line arguments, builds a connection string with `make_conn_str`, and calls `run_age_transformation`.  

*Callees* –  
- `combine_dicts`, `get_conn_info_dict`, `primary_schema`, `create_schema`, `conn_str_with_search_path`, `set_logged`, `drop_schema_statement`, `create_schema_statement`, `StatementList.serial_execute` are used for logging, schema handling, and transaction execution.  
- `_transform_age` performs the actual creation of transformed tables.  
- `_move_target_tables_statements` generates the ALTER TABLE statements required to move tables between schemas.  
- `pg_error` and `DatabaseError` are used for fine‑grained error classification and exception raising.  

**Note**:  
- The function assumes that the primary schema extracted from `search_path` already contains the target tables.  
- `force=True` only suppresses duplicate‑schema errors during temporary schema creation and duplicate‑table errors inside `_transform_age`. Other errors are still propagated.  
- All DDL statements that move or drop schemas are executed inside a single transaction; if any statement fails (except `IN_FAILED_SQL_TRANSACTION`), the entire transaction is rolled back and a `DatabaseError` is raised.  
- The temporary schema is always dropped after the swap, even when an error occurs later in the process.  
- Logging relies on the project‑wide `logger` and uses structured dictionaries for consistency.  

**Output Example**:  

```python
result = run_age_transformation(
    conn_str="host=localhost dbname=pedsnet user=loader password=secret",
    model_version="2.3.0",
    site="stlouis",
    search_path="dcc_pedsnet, public, vocabulary",
    target_table="person,measurement",
    force=False
)
print(result)   # -> True
```
## FunctionDef run_concept_transformation(conn_str, model_version, site, search_path, target_table, force)
**run_concept_transformation**: The function of run_concept_transformation is to orchestrate the full concept‑name transformation workflow for a set of PEDSnet tables, including schema preparation, execution of the transformation, logging, and final schema swapping.

**parameters**:  
· **conn_str** (str): libpq‑style PostgreSQL connection string used for all database operations.  
· **model_version** (str): PEDSnet model version identifier (e.g., “2.3.0”).  
· **site** (str): Site label required by some transforms (e.g., “stlouis”).  
· **search_path** (str): Original PostgreSQL schema search_path (comma‑separated list).  
· **target_table** (str): Comma‑separated list of table names that will be transformed.  
· **force** (bool, optional, default False): When True, benign errors such as duplicate‑schema, duplicate‑table, or “IN_FAILED_SQL_TRANSACTION” are ignored; otherwise any error aborts the process.

**Code Description**:  
1. **Log context preparation** – The function builds a dictionary (`log_dict`) that merges the model version, search path, force flag, and connection‑info extracted from `conn_str` via `get_conn_info_dict`. This dictionary is used for structured logging throughout the routine.  
2. **Task start logging** – A start message containing the task name (“running concept transformation”) and the prepared log dictionary is emitted with `logger.info`. The start time is recorded (`start_time = time.time()`).  
3. **Primary schema resolution** – `primary_schema(search_path)` extracts the first schema name from the supplied search_path; this is the schema that currently holds the target tables.  
4. **Temporary transformed schema creation** – A new schema name is constructed by appending “_transformed” to the primary schema (`tmp_schema = schema + '_' + 'transformed'`). `create_schema(conn_str, tmp_schema, force)` creates the schema, optionally ignoring a duplicate‑schema error when `force=True`.  
5. **Concept transformation execution** – `_transform_concept(conn_str, model_version, site, tmp_schema, target_table, force)` runs the actual transformation logic. It creates the necessary PL/pgSQL functions, generates CREATE‑TABLE‑AS‑SELECT statements that add “_concept_name” columns, and executes them in parallel. Errors are raised as `DatabaseError` unless suppressed by `force`.  
6. **Completion logging** – After the transformation finishes, an informational log entry records the elapsed time using `secs_since(start_time)`.  
7. **Connection string for post‑processing** – A new search_path is built that places the temporary transformed schema first, followed by the original schema and the vocabulary schema (`new_search_path = ','.join((tmp_schema, schema, 'vocabulary'))`). `conn_str_with_search_path` injects this search_path into a copy of the original connection string (`new_conn_str`).  
8. **Set tables to logged** – `set_logged(new_conn_str, model_version, False, target_table.split(","))` changes the newly created tables from UNLOGGED to LOGGED, ensuring they have the default durability.  
9. **Schema swapping preparation** –  
   * `backup_schema` is defined as `<primary>_backup`.  
   * A `StatementList` (`stmts`) is created to hold all DDL statements required for moving tables and cleaning up schemas.  
   * The list is populated with:  
     - `DROP SCHEMA` for the backup schema if it exists (`if_exists=True, cascade=True`).  
     - `CREATE SCHEMA` for the backup schema.  
     - Statements generated by `_move_target_tables_statements` to move the original tables from the primary schema to the backup schema.  
     - Statements generated by `_move_target_tables_statements` to move the transformed tables from the temporary schema into the primary schema.  
     - `DROP SCHEMA` for the temporary transformed schema (no IF EXISTS, cascade).  
10. **Transactional execution** – `stmts.serial_execute(conn_str, transaction=True)` runs the entire list inside a single PostgreSQL transaction. This guarantees atomicity: either all moves succeed or the transaction is rolled back.  
11. **Error inspection after transaction** – The function iterates over each statement in `stmts`. For any statement that recorded an error (`stmt.err`):  
    * If the error condition is `'IN_FAILED_SQL_TRANSACTION'` (a secondary error caused by a prior failure), it is ignored and the loop continues.  
    * Otherwise, a structured error log is emitted (`logger.error`) containing the message, sub‑message, error text, and offending SQL. An abort log with elapsed time follows. A `DatabaseError` is raised with a formatted message that includes the SQL and error text, terminating the function.  
12. **Final success logging** – If no unhandled errors occurred, a final log entry records the successful completion of the task and the total elapsed time.  
13. **Return value** – The function returns `True` to indicate that the transformation completed without raising an exception.

**Reference relationships**:  
*Callers* – The primary caller is `concept_transform` in `pedsnetdcc/main.py`. That CLI entry point builds the connection string, parses command‑line options, and invokes `run_concept_transformation`. The return value is used to decide the exit status of the command‑line tool.  

*Callees* –  
- `combine_dicts` (utils) – merges logging dictionaries.  
- `get_conn_info_dict` (utils) – extracts connection metadata for logs.  
- `primary_schema` (schema) – determines the base schema from the search_path.  
- `create_schema` (schema) – creates the temporary transformed schema.  
- `_transform_concept` (transform_runner) – performs the core concept‑name transformation.  
- `secs_since` (dict_logging) – formats elapsed time for log entries.  
- `conn_str_with_search_path` (utils) – builds a connection string that includes the temporary schema in the search_path.  
- `set_logged` (utils) – converts the newly created tables from UNLOGGED to LOGGED.  
- `_move_target_tables_statements` (transform_runner) – generates ALTER TABLE … SET SCHEMA statements for moving tables between schemas.  
- `StatementList` and its `serial_execute` method (db) – execute the collected DDL statements atomically.  
- `drop_schema_statement` and `create_schema_statement` (schema) – provide the individual Statement objects used in the move/cleanup sequence.  
- `pg_error` (utils) – translates PostgreSQL error codes to symbolic names for conditional error handling.  
- `DatabaseError` (utils) – the exception type raised on unrecoverable failures.

**Note**:  
- The function assumes that `target_table` is a comma‑separated list without surrounding whitespace; callers should provide a clean string.  
- `force=True` only suppresses duplicate‑schema, duplicate‑table, and transaction‑failure side‑effects; any other error will still raise `DatabaseError`.  
- All DDL that moves tables is executed inside a single transaction; if any statement fails, the entire transaction is rolled back and the original schemas remain unchanged.  
- Logging is performed with a structured dictionary that includes connection details, making it suitable for downstream log aggregation tools.  
- The temporary transformed schema is always dropped at the end of the process, regardless of success, to avoid leaving stray schemas.  

**Output Example**:  
When called with valid arguments and no errors, the function returns the boolean value `True`. If an unrecoverable database error occurs, a `DatabaseError` exception is raised and the function does not return.
## FunctionDef run_concept_age_transformation(conn_str, model_version, site, search_path, target_table, force)
**run_concept_age_transformation**: The function of **run_concept_age_transformation** is to execute the full concept‑name transformation workflow for a specified table, including creation of a temporary transformed schema, population of transformed tables, logging, and safe replacement of the original tables with the transformed versions.

**parameters**:  
· **conn_str** (str): PostgreSQL libpq connection string used for all database operations.  
· **model_version** (str): PEDSnet data‑model version identifier (e.g., “2.3.0”).  
· **site** (str): Site label required by some transforms (e.g., “stlouis”).  
· **search_path** (str): Original PostgreSQL schema search_path (comma‑separated list).  
· **target_table** (str): Comma‑separated list of table names that should be transformed.  
· **force** (bool, optional, default False): When True, benign errors such as duplicate‑schema or duplicate‑table are ignored; otherwise any error aborts the process.

**Code Description**:  

1. **Log context preparation** – The function builds a dictionary (`log_dict`) that merges the model version, search path, force flag, and connection‑info extracted from `conn_str` via `get_conn_info_dict`. This dictionary is used for structured logging throughout the routine.

2. **Start logging** – A log entry with the message “started running concept transformation” is emitted using `logger.info`.

3. **Determine primary schema** – The first schema in the supplied `search_path` is obtained by calling `primary_schema(search_path)`. This schema is the one that currently holds the original tables.

4. **Create temporary transformed schema** – A new schema name is constructed by appending “_transformed” to the primary schema (`tmp_schema`). The helper `create_schema(conn_str, tmp_schema, force)` creates this schema, optionally suppressing “duplicate schema” errors when `force=True`.

5. **Run the concept transformation** – The private helper `_transform_concept` is invoked with the original connection string, model version, site, the temporary schema name, the target table list, and the `force` flag. `_transform_concept` creates the transformed copies of the requested tables inside `tmp_schema` by generating and executing `CREATE UNLOGGED TABLE … AS SELECT …` statements that add concept‑name columns.

6. **Log completion of transformation** – After `_transform_concept` returns, a log entry records the elapsed time using `secs_since(start_time)`.

7. **Adjust connection for post‑processing** – A new search path is built that places the temporary schema first, followed by the original schema and the `vocabulary` schema. `conn_str_with_search_path` injects this search path into a copy of the original connection string (`new_conn_str`).

8. **Set tables to logged** – `set_logged` is called with `new_conn_str`, the model version, `vocabulary=False`, and the list of target tables (split on commas). This converts the unlogged tables created in step 5 back to the default logged state.

9. **Prepare schema‑swap statements** –  
   - A backup schema name (`backup_schema`) is derived by appending “_backup” to the primary schema.  
   - An empty `StatementList` (`stmts`) is created.  
   - A `DROP SCHEMA` statement for the backup schema (if it exists) is appended using `drop_schema_statement(..., if_exists=True, cascade=True)`.  
   - A `CREATE SCHEMA` statement for the backup schema is appended.  
   - `_move_target_tables_statements` is called twice: first to generate `ALTER TABLE … SET SCHEMA` statements that move the original tables from the primary schema to the backup schema, and second to move the newly transformed tables from the temporary schema into the primary schema.  
   - Finally, a `DROP SCHEMA` statement for the temporary schema (without `IF EXISTS`) is appended.

10. **Execute schema‑swap transactionally** – `stmts.serial_execute(conn_str, transaction=True)` runs all the collected DDL statements inside a single PostgreSQL transaction. If any statement fails, the transaction is rolled back automatically.

11. **Error inspection** – After execution, each statement in `stmts` is examined. If a statement has an error (`stmt.err` is truthy) and the error is not the expected “IN_FAILED_SQL_TRANSACTION” (which can appear after a prior failure), the function logs the error with `logger.error` and raises a `DatabaseError` that includes the offending SQL and error message. This ensures that the caller is notified of any unrecoverable failure.

12. **Final logging and return** – A concluding log entry records the total elapsed time for the entire operation, and the function returns `True` to indicate successful completion without raising an exception.

**Note**:  
- The `force` flag only suppresses duplicate‑schema errors during temporary schema creation and duplicate‑table errors inside `_transform_concept`. All other errors, including failures in the schema‑swap phase, will raise `DatabaseError`.  
- The function relies on the order of operations: the temporary schema must be created before the transformation, and the backup schema must be prepared before tables are moved.  
- Logging is performed with a structured dictionary that merges static messages with connection metadata; this aids automated log parsing.  
- The search path is explicitly rebuilt for the “set logged” step to ensure that the temporary schema is visible to the session.  
- All DDL statements that modify schemas are executed inside a single transaction (`serial_execute` with `transaction=True`), guaranteeing atomicity of the schema‑swap.  
- The function does not return any data from the transformed tables; it only signals success (`True`) or propagates an exception on failure.

**Output Example**:  

```python
result = run_concept_age_transformation(
    conn_str="host=pg host port=5432 dbname=research user=loader password=secret",
    model_version="2.3.0",
    site="stlouis",
    search_path="dcc_pedsnet, public, vocabulary",
    target_table="condition_occurrence,drug_exposure",
    force=False
)

print(result)   # -> True
```
## FunctionDef run_site_transformation(conn_str, model_version, site, search_path, target_table, force)
**run_site_transformation**: The function of run_site_transformation is to orchestrate the complete site‑specific transformation workflow, creating a temporary transformed schema, populating it with transformed tables, setting those tables to logged, and finally swapping the new tables into the original schema while backing up the old tables.

**parameters**:  
· **conn_str** (str): libpq‑style PostgreSQL connection string used for all database operations.  
· **model_version** (str): PEDSnet data‑model version (e.g., “2.3.0”).  
· **site** (str): Identifier of the site whose name will be written into the new *site* column (e.g., “stlouis”).  
· **search_path** (str): The current PostgreSQL schema search_path (comma‑separated list).  
· **target_table** (str): Comma‑separated list of table names that should be transformed.  
· **force** (bool, optional, default False): When True, benign errors such as duplicate‑schema or duplicate‑table are ignored; otherwise any error aborts the process.

**Code Description**:  

1. **Logging preparation** – The function builds a log dictionary (`log_dict`) that merges the model version, search path, force flag, and connection‑info extracted from `conn_str` via `combine_dicts` and `get_conn_info_dict`. This dictionary is used for structured log messages throughout the function.

2. **Task start logging** – A start message is emitted with `logger.info`, indicating that the site transformation is beginning and recording the start time (`start_time = time.time()`).

3. **Determine primary schema** – `primary_schema(search_path)` extracts the first schema name from the supplied search_path. This schema is the one that currently holds the target tables.

4. **Create temporary transformed schema** – A new schema name is constructed by appending “_transformed” to the primary schema (`tmp_schema = schema + '_' + 'transformed'`). `create_schema(conn_str, tmp_schema, force)` creates this schema, optionally ignoring “duplicate schema” errors when `force=True`.

5. **Run the site‑specific transformation** – `_transform_site(conn_str, model_version, site, tmp_schema, target_table, force)` creates the transformed tables inside `tmp_schema`. The helper builds a set of `CREATE UNLOGGED TABLE … AS SELECT …` statements that add a constant site column, executes them in parallel, and raises `DatabaseError` on any non‑ignorable error.

6. **Log completion of transformation step** – After `_transform_site` returns, a log entry records the elapsed time for the transformation step.

7. **Prepare a connection string that includes the temporary schema** – The new search_path is built as `tmp_schema, schema, vocabulary` and injected into the original connection string via `conn_str_with_search_path`. This ensures subsequent statements see the transformed tables first.

8. **Set transformed tables to LOGGED** – `set_logged(new_conn_str, model_version, False, target_table.split(","))` converts the unlogged tables created in step 5 to logged tables, which is required before moving them into the permanent schema.

9. **Build statements to move tables** –  
   * `backup_schema` is defined as `<primary>_backup`.  
   * A `StatementList` (`stmts`) is created and populated with:  
     - A `DROP SCHEMA` for the backup schema (if it exists) with cascade.  
     - A `CREATE SCHEMA` for the backup schema.  
     - Statements generated by `_move_target_tables_statements` to move the original target tables from the primary schema into the backup schema.  
     - Statements generated by `_move_target_tables_statements` to move the newly transformed tables from the temporary schema into the primary schema.  
     - A `DROP SCHEMA` for the temporary schema (no IF EXISTS, cascade).  

10. **Execute the schema‑move statements transactionally** – `stmts.serial_execute(conn_str, transaction=True)` runs all statements inside a single PostgreSQL transaction, guaranteeing atomicity of the backup‑move‑swap sequence.

11. **Error handling after transaction** – The function iterates over each statement in `stmts`. If a statement has an error (`stmt.err`), it checks whether the error is the PostgreSQL condition `IN_FAILED_SQL_TRANSACTION` (which indicates a prior statement already caused the transaction to abort). Those errors are ignored because the transaction is already rolled back. For any other error, a structured error log is emitted and a `DatabaseError` is raised with the offending SQL and error message, aborting the whole transformation.

12. **Final logging and return** – Upon successful completion of all steps, a final log entry records the total elapsed time, and the function returns `True` to signal that no exception was raised.

**Reference relationships**:  

*Callers* – The primary caller is `site_transform` in `pedsnetdcc/main.py`. That CLI entry point parses command‑line arguments, builds a connection string, and invokes `run_site_transformation`. The return value is used to decide the exit status of the command‑line tool.

*Internal callees* –  
- `combine_dicts` and `get_conn_info_dict` for logging context.  
- `primary_schema` to obtain the primary schema name.  
- `create_schema` to provision the temporary transformed schema.  
- `_transform_site` (private helper) to generate and execute the site‑specific transformation SQL.  
- `conn_str_with_search_path` to adjust the connection string for the temporary schema.  
- `set_logged` to convert unlogged tables to logged.  
- `_move_target_tables_statements` to generate `ALTER TABLE … SET SCHEMA` statements for moving tables.  
- `drop_schema_statement` and `create_schema_statement` (via the helper functions that call them) to manage backup and temporary schemas.  
- `StatementList.serial_execute` to run the move statements inside a transaction.  
- `pg_error` to interpret PostgreSQL error codes for special‑case handling.  
- `DatabaseError` to surface any fatal database failures to the caller.

**Note**:  
- The function assumes that `target_table` is a comma‑separated list without surrounding whitespace; callers should provide a clean list (e.g., `"person,measurement"`).  
- All DDL statements that affect schemas are executed inside a single transaction; if any statement fails (except the expected `IN_FAILED_SQL_TRANSACTION` after a prior failure), the entire transaction is rolled back and a `DatabaseError` is raised.  
- The `force` flag only suppresses duplicate‑schema errors during schema creation and duplicate‑table errors inside `_transform_site`. It does not suppress errors that occur while moving tables or dropping schemas.  
- Logging is performed with structured dictionaries that include connection metadata, making it easy to filter logs by model version, site, or search path.  
- The temporary transformed schema is always dropped at the end of the process, even if an earlier step fails, because the `DROP SCHEMA` statement is part of the transactional `StatementList`. If the transaction aborts, the drop is rolled back automatically.

**Output Example**:  

```python
>>> success = run_site_transformation(
...     conn_str="host=localhost dbname=pedsnet user=loader password=secret",
...     model_version="2.3.0",
...     site="stlouis",
...     search_path="dcc_pedsnet,public,vocabulary",
...     target_table="person,measurement",
...     force=False)
>>> print(success)
True
```
## FunctionDef run_id_transformation(conn_str, model_version, site, search_path, target_table, id_name, id_type, force)
**run_id_transformation**: The function of run_id_transformation is to orchestrate the complete ID‑mapping transformation workflow for a set of PEDSnet tables, including schema preparation, execution of the transformation, logging, and final schema cleanup.

**parameters**:  
· **conn_str** (str): PostgreSQL libpq connection string that identifies the target database.  
· **model_version** (str): PEDSnet model version (e.g., “2.3.0”) used to locate the appropriate metadata.  
· **site** (str): Site label (e.g., “stlouis”) required by the ID‑mapping logic.  
· **search_path** (str): The original PostgreSQL schema search_path (comma‑separated list).  
· **target_table** (str): Comma‑separated list of table names that will be transformed.  
· **id_name** (str): Base name of the identifier set (e.g., “dcc” or “onco”).  
· **id_type** (str): Data type of the site identifier (“BigInteger” or “String”).  
· **force** (bool, optional, default False): When True, benign errors such as duplicate‑table creation are ignored.

**Code Description**:  
1. **Log preparation** – The function builds a log dictionary (`log_dict`) by merging the connection information (extracted with `get_conn_info_dict`) with static fields (`model_version`, `search_path`, `force`). This dictionary is used for all subsequent structured log entries.  

2. **Task start logging** – A message indicating the start of the “id map transformation” task is logged with `logger.info`. The start time (`start_time`) is recorded using `time.time()`.  

3. **Primary schema resolution** – The primary schema is derived from the supplied `search_path` by calling `primary_schema`. This schema is the one that currently holds the original tables.  

4. **Temporary transformed schema creation** – A new schema name is constructed by appending “_transformed” to the primary schema (`tmp_schema`). The helper `create_schema` is invoked with `force` to create this schema; any error other than a duplicate‑schema condition (handled inside `create_schema`) propagates as a `DatabaseError`.  

5. **ID‑mapping transformation** – The private helper `_transform_id` is called with the connection string, model version, site, the temporary schema, the target tables, the identifier name and type, and the `force` flag. `_transform_id` performs the pre‑transform setup, generates the `CREATE UNLOGGED TABLE … AS SELECT …` statements for each target table, and executes them in parallel. Errors other than duplicate‑table (when `force=True`) cause a `DatabaseError`.  

6. **Task completion logging** – Upon successful return from `_transform_id`, a log entry records the elapsed time (`secs_since(start_time)`).  

7. **Search path adjustment for post‑transform operations** – A new search path is built that places the temporary transformed schema first, followed by the original schema and the `vocabulary` schema. `conn_str_with_search_path` creates a new connection string (`new_conn_str`) that includes this search path.  

8. **Set tables to logged** – `set_logged` is called with the new connection string, model version, `False` for the vocabulary flag, and the list of target tables (split on commas). This converts the newly created unlogged tables back to the default logged state.  

9. **Schema migration** –  
   * A backup schema name (`backup_schema`) is derived by appending “_backup” to the primary schema.  
   * A `StatementList` (`stmts`) is instantiated to hold the DDL statements required for moving tables and cleaning up schemas.  
   * The statements added to `stmts` are:  
     – Drop the backup schema if it exists (`drop_schema_statement(..., if_exists=True, cascade=True)`).  
     – Create a fresh backup schema (`create_schema_statement(backup_schema)`).  
     – Move the original target tables from the primary schema to the backup schema (`_move_target_tables_statements(..., schema, backup_schema, target_table)`).  
     – Move the transformed tables from the temporary schema into the original schema (`_move_target_tables_statements(..., tmp_schema, schema, target_table)`).  
     – Drop the temporary transformed schema (`drop_schema_statement(tmp_schema, if_exists=False, cascade=True)`).  
   * The entire list is executed transactionally with `stmts.serial_execute(conn_str, transaction=True)`.  

10. **Error handling for schema moves** – After execution, each statement in `stmts` is inspected. If a statement has an error (`stmt.err`), the error code is examined with `pg_error`. Errors equal to `'IN_FAILED_SQL_TRANSACTION'` are ignored because they are side‑effects of a prior failure. All other errors are logged at `ERROR` level with detailed context (message, SQL, error text) and re‑raised as a `DatabaseError`, aborting the function.  

11. **Final logging and return** – A final log entry records the successful completion of the task and the total elapsed time. The function returns `True` to indicate that no exception was raised.  

**Reference Relationships**:  
*Callers* – The only public entry point that invokes `run_id_transformation` is the CLI command `id_transform` defined in `pedsnetdcc/main.py`. That command builds the connection string, parses command‑line options, and calls `run_id_transformation`.  

*Callees* – `run_id_transformation` relies on the following internal utilities:  
- `combine_dicts` (pedsnetdcc/utils.py) for building log dictionaries.  
- `get_conn_info_dict` (pedsnetdcc/utils.py) for extracting connection metadata.  
- `primary_schema` (pedsnetdcc/schema.py) to obtain the primary schema name.  
- `create_schema` (pedsnetdcc/schema.py) to create the temporary transformed schema.  
- `_transform_id` (pedsnetdcc/transform_runner.py) to perform the actual ID‑mapping table creation.  
- `secs_since` (pedsnetdcc/dict_logging.py) for elapsed‑time formatting.  
- `conn_str_with_search_path` (pedsnetdcc/utils.py) to embed the new search path into a connection string.  
- `set_logged` (pedsnetdcc/utils.py) to convert unlogged tables to logged.  
- `drop_schema_statement`, `create_schema_statement` (pedsnetdcc/schema.py) to generate DDL statements for schema cleanup.  
- `_move_target_tables_statements` (pedsnetdcc/transform_runner.py) to generate the ALTER TABLE statements that move tables between schemas.  
- `StatementList.serial_execute` (pedsnetdcc/db.py) to run the DDL batch transactionally.  
- `pg_error` (pedsnetdcc/utils.py) to interpret PostgreSQL error codes.  
- `DatabaseError` (pedsnetdcc/utils.py) for consistent exception raising.  

**Note**:  
- The function assumes that the target tables exist in the primary schema and that the caller has sufficient privileges to create schemas, create tables, alter tables, and drop schemas.  
- The `force` flag only suppresses duplicate‑table errors during the transformation step; other errors (e.g., permission denied, syntax errors) are always propagated.  
- All logging is performed with a structured dictionary that includes connection details, model version, search path, and the `force` flag, facilitating downstream log aggregation.  
- The transactional execution of schema moves guarantees atomicity: if any DDL statement fails, the entire transaction is rolled back, leaving the database in its pre‑move state.  
- After the function returns, the original tables are backed up in `<primary>_backup`, and the transformed tables replace the originals in the primary schema.  

**Output Example**:  
```python
result = run_id_transformation(
    conn_str="host=localhost dbname=pedsnet user=loader password=secret",
    model_version="2.3.0",
    site="stlouis",
    search_path="dcc_pedsnet, public, vocabulary",
    target_table="person,visit_occurrence",
    id_name="dcc",
    id_type="BigInteger",
    force=False
)
print(result)   # => True
```
## FunctionDef run_index_transformation(conn_str, model_version, site, search_path, target_table, force)
**run_index_transformation**: The function of run_index_transformation is to execute the complete index‑only transformation workflow for a given PEDSnet model, including schema preparation, table materialisation, logging, and final schema swapping.

**parameters**:  
· **conn_str** (str): PostgreSQL libpq connection string that identifies the target database.  
· **model_version** (str): PEDSnet model version identifier (e.g., “2.3.0”).  
· **site** (str): Site label used by the transformation (e.g., “stlouis”).  
· **search_path** (str): PostgreSQL schema search_path string (comma‑separated list).  
· **target_table** (str): Comma‑separated list of table names that should be transformed.  
· **force** (bool, optional, default False): When True, benign errors such as duplicate‑schema or duplicate‑table are ignored.

**Code Description**:  

1. **Logging context creation** –  
   * `combine_dicts` merges a dictionary containing `model_version`, `search_path`, and `force` with the connection‑info dictionary returned by `get_conn_info_dict(conn_str)`.  
   * The resulting `log_dict` is used for all subsequent structured log entries.

2. **Task start logging** –  
   * The variable `task` is set to “running index transformation”.  
   * `logger.info` records a start message that includes the task name and the combined log dictionary.

3. **Primary schema extraction** –  
   * `primary_schema(search_path)` parses the supplied `search_path` and returns the first schema name. This schema is the one that currently holds the target tables.

4. **Temporary transformed schema creation** –  
   * `tmp_schema` is built by appending “_transformed” to the primary schema name.  
   * `create_schema(conn_str, tmp_schema, force)` creates the new schema that will receive the transformed tables. The `force` flag allows the call to succeed silently if the schema already exists.

5. **Index transformation execution** –  
   * `_transform_index(conn_str, model_version, site, tmp_schema, target_table, force)` runs the index‑only transformation. It creates the required tables in `tmp_schema` using parallel `CREATE UNLOGGED TABLE … AS SELECT …` statements. Errors are raised as `DatabaseError` unless `force` suppresses a duplicate‑table condition.

6. **Task completion logging** –  
   * After `_transform_index` returns, a log entry records the finished task and the elapsed time, calculated by `secs_since(start_time)`.

7. **Connection string for post‑transform operations** –  
   * `new_search_path` is constructed as a comma‑separated list: `tmp_schema, schema, vocabulary`.  
   * `new_conn_str = conn_str_with_search_path(conn_str, new_search_path)` produces a connection string that sets this search_path via the `options` parameter.

8. **Set transformed tables to logged** –  
   * `set_logged(new_conn_str, model_version, False, target_table.split(","))` changes the newly created unlogged tables to the default logged state.

9. **Schema swapping preparation** –  
   * `backup_schema` is defined as `<primary>_backup`.  
   * An empty `StatementList` (`stmts`) is created to hold the DDL statements required for moving tables and cleaning up schemas.

10. **Drop existing backup schema (if any)** –  
    * `drop_schema_statement(backup_schema, if_exists=True, cascade=True)` is appended to `stmts`.

11. **Create fresh backup schema** –  
    * `create_schema_statement(backup_schema)` is appended to `stmts`.

12. **Move original tables to backup** –  
    * `_move_target_tables_statements(model_version, schema, backup_schema, target_table)` returns a `StatementList` that contains `ALTER TABLE … SET SCHEMA` statements for each target table. These statements are extended into `stmts`.

13. **Move transformed tables into the original schema** –  
    * `_move_target_tables_statements(model_version, tmp_schema, schema, target_table)` generates the reverse move statements (from the temporary schema to the primary schema) and they are also extended into `stmts`.

14. **Drop the temporary transformed schema** –  
    * `drop_schema_statement(tmp_schema, if_exists=False, cascade=True)` is appended to `stmts`.

15. **Execute all schema‑swap statements transactionally** –  
    * `stmts.serial_execute(conn_str, transaction=True)` runs the entire list inside a single PostgreSQL transaction. If any statement fails, the transaction is rolled back automatically.

16. **Error handling after transaction** –  
    * The loop iterates over each statement in `stmts`. For the first statement that has a non‑null `err` (excluding the “IN_FAILED_SQL_TRANSACTION” error that results from a prior failure), an error log entry is written with the statement’s message, SQL text, and error text.  
    * The function then logs an abort message with elapsed time and raises `DatabaseError` containing the failing SQL and error.

17. **Final success logging** –  
    * If no statement produced an error, a final log entry records the successful completion of the task and the total elapsed time.

18. **Return value** –  
    * The function returns `True` to indicate that the transformation completed without raising an exception.

**Reference Relationships**  

*Callers* – The primary public entry point is `index_transform` in `pedsnetdcc/main.py`. That CLI wrapper builds the connection string, parses command‑line options, and invokes `run_index_transformation`. No other module calls this function directly.

*Callees* –  
- `combine_dicts` and `get_conn_info_dict` are used to build structured log dictionaries.  
- `primary_schema` extracts the primary schema name from `search_path`.  
- `create_schema` creates the temporary transformed schema.  
- `_transform_index` performs the actual materialisation of index tables.  
- `secs_since` provides elapsed‑time formatting for log messages.  
- `conn_str_with_search_path` creates a connection string that includes the temporary schema in the search_path.  
- `set_logged` converts the newly created unlogged tables to logged.  
- `drop_schema_statement`, `create_schema_statement`, and `_move_target_tables_statements` generate the DDL statements required for moving tables between schemas and cleaning up.  
- `StatementList` and its method `serial_execute` execute the DDL batch inside a transaction.  
- `pg_error` is consulted when logging errors to identify PostgreSQL error conditions.  
- `DatabaseError` is raised for any unrecoverable failure.

**Note**  
- The function assumes that `target_table` is a comma‑separated list without surrounding whitespace; callers should provide a clean string (e.g., `"person,measurement"`).  
- All DDL statements that move tables are executed inside a single transaction; therefore, any failure aborts the entire schema‑swap operation and leaves the database unchanged.  
- The `force` flag only suppresses duplicate‑schema and duplicate‑table errors. Other errors (permission, syntax, missing objects) will cause a `DatabaseError`.  
- Logging relies on the project‑wide `logger` and expects the combined dictionary to contain connection metadata; missing keys will not break the function but may reduce log usefulness.  
- The temporary schema name is deterministic (`<primary>_transformed`), and the backup schema name is `<primary>_backup`. Existing backup schemas are dropped unconditionally before being recreated.  
- After the transformation, the function ensures that the target tables are in the logged state, which is required for subsequent constraint and index creation steps.

**Output Example**  
```python
result = run_index_transformation(
    conn_str="host=localhost dbname=pedsnet user=loader password=secret",
    model_version="2.3.0",
    site="stlouis",
    search_path="dcc_pedsnet,public,vocabulary",
    target_table="person,measurement",
    force=False
)
print(result)   # -> True
```
## FunctionDef undo_transformation(conn_str, model_version, search_path)
**undo_transformation**: The function of undo_transformation is to revert a previously applied transformation by restoring the original tables from a backup schema and cleaning up the backup objects.

**parameters**:  
· conn_str (str): PostgreSQL libpq‑style connection string used to open a database connection.  
· model_version (str): PEDSnet data‑model version identifier (e.g., “2.3.0”).  
· search_path (str): PostgreSQL schema search_path string; the first schema in the list is treated as the primary schema for the operation.

**Code Description**:  
1. **Logging preparation** – A dictionary `log_dict` is built by merging the supplied `model_version` and `search_path` with connection‑metadata obtained from `get_conn_info_dict(conn_str)`. The start time is recorded with `time.time()`, and an informational log entry marks the beginning of the “undoing transformation” task.  

2. **Determine schemas** – `primary_schema(search_path)` extracts the first schema name from the comma‑separated `search_path`. The backup schema name is constructed by appending “_backup” to the primary schema (`backup_schema = schema + '_' + 'backup'`).  

3. **Validate backup schema existence** – `schema_exists(conn_str, backup_schema)` checks that the backup schema is present. If it is missing, a `RuntimeError` is raised with a clear message.  

4. **Validate table set in backup schema** –  
   * `stock_metadata(model_version)` retrieves the canonical PEDSnet metadata for the given model version.  
   * `tables_in_schema(conn_str, backup_schema)` returns the set of table names actually present in the backup schema.  
   * `expected_tables` is computed as the set of all tables defined in the metadata minus the constant `VOCAB_TABLES` (vocabulary tables are never moved).  
   * If the two sets differ, a `RuntimeError` is raised indicating a mismatch between the backup schema and the expected data model.  

5. **Drop constraints that would block table moves** –  
   * `drop_foreign_keys(conn_str, model_version, force=True)` removes all foreign‑key constraints on the data tables. The `force=True` flag suppresses “constraint does not exist” errors, making the operation idempotent.  
   * `drop_indexes(conn_str, model_version, force=True)` removes all indexes on the data tables, also with `force=True`.  

6. **Assemble DDL statements** – A `StatementList` instance `stmts` is created. The list is populated with:  
   * Statements to drop the current (transformed) tables in the primary schema: `_drop_tables_statements(model_version, schema)`.  
   * Statements to move each table from the backup schema back into the primary schema: `_move_tables_statements(model_version, backup_schema, schema)`.  
   * A statement that drops the now‑empty backup schema: `drop_schema_statement(backup_schema, if_exists=False, cascade=True)`.  

7. **Execute statements transactionally** – `stmts.serial_execute(conn_str, transaction=True)` runs all statements inside a single PostgreSQL transaction. If any statement fails, the transaction is rolled back automatically by the `StatementList` implementation.  

8. **Error handling after execution** – The function iterates over each statement in `stmts`. For any statement that recorded an error (`stmt.err` is truthy) and whose PostgreSQL error condition is not `IN_FAILED_SQL_TRANSACTION`, an error log entry is written and a `DatabaseError` is raised with the offending SQL and error message. Errors that are a side‑effect of a previously failed transaction are ignored because the transaction is already aborted.  

9. **Completion logging** – If no unhandled errors are encountered, a final log entry records the successful completion of the undo operation together with the elapsed time (`secs_since(start_time)`).  

10. **Return value** – The function returns `True` to indicate that the rollback completed without raising an exception.

**Reference relationships**  
*Callers*:  
  – `pedsnetdcc/main.py` invokes `undo_transformation` when the command‑line option `--undo` is supplied.  
  – `pedsnetdcc/tests/transform_runner_test.py/TransformRunnerTest.test_without_data` calls `undo_transformation` to verify that a transformation can be rolled back correctly.  

*Callees*:  
  – `combine_dicts` and `get_conn_info_dict` for building log dictionaries.  
  – `primary_schema`, `schema_exists`, `tables_in_schema`, and the constant `VOCAB_TABLES` for schema and table validation.  
  – `stock_metadata` to obtain the authoritative list of tables for the given model version.  
  – `drop_foreign_keys` and `drop_indexes` to remove constraints that would prevent table moves.  
  – `_drop_tables_statements`, `_move_tables_statements`, and `drop_schema_statement` to generate the required DDL statements.  
  – `StatementList.serial_execute` to run the statements inside a transaction.  
  – `secs_since` for elapsed‑time formatting, `pg_error` for PostgreSQL error classification, and `DatabaseError` for raising a uniform database‑level exception.  
  – The module‑level `logger` for structured logging throughout the process.

**Note**:  
- The function assumes that a backup schema named `<primary_schema>_backup` exists and contains exactly the non‑vocabulary tables defined by the specified model version.  
- All foreign keys and indexes must be dropped before attempting to move tables; otherwise PostgreSQL will raise constraint‑related errors.  
- The transaction flag is set to `True`; therefore any failure in the DDL sequence causes a full rollback, leaving the database in its pre‑undo state.  
- Errors with PostgreSQL condition `IN_FAILED_SQL_TRANSACTION` are ignored because they are a consequence of an earlier failure that already aborted the transaction.  
- The function does not catch `RuntimeError` raised during validation; callers should be prepared to handle those exceptions separately from `DatabaseError`.  
- The backup schema is permanently removed (`DROP SCHEMA … CASCADE`) after the tables have been moved back, so the undo operation cannot be repeated without re‑creating a backup.

**Output Example**:  
```python
result = undo_transformation(
    conn_str="host=localhost port=5432 dbname=pedsnet user=loader",
    model_version="2.3.0",
    search_path="clinical, public"
)
print(result)   # -> True
```
## FunctionDef run_vocab_indexes(conn_str, model_version, search_path, force)
**run_vocab_indexes**: The function of run_vocab_indexes is to adjust (add and drop) indexes on the vocabulary schema of a transformed PEDSnet database model.

**parameters**:  
· conn_str (str): PostgreSQL libpq connection string used to open a database connection.  
· model_version (str): PEDSnet data‑model version identifier (e.g., “2.3.0”).  
· search_path (str): PostgreSQL schema search_path string; the first schema in the list is treated as the primary schema.  
· force (bool, optional, default False): When True, benign errors such as “object already exists” or “object does not exist” are ignored during index creation or removal.

**Code Description**:  
1. **Logging preparation** – The function builds a dictionary (`log_dict`) that merges the supplied arguments (`model_version`, `search_path`, `force`) with connection‑metadata extracted by `get_conn_info_dict(conn_str)`. This dictionary is used for all subsequent log entries so that each log line contains both operational context and connection details.  
2. **Task start** – A human‑readable task description (`'updating vocabulary indexes'`) is stored in `task`. The current epoch time is captured in `start_time`. An INFO‑level log entry is emitted indicating that the task has started.  
3. **Primary schema extraction** – The helper `primary_schema(search_path)` is called to obtain the first schema name from the supplied `search_path`. The result is stored in `schema`, although the variable is not used later; the call serves as a validation step to ensure that `search_path` is well‑formed.  
4. **Add required indexes** – `add_vocab_indexes(conn_str, model_version, force)` is invoked. This function generates the appropriate CREATE INDEX statements for the vocabulary tables of the given model version, executes them in parallel, and logs start, error, and completion messages. Errors that are not benign cause an exception to propagate out of `add_vocab_indexes`.  
5. **Drop unneeded indexes** – `drop_vocab_unneeded_indexes(conn_str, model_version, force)` is invoked immediately after the addition step. It generates DROP INDEX statements for indexes that are no longer required, runs them in parallel, and applies the same error‑handling logic as the addition step.  
6. **Task completion** – After both sub‑operations finish without raising an exception, a final INFO log entry records that the task has finished and includes the elapsed time, calculated by `secs_since(start_time)`.  
7. **Return value** – The function returns the boolean literal `True` to signal successful completion. Any exception raised by the called functions propagates outward, causing the caller to receive a failure indication.

**Reference relationships**:  
- **Caller**: `vocab_indexes` in `pedsnetdcc/main.py` parses command‑line arguments, builds a connection string, and calls `run_vocab_indexes`. The CLI command expects a boolean success flag; if `False` the process exits with status 1.  
- **Callees**:  
  * `primary_schema` (pedsnetdcc/schema.py) – validates and extracts the primary schema from `search_path`.  
  * `add_vocab_indexes` (pedsnetdcc/indexes.py) – creates required vocabulary indexes.  
  * `drop_vocab_unneeded_indexes` (pedsnetdcc/indexes.py) – removes obsolete vocabulary indexes.  
  * `combine_dicts` (pedsnetdcc/utils.py) – merges dictionaries for logging.  
  * `get_conn_info_dict` (pedsnetdcc/utils.py) – parses the connection string for log enrichment.  
  * `secs_since` (pedsnetdcc/dict_logging.py) – formats elapsed seconds for log messages.  
  * The logging module (`logger`) – records start, error, and completion messages.  

**Note**:  
- The function does not catch exceptions from its sub‑calls; any error will propagate to the caller. This design allows the CLI wrapper to terminate with a non‑zero exit code on failure.  
- The `force` flag is passed unchanged to both `add_vocab_indexes` and `drop_vocab_unneeded_indexes`. When `True`, benign errors (e.g., attempting to create an index that already exists or drop an index that does not exist) are suppressed; otherwise such errors abort the process.  
- The variable `schema` obtained from `primary_schema` is currently unused beyond validation; future extensions may rely on it.  
- Logging dictionaries always contain the keys `model_version`, `search_path`, `force`, and the connection metadata (`user`, `host`, `port`, `dbname`, `search_path`). This ensures consistent log formatting across the project.  

**Output Example**:  
```python
result = run_vocab_indexes(
    conn_str="host=pghost port=5432 dbname=pedsnet user=loader options='-c search_path=vocab'",
    model_version="2.3.0",
    search_path="vocab, public",
    force=False
)
print(result)   # True
```
