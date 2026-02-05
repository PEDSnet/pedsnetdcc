## ClassDef Transform
**Transform**: The function of Transform is to define a common abstract interface for all table‑level transformations applied to the PEDSNet Common Data Model (CDM) before it is materialised as a temporary table.

**attributes**:  
· __metaclass__: `ABCMeta` – makes the class abstract and requires subclasses to implement the abstract methods.  
· No instance attributes are defined; the class operates entirely through class methods.

**Code Description**  

The `Transform` class is the foundation for every concrete transformation used in the PEDSNet DCC pipeline. Its purpose is to standardise how a transformation:

1. **Pre‑processes the database** – optional statements that must run before any SELECT manipulation (e.g., creating PL/pgSQL functions). Implemented via the overridable class method `pre_transform`. The base implementation does nothing, allowing subclasses such as `AgeTransform` and `IDMappingTransform` to create required database objects.

2. **Modifies a SQLAlchemy `Select` object** – the core of a transformation. Subclasses must implement the abstract method `modify_select`. The method receives:
   - `metadata` – a SQLAlchemy `MetaData` instance describing all tables and columns.
   - `table_name` – the name of the table currently being transformed.
   - `select` – the current `Select` expression (initially a plain `select([table])`).
   - `join` – the current `Join` expression (initially the table itself).
   - `id_name` – the name of the site identifier column (default `'dcc'`).
   - `id_type` – the SQLAlchemy type of the site identifier (default `'BigInteger'`).

   The method must return a tuple `(new_select, new_join)`. Subclasses may add columns, replace existing columns, or chain additional joins. For example:
   * `AgeTransform` adds age‑in‑months columns and joins the `person` table once per target table.
   * `ConceptNameTransform` adds `_concept_name` columns by outer‑joining the `concept` table.
   * `SiteNameTransform` adds a literal `site` column without altering joins.
   * `AddIndexTransform` and `DropIndexTransform` return the original `(select, join)` because they only affect metadata, not the SELECT clause.

3. **Alters the SQLAlchemy metadata** – after all SELECT modifications are defined, the pipeline may need to update the in‑memory table definitions (e.g., add new columns, create indexes). The concrete method `modify_metadata` iterates over every non‑vocabulary table in `metadata` and calls the subclass‑implemented `modify_table`. Subclasses such as `AddIndexTransform`, `AgeTransform`, `ConceptNameTransform`, `SiteNameTransform`, `VocabularyAddIndexTransform`, and `VocabularyDropIndexTransform` implement `modify_table` to append new columns and/or create `Index` objects. `DropIndexTransform` overrides `modify_metadata` to return a list of `Index` objects that should be dropped.

4. **Utility for index naming** – the static method `make_index_name(table_name, column_name)` builds an Oracle‑compatible index identifier that respects the 30‑character limit (`NAME_LIMIT`). It creates a deterministic, collision‑resistant name by:
   * Taking the first three characters of the table name.
   * Concatenating the first letters of each component of the column name.
   * Appending an MD5 hash of the full `table.column` string, truncated to fit the limit.
   * Adding the suffix `_ix`.

   This method is used by all subclasses that generate indexes (e.g., `AddIndexTransform`, `AgeTransform`, `ConceptNameTransform`, `SiteNameTransform`).

**Relationship with Callers**  

Every concrete transformation class in the `pedsnetdcc` package inherits from `Transform`. The typical workflow in the DCC ETL process is:

1. **Pre‑transform step** – the pipeline calls `Transform.pre_transform` (or the subclass override) to set up any required database objects.
2. **Select building** – for each CDM table, the pipeline starts with `select([table])` and `join = table`. It then passes the pair through a chain of `modify_select` calls, one per enabled transformation subclass, allowing each to augment the SELECT and join tree.
3. **Metadata adjustment** – after the SELECT chain is built, the pipeline invokes `modify_metadata` on each transformation to ensure the in‑memory `MetaData` reflects added columns and indexes. This enables later steps (e.g., automatic DDL generation) to create the correct schema.
4. **Execution** – the final SELECT (combined with its final join) is rendered as a `CREATE TABLE foo_tmp AS SELECT …` statement, materialising the transformed table.

Thus, `Transform` provides the contract that all transformation classes obey, ensuring a consistent and composable approach to CDM customisation.

**Note**  
- Subclasses must implement both `modify_select` and `modify_table`; failing to do so will raise a `TypeError` because `Transform` is abstract.  
- `modify_select` should never remove columns; if column removal is required, the API would need to be extended.  
- The `id_name` and `id_type` parameters allow the same transformation logic to be reused for different identifier schemes (e.g., integer vs. string IDs).  
- Index names generated by `make_index_name` are intentionally non‑human‑readable to avoid collisions; they are deterministic for a given table/column pair.  
- `modify_metadata` returns the (possibly modified) `MetaData` object, while `DropIndexTransform.modify_metadata` returns a list of `Index` objects to be dropped; callers must handle both return types appropriately.

**Output Example**  

When a concrete subclass such as `AgeTransform` processes the `measurement` table, the returned tuple might look like:

```
(
    SELECT measurement.*, 
           months_in_interval(person.birth_datetime, measurement.measurement_datetime) AS measurement_age_in_months,
           months_in_interval(person.birth_datetime, measurement.measurement_result_datetime) AS measurement_result_age_in_months
    FROM measurement
    JOIN person ON person.person_id = measurement.person_id
) ,
JOIN object representing the same join chain
```

The `modify_table` part of the same subclass would have added two new columns (`measurement_age_in_months`, `measurement_result_age_in_months`) to the `MetaData` table definition and created corresponding indexes using `Transform.make_index_name`.
### FunctionDef pre_transform(cls, conn_str, metadata, id_name, id_type)
**pre_transform**: The function of pre_transform is to provide a hook for executing prerequisite SQL statements before a data transformation runs.

**parameters**:  
· cls: the Transform subclass invoking the method (passed automatically as a class method).  
· conn_str (str): libpq connection string or URI used to connect to the PostgreSQL database.  
· metadata (sqlalchemy.schema.MetaData): SQLAlchemy MetaData object describing the model’s tables; used by concrete transforms to inspect or modify schema objects.  
· id_name (str, optional): Name of the identifier column that will be added to transformed tables; defaults to `'dcc'`.  
· id_type (str, optional): SQL type of the identifier column; defaults to `'BigInteger'`.

**Code Description**:  
`pre_transform` is defined as a class method of the abstract base class `Transform`. Its body contains only a `pass` statement, meaning the base implementation performs no action. The method’s purpose is to be overridden by concrete subclasses (e.g., `AgeTransform`, `IDMappingTransform`, `AddIndexTransform`, etc.). When a transformation is executed, the runner functions in `pedsnetdcc/transform_runner.py` iterate over all registered transform classes and invoke `transform.pre_transform(conn_str, stock_metadata(model_version), id_name, id_type)`. This call gives each transform an opportunity to set up required database objects—such as user‑defined functions, temporary tables, or custom types—before the main transformation SQL is generated and executed. Because the base method does nothing, a subclass that does not need any preparation can simply inherit the default implementation without additional code.

From a functional perspective, `pre_transform` acts as a preparatory step in the transformation pipeline:

1. The runner builds a connection string (`conn_str`) and obtains model metadata (`stock_metadata`).  
2. For each transform class in the global `TRANSFORMS` list, the runner calls `pre_transform`.  
3. If a subclass overrides the method, its custom SQL statements are executed at this point (typically via a helper that runs statements against `conn_str`).  
4. After all `pre_transform` calls complete, the runner proceeds to generate and execute the main transformation statements.

The unit tests `AgeTest.test_pre_transform` and `IDMappingTransformTest.test_pre_transform` verify that the method can be called without error, ensuring that the hook is present and callable with the expected arguments.

**Note**:  
- The base implementation returns `None`; any subclass that overrides the method should also return `None` or raise an exception on failure.  
- Because the method may execute SQL, callers should be prepared to handle `psycopg2.ProgrammingError` or other database exceptions that can propagate from overridden implementations.  
- The default values for `id_name` and `id_type` are used by transforms that rely on a site identifier column; subclasses can ignore these arguments if they do not need an identifier.

**Output Example**:  
Since the base method contains only `pass`, the return value is always `None`. Example call:

```python
result = Transform.pre_transform(conn_str, metadata)
print(result)   # Output: None
```
***
### FunctionDef modify_select(cls, metadata, table_name, select, join, id_name, id_type)
**modify_select**: Transform a given SQLAlchemy Select object (and its associated Join) into a new Select (and Join) that incorporates additional column expressions and chained joins as defined by a specific Transform implementation.  

**parameters**:  
· metadata: sqlalchemy.schema.MetaData – the MetaData instance that contains the table and column definitions for the data model.  
· table_name: str – the name of the table that the Transform is operating on.  
· select: sqlalchemy.sql.expression.Select – the original Select statement, typically a simple “select all columns from table_name” construct (e.g., `select([a_table])`).  
· join: sqlalchemy.sql.expression.Join – the original Join object; for the first Transform in a chain this is usually just the table object itself.  
· id_name: str – the column name that represents the site‑wide identifier (default `'dcc'`).  
· id_type: str – the SQLAlchemy type name for the identifier column (default `'BigInteger'`).  

**Code Description**:  
The method is defined as a class method (`cls`) intended to be overridden by concrete Transform subclasses. Its purpose is to receive a baseline SELECT‑FROM‑JOIN representation of a table and return a modified pair `(new_select, new_join)`. The transformation may add computed columns, rename existing columns, or otherwise enrich the SELECT clause. Correspondingly, any additional tables required for those new columns must be attached to the incoming `join` by chaining further Join objects (e.g., `join = join.join(other_table, onclause)`).  

The docstring specifies the expected workflow:  
1. The first Transform in a pipeline receives a plain SELECT that selects all columns from `table_name` and a `join` that is simply the table object.  
2. Each Transform can augment the SELECT by appending column expressions (using `select = select.column(new_expr)` or similar).  
3. If new columns depend on other tables, the Transform must extend the `join` chain so that the final SELECT can resolve those references.  
4. The returned tuple `(select, join)` can be passed to the next Transform’s `modify_select` method, allowing multiple independent transformations to be composed.  
5. After the last Transform, the caller can produce the final executable statement with `final_select.select_from(final_join)`.  

All Transform‑specific configuration values are expected to be supplied through `metadata.info`, enabling flexible, data‑driven behavior without altering the method signature.  

In the current abstract implementation the body contains only `pass`, meaning the method does not perform any operation by itself; concrete subclasses must provide the actual logic.  

**Note**:  
- Because the base implementation does nothing, invoking `Transform.modify_select` directly will return `None`. Subclasses must override the method and return a tuple `(Select, Join)`.  
- The API assumes that column removal is not required; if a transformation needs to drop columns, the method signature or implementation should be extended accordingly.  
- The `id_name` and `id_type` parameters are provided for convenience but are not used in the abstract method; subclasses may incorporate them when constructing identifier columns.  

**Output Example**:  
A typical concrete implementation might return something like:  

```
(
    select([a_table.c.id, a_table.c.name, func.upper(b_table.c.code).label('code_upper')]),
    a_table.join(b_table, a_table.c.b_id == b_table.c.id)
)
```  

Here the SELECT now includes an additional computed column `code_upper`, and the JOIN chain has been extended to include `b_table` so that the expression can be resolved. The tuple can be fed into the next Transform or finalized with `final_select.select_from(final_join)`.
***
### FunctionDef modify_metadata(cls, metadata)
**modify_metadata**: The function of modify_metadata is to iterate over all non‑vocabulary tables in a SQLAlchemy MetaData object and apply a table‑level transformation to each table.

**parameters**:  
· **cls** – the Transform class (or subclass) on which the method is invoked.  
· **metadata** – a `sqlalchemy.MetaData` instance that contains the full set of Table objects for the PEDSnet schema.

**Code Description**:  
The method receives a MetaData collection that describes every table and column of the target database model. It loops through `metadata.tables.items()`, which yields `(table_name, table)` pairs. For each table, it checks whether the table name appears in the constant `VOCAB_TABLES`. Tables listed in `VOCAB_TABLES` are vocabulary tables that must remain unchanged, so the loop skips them with `continue`.  

For every remaining table (i.e., all non‑vocabulary tables), the method calls `cls.modify_table(metadata, table)`. `modify_table` is an abstract hook defined in the same `Transform` base class; concrete subclasses override it to perform site‑specific or transformation‑specific alterations such as adding columns, indexes, or constraints. Because `modify_table` works in‑place on the supplied `table` object, any changes are automatically reflected in the original `metadata` instance.  

After processing all applicable tables, `modify_metadata` returns the (now possibly mutated) `metadata` object, allowing callers to chain further operations or to use the transformed schema for DDL generation, index extraction, or data‑merge workflows.

**Reference Relationships**:  
- **Callee**: `modify_table` – the only method invoked inside `modify_metadata`. Subclasses provide the actual implementation; the base implementation is a no‑op.  
- **Callers**:  
  * `pedsnetdcc/indexes.py::_indexes_from_metadata` – calls each Transform’s `modify_metadata` to obtain a transformed MetaData before extracting index objects.  
  * `pedsnetdcc/merge_site_data.py:merge_site_data` and `merge_data_to_schema` – invoke `modify_metadata` for every Transform in `TRANSFORMS` to ensure the schema reflects all required transformations before building merge statements.  
  * Test suites (`tests/age_transform_test.py`, `tests/concept_name_transform_test.py`, `tests/site_name_transform_test.py`, `tests/id_mapping_transform_test.py`, etc.) – call `modify_metadata` on specific Transform subclasses to verify that the expected indexes, columns, or other schema changes are present.  
  * `tests/transform_runner_test.py` – uses `modify_metadata` as part of the full transformation pipeline that creates a transformed schema in a temporary PostgreSQL database.

**Note**:  
- The method deliberately excludes tables listed in `VOCAB_TABLES`; this behavior is verified by the `SiteNameTest` which asserts that only non‑vocabulary tables receive modifications.  
- Because `modify_table` is abstract, calling `Transform.modify_metadata` on the base class will result in no changes to the metadata. Subclasses must override `modify_table` to have any effect.  
- The returned `metadata` is the same object that was passed in; the method does not create a copy. Therefore, callers should not rely on the original metadata remaining unchanged after the call.  
- The method does not accept an `id_type` argument; that parameter is part of `modify_table` and can be used by subclasses when they need to add a site‑identifier column.

**Output Example**:  
```python
# Assume AgeTransform overrides modify_table to add two indexes.
metadata = stock_metadata('2.2.0')
metadata = AgeTransform.modify_metadata(metadata)

# After the call, metadata.tables['table1'] now contains two Index objects.
len(metadata.tables['table1'].indexes)   # -> 2
```
***
### FunctionDef modify_table(cls, metadata, table, id_type)
**modify_table**: The function of modify_table is a helper method intended to apply a custom transformation to a specific SQLAlchemy Table object in place.  

**parameters**:  
· metadata: sqlalchemy.schema.MetaData – the MetaData collection that contains the table definitions for the database schema.  
· table: sqlalchemy.schema.Table – the Table object that should be transformed.  
· id_type: str – a string indicating the SQLAlchemy column type to be used for the site identifier column (default value is `'BigInteger'`).  

**Code Description**:  
`modify_table` is defined as a class method (conventionally invoked via `cls.modify_table`) inside the abstract `Transform` class. The method body consists solely of a `pass` statement, which means it performs no operation by itself. Its purpose is to serve as an abstract hook that concrete subclasses must override to implement site‑specific or transformation‑specific logic.  

When a subclass provides its own implementation, the method will receive the full MetaData object, a single Table object, and the optional `id_type`. Inside the overridden method, developers can add, remove, or alter columns, indexes, constraints, or any other table attributes using SQLAlchemy’s schema manipulation API. Because the method works directly on the supplied `table` instance, any modifications are applied in place and will be reflected in the MetaData that is later used to generate DDL statements or to reflect the transformed schema.  

The only place where `modify_table` is invoked in the current codebase is the `modify_metadata` class method of the same `Transform` class. `modify_metadata` iterates over every table stored in `metadata.tables`, skips tables listed in the `VOCAB_TABLES` constant (which are vocabulary tables that should remain unchanged), and calls `cls.modify_table(metadata, table)` for each remaining table. Consequently, `modify_table` is the extension point that allows developers to customize the schema of all non‑vocabulary tables before further processing such as index creation or constraint enforcement.  

**Note**:  
- The base implementation does nothing; it must be overridden in a subclass for any effect.  
- The `id_type` parameter is provided for convenience; subclasses may use it to create a site‑identifier column with the specified SQLAlchemy type.  
- Because the method mutates the `table` object directly, it should not return a value; returning anything other than `None` is unnecessary and ignored by the caller.  
- Ensure that any added columns or constraints are compatible with the target database dialect to avoid runtime errors during schema generation.  

**Output Example**:  
The method does not produce a return value; its effect is the in‑place modification of the `table` object within the supplied `metadata`. After a subclass overrides `modify_table`, the `metadata` passed to `modify_metadata` will contain the transformed tables ready for further processing.
***
### FunctionDef make_index_name(table_name, column_name)
**make_index_name**: The function creates a deterministic, length‑limited index name for a given table and column combination, suitable for Oracle databases.

**parameters**:  
· table_name (str): The name of the database table for which the index is being created.  
· column_name (str): The name of the column (or derived column) on which the index will be built.

**Code Description**:  
The function follows a fixed algorithm to generate an index identifier that respects the Oracle `NAME_LIMIT` (the maximum length allowed for object names).  

1. **Table abbreviation** – The first three characters of `table_name` are taken (`table_abbrev`). This provides a short, recognizable prefix.  
2. **Column abbreviation** – The column name is split on underscores (`_`). The first character of each segment is concatenated to form `column_abbrev`. For example, `gender_source_concept_name` becomes `gscn`.  
3. **Hash component** – The full qualified name `"{table_name}.{column_name}"` is encoded as UTF‑8 and hashed with MD5. The hexadecimal digest (`md5`) is a 32‑character string that uniquely represents the table/column pair.  
4. **Length calculation** – The remaining length available for the hash part is computed as:  
   `hashlen = NAME_LIMIT - (len(table_abbrev) + len(column_abbrev) + 3 * len('_') + len('ix'))`  
   This subtracts the lengths of the two abbreviations, the three underscore separators, and the trailing literal `ix` from the maximum allowed name length.  
5. **Final assembly** – The index name is built by joining the components with underscores:  
   `"{table_abbrev}_{column_abbrev}_{md5[:hashlen]}_ix"`  

The resulting name is deterministic (same inputs always give the same output) and highly unlikely to collide, even for similarly named columns in different tables (e.g., `provider.gender_source_concept_name` vs. `person.gender_source_concept_name`). The function deliberately sacrifices human readability for uniqueness and compliance with the length restriction.

**Relationship with Callers**:  
Several transformation classes in the project use `make_index_name` when they add new columns to tables and need to create corresponding indexes:

- **AddIndexTransform.modify_table** – Directly creates an index for existing columns listed in `create_by_table`.  
- **AgeTransform.modify_table** – After adding an `_age_in_months` column derived from a `_datetime` column, it calls `make_index_name` to generate an index for the new column, unless the column is listed in `ignore_index_by_table`.  
- **ConceptNameTransform.modify_table** – When a `_concept_name` column is added based on a `_concept_id` column, the function supplies the index name.  
- **SiteNameTransform.modify_table** – Adds a `site` column and creates an index using the generated name, unless the table is excluded from indexing.

In each case, `make_index_name` ensures that the index identifiers conform to Oracle’s naming limits and remain unique across the entire schema.

**Note**:  
- The function relies on the global constant `NAME_LIMIT` (defined elsewhere) to enforce the maximum length; if `NAME_LIMIT` is changed, the hash portion length will adjust automatically.  
- The MD5 hash is truncated to fit the remaining length; therefore, the final index name may contain only a portion of the full hash.  
- Because the table abbreviation uses only the first three characters, very short table names may produce less distinctive prefixes; however, the hash component compensates for uniqueness.  
- The function does not perform any validation on the input strings; callers should ensure that `table_name` and `column_name` are non‑empty and contain only characters allowed in Oracle identifiers.

**Output Example**:  
```python
make_index_name('provider', 'gender_source_concept_name')
# Returns: 'pro_gscn_ae1fd5b22b92397ca9_ix'  (example assuming NAME_LIMIT = 30)
```
***
