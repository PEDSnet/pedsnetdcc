## ClassDef ConceptNameTransform
**ConceptNameTransform**: The function of ConceptNameTransform is to add a human‑readable *_concept_name* column for every *_concept_id* column in a CDM table by joining the `concept` vocabulary table, and to create appropriate indexes for the new columns.

**attributes**  
· `ignore_index_by_table` – dictionary mapping CDM table names to tuples of column names for which an index should **not** be created after the *_concept_name* column is added. This prevents unnecessary indexes on columns that are already indexed or otherwise not needed.  

**Code Description**  
ConceptNameTransform inherits from the abstract base class **Transform**, therefore it must implement the two abstract class methods `modify_select` and `modify_table`.  

1. **modify_select**  
   - Signature: `modify_select(cls, metadata, table_name, select, join, id_name='dcc', id_type='BigInteger')`  
   - The method iterates over every column expression in the incoming `select` object. For each column whose name ends with `_concept_id` it:  
     a. Constructs a new column name by replacing the suffix `_concept_id` with `_concept_name`.  
     b. Creates an alias of the `concept` table (`concept.alias()`).  
     c. Adds a new column expression `concept_alias.c.concept_name.label(new_name)` to the SELECT list.  
     d. Extends the join tree with an **outer join** (`join.outerjoin`) that links the alias on `concept_alias.c.concept_id == <original_concept_id_column>`.  
   - The method returns the updated `(select, join)` tuple, which now selects both the original `_concept_id` columns and the newly added `_concept_name` columns, and includes the necessary LEFT OUTER JOINs to the `concept` table.  

2. **modify_table**  
   - Signature: `modify_table(cls, metadata, table, id_type='BigInteger')`  
   - Retrieves the `concept_name` column object from the `concept` table (`metadata.tables['concept'].c.concept_name`).  
   - Scans the existing columns of the target `table`. For each column ending with `_concept_id` it:  
     a. Generates the corresponding `_concept_name` column name.  
     b. Creates a new `Column` instance with the same data type as `concept_name`.  
     c. Appends this column to the table definition (`table.append_column`).  
     d. Checks `ignore_index_by_table` to decide whether an index should be created for the new column. If the column is not listed for omission, it creates an `Index` using `Transform.make_index_name` to generate an Oracle‑compatible index identifier.  
   - The method mutates the `metadata` in place; the transformed metadata will later be used by the pipeline to emit DDL for the temporary transformed tables.  

3. **Interaction with other components**  
   - **Transform (abstract base class)** provides the contract (`modify_select`, `modify_table`) and utility method `make_index_name`. ConceptNameTransform fulfills this contract, allowing it to be chained with other transformations (e.g., AgeTransform, SiteNameTransform).  
   - **transform_runner._transform_concept_select_sql** calls `ConceptNameTransform.modify_select` for each target table when building the final `CREATE UNLOGGED TABLE … AS SELECT …` statement. The runner supplies the `metadata` object, the table name, and the initial `select`/`join` objects. The returned SELECT and Join are then compiled into SQL that creates a transformed copy of the source table.  
   - **tests/concept_name_transform_test.py** validates both methods:  
     * `test_modify_select` confirms that the generated SELECT includes the correct LEFT OUTER JOINs and column aliases.  
     * `test_modify_metadata` checks that two indexes are created for the new `_concept_name` columns, and that the generated index DDL matches the expected naming scheme.  
   - No external functions are called from within ConceptNameTransform other than standard SQLAlchemy constructs (`Column`, `Index`) and the `make_index_name` helper from its superclass.  

**Note**  
- The transformation assumes that a `concept` table with columns `concept_id` and `concept_name` exists in the supplied `metadata`. If the table is missing, `modify_select` will raise a `KeyError`.  
- Only LEFT OUTER JOINs are used; rows with a NULL `_concept_id` will still appear with a NULL `_concept_name`.  
- Index creation is deliberately skipped for columns listed in `ignore_index_by_table` to avoid duplicate or unnecessary indexes. Users can extend this dictionary if additional columns should be exempt.  
- The class operates entirely through class methods; no instance of ConceptNameTransform is ever created.  
- The generated index names are deterministic but not human‑readable, following the algorithm in `Transform.make_index_name`. This ensures compliance with Oracle’s 30‑character identifier limit.  

**Output Example**  
When `ConceptNameTransform.modify_select` processes a table `table1` that contains columns `foo_concept_id` and `bar_concept_id`, the resulting SELECT (after calling `select_obj.select_from(join_obj)`) compiles to:

```
SELECT table1.foo_concept_id,
       table1.bar_concept_id,
       concept_1.concept_name AS foo_concept_name,
       concept_2.concept_name AS bar_concept_name
FROM table1
LEFT OUTER JOIN concept AS concept_1
       ON concept_1.concept_id = foo_concept_id
LEFT OUTER JOIN concept AS concept_2
       ON concept_2.concept_id = bar_concept_id
```

After `modify_table` runs, the `metadata.tables['table1']` definition contains two new columns (`foo_concept_name`, `bar_concept_name`) of type `VARCHAR` (matching `concept.concept_name`) and two indexes, for example:

```
CREATE INDEX tab_fcn_81387a6132eed7f69b5_ix ON table1 (foo_concept_name);
CREATE INDEX tab_bcn_7baa5e16ad1f8129b90_ix ON table1 (bar_concept_name);
```
### FunctionDef modify_select(cls, metadata, table_name, select, join, id_name, id_type)
**modify_select**: The function of modify_select is to add a *_concept_name* column for each *_concept_id* column present in a table’s SELECT clause by joining the table to the *concept* lookup table.

**parameters**:
· **cls** – the class on which the method is invoked (not used inside the method).  
· **metadata** – a SQLAlchemy MetaData object that contains the database schema, including the *concept* table.  
· **table_name** – name of the table being transformed (string). It is not directly used in the method but is part of the required signature for all transform classes.  
· **select** – a SQLAlchemy Select object that currently selects columns from the target table.  
· **join** – a SQLAlchemy Join object (or a Table object) that represents the current FROM/JOIN clause for the query.  
· **id_name** – identifier name for the site‑specific ID set (default *'dcc'*). Not used in this method but required by the common signature.  
· **id_type** – SQLAlchemy type of the site‑specific ID (default *'BigInteger'*). Not used in this method but required by the common signature.

**Code Description**:  
1. The method retrieves the *concept* table from the supplied *metadata* (`concept = metadata.tables['concept']`).  
2. It iterates over every column expression (`col`) that appears in the SELECT clause (`for col in select.c:`).  
3. For each column whose name ends with the suffix *_concept_id*, the method:  
   a. Constructs a new column name by replacing the suffix with *_concept_name* (`new_name = col.name.replace('_concept_id', '_concept_name')`).  
   b. Creates an alias of the *concept* table (`concept_alias = concept.alias()`). The alias ensures that multiple concept joins receive distinct table identifiers (e.g., *concept_1*, *concept_2*).  
   c. Builds a new column expression that selects the *concept_name* field from the alias and labels it with the new column name (`new_col = concept_alias.c.concept_name.label(new_name)`).  
   d. Extends the current join expression with a **LEFT OUTER JOIN** (implemented as `outerjoin`) that links the alias’s *concept_id* column to the original *_concept_id* column (`join = join.outerjoin(concept_alias, concept_alias.c.concept_id == col)`).  
   e. Adds the newly created *concept_name* column to the SELECT clause (`select = select.column(new_col)`).  
4. After processing all *_concept_id* columns, the method returns the updated SELECT and JOIN objects (`return select, join`).  

The method therefore enriches any table that stores concept identifiers with their human‑readable names, without altering the original columns.

**Relationship with Callers**:  
*ConceptNameTransform.modify_select* is invoked by the transformation runner functions (`_transform_concept_select_sql` and the generic `_transform_select_sql`) when building the final CREATE TABLE … AS SELECT statements for a target schema. In those runners, a fresh SELECT and JOIN are created for each source table; the runner then passes them through every registered transform class, including *ConceptNameTransform*. The returned SELECT/JOIN pair, now containing the additional *_concept_name* columns and the necessary LEFT OUTER JOINs to the *concept* table, is compiled into raw SQL and used to create the transformed copy of the source table. The unit test `ConceptNameTest.test_modify_select` demonstrates the expected SQL output: original *_concept_id* columns are retained, and for each such column a corresponding *_concept_name* column appears, joined via distinct aliases (`concept_1`, `concept_2`).

**Note**:
- The method assumes that a table named *concept* exists in the supplied metadata; if it is missing, a KeyError will be raised.  
- The join is always an outer join, preserving rows that have NULL concept identifiers.  
- Because the method creates a new alias for each *_concept_id* column, the generated SQL will contain sequential alias names (`concept_1`, `concept_2`, …) as shown in the test expectations.  
- The parameters *id_name* and *id_type* are part of the common transform interface but are not used here; they are kept for signature compatibility.

**Output Example**:  
Given a source table *table1* with columns *foo_concept_id* and *bar_concept_id*, the method returns a SELECT that includes:

```
SELECT table1.foo_concept_id,
       table1.bar_concept_id,
       concept_1.concept_name AS foo_concept_name,
       concept_2.concept_name AS bar_concept_name
```

and a JOIN expression equivalent to:

```
FROM table1
LEFT OUTER JOIN concept AS concept_1 ON concept_1.concept_id = table1.foo_concept_id
LEFT OUTER JOIN concept AS concept_2 ON concept_2.concept_id = table1.bar_concept_id
```
***
### FunctionDef modify_table(cls, metadata, table, id_type)
**modify_table**: The function of **modify_table** is to add a `_concept_name` column for each `_concept_id` column in a given table and to create an index on the new column, respecting any table‑specific index‑ignore rules.

**parameters**:  
· **metadata** – a SQLAlchemy `MetaData` instance that contains the full schema, including the `concept` table.  
· **table** – the SQLAlchemy `Table` object that is currently being processed; this is the table to which new columns and indexes will be appended.  
· **id_type** – a string indicating the SQLAlchemy type of the site identifier (default `'BigInteger'`). This parameter is part of the abstract signature but is not used directly in the implementation.

**Code Description**:  
1. The method retrieves the `concept_name` column object from the `concept` table (`metadata.tables['concept'].c.concept_name`). This column’s type is reused for every new `_concept_name` column that will be added.  
2. It iterates over all columns (`orig_columns = table.c`) of the target table.  
3. For each column whose name ends with `'_concept_id'`, it constructs a new column name by replacing the suffix with `'_concept_name'`.  
4. A new `Column` instance is created with that name and the type taken from `concept_name.type`. The column is appended to the table definition via `table.append_column(new_col)`.  
5. Before creating an index, the method checks whether the current table is listed in the class‑level dictionary `ignore_index_by_table`. If the table is present and the new column name is listed for that table, index creation is skipped.  
6. If indexing is not ignored, an `Index` object is instantiated. The index name is generated by calling `Transform.make_index_name(table.name, new_col_name)`, which produces an Oracle‑compatible, deterministic identifier. The `Index` is associated with the newly added column.  

The function operates directly on the supplied `Table` object, mutating its column collection and index collection in place. It does not return a value; the modifications are reflected in the `metadata` structure that the caller holds.

**Relationship with callees**:  
- Calls `Transform.make_index_name` (a static method defined in the abstract base class `Transform`) to generate a compliant index name.  
- Relies on the `Column` and `Index` classes from SQLAlchemy to construct schema objects.  
- Uses the `concept` table’s column definition (`concept_name`) as a template for the new column’s type, ensuring type consistency across all generated `_concept_name` columns.  

**Note**:  
- The method only adds columns and indexes; it never removes or modifies existing columns.  
- Index creation is suppressed for any column listed in `ConceptNameTransform.ignore_index_by_table[table.name]`. This allows selective disabling of indexes for performance or schema‑compatibility reasons.  
- The `id_type` argument is retained to satisfy the abstract method signature but is not utilized in the current logic.  
- Because the function mutates the `Table` object in place, callers must ensure that the same `metadata` instance is used throughout the transformation pipeline to capture all schema changes.
***
