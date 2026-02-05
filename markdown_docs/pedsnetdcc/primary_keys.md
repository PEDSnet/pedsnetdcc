## FunctionDef _primary_keys_from_model_version(model_version)
**_primary_keys_from_model_version**: The function of _primary_keys_from_model_version is to return a list of SQLAlchemy primary‑key constraint objects that belong to the stock PEDSnet data model for a specified model version.

**parameters**:  
· model_version: a string that identifies the PEDSnet model version (e.g., “2.2.0”).  

**Code Description**:  
1. The function begins by obtaining a fresh SQLAlchemy MetaData object that represents the unmodified (stock) PEDSnet schema for the supplied *model_version*. This is done by calling **stock_metadata(model_version)**, which contacts the PEDSnet data‑model service and populates the MetaData with tables, columns, and constraints.  
2. An empty list named *primary_keys* is created to collect the primary‑key constraints that will be discovered.  
3. The function iterates over every entry in `metadata.tables`. Each entry provides a *name* (the table name) and a *table* object (an instance of `sqlalchemy.Table`).  
4. For each table, the function first checks whether the table name is **not** present in the global collection `VOCAB_TABLES`. Vocabulary tables are deliberately excluded because they are handled separately elsewhere in the package.  
5. If the table passes the exclusion test, the function examines the table’s `constraints` collection. For every constraint *con* in that collection, it verifies two conditions: (a) the constraint object is not `None`, and (b) the constraint is an instance of `sqlalchemy.PrimaryKeyConstraint`.  
6. When both conditions are satisfied, the primary‑key constraint object *con* is appended to the *primary_keys* list.  
7. After all tables have been processed, the function returns the *primary_keys* list, which now contains one `PrimaryKeyConstraint` object for each non‑vocabulary table that defines a primary key in the stock model.

**Reference Relationships**:  
- **Callee**: The only direct dependency of this function is **stock_metadata**, which supplies the MetaData containing the full schema for the requested model version.  
- **Callers**:  
  * `add_primary_keys` (pedsnetdcc/primary_keys.py) invokes `_primary_keys_from_model_version` to obtain the constraints that must be added back to a transformed database. It then generates `ALTER TABLE … ADD CONSTRAINT` statements for each returned primary‑key object.  
  * Unit‑test `AddPrimaryKeysTest.test_pk_names` (pedsnetdcc/tests/primary_keys_test.py) calls the function to verify that the expected primary‑key names are present in the returned list.  
  * Helper method `AddPrimaryKeysTest._make_update_tables` also calls the function to create temporary tables that mimic the original schema before primary‑key constraints are re‑applied.  

These callers rely on the function to provide an accurate, version‑specific enumeration of primary‑key constraints, excluding vocabulary tables, so that subsequent operations (SQL generation, testing, or schema migration) can be performed correctly.

**Note**:  
- The function works on the *stock* model only; if a transformation has altered primary‑key definitions, those changes will not be reflected unless the caller modifies the returned MetaData after this function runs.  
- Vocabulary tables listed in `VOCAB_TABLES` are intentionally omitted; if a project requires primary keys from those tables, the exclusion logic must be adjusted.  
- The function does not cache metadata; each invocation creates a new MetaData instance via `stock_metadata`, which may involve a network request to the data‑model service. Re‑using the result within a single workflow can improve performance.  
- The returned objects are instances of `sqlalchemy.PrimaryKeyConstraint`. They contain attributes such as `.name` (the constraint name) and `.table` (the associated `Table` object), which are used by callers to construct DDL statements.

**Output Example**:  
```python
>>> pk_list = _primary_keys_from_model_version('2.2.0')
>>> [pk.name for pk in pk_list]
['xpk_person',
 'xpk_condition_occurrence',
 'xpk_measurement',
 'xpk_observation',
 'xpk_visit_occurrence',
 ...]   # one entry per non‑vocabulary table
>>> pk_list[0]
PrimaryKeyConstraint('person_id', name='xpk_person')
```
## FunctionDef _check_stmt_err(stmt, force)
**_check_stmt_err**: The function validates the outcome of a database Statement execution and conditionally suppresses benign errors when the `force` flag is set.

**parameters**:  
· stmt: a `Statement` object that has been executed; it must expose an `err` attribute containing any exception raised during execution.  
· force: a boolean flag indicating whether to ignore a specific “already exists” error (duplicate primary‑key constraint).

**Code Description**:  
1. The function first checks `stmt.err`. If it is `None`, the statement completed without error and the function returns immediately.  
2. If an error is present, the code determines whether the error corresponds to PostgreSQL error code **42P16**, which maps to the symbolic name `INVALID_TABLE_DEFINITION`. This error is raised when a primary‑key constraint is applied to a table that already has a primary key (i.e., a redundant primary‑key creation). The detection uses `hasattr(stmt.err, 'pgcode')` and the `psycopg2_errorcodes.lookup` helper.  
3. When `force` is `True` **and** the error is identified as the “already exists” condition, the function treats the error as benign and returns without raising. This allows callers to continue processing even if the primary‑key constraint already exists.  
4. For any other error condition, the function re‑raises the original exception (`stmt.err`). The caller is expected to handle or propagate this exception.

**Relationship with Callers**:  
`_check_stmt_err` is invoked inside `add_primary_keys` after a batch of `Statement` objects has been executed in parallel. `add_primary_keys` iterates over each `Statement` in the `StatementSet`, calling `_check_stmt_err(stmt, force)`. If `_check_stmt_err` raises an exception, `add_primary_keys` logs the failure, records the elapsed time, and re‑raises the exception, causing the overall primary‑key addition process to abort. When `force=True`, duplicate‑primary‑key errors are silently ignored, enabling idempotent execution of `add_primary_keys`.

**Note**:  
- The function relies on the `psycopg2_errorcodes` module; ensure it is imported in the module where `_check_stmt_err` resides.  
- Only the specific error code `42P16` (`INVALID_TABLE_DEFINITION`) is suppressed; all other database errors will propagate.  
- The `Statement` object must expose both `err` and `sql` attributes for proper logging by the caller.  
- The function does not return a value; its effect is either silent success or raising an exception.

**Output Example**:  
- Successful validation (no error or benign duplicate primary‑key error with `force=True`): the function returns `None`.  
- Failure case (e.g., syntax error in SQL): the function raises the original exception, such as `psycopg2.errors.SyntaxError`.
## FunctionDef add_primary_keys(conn_str, model_version, force)
**add_primary_keys**: The function of add_primary_keys is to add primary‑key constraints to all non‑vocabulary tables of a PEDSnet data model after a transformation has been applied.

**parameters**  
· conn_str (str): PostgreSQL libpq connection string that identifies the target database and schema (search_path must include the schema where the tables reside).  
· model_version (str): Identifier of the PEDSnet model version (e.g., “2.2.0”) whose stock metadata defines the primary‑key constraints to be added.  
· force (bool, optional, default False): When True, benign “primary key already exists” errors (PostgreSQL error code 42P16) are ignored; otherwise any error causes the function to raise an exception.

**Code Description**  
1. **Logging preparation** – The function builds a log dictionary that merges the supplied `model_version` and `force` flag with connection‑information extracted by `get_conn_info_dict(conn_str)`. It logs a “starting adding primary keys” message and records the start time.  

2. **Determine required primary keys** – It calls the internal helper `_primary_keys_from_model_version(model_version)`. This helper loads the stock metadata for the given model version, iterates over all tables except those listed in `VOCAB_TABLES`, and collects every `sqlalchemy.PrimaryKeyConstraint` object found. The result is a list of constraint objects, each exposing attributes such as `.name` and `.table`.  

3. **Create executable statements** – An empty `StatementSet` is instantiated. For each primary‑key constraint `pk` in the list, the function builds an `ALTER TABLE … ADD CONSTRAINT` DDL statement by invoking `sqlalchemy.schema.AddConstraint(pk).compile(dialect=pg)`, where `pg` is a PostgreSQL dialect object. The compiled SQL string is wrapped in a `Statement` object and added to the `StatementSet`.  

4. **Parallel execution** – The `StatementSet.parallel_execute(conn_str)` method launches a pool of worker processes (up to 25, or one per statement) that each execute the stored `Statement` objects against the database. Execution is performed in autocommit mode, suitable for DDL.  

5. **Error handling** – After all statements have completed, the function iterates over the `StatementSet`. For each `stmt`, it calls `_check_stmt_err(stmt, force)`. `_check_stmt_err` inspects `stmt.err`; if the error corresponds to PostgreSQL code 42P16 (duplicate primary‑key) and `force` is True, the error is suppressed. Any other error is re‑raised, causing the surrounding `try/except` block in `add_primary_keys` to log a fatal error, record the elapsed time, and propagate the exception.  

6. **Completion logging** – If no exception is raised, a “finished adding primary keys” message is logged together with the total elapsed time.  

7. **Return value** – The function returns `True` to indicate successful completion.

**Reference relationships**  
*Callers* – `add_primary_keys` is invoked by several high‑level workflows that need to finalize a transformed schema:  
  • `merge_site_data` and `merge_data_to_schema` (both in `pedsnetdcc/merge_site_data.py`) call it after creating tables, setting NOT NULLs, indexes, and before adding foreign keys.  
  • `run_transformation` and `run_target_transformation` (in `pedsnetdcc/transform_runner.py`) call it to add primary keys to transformed core tables.  
  • `run_subset_by_cohort` (in `pedsnetdcc/subset_by_cohort.py`) calls it on the subset schema after creating the subset tables.  
  • Unit tests in `pedsnetdcc/tests/primary_keys_test.py` verify correct behavior and error handling.  

*Callees* – Within the function the following helpers are used:  
  • `_primary_keys_from_model_version` – obtains the list of primary‑key constraint objects for the specified model version.  
  • `StatementSet` – aggregates the generated `Statement` objects and provides the `parallel_execute` method.  
  • `Statement` – encapsulates each DDL command.  
  • `_check_stmt_err` – validates each statement’s execution result, optionally suppressing benign duplicate‑key errors.  
  • `combine_dicts`, `get_conn_info_dict`, and `secs_since` – support structured logging and elapsed‑time calculation.  

**Note**  
- The function assumes that the database search_path is correctly set to include the target schema; otherwise the generated `ALTER TABLE` statements will fail.  
- Only primary‑key constraints defined in the *stock* model are added. If a transformation modifies primary‑key definitions, those changes are not reflected unless the metadata is altered before calling this function.  
- When `force=False`, any attempt to add a primary key that already exists raises the original database exception, causing the whole operation to abort.  
- The parallel execution pool size is automatically limited to a maximum of 25 workers; this limit can be overridden only by modifying `StatementSet.parallel_execute`.  
- Errors other than the specific “invalid table definition” (42P16) are never suppressed, regardless of the `force` flag.  

**Output Example**  
```python
result = add_primary_keys(conn_str, '2.2.0', force=True)
print(result)   # -> True
```
