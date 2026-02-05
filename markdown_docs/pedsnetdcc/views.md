## FunctionDef create_oracle_views(model_version, source_schema, target_schema, view_file_name)
**create_oracle_views**: The function of create_oracle_views is to generate Oracle‑compatible CREATE VIEW and GRANT SELECT SQL statements for every table defined in a given PEDSnet model version and write them to a specified file.

**parameters**:  
· model_version: string indicating the PEDSnet model version (e.g., “2.3.0”).  
· source_schema: string name of the schema that contains the original tables.  
· target_schema: string name of the schema where the Oracle views will be created.  
· view_file_name: string path of the file that will receive the generated SQL statements.

**Code Description**:  
1. A logger for the current module is obtained and an informational message “starting Oracle view SQL file” is logged. The current time is recorded to later compute elapsed time.  
2. The function calls **stock_metadata(model_version)** (from *pedsnetdcc/utils.py*) to retrieve a SQLAlchemy MetaData object that contains the canonical table and column definitions for the requested model version.  
3. Both *target_schema* and *source_schema* are converted to upper‑case because Oracle identifiers are case‑insensitive and the generated SQL uses quoted upper‑case names.  
4. Three containers are prepared:  
   - *table_list*: a list that will hold the names of all tables in the model.  
   - *create_dict*: a dictionary mapping each table name to its CREATE VIEW statement.  
   - *grant_dict*: a dictionary mapping each table name to its GRANT SELECT statement.  
5. The function iterates over **metadata.tables.items()**. For each table:  
   a. The table name is appended to *table_list*.  
   b. A CREATE VIEW string is built:  
      - Begins with `create or replace view "<TARGET_SCHEMA>"."<TABLE_NAME>" as select `.  
      - For every column in the table, the column name is added twice, first quoted in upper‑case (the source column) and then quoted in its original case (the view column alias), separated by `as`. Each column entry ends with a comma and a space.  
      - After the loop, the trailing comma‑space is removed (`create = create[:-2]`).  
      - The FROM clause is appended, referencing the source schema and the upper‑case table name, and the statement is terminated with a semicolon.  
   c. The completed CREATE VIEW statement is stored in *create_dict* under the table name.  
   d. A GRANT SELECT statement is built: `grant select on "<TARGET_SCHEMA>"."<TABLE_NAME>" to "<TARGET_SCHEMA>";` and stored in *grant_dict*.  
6. After processing all tables, the function opens *view_file_name* for writing. It writes the CREATE VIEW statements first, iterating over *table_list* in sorted order, each followed by a platform‑specific newline (`os.linesep`). Then it writes the GRANT statements in the same sorted order.  
7. Once the file is closed, an informational log entry records completion and the elapsed time using the helper `secs_since(start_time)`.  
8. The function returns **True** to indicate successful execution.  

**Reference relationships**:  
- **Caller**: *pedsnetdcc/main.py/create_oracle_views_sql* invokes this function directly, passing command‑line arguments for model version, source schema, target schema, and output file name. The caller interprets the Boolean return value to decide the exit status of the script.  
- **Callee**: The only external dependency is **stock_metadata** which supplies the model definition required to enumerate tables and columns. No other project functions are called from within *create_oracle_views*.  

**Note**:  
- The generated view names and column aliases preserve the original column case for the view alias while referencing the source columns in upper‑case; this matches Oracle’s handling of quoted identifiers.  
- The function assumes that the source schema already contains tables with the exact column names defined in the model; it does not validate their existence.  
- All identifiers are quoted, so reserved words or special characters are safely handled.  
- The function writes the entire output file in one pass; if the file cannot be opened for writing, an exception will propagate and the function will not return.  
- The returned value is always **True** when the function reaches its final line; any error that raises an exception will prevent the return.  

**Output Example**:  
If *model_version* “2.3.0” defines a table `person` with columns `person_id` and `gender_concept_id`, and the call is  

```python
create_oracle_views('2.3.0', 'pedsnet_raw', 'pedsnet_view', 'oracle_views.sql')
```  

the resulting *oracle_views.sql* file will contain (order may vary depending on sorting):

```
create or replace view "PEDNET_VIEW"."person" as select "PERSON_ID" as "person_id", "GENDER_CONCEPT_ID" as "gender_concept_id" from "PEDNET_RAW"."PERSON";
create or replace view "PEDNET_VIEW"."measurement" as select "MEASUREMENT_ID" as "measurement_id", "PERSON_ID" as "person_id", ... from "PEDNET_RAW"."MEASUREMENT";
...
grant select on "PEDNET_VIEW"."person" to "PEDNET_VIEW";
grant select on "PEDNET_VIEW"."measurement" to "PEDNET_VIEW";
...
```  

The function will log start and finish messages and finally return **True**.
