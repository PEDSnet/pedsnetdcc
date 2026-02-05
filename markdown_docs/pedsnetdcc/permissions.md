## FunctionDef _loading_user_privileges_sql(site, owner)
**_loading_user_privileges_sql**: The function of _loading_user_privileges_sql is to generate a list of SQL statements that configure the appropriate permissions for the `loading_user` role during the database preparation phase.

**parameters**:  
· site: a string representing the site name (e.g., `'dcc'` or `'stlouis'`).  
· owner: a string indicating the role that should own the schema; defaults to `'loading_user'`.

**Code Description**:  
The function begins by obtaining the SQL template stored in the constant `PREP_DB_SQL_TEMPLATE`. This template contains placeholder tokens `{{.Site}}` and `{{.Owner}}` that need to be replaced with the actual values supplied via the `site` and `owner` arguments. The code performs two successive `str.replace` operations: first substituting `{{.Site}}` with the `site` argument, then substituting `{{.Owner}}` with the `owner` argument.  

After the placeholders are replaced, the resulting multi‑line string is split on newline characters, producing an iterable of individual SQL lines. Each line is passed through the helper function `_despace`, which collapses any consecutive spaces into a single space, ensuring that the generated statements are syntactically clean. Lines that are empty after splitting are filtered out.  

Finally, the function returns a list containing the cleaned SQL statements. The return type is `list(str)`. Although the docstring mentions a possible `ValueError`, the implementation does not raise any exceptions explicitly; any error would arise only from unexpected input types that cause the string replacement or split operations to fail.

From a functional perspective, `_loading_user_privileges_sql` is invoked by `grant_loading_user_permissions` in the same module. That caller iterates over a list of site identifiers, calls `_loading_user_privileges_sql` for each site, wraps each returned statement in a `Statement` object, and then executes the collection of statements against the target database. The only direct callee of `_loading_user_privileges_sql` is `_despace`, which normalizes spacing in each generated line.

**Note**:  
- The function assumes that `PREP_DB_SQL_TEMPLATE` is a well‑formed multi‑line SQL string containing the exact placeholder tokens `{{.Site}}` and `{{.Owner}}`. If the template is missing either token, the replacement will have no effect, potentially producing incorrect SQL.  
- No validation is performed on the `site` or `owner` arguments; callers should ensure that they provide valid, non‑empty strings.  
- The function does not trim leading or trailing whitespace on each line; it only collapses internal runs of spaces via `_despace`.  
- Because the function returns a list of raw SQL strings, the caller is responsible for handling execution order, transaction management, and error checking.

**Output Example**:  
```python
>>> _loading_user_privileges_sql('dcc')
['GRANT USAGE ON SCHEMA dcc TO loading_user;',
 'ALTER SCHEMA dcc OWNER TO loading_user;',
 'GRANT SELECT ON ALL TABLES IN SCHEMA dcc TO loading_user;']
```
## FunctionDef _database_privileges_sql(database_name)
**_database_privileges_sql**: The function returns a tuple containing SQL statements that grant CREATE privileges on a specified database to the PEDSnet staff and loading users.  

**parameters**:  
· database_name: The name of the target PostgreSQL database (type: str).  

**Code Description**:  
The function builds a template string `tmpl` that represents a GRANT command: `'grant create on database {db} to {usr}'`. It then formats this template twice, once with `usr='peds_staff'` and once with `usr='loading_user'`, inserting the supplied `database_name` in place of `{db}`. The two formatted strings are placed into a Python tuple, which is returned to the caller.  

In the project, the only caller is `grant_database_permissions` in the same module. That caller obtains the tuple from `_database_privileges_sql`, wraps each SQL string in a `Statement` object, adds them to a `StatementList`, and executes the statements against the database connection. Thus, `_database_privileges_sql` isolates the SQL generation logic, allowing the higher‑level permission‑granting routine to focus on logging, execution, and error handling.  

**Note**:  
- The function does not perform any validation on `database_name`; callers should ensure the name is a valid identifier.  
- The returned tuple is immutable; if additional users need privileges, the function must be updated accordingly.  
- The function assumes the existence of PostgreSQL roles named `peds_staff` and `loading_user`.  

**Output Example**:  
If `database_name` is `'clinical_data'`, the function returns:  

```python
(
    'grant create on database clinical_data to peds_staff',
    'grant create on database clinical_data to loading_user'
)
```
## FunctionDef _database_privileges_sql_limited(database_name, owner)
**_database_privileges_sql_limited**: The function of _database_privileges_sql_limited is to generate the SQL statement(s) required to grant CREATE privileges on a specific PostgreSQL database to a designated role.

**parameters**: The parameters of this Function.  
· database_name: The name of the target database on which the privilege will be granted. Type: `str`.  
· owner (optional): The role (user) that will receive the CREATE privilege. Defaults to `'loading_user'`. Type: `str`.

**Code Description**:  
The function builds a single SQL command using a template string. The template `'grant create on database {db} to {usr}'` is formatted with the supplied `database_name` and `owner` values, producing a concrete `GRANT` statement such as `grant create on database mydb to loading_user`. The formatted string is then placed inside a one‑element tuple and returned. The trailing comma after the closing parenthesis creates a tuple containing exactly one string element.  

In the broader project, this function is invoked by `grant_database_permissions_limited` (found in the same `permissions.py` module). That caller collects the tuple returned by `_database_privileges_sql_limited`, wraps each element in a `Statement` object, adds them to a `StatementList`, and executes the statements against the database connection string. Thus, `_database_privileges_sql_limited` serves as the low‑level SQL generator, while the caller handles execution, logging, and error checking.

**Note**:  
- The function only generates the privilege‑granting SQL; it does **not** execute the statement. Execution responsibility lies with the caller.  
- The returned value is always a tuple with a single string element; callers should unpack or iterate accordingly.  
- Changing the default `owner` value will affect which role receives the CREATE privilege, so ensure the role exists and has appropriate permissions in the PostgreSQL cluster.

**Output Example**:  
```python
result = _database_privileges_sql_limited('clinical_data', owner='etl_user')
# result is:
('grant create on database clinical_data to etl_user',)
```
## FunctionDef _despace(statement)
**_despace**: The function of _despace is to collapse consecutive spaces in a string to a single space.  

**parameters**:  
· statement: a string that may contain runs of one or more space characters.  

**Code Description**:  
The implementation uses Python’s regular‑expression engine (`re.sub`). The pattern `r' +'` matches any sequence of one or more space characters. `re.sub` replaces each matched sequence with a single literal space `' '`. The resulting string, now containing no consecutive spaces, is returned to the caller.  

In the *pedsnetdcc/permissions.py* module, `_despace` is invoked by several helper functions that generate SQL statements from template strings (`_loading_user_privileges_sql`, `_permissions_sql`, `_permissions_sql_limited`, `_vocabulary_permissions_sql`, `_vocabulary_permissions_sql_limited`, `_vocabulary_only_permissions_sql_limited`, `_ids_permissions_sql_limited`). Those functions first substitute placeholder tokens (e.g., `{{.Site}}`, `{{.Owner}}`) in multi‑line SQL templates, then split the resulting text on newline characters. Each line is passed through `_despace` to ensure that any accidental multiple spaces introduced by the templating or replacement process are normalized to a single space before the line is added to the final list of SQL statements. This guarantees that the generated SQL is clean and syntactically consistent.  

**Note**:  
- The function does not trim leading or trailing whitespace; it only reduces internal runs of spaces.  
- It operates only on the space character (`' '`), not on other whitespace such as tabs or newlines.  
- Ensure that the `re` module is imported in the file where `_despace` is defined.  

**Output Example**:  
```python
>>> _despace('SELECT  *  FROM   table   WHERE  id =  5;')
'SELECT * FROM table WHERE id = 5;'
```
## FunctionDef _permissions_sql(site)
**_permissions_sql**: The function of _permissions_sql is to generate a list of SQL statements that set the appropriate database permissions for a specified site.

**parameters**:  
· site: a string representing the site name (e.g., `'dcc'` or `'stlouis'`).  

**Code Description**:  
The function begins by loading the permission‑related SQL template stored in the module‑level constant `PERMISSIONS_SQL_TEMPLATE`. It substitutes the placeholder token `{{.Site}}` with the value supplied in the `site` argument, producing a site‑specific SQL script.  

The resulting script is split on newline characters. Each non‑empty line is passed through the helper function `_despace`, which collapses any consecutive spaces into a single space. The cleaned lines are collected into the list `statements`.  

If the supplied `site` is anything other than `'dcc'`, the function also processes a second template, `ID_MAPS_SQL_TEMPLATE`. This template contains additional statements required for non‑DCC sites. The same placeholder replacement and line‑by‑line despace processing are applied, and the resulting statements are appended to the original `statements` list.  

Finally, the complete list of cleaned SQL statements is returned to the caller.  

From a functional perspective, `_permissions_sql` is invoked by `grant_schema_permissions` (found in the same module). `grant_schema_permissions` iterates over a list of sites, calls `_permissions_sql` for each site, wraps each returned string in a `Statement` object, and then executes the statements against a database connection. The only direct callee of `_permissions_sql` is `_despace`, which ensures the generated SQL lines are syntactically tidy. The function relies on the external constants `PERMISSIONS_SQL_TEMPLATE` and `ID_MAPS_SQL_TEMPLATE` for its template content.

**Note**:  
- The `site` argument must be a non‑empty string; the function does not perform explicit validation beyond the placeholder replacement.  
- Although the docstring mentions a `ValueError`, the current implementation does not raise this exception; callers should ensure that a valid site name is provided.  
- The function only normalizes spaces; it does not trim leading or trailing whitespace, nor does it modify other whitespace characters such as tabs.  
- If additional site‑specific templates are added in the future, they should follow the same `{{.Site}}` placeholder convention to be compatible with this function.

**Output Example**:  
```python
>>> _permissions_sql('stlouis')
[
    'GRANT USAGE ON SCHEMA stlouis TO some_user;',
    'GRANT SELECT ON ALL TABLES IN SCHEMA stlouis TO some_user;',
    'INSERT INTO stlouis.id_maps (src_id, dst_id) VALUES (1, 100);',
    '... additional cleaned SQL lines ...'
]
```
## FunctionDef _permissions_sql_limited(site, owner, id_name)
**_permissions_sql_limited**: The function of _permissions_sql_limited is to generate a list of SQL statements that set limited permissions for a specified site, owner, and identifier name.

**parameters**:  
· site: the name of the site for which permissions are being generated (e.g., `'dcc'` or `'stlouis'`).  
· owner: the database role that should receive the permissions; defaults to `'loading_user'`.  
· id_name: the identifier name used in the permission templates; defaults to `'dcc'`.

**Code Description**:  
The function first selects the appropriate SQL template based on the value of *id_name*.  
- If *id_name* equals `'dcc'`, it uses the constant `PERMISSIONS_SQL_TEMPLATE_LIMITED`.  
- Otherwise it uses `ID_MAPS_SQL_TEMPLATE_ID_NAME_LIMITED`.  

Place‑holder tokens `{{.Site}}`, `{{.Owner}}`, and optionally `{{.IdName}}` are replaced with the supplied arguments. The resulting multi‑line string is split on newline characters, and each non‑empty line is passed through the helper `_despace` to collapse consecutive spaces into a single space. The cleaned lines are collected into the list *statements*.

A second block of logic adds additional “id‑maps” statements when the site is neither `'dcc'` nor equal to *id_name*. The appropriate id‑maps template (`ID_MAPS_SQL_TEMPLATE_LIMITED` or `ID_MAPS_SQL_TEMPLATE_ID_NAME_LIMITED`) is selected, placeholders are substituted, and the cleaned lines are appended to *statements*.

Finally, the function returns the complete list of SQL statement strings.  

**Reference relationship**:  
- **Caller**: `grant_schema_permissions_limited` iterates over a list of sites and calls `_permissions_sql_limited` for each site to build a `StatementList` that is later executed against the database.  
- **Callee**: `_despace` is invoked to normalise whitespace in each generated SQL line. The function also depends on several module‑level template constants (`PERMISSIONS_SQL_TEMPLATE_LIMITED`, `ID_MAPS_SQL_TEMPLATE_LIMITED`, `ID_MAPS_SQL_TEMPLATE_ID_NAME_LIMITED`) that contain the raw SQL with placeholder tokens.

**Note**:  
- The function does not trim leading or trailing whitespace; it only reduces internal runs of spaces.  
- If *site* equals *id_name* and *id_name* is not `'dcc'`, the function returns an empty list because no permission statements are required for that configuration.  
- The returned list may be empty when the site is `'dcc'` and *id_name* differs, as the second block adds only id‑maps statements for non‑dcc sites.  
- Ensure that the required template constants are defined in the module; otherwise a `NameError` will be raised.

**Output Example**:  
```python
>>> _permissions_sql_limited('stlouis', owner='loading_user', id_name='dcc')
[
    'GRANT SELECT ON SCHEMA stlouis TO loading_user;',
    'GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA stlouis TO loading_user;',
    'ALTER DEFAULT PRIVILEGES IN SCHEMA stlouis GRANT SELECT ON TABLES TO loading_user;',
    'INSERT INTO id_maps (site, id) SELECT ...'  # example id‑maps statement
]
```
## FunctionDef _vocabulary_permissions_sql
**_vocabulary_permissions_sql**: The function of _vocabulary_permissions_sql is to generate a list of cleaned SQL statement fragments for vocabulary‑related permissions.  

**parameters**: This function does not accept any parameters.  

**Code Description**:  
The function accesses the module‑level constant `VOCABULARY_PERMISSIONS_SQL_TEMPL`, which holds a multi‑line SQL template string. It assigns this template to the local variable `sql`. The template is then split into individual lines using the newline character (`"\n"`). For each resulting line, the function applies `_despace` to collapse any consecutive space characters into a single space. Empty lines (those that evaluate to false after the split) are filtered out by the `if x` condition in the list comprehension. The final result is a list containing each non‑empty, space‑normalized line of the original template.  

From a functional perspective, `_vocabulary_permissions_sql` is a helper that prepares raw SQL text for execution. It is invoked by `grant_vocabulary_permissions`, which wraps each returned line in a `Statement` object, aggregates them into a `StatementList`, and then executes the statements against a database connection. The only internal dependency of `_vocabulary_permissions_sql` is the `_despace` function, which performs the space‑normalization step.  

**Note**:  
- The function relies on the existence of the constant `VOCABULARY_PERMISSIONS_SQL_TEMPL`; if this constant is missing or not a string, a runtime error will occur.  
- `_despace` removes only duplicate space characters; it does not trim leading or trailing whitespace, nor does it affect tabs or other whitespace characters.  
- Because the function returns a list of strings, callers must handle the conversion to executable SQL statements (e.g., by wrapping each string in a `Statement` object as done in `grant_vocabulary_permissions`).  

**Output Example**:  
Assuming `VOCABULARY_PERMISSIONS_SQL_TEMPL` contains:

```
GRANT SELECT ON {{.Schema}}.vocab_table TO {{.User}};
GRANT INSERT ON {{.Schema}}.vocab_table TO {{.User}};
```

Calling `_vocabulary_permissions_sql()` would produce:

```
[
    'GRANT SELECT ON {{.Schema}}.vocab_table TO {{.User}};',
    'GRANT INSERT ON {{.Schema}}.vocab_table TO {{.User}};'
]
```
## FunctionDef _vocabulary_permissions_sql_limited(owner)
**_vocabulary_permissions_sql_limited**: The function of _vocabulary_permissions_sql_limited is to generate a list of SQL statement fragments for granting limited vocabulary permissions, with the owner placeholder substituted and excess spaces collapsed.

**parameters**:  
· owner: string (default `'loading_user'`) – the database role that will be inserted into the SQL template in place of the `{{.Owner}}` placeholder.

**Code Description**:  
1. The function retrieves the SQL template stored in the module‑level constant `VOCABULARY_PERM_SQL_TEMPL_LIMITED`. This template is a multi‑line string that contains the placeholder token `{{.Owner}}`.  
2. It replaces every occurrence of `{{.Owner}}` in the template with the value supplied by the `owner` argument using `str.replace`.  
3. The resulting string is split on newline characters (`"\n"`), producing an iterable of individual lines.  
4. Each line is passed through the helper function `_despace`. `_despace` collapses any consecutive space characters within the line to a single space, ensuring the generated SQL is free of accidental double‑spacing introduced by the templating process.  
5. A list comprehension filters out any empty strings that may result from blank lines in the template. The final list, containing cleaned, non‑empty SQL lines, is returned to the caller.

**Reference Relationships**:  
- **Callee**: `_despace` – used to normalise spacing of each SQL line.  
- **Caller**: `grant_vocabulary_permissions_limited` – obtains the list returned by `_vocabulary_permissions_sql_limited`, wraps each line in a `Statement` object, and executes the statements to grant the limited vocabulary permissions.

**Note**:  
- The function does not trim leading or trailing whitespace; it only reduces internal runs of spaces.  
- If the template contains other placeholder tokens (e.g., `{{.Site}}`), they are not processed by this function and will remain unchanged.  
- The caller must ensure that the `owner` argument matches an existing database role; otherwise the generated SQL will fail at execution time.  
- The function assumes that `VOCABULARY_PERM_SQL_TEMPL_LIMITED` is defined elsewhere in the module as a valid multi‑line SQL template.

**Output Example**:  
Assuming `VOCABULARY_PERM_SQL_TEMPL_LIMITED` contains:

```
GRANT SELECT ON vocab_table TO {{.Owner}};
GRANT INSERT ON vocab_table TO {{.Owner}};
```

Calling `_vocabulary_permissions_sql_limited('research_user')` would return:

```
[
    'GRANT SELECT ON vocab_table TO research_user;',
    'GRANT INSERT ON vocab_table TO research_user;'
]
```
## FunctionDef _vocabulary_only_permissions_sql_limited(owner)
**_vocabulary_only_permissions_sql_limited**: The function of _vocabulary_only_permissions_sql_limited is to generate a list of cleaned SQL statements for granting limited vocabulary‑only permissions, inserting the specified owner into a predefined SQL template.

**parameters**:  
· owner: a string that identifies the database role to be used as the owner in the generated SQL. The default value is `'loading_user'`.

**Code Description**:  
1. The function retrieves the raw SQL template from the module‑level constant `VOCABULARY_ONLY_PERM_SQL_TEMPL_LIMITED`. This constant contains a multi‑line SQL script with a placeholder token `{{.Owner}}`.  
2. It replaces every occurrence of the placeholder `{{.Owner}}` with the value supplied in the `owner` argument using the standard string `replace` method. This produces a concrete SQL script where the owner role is explicitly set.  
3. The resulting script is split on newline characters (`"\n"`), yielding an iterable of individual lines. Empty lines are discarded by the `if x` filter in the list comprehension.  
4. Each non‑empty line is passed to the helper function `_despace`. `_despace` collapses any consecutive space characters within the line to a single space, ensuring that the generated SQL is free of accidental extra spacing that could arise from template formatting or the replacement step.  
5. The list comprehension collects the cleaned lines into a Python list, which is returned to the caller.

**Reference Relationships**:  
- **Caller**: `grant_vocabulary_only_permissions_limited` invokes `_vocabulary_only_permissions_sql_limited` to obtain the list of SQL statements that it subsequently wraps in `Statement` objects and executes against the target database.  
- **Callee**: `_despace` is called for each line of the generated script to normalise spacing. No other functions are directly involved.

**Note**:  
- The function assumes that `VOCABULARY_ONLY_PERM_SQL_TEMPL_LIMITED` is defined elsewhere in the module and contains the `{{.Owner}}` placeholder. If the placeholder is missing, the replacement step will have no effect.  
- Only space characters are normalised; tabs, newlines, or other whitespace are left unchanged.  
- The returned list contains each SQL clause as a separate string; callers must handle statement termination (e.g., adding semicolons) if required by the execution layer.  
- The default owner `'loading_user'` should correspond to a valid database role; otherwise, the generated SQL may fail at execution time.

**Output Example**:  
```python
>>> _vocabulary_only_permissions_sql_limited('etl_user')
[
    'GRANT SELECT ON vocab.concept TO etl_user;',
    'GRANT SELECT ON vocab.concept_relationship TO etl_user;',
    'GRANT SELECT ON vocab.vocabulary TO etl_user;'
]
```
## FunctionDef _ids_permissions_sql_limited(owner, id_name)
**_ids_permissions_sql_limited**: The function of _ids_permissions_sql_limited is to generate a list of cleaned SQL statement fragments for granting limited permissions on ID tables, based on a template and supplied owner and identifier name.

**parameters**:  
· owner (str, optional): The database role that will own the granted permissions. Defaults to `'loading_user'`.  
· id_name (str, optional): The base name of the identifier tables (e.g., `'dcc'`). Defaults to `'dcc'`.

**Code Description**:  
The function begins by loading a pre‑defined SQL template string named `IDS_PERM_SQL_TEMPL_LIMITED`. This template contains placeholder tokens `{{.Owner}}` and `{{.IdName}}` that represent, respectively, the role that should receive the permissions and the identifier table prefix.  

1. `sql = IDS_PERM_SQL_TEMPL_LIMITED` – copies the template into a local variable.  
2. `sql = sql.replace('{{.Owner}}', owner)` – substitutes the `{{.Owner}}` token with the value supplied in the `owner` argument.  
3. `sql = sql.replace('{{.IdName}}', id_name)` – substitutes the `{{.IdName}}` token with the value supplied in the `id_name` argument.  

After the two replacements, the template becomes a concrete multi‑line SQL script. The function then splits this script on newline characters: `sql.split("\n")`. For each resulting line, it discards empty strings (the `if x` filter) and passes the line through the helper `_despace`. `_despace` collapses any consecutive spaces inside the line to a single space, ensuring that the generated SQL is free of accidental double‑spacing that may have been introduced by the templating process.  

Finally, the list comprehension collects all cleaned, non‑empty lines and returns them as a Python list. The return type is therefore `list[str]`, where each element is a single, space‑normalized SQL statement fragment ready for execution.

**Reference Relationships**:  
- **Callee**: `_despace` – used to normalize spacing in each line of the generated SQL.  
- **Caller**: `grant_ids_permissions_limited` – this higher‑level function builds a `StatementList`, converts each line returned by `_ids_permissions_sql_limited` into a `Statement` object, and executes the statements against a database connection. Thus, `_ids_permissions_sql_limited` supplies the raw SQL fragments that `grant_ids_permissions_limited` ultimately runs.  

**Note**:  
- The function does not trim leading or trailing whitespace; it only reduces internal runs of spaces.  
- It assumes that `IDS_PERM_SQL_TEMPL_LIMITED` is defined elsewhere in the module and contains the exact placeholder tokens `{{.Owner}}` and `{{.IdName}}`.  
- If the template contains additional placeholders, they will remain unchanged because this function only replaces the two mentioned tokens.  
- The returned list preserves the original line order from the template, which may be important for statement sequencing.  

**Output Example**:  
Assuming `IDS_PERM_SQL_TEMPL_LIMITED` contains:

```
GRANT SELECT ON {{.IdName}}_ids TO {{.Owner}};
GRANT INSERT, UPDATE ON {{.IdName}}_ids TO {{.Owner}};
```

Calling `_ids_permissions_sql_limited(owner='research_user', id_name='omop')` would produce:

```python
[
    'GRANT SELECT ON omop_ids TO research_user;',
    'GRANT INSERT, UPDATE ON omop_ids TO research_user;'
]
```
## FunctionDef grant_loading_user_permissions(conn_str, inc_external, owner)
**grant_loading_user_permissions**: The function of grant_loading_user_permissions is to grant the appropriate PostgreSQL privileges to the `loading_user` role for all site schemas (and optionally external site schemas) after a database has been created or prepared.

**parameters**:  
· conn_str (str): libpq‑style connection string that identifies the target database.  
· inc_external (bool, optional): when True, permissions are also granted for external site schemas defined in `SITES_EXTERNAL_ADD_DCC`; when False (default) only the core sites in `SITES_AND_DCC` are processed.  
· owner (str, optional): name of the role that should own the schemas; defaults to `'loading_user'`.

**Code Description**:  
1. **Logging start of operation** – The function extracts connection metadata with `get_conn_info_dict(conn_str)` and writes an INFO log entry indicating that the granting process is beginning. The start time is recorded with `time.time()` for later elapsed‑time reporting.  
2. **Prepare a StatementList** – An empty `StatementList` instance (`stmnts`) is created to hold the individual GRANT/ALTER statements that will be executed.  
3. **Select site list** –  
   * If `inc_external` is True, `site_list` is set to the constant `SITES_EXTERNAL_ADD_DCC`.  
   * Otherwise, `site_list` is set to `SITES_AND_DCC`.  
   These constants contain the identifiers of the schemas that require permission changes.  
4. **Generate SQL for each site** – For every `site` in `site_list` the helper `_loading_user_privileges_sql(site, owner)` is called. This helper returns a list of raw SQL strings (GRANT USAGE, ALTER SCHEMA OWNER, GRANT SELECT, etc.) with the placeholders `{{.Site}}` and `{{.Owner}}` replaced by the actual `site` and `owner` values.  
5. **Wrap SQL in Statement objects** – Each SQL string returned by the helper is wrapped in a `Statement` object (`Statement(x)`). The resulting `Statement` objects are added to `stmnts` via `extend`. This step creates a collection of executable, self‑describing statements.  
6. **Execute statements serially** – `stmnts.serial_execute(conn_str)` is called. The `serial_execute` method of `StatementList` runs each `Statement` in order, opening a separate connection for each statement because the default `transaction=False` is used. This is required because many GRANT/ALTER commands cannot be executed inside a single transaction block.  
7. **Error checking** – After execution, the function iterates over the `Statement` objects in `stmnts` and invokes `check_stmt_err(stmnt, 'granting loading permissions')`. `check_stmt_err` inspects the `err` attribute of each statement; if an error is present it logs the failure and raises a `DatabaseError`, causing the whole permission‑granting step to abort.  
8. **Logging completion** – An INFO log entry is emitted with the message “finished granting of loading permissions” together with the elapsed time calculated by `secs_since(start_time)`. The same connection metadata dictionary is merged into the log entry via `combine_dicts`.  
9. **Return value** – The function does not return a value; its purpose is side‑effectful (granting privileges) and any failure is propagated as an exception.

**Reference relationships**:  
* **Callees** –  
  * `get_conn_info_dict` (pedsnetdcc.utils) – parses the connection string for logging.  
  * `combine_dicts` (pedsnetdcc.utils) – merges static log messages with connection metadata.  
  * `_loading_user_privileges_sql` (same module) – builds the list of privilege‑granting SQL statements for a given site and owner.  
  * `Statement` and `StatementList` (pedsnetdcc.db) – encapsulate individual SQL commands and provide ordered batch execution.  
  * `check_stmt_err` (pedsnetdcc.utils) – validates each executed statement and raises an exception on error.  
  * `secs_since` (pedsnetdcc.dict_logging) – formats the elapsed time for the final log entry.  

* **Callers** –  
  * `prepare_database` (pedsnetdcc.prepdb) – after creating schemas, calls `grant_loading_user_permissions(new_conn_str, True)` to grant permissions for both core and external sites.  
  * `prepare_database_altname` (pedsnetdcc.prepdb) – conditionally calls `grant_loading_user_permissions(new_conn_str, inc_ext, owner)` when the database is not limited‑permission mode.  
  * `grant_permissions` (pedsnetdcc.main) – command‑line entry point that invokes `grant_loading_user_permissions(conn_str)` when the `--full` flag is supplied.  

These relationships show that `grant_loading_user_permissions` is a central step in the database preparation workflow, ensuring that the `loading_user` role can read and manipulate data across all relevant schemas.

**Note**:  
* The function always executes statements without a surrounding transaction (`transaction=False`). This is intentional because PostgreSQL does not allow many GRANT/ALTER commands inside a transaction block.  
* If `inc_external` is False, external site schemas are omitted; callers must set this flag to True when the target database includes external sites.  
* The `owner` parameter allows the caller to specify a different role to own the schemas (e.g., a custom loading user). The default `'loading_user'` matches the typical workflow.  
* Errors are not silently ignored; any failure in a single GRANT statement triggers `check_stmt_err`, which logs the problem and raises a `DatabaseError`. Callers should be prepared to handle this exception.  
* Logging includes both connection metadata and elapsed time, providing traceability for audit and debugging purposes.  
* The function does not return a value; success is indicated by the absence of an exception.
## FunctionDef grant_database_permissions(conn_str, database_name)
**grant_database_permissions**: The function of grant_database_permissions is to grant CREATE privileges on a newly created PostgreSQL database to the standard PEDSnet roles.

**parameters**:  
· conn_str: libpq‑style connection string used to connect to the PostgreSQL server (type: str).  
· database_name: name of the database on which the privileges will be granted (type: str).

**Code Description**:  
1. The function begins by extracting connection‑metadata from the supplied `conn_str` using `get_conn_info_dict`. The resulting dictionary (`log_dict`) contains keys such as `user`, `host`, `port`, `dbname`, and `search_path`.  
2. An informational log entry is written with the message “starting granting of database permissions” together with the connection metadata. The current time is stored in `start_time` for later elapsed‑time reporting.  
3. A `StatementList` instance (`stmnts`) is created to hold the SQL commands that will be executed.  
4. The helper `_database_privileges_sql(database_name)` is called; it returns a two‑element tuple containing the SQL strings:  
   - `grant create on database <database_name> to peds_staff`  
   - `grant create on database <database_name> to loading_user`  
   These strings are wrapped in `Statement` objects (one per SQL) and added to `stmnts` via `extend`.  
5. `stmnts.serial_execute(conn_str)` is invoked. Because the default `transaction` argument is `False`, each `Statement` opens its own connection, executes the GRANT command, and records any result or error inside the `Statement` instance. The statements are executed sequentially, not inside a single transaction, which is required for PostgreSQL DDL commands such as `GRANT`.  
6. After execution, the function iterates over the `Statement` objects in `stmnts` and calls `check_stmt_err(stmnt, 'granting database permissions')`. This utility inspects the `err` attribute of each statement; if an error is present it logs the failure and raises a `DatabaseError`, causing the calling routine to abort.  
7. When all statements have completed without error, a final informational log entry is written with the message “finished granting of database permissions”, the elapsed time calculated by `secs_since(start_time)`, and the same connection metadata.  

**Reference Relationships**:  
- **Callers**: `grant_database_permissions` is invoked from `prepare_database` (pedsnetdcc/prepdb.py) and from `prepare_database_altname` (pedsnetdcc/prepdb.py). In both cases it is called immediately after the `CREATE DATABASE` statement(s) have been executed, ensuring that the new database is ready for further schema creation.  
- **Callees**:  
  * `_database_privileges_sql` – generates the specific GRANT statements for the target database.  
  * `Statement` – encapsulates each GRANT command, providing execution metadata and error handling.  
  * `StatementList.serial_execute` – runs the list of `Statement` objects in order, without a transaction.  
  * `check_stmt_err` – validates that each statement succeeded and raises an exception on failure.  
  * Utility functions `get_conn_info_dict`, `combine_dicts`, and `secs_since` are used for logging and elapsed‑time calculation.  
  * The module‑level `logger` records start, end, and error information.  

**Note**:  
- The GRANT commands are executed outside a transaction (`transaction=False`) because PostgreSQL does not allow certain DDL statements inside a transaction block.  
- If the database name is invalid or the required roles (`peds_staff`, `loading_user`) do not exist, the corresponding `Statement` will capture the error and `check_stmt_err` will raise a `DatabaseError`. Callers should be prepared to handle this exception.  
- The function does not modify the connection string; it only uses it to open temporary connections for each GRANT statement.  
- Logging includes both the start and finish messages with connection metadata, which aids troubleshooting in multi‑site or automated deployment environments.  
- No return value is provided; success is indicated by the absence of an exception.  
## FunctionDef grant_database_permissions_limited(conn_str, database_name, owner)
**grant_database_permissions_limited**: The function of grant_database_permissions_limited is to grant CREATE privileges on a newly created PostgreSQL database to a specified role, using the limited‑privilege SQL generator.

**parameters**: The parameters of this Function.  
· conn_str: connection string to the PostgreSQL server (libpq format).  
· database_name: name of the database on which the privileges are to be granted.  
· owner: role that will receive the CREATE privilege; defaults to `'loading_user'`.

**Code Description**:  
1. The function begins by extracting connection‑metadata from *conn_str* via `get_conn_info_dict`. The resulting dictionary (`log_dict`) contains keys such as `user`, `host`, `port`, `dbname`, and `search_path`.  
2. An informational log entry is emitted with the message “starting granting of database permissions” together with the connection metadata. The current wall‑clock time is stored in `start_time` for later elapsed‑time calculation.  
3. A `StatementList` instance (`stmnts`) is created to hold the SQL commands that will be executed.  
4. The helper `_database_privileges_sql_limited(database_name, owner)` is called. This helper returns a one‑element tuple containing the SQL string `grant create on database {database_name} to {owner}`. Each element of the tuple is wrapped in a `Statement` object (`Statement(x)`) and added to the `StatementList` via `extend`.  
5. The `StatementList.serial_execute(conn_str)` method is invoked. Because the default `transaction` argument is `False`, each `Statement` opens its own connection, executes the GRANT command, and stores any result or error in its attributes (`err`, `rowcount`, etc.).  
6. After execution, the function iterates over the statements in `stmnts` and calls `check_stmt_err(stmnt, 'granting database permissions')`. If any statement has a non‑null `err` attribute, `check_stmt_err` logs the error and raises a `DatabaseError`, causing the function to abort.  
7. When all statements have succeeded, a final informational log entry is written with the message “finished granting of database permissions”. The elapsed time is calculated by `secs_since(start_time)` and added to the log dictionary.  

**Reference Relationships**  
- **Callers**: The function is invoked from `prepare_database_altname` (in `pedsnetdcc/prepdb.py`) when the `limit` flag is true. In that context it follows the creation of the database and precedes the creation of schemas, ensuring that only the minimal CREATE privilege is granted to the loading user.  
- **Callees**:  
  * `get_conn_info_dict` – parses the connection string for logging.  
  * `combine_dicts` – merges static log messages with connection metadata.  
  * `_database_privileges_sql_limited` – generates the GRANT SQL statement.  
  * `Statement` – wraps each SQL string for execution and error handling.  
  * `StatementList.serial_execute` – runs the statements sequentially without a transaction.  
  * `check_stmt_err` – validates that each statement executed without error and raises an exception if needed.  
  * `secs_since` – formats the elapsed time for the final log entry.  

**Note**:  
- The function only grants the `CREATE` privilege; it does not grant other privileges such as CONNECT, TEMP, or USAGE.  
- Because each `Statement` opens its own connection, the GRANT commands are executed outside of a transaction block, which is required for certain DDL statements in PostgreSQL.  
- Errors are not caught inside the function; they are propagated by `check_stmt_err`. Callers must be prepared to handle `DatabaseError` exceptions.  
- The default owner `'loading_user'` must exist in the PostgreSQL cluster; otherwise the GRANT statement will fail.  
- The function relies on the logging infrastructure (`logger`) to provide structured log output; ensure that the logger is configured appropriately in the application.
## FunctionDef grant_schema_permissions(conn_str, inc_external)
**grant_schema_permissions**: The function of grant_schema_permissions is to apply the appropriate PostgreSQL schema‑ and table‑level GRANT statements for all PEDSnet sites (and optionally external sites) on a target database.

**parameters**  
· conn_str: a libpq‑style PostgreSQL connection string that identifies the database to which permissions will be granted.  
· inc_external (optional, default False): a Boolean flag indicating whether permission statements for external sites should be included (True) or only the core DCC and internal sites (False).

**Code Description**  
1. **Connection metadata extraction** – The function calls `get_conn_info_dict(conn_str)` to parse the connection string and obtain a dictionary containing user, host, port, database name and search_path. This dictionary is stored in `log_dict` and later merged into log messages.  

2. **Start‑up logging** – A structured log entry is emitted with the message “starting granting of schema permissions” together with the connection metadata. The current wall‑clock time is saved in `start_time` for later elapsed‑time reporting.  

3. **Statement collection** – An empty `StatementList` instance (`stmnts`) is created. The list of sites to process is chosen based on the `inc_external` flag:  
   * If `inc_external` is True, the constant `SITES_EXTERNAL_ADD_DCC` (which includes DCC plus all external sites) is used.  
   * Otherwise, `SITES_AND_DCC` (DCC plus the regular internal sites) is used.  

   For each site name in the selected list, the helper `_permissions_sql(site)` is called. `_permissions_sql` returns a list of raw SQL strings that implement the required GRANT commands for that site. Each SQL string is wrapped in a `Statement` object and added to the `StatementList` via `extend`. At the end of the loop, `stmnts` contains a `Statement` for every permission‑granting command that must be executed.  

4. **Serial execution** – `stmnts.serial_execute(conn_str)` is invoked. Because the default `transaction` argument of `StatementList.serial_execute` is False, each `Statement` opens its own connection, runs its SQL, and closes the connection. This non‑transactional mode is required because many GRANT statements cannot be executed inside a PostgreSQL transaction block.  

5. **Error handling** – After execution, the function iterates over the `Statement` objects in `stmnts` and calls `check_stmt_err(stmnt, 'granting schema permissions')`. `check_stmt_err` inspects the `err` attribute of each statement; if an error is present it logs a detailed error message and raises a `DatabaseError`. This guarantees that any failure aborts the permission‑granting step and propagates the exception to the caller.  

6. **Completion logging** – When all statements have succeeded, a final log entry is written. The entry combines the message “finished granting of schema permissions”, the elapsed time calculated by `secs_since(start_time)`, and the original connection metadata.  

**Reference Relationships**  
*Callers* – `grant_schema_permissions` is invoked by several high‑level workflows:  
  • `grant_permissions` in `pedsnetdcc/main.py` (used by the CLI command `grant_permissions`).  
  • `prepare_database` and `prepare_database_altname` in `pedsnetdcc/prepdb.py` during the initial database creation and schema setup.  
  • `run_transformation` in `pedsnetdcc/transform_runner.py` after a transformation has renamed schemas, to re‑apply the correct permissions on the new schema layout.  

*Callees* – The function relies on the following utilities and objects:  
  • `get_conn_info_dict` (pedsnetdcc/utils.py) – parses the connection string for logging.  
  • `combine_dicts` (pedsnetdcc/utils.py) – merges static log dictionaries with connection metadata.  
  • `secs_since` (pedsnetdcc/dict_logging.py) – formats elapsed seconds for log output.  
  • `StatementList` and `Statement` (pedsnetdcc/db.py) – encapsulate individual GRANT SQL commands and execute them in order.  
  • `_permissions_sql` (same module) – generates the site‑specific GRANT statements from template strings (`PERMISSIONS_SQL_TEMPLATE` and, for non‑DCC sites, `ID_MAPS_SQL_TEMPLATE`).  
  • `check_stmt_err` (pedsnetdcc/utils.py) – validates each statement’s execution result and raises an exception on failure.  
  • Module‑level `logger` – provides structured logging throughout the function.  

**Note**  
- The function deliberately executes statements without a surrounding transaction because PostgreSQL rejects many DDL/GRANT commands inside a transaction block.  
- If `inc_external` is True, permission statements for external sites are added; callers must ensure that the external site schemas already exist, otherwise the GRANT statements will fail.  
- All errors are surfaced via `DatabaseError` raised by `check_stmt_err`; callers should be prepared to catch this exception to perform cleanup or retry logic.  
- The function does not return a value; its success is indicated by the absence of an exception.  
- Logging is performed at INFO level for start and finish messages and at DEBUG level inside the lower‑level `Statement` execution; error conditions are logged at ERROR level by `check_stmt_err`.  
## FunctionDef grant_schema_permissions_limited(conn_str, inc_external, owner, id_name, sites)
**grant_schema_permissions_limited**: The function of **grant_schema_permissions_limited** is to grant a limited set of schema‑level and table‑level privileges to a specified database role for one or more sites, optionally including external sites.

**parameters**:  
· **conn_str** – PostgreSQL libpq connection string used to open a database connection.  
· **inc_external** – Boolean flag; when True the list of sites includes the external sites defined in `SITES_EXTERNAL_ADD_DCC`, otherwise only the core sites (`SITES_AND_DCC`).  
· **owner** – Name of the database role that will receive the permissions (default `'loading_user'`).  
· **id_name** – Identifier name used in the permission templates (default `'dcc'`).  
· **sites** – Tuple of site names to which permissions should be granted explicitly. If empty, the function builds the site list from `inc_external` as described above.

**Code Description**:  
1. **Connection metadata extraction** – The function calls `get_conn_info_dict(conn_str)` to parse the connection string and obtain a dictionary containing `user`, `host`, `port`, `dbname`, and `search_path`. This dictionary is used for structured logging.  
2. **Start logging** – An informational log entry is emitted with the message *“starting granting of schema permissions”* together with the connection metadata. The current epoch time is stored in `start_time` for later elapsed‑time reporting.  
3. **Statement collection** – A `StatementList` instance (`stmnts`) is created to hold the SQL commands that will be executed.  
4. **Determine target sites** –  
   * If the caller supplied a non‑empty `sites` tuple, that tuple is used directly.  
   * If `sites` is empty, the function selects a site list based on `inc_external`:  
     – `inc_external=True` → `site_list = SITES_EXTERNAL_ADD_DCC` (core sites plus external sites).  
     – `inc_external=False` → `site_list = SITES_AND_DCC` (core sites plus the DCC schema).  
5. **Generate permission statements** – For each `site` in `site_list`, the helper `_permissions_sql_limited(site, owner, id_name)` is invoked. This helper returns a list of raw SQL strings that implement the limited permission set for the given site, owner, and identifier name. Each SQL string is wrapped in a `Statement` object (`Statement(x)`) and added to `stmnts`.  
6. **Execute statements** – `stmnts.serial_execute(conn_str)` runs all collected statements sequentially. The execution is **non‑transactional** (the default `transaction=False`), because GRANT statements cannot be executed inside a transaction block in PostgreSQL.  
7. **Error checking** – After execution, the function iterates over each `Statement` in `stmnts` and calls `check_stmt_err(stmnt, 'granting schema permissions')`. If any statement encountered an error, `check_stmt_err` logs the failure and raises a `DatabaseError`, causing the whole function to abort.  
8. **Finish logging** – An informational log entry records the completion of the permission‑granting step, including the elapsed time calculated by `secs_since(start_time)`.  

**Reference relationships**:  
* **Callers** –  
  * `prepare_database_altname` (in `pedsnetdcc/prepdb.py`) invokes `grant_schema_permissions_limited` when the `limit` flag is set, passing the newly created database connection string, the `inc_ext` flag, the owner role, and the appropriate site collections (`primary_sites` and `alt_sites`).  
  * `run_transformation` (in `pedsnetdcc/transform_runner.py`) calls `grant_schema_permissions_limited` after moving tables, when the transformation is executed with the `limit` option. It supplies the transformed database connection string, `inc_external=False`, the owner, the `id_name`, and a single‑element tuple containing the current site.  
* **Callees** –  
  * `get_conn_info_dict` – extracts connection details for logging.  
  * `_permissions_sql_limited` – builds the list of SQL GRANT statements specific to each site and permission scope.  
  * `Statement` – wraps each generated SQL string for execution and result tracking.  
  * `StatementList.serial_execute` – runs the statements in order without a transaction.  
  * `check_stmt_err` – validates each statement’s `err` attribute and raises a `DatabaseError` on failure.  
  * `secs_since` – formats the elapsed time for the final log entry.  

**Note**:  
* The function deliberately uses non‑transactional execution because PostgreSQL does not allow `GRANT` commands inside a transaction block.  
* If `sites` is provided, it overrides the `inc_external` logic; callers must ensure the tuple contains valid site identifiers that exist in the database.  
* Permission templates generated by `_permissions_sql_limited` differ depending on the value of `id_name`. When `id_name` is not `'dcc'`, additional “id‑maps” statements may be added to the list.  
* All errors are propagated as `DatabaseError` via `check_stmt_err`; callers should be prepared to catch this exception if they need custom cleanup.  
* Logging is performed with the module‑level `logger`; the log dictionaries always contain connection metadata, which aids troubleshooting in multi‑site environments.
## FunctionDef grant_vocabulary_permissions(conn_str)
**grant_vocabulary_permissions**: The function of grant_vocabulary_permissions is to apply a predefined set of GRANT statements that give the appropriate privileges on all vocabulary‑related tables and schemas to the loading user (or other designated role).

**parameters**  
· conn_str: a libpq‑style PostgreSQL connection string that identifies the target database and the user credentials to be used for executing the permission‑granting SQL commands.

**Code Description**  
The function begins by extracting connection‑metadata from the supplied `conn_str` using `get_conn_info_dict`. The resulting dictionary (`log_dict`) is merged with a static message indicating the start of the permission‑granting step and written to the module logger via `logger.info`. The current wall‑clock time is saved in `start_time` for later elapsed‑time reporting.

A `StatementList` instance (`stmnts`) is created to hold the individual SQL commands. The helper `_vocabulary_permissions_sql()` returns a list of cleaned SQL fragments that constitute the vocabulary GRANT statements (the fragments are derived from the module‑level template `VOCABULARY_PERMISSIONS_SQL_TEMPL`). Each fragment is wrapped in a `Statement` object and added to the list with `stmnts.extend([...])`.

The list is then executed with `stmnts.serial_execute(conn_str)`. Because the default `transaction` argument of `serial_execute` is `False`, each `Statement` opens its own connection, runs the GRANT command, and closes the connection immediately. This non‑transactional mode is required for DDL/GRANT statements that PostgreSQL does not allow inside an explicit transaction block.

After execution, the function iterates over every `Statement` in `stmnts` and calls `check_stmt_err(stmnt, 'granting vocabulary permissions')`. `check_stmt_err` inspects the `err` attribute of each statement; if an error is present it logs the failure and raises a `DatabaseError`, causing the whole `grant_vocabulary_permissions` call to abort.

Finally, a completion log entry is emitted. The elapsed time is calculated with `secs_since(start_time)` and combined with the original connection‑info dictionary. The log message indicates that the vocabulary permission‑granting step has finished.

**Reference Relationships**  
*Callers* – `grant_vocabulary_permissions` is invoked by several high‑level workflows that need to ensure proper access rights after a database is created or transformed:  
  • `grant_permissions` in `pedsnetdcc/main.py` calls it after schema permissions have been granted.  
  • `prepare_database` and `prepare_database_altname` in `pedsnetdcc/prepdb.py` call it after all site and DCC schemas have been built.  
  • `run_transformation` in `pedsnetdcc/transform_runner.py` re‑executes it after table renaming to restore vocabulary privileges.

*Callees* – Within its body the function relies on:  
  • `get_conn_info_dict` (pedsnetdcc/utils.py) for logging context.  
  • `combine_dicts` (pedsnetdcc/utils.py) to merge dictionaries for log messages.  
  • `logger` (module‑level logger) for informational and error logging.  
  • `secs_since` (pedsnetdcc/dict_logging.py) to format elapsed seconds.  
  • `Statement` and `StatementList` (pedsnetdcc/db.py) to encapsulate and execute each GRANT command.  
  • `_vocabulary_permissions_sql` (pedsnetdcc/permissions.py) to generate the raw SQL strings.  
  • `check_stmt_err` (pedsnetdcc/utils.py) to detect and raise any execution errors.

**Note**  
- The GRANT statements are executed outside a transaction (`transaction=False`). This is intentional because PostgreSQL forbids certain DDL/GRANT commands inside a transaction block.  
- Any error in a single GRANT statement aborts the whole function via `check_stmt_err`, and the error is logged with the caller name “granting vocabulary permissions”.  
- The function does not return a value; its success is indicated by the absence of an exception.  
- Callers should ensure that the database role referenced in the generated SQL (typically the loading user) exists and has the necessary rights to receive the granted privileges.  
- Because each `Statement` opens its own connection, the function may be slower than a transactional batch, but it avoids transaction‑related restrictions and isolates failures to individual statements.
## FunctionDef grant_vocabulary_permissions_limited(conn_str, owner)
**grant_vocabulary_permissions_limited**: The function of grant_vocabulary_permissions_limited is to grant a predefined set of limited vocabulary‑related privileges to a specified database role.

**parameters**: The parameters of this Function.  
· conn_str: a libpq‑style PostgreSQL connection string that identifies the target database.  
· owner: the name of the database role that will receive the limited vocabulary permissions; this string is substituted into the SQL template.

**Code Description**:  
1. The function begins by extracting connection metadata from *conn_str* using **get_conn_info_dict**. The resulting dictionary (host, user, dbname, etc.) is stored in *log_dict* for later logging.  
2. A start‑up log entry is emitted with the message “starting granting of vocabulary permissions”, combined with *log_dict* via **combine_dicts**. This provides a structured log record that includes both the operation description and connection details.  
3. The current epoch time is captured in *start_time* to later compute the elapsed duration of the permission‑granting step.  
4. An empty **StatementList** instance (*stmnts*) is created. The helper **_vocabulary_permissions_sql_limited(owner)** is called; it returns a list of SQL command strings in which the placeholder `{{.Owner}}` has been replaced by the supplied *owner* and whitespace has been normalised.  
5. Each SQL string from the list is wrapped in a **Statement** object (with the default message “executing SQL”) and appended to *stmnts* via *extend*. At this point *stmnts* holds a sequence of executable statements, each representing a single GRANT command.  
6. The statements are executed sequentially by invoking **StatementList.serial_execute(conn_str)**. The execution is performed without an explicit transaction (the default *transaction=False*), which is appropriate for GRANT statements that PostgreSQL may not allow inside a transaction block. Each statement creates its own connection, runs the SQL, and records any result or error internally.  
7. After execution, the function iterates over the statements in *stmnts* and calls **check_stmt_err(stmnt, 'granting vocabulary permissions')** for each. If any statement encountered an error, **check_stmt_err** logs the failure and raises a **DatabaseError**, causing the whole function to abort. Successful statements leave their *err* attribute as *None*.  
8. When all statements have completed without error, a final log entry is written. The message “finished granting of vocabulary permissions” is combined with the elapsed time (computed by **secs_since(start_time)**) and the original connection info dictionary, then emitted via the module logger.  
9. The function returns implicitly *None* after logging; its effect is the modification of database privileges.

**Reference Relationships**:  
- **Callees**:  
  • **_vocabulary_permissions_sql_limited** – supplies the concrete GRANT statements tailored to the *owner* role.  
  • **Statement**, **StatementList** – provide the container and execution mechanisms for the SQL commands.  
  • **get_conn_info_dict**, **combine_dicts**, **secs_since**, **check_stmt_err**, and the module‑level **logger** – support logging, timing, and error handling.  
- **Callers**: The primary caller is **prepare_database_altname** in *pedsnetdcc/prepdb.py*. When the *limit* flag is true and the database being prepared is the “dcc” vocabulary schema, *prepare_database_altname* invokes **grant_vocabulary_permissions_limited** to restrict access to the vocabulary objects for non‑super‑user roles. Other permission‑granting functions in the same module (e.g., **grant_vocabulary_permissions**, **grant_vocabulary_only_permissions_limited**) follow a similar pattern but either grant full privileges or a different subset of permissions.  

**Note**:  
- The *owner* role must already exist in the target database; otherwise the generated GRANT statements will fail and **check_stmt_err** will raise a **DatabaseError**.  
- Execution is deliberately non‑transactional; each GRANT runs in its own autocommit connection, which avoids PostgreSQL errors for statements that cannot be wrapped in a transaction block.  
- The function does not return the list of statements; callers should rely on the logging output and the absence of raised exceptions to confirm success.  
- Because the SQL template is defined elsewhere as **VOCABULARY_PERM_SQL_TEMPL_LIMITED**, any changes to that template (e.g., adding or removing privileges) will automatically affect the behavior of this function without further code modification.  
- Timing information is reported with a zero‑padded four‑character string via **secs_since**, ensuring consistent log formatting across the project.
## FunctionDef grant_vocabulary_only_permissions_limited(conn_str, owner)
**grant_vocabulary_only_permissions_limited**: The function of grant_vocabulary_only_permissions_limited is to grant a restricted set of vocabulary‑only database privileges to a specified role.

**parameters**:  
· conn_str: a libpq‑style PostgreSQL connection string used to open a database connection.  
· owner: the name of the database role that will receive the limited vocabulary permissions.

**Code Description**:  
1. The function extracts connection metadata from *conn_str* by calling **get_conn_info_dict** and stores the result in *log_dict*.  
2. A start‑up log entry is emitted with the message “starting granting of vocabulary permissions”, merging the static message dictionary with *log_dict* via **combine_dicts**.  
3. The current epoch time is saved in *start_time* to later compute elapsed time.  
4. A **StatementList** instance (*stmnts*) is created. The helper **_vocabulary_only_permissions_sql_limited(owner)** generates a list of raw SQL strings that contain the placeholder‑filled GRANT statements for the supplied *owner*. For each SQL string a **Statement** object is instantiated and added to *stmnts* using *extend*.  
5. The entire list of statements is executed sequentially with **StatementList.serial_execute(conn_str)**. The execution is non‑transactional (the default for *serial_execute*), which is appropriate because GRANT commands cannot be run inside a transaction block.  
6. After execution, each *Statement* in *stmnts* is examined by **check_stmt_err(stmnt, 'granting vocabulary permissions')**. If any statement recorded an error, *check_stmt_err* logs the failure and raises a **DatabaseError**, causing the function to abort.  
7. When all statements succeed, a final log entry is written with the message “finished granting of vocabulary permissions” and the elapsed time, calculated by **secs_since(start_time)** and merged with *log_dict*.

**Reference Relationships**:  
- **Callees**:  
  * **get_conn_info_dict** – parses the connection string for logging.  
  * **combine_dicts** – merges dictionaries for structured log messages.  
  * **_vocabulary_only_permissions_sql_limited** – builds the list of GRANT statements specific to limited vocabulary access.  
  * **Statement** – wraps each individual SQL command.  
  * **StatementList** – holds the ordered collection of *Statement* objects and provides *serial_execute*.  
  * **check_stmt_err** – validates each statement’s *err* attribute and raises an exception on failure.  
  * **secs_since** – formats the elapsed time for the final log entry.  
- **Callers**:  
  * **prepare_database_altname** (pedsnetdcc/prepdb.py) invokes this function when the *limit* flag is true and the database being prepared is not the core “dcc” vocabulary.  
  * **run_transformation** (pedsnetdcc/transform_runner.py) calls this function after a transformation when the *limit* flag is set, to re‑grant limited vocabulary permissions on the transformed schema.  

Both callers pass the connection string that points to the target database (either the newly created database in *prepare_database_altname* or the transformed database in *run_transformation*) and the role that should receive the limited permissions.

**Note**:  
- The function does not return a value; its success is indicated by the absence of an exception.  
- All GRANT statements are executed outside a transaction; attempting to run this function with a connection string that points to a read‑only or otherwise restricted database will cause *check_stmt_err* to raise a **DatabaseError**.  
- The *owner* argument must correspond to an existing PostgreSQL role; otherwise the generated SQL will fail at execution time.  
- Because the function relies on *check_stmt_err* for error handling, callers should be prepared to catch **DatabaseError** if they need to perform additional cleanup.  
- Logging is performed at INFO level for start and finish messages and at ERROR level inside *check_stmt_err* when a statement fails. This provides a clear audit trail in the application logs.
## FunctionDef grant_ids_permissions_limited(conn_str, owner, id_name)
**grant_ids_permissions_limited**: The function of grant_ids_permissions_limited is to grant a restricted set of database privileges on the “*_ids” tables to a specified role.

**parameters**:  
· conn_str: PostgreSQL libpq‑style connection string used to open a database connection.  
· owner: name of the database role that will receive the limited permissions (default in the template is “loading_user”).  
· id_name: base name of the identifier tables for which the permissions are being granted (e.g., “dcc”, “omop”).

**Code Description**:  
1. The function begins by extracting connection metadata from the supplied `conn_str` via `get_conn_info_dict`. The resulting dictionary (`log_dict`) contains keys such as `user`, `host`, `port`, `dbname`, and `search_path`.  
2. A start‑up log entry is emitted. `combine_dicts` merges a static message dictionary (`{'msg': 'starting granting of <id>_ids permissions'}`) with the connection‑info dictionary, and the combined payload is sent to the module‑level `logger` at INFO level.  
3. The current epoch time is stored in `start_time` to later compute the elapsed duration of the operation.  
4. An empty `StatementList` instance (`stmnts`) is created. The helper `_ids_permissions_sql_limited(owner, id_name)` is called; it returns a list of cleaned SQL statement strings that implement the limited GRANT commands for the given `owner` and `id_name`. Each string is wrapped in a `Statement` object, and the resulting `Statement` objects are added to the `StatementList` via `extend`.  
5. The `StatementList.serial_execute(conn_str)` call runs each `Statement` sequentially. Because the default `transaction` argument is `False`, each statement opens its own connection, executes the GRANT command, and then closes the connection. This avoids issues with statements that cannot be run inside a transaction block.  
6. After execution, the function iterates over all `Statement` objects in `stmnts`. For each statement, `check_stmt_err(stmnt, 'granting ids permissions')` is invoked. If any statement encountered an error, `check_stmt_err` logs the error and raises a `DatabaseError`, causing the whole function to abort. Successful statements have `err` set to `None` and are simply ignored in this loop.  
7. When all statements have completed without error, a final log entry is written. The message dictionary (`{'msg': 'finished granting of ids permissions', 'elapsed': secs_since(start_time)}`) is merged with the original connection‑info dictionary and logged at INFO level. `secs_since(start_time)` formats the elapsed seconds as a zero‑padded four‑character string.  
8. The function then returns implicitly (`None`), having performed its side‑effect of granting the permissions.

**Reference Relationships**:  
- **Callees**:  
  * `_ids_permissions_sql_limited` – generates the concrete GRANT SQL fragments based on a template.  
  * `Statement` – wraps each SQL fragment for execution and result tracking.  
  * `StatementList` – holds the ordered collection of `Statement` objects and provides the `serial_execute` method.  
  * `get_conn_info_dict` – parses the connection string for logging.  
  * `combine_dicts` – merges static log messages with connection metadata.  
  * `secs_since` – computes the elapsed time for the final log entry.  
  * `check_stmt_err` – validates each statement’s execution result and raises an exception on failure.  
  * `logger` – the module‑level logger used for all informational and error messages.  

- **Callers**:  
  * `prepare_database_altname` (in `pedsnetdcc/prepdb.py`) invokes `grant_ids_permissions_limited` when the database preparation process requires limited ID‑table permissions for an alternate identifier namespace. The caller supplies the new database connection string, the owner role, and the identifier name, then proceeds with further schema creation steps.  
  * No other direct callers are present in the current code base, but any higher‑level routine that needs to apply the limited ID permissions can call this function with appropriate arguments.

**Note**:  
- The function executes GRANT statements without wrapping them in a single transaction; this is intentional because certain GRANT commands may be prohibited inside a transaction block.  
- All SQL fragments are generated from the `IDS_PERM_SQL_TEMPL_LIMITED` template; if the template changes, the granted privileges will change accordingly.  
- Error handling is centralized in `check_stmt_err`; any failure aborts the function and propagates a `DatabaseError`. Callers should be prepared to catch this exception if they need to perform cleanup.  
- Logging includes both the start and finish messages together with connection details, which aids troubleshooting in multi‑site or multi‑database environments.  
- The function does not return a value; its purpose is purely side‑effectual (modifying database permissions).
