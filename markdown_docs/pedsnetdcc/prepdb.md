## FunctionDef _despace(statement)
**_despace**: The function of _despace is to collapse consecutive space characters in a string into a single space.  

**parameters**: The parameters of this Function.  
· statement: a string that may contain runs of one or more space characters.  

**Code Description**:  
The function receives a single argument, *statement*, which is expected to be a textual SQL fragment or any generic string. Inside the function, the `re.sub` call uses the regular expression `r' +'` to match any sequence of one or more space characters. Each matched sequence is replaced with a single literal space `' '`. The result of this substitution is returned to the caller.  

In the surrounding module, `_despace` is employed by two helper functions, `_site_sql` and `_other_sql`. Both of these functions generate multi‑line SQL strings, split them on newline characters, and then apply `_despace` to each non‑empty line. This ensures that the final list of SQL statements contains no superfluous spacing, which can be important for downstream processing, logging, or comparison of generated SQL. By centralizing the space‑normalization logic in `_despace`, the codebase maintains consistent formatting across all generated statements.  

**Note**:  
- The function only normalizes space characters; it does not affect other whitespace such as tabs or newline characters.  
- If the input string contains leading or trailing spaces, they will be reduced to a single leading or trailing space, not stripped entirely.  
- The function relies on the `re` module; ensure that `import re` is present in the module where `_despace` is defined.  

**Output Example**:  
Input: `"SELECT  *  FROM   patients   WHERE  id  =  123;"`  
Return: `"SELECT * FROM patients WHERE id = 123;"`
## FunctionDef _conn_str_with_database(conn_str, dbname)
**_conn_str_with_database**: The function of _conn_str_with_database is to adjust a libpq/psycopg2 connection string so that it points to a specific PostgreSQL database, overriding any existing database specification.

**parameters**:  
· conn_str: The original libpq/psycopg2 connection string (type: str).  
· dbname: The name of the database that should be used in the resulting connection string (type: str).

**Code Description**:  
The function builds a clause `dbname={dbname}` where `{dbname}` is the value passed in the `dbname` argument. It then checks whether the original `conn_str` already contains a `dbname=` fragment.  

- If `dbname=` is present, the function uses a regular‑expression substitution (`re.sub`) to replace the existing database name with the new clause. The pattern `r'dbname=([^ ]+)'` matches the substring starting with `dbname=` followed by any characters up to the next space, ensuring only the database name part is changed.  
- If `dbname=` is not present, the function simply appends a space and the new `dbname` clause to the end of the original connection string.  

Finally, the newly constructed connection string (`new_conn_str`) is returned.

**Relationship with Callers**:  
Both `prepare_database` and `prepare_database_altname` create a new PostgreSQL database using an initial connection string that does **not** specify the target database (it is used only for the `CREATE DATABASE` command). After the database is created, they need to run further SQL statements inside that newly created database. To do this, they call `_conn_str_with_database(conn_str, database_name)` to produce a connection string that points to the new database. The returned string is then used for subsequent `StatementList.serial_execute` calls and for granting permissions.  

In the test suite (`pedsnetdcc/tests/prepdb_test.py`), the helper method `check` also calls `_conn_str_with_database` to obtain a connection string that connects to the test database before introspecting its schemas. This demonstrates that the function is the canonical way within the project to switch the target database of an existing connection string.

**Note**:  
- The function does not validate the format of `conn_str`; it assumes a space‑separated list of key=value pairs as used by libpq.  
- If the original connection string contains multiple `dbname=` fragments (unlikely in normal usage), only the first match will be replaced.  
- No trailing or leading whitespace is trimmed; the caller should ensure `conn_str` is well‑formed.  
- The function relies on the `re` module, which must be imported in the module where this function resides.

**Output Example**:  
```python
original = "host=localhost port=5432 user=loader password=secret"
new = _conn_str_with_database(original, "pedsnet_1_0")
# new == "host=localhost port=5432 user=loader password=secret dbname=pedsnet_1_0"
```
## FunctionDef _sites_and_dcc(dcc_only, inc_external)
**_sites_and_dcc**: The function returns a tuple that always starts with the string ``'dcc'`` and optionally includes the internal site identifiers and, when requested, external site identifiers.

**parameters**:  
· **dcc_only** (bool): When set to ``True`` the function returns a one‑element tuple containing only ``'dcc'``.  
· **inc_external** (bool): When ``True`` (and ``dcc_only`` is ``False``) the function appends the list ``SITES_AND_EXTERNAL`` to the result; otherwise it appends the list ``SITES``.

**Code Description**:  
The function begins by checking the ``dcc_only`` flag.  
- If ``dcc_only`` is ``True``, it short‑circuits and returns the tuple ``('dcc',)``.  
- If ``dcc_only`` is ``False``, the function proceeds to evaluate ``inc_external``.  
  * When ``inc_external`` is ``True``, it constructs a tuple consisting of ``'dcc'`` followed by the contents of the global constant ``SITES_AND_EXTERNAL``. This constant holds the identifiers of all internal sites plus any external sites that should be treated as part of the database preparation process.  
  * When ``inc_external`` is ``False``, it constructs a tuple consisting of ``'dcc'`` followed by the contents of the global constant ``SITES`` (the internal site identifiers only).  

The returned value is always a tuple, which makes it suitable for iteration in calling code.

**Relationship with Callers**:  
- In **prepare_database** (pedsnetdcc/prepdb.py), the function is invoked as ``_sites_and_dcc(dcc_only, True)``. The ``True`` argument forces inclusion of external sites when the caller is not in “dcc‑only” mode. The resulting tuple is iterated over to generate and execute the SQL statements that create the schema for each site (including the DCC schema).  
- In the test suite (**pedsnetdcc/tests/prepdb_test.py**), the function is called as ``_sites_and_dcc(dcc_only=dcc_only)`` without the ``inc_external`` argument, thereby using its default value ``False``. The test builds an expected list of schema names by iterating over the returned site identifiers and appending known suffixes. This verifies that the database preparation logic creates exactly the schemas that the function reports should exist.

**Note**:  
- The function does not perform any validation of the global constants ``SITES`` and ``SITES_AND_EXTERNAL``; it assumes they are defined elsewhere in the module and contain iterable collections of strings.  
- Because the function always returns a tuple, callers can safely unpack or iterate over the result without checking the type.  
- The ``dcc_only`` flag overrides ``inc_external``; when ``dcc_only`` is ``True`` the function never includes any site identifiers, even if ``inc_external`` is also ``True``.

**Output Example**:  
- ``_sites_and_dcc(dcc_only=True)`` → ``('dcc',)``  
- ``_sites_and_dcc(dcc_only=False, inc_external=False)`` → ``('dcc', 'pediatrics', 'pcornet')`` (example where ``SITES = ('pediatrics', 'pcornet')``)  
- ``_sites_and_dcc(dcc_only=False, inc_external=True)`` → ``('dcc', 'pediatrics', 'pcornet', 'external1', 'external2')`` (example where ``SITES_AND_EXTERNAL`` extends ``SITES`` with external site names).
## FunctionDef _version_to_shorthand(version)
**_version_to_shorthand**: The function converts a version string of the form “X.Y” or “X.Y.Z” into a compact two‑character shorthand “XY”.

**parameters**:  
· version: a string representing a PEDSnet version, either “X.Y” or “X.Y.Z”.

**Code Description**:  
The function begins by splitting the supplied *version* string on the period character, producing a list named *parts*. It then validates that the resulting list contains exactly two or three elements; any other length is considered malformed. If the length check fails, a *ValueError* is raised with a message indicating the required format. When the format is valid, the function concatenates the first two elements of *parts* (the major version *X* and the minor version *Y*) and returns this concatenated string. The optional patch component *Z* is ignored. This shorthand is used by other functions in the same module to construct database identifiers. Specifically, `_make_database_name` and `_make_database_name_alt` call `_version_to_shorthand` to obtain the “XY” fragment, which they embed in the final database name (e.g., “pedsnet_dcc_vXY”). Thus, `_version_to_shorthand` provides a consistent, reusable transformation for version handling throughout the preparation scripts.

**Note**:  
- The function assumes that the major and minor components are single‑digit numbers; if the major version reaches 10 or higher, the resulting shorthand will contain more than two characters (e.g., “10.2” → “102”), which may not match the intended naming convention.  
- Only string inputs are accepted; passing non‑string types will raise an AttributeError before the function’s own validation.  
- The function does not validate that the components are numeric; it merely checks the count of dot‑separated parts.

**Output Example**:  
```python
_version_to_shorthand('2.1')      # returns '21'
_version_to_shorthand('2.1.3')    # returns '21'
```
## FunctionDef _make_database_name(model_version)
**_make_database_name**: The function of _make_database_name is to construct the canonical PEDSnet DCC database name for a given model version.  

**parameters**:  
· model_version: a string representing the PEDSnet model version, formatted as “X.Y” or “X.Y.Z”.  

**Code Description**:  
The function receives a version string (e.g., “2.1” or “2.1.3”). It first delegates the conversion of this version into a compact two‑character shorthand by calling the internal helper `_version_to_shorthand`. That helper splits the version on the period character, validates that exactly two or three components are present, and returns the concatenation of the major and minor components (e.g., “2.1” → “21”). The returned shorthand is stored in the local variable `short_version`.  
The function then formats the final database identifier by inserting the shorthand into the template string `'pedsnet_dcc_v{}'`. The `format` method substitutes `{}` with `short_version`, producing a name such as `pedsnet_dcc_v21`. The resulting string is returned to the caller.  

**Reference Relationships**:  
- **Callee**: `_make_database_name` calls `_version_to_shorthand` to obtain the “XY” fragment that represents the major and minor version numbers.  
- **Callers**:  
  * `prepare_database` (in the same module) invokes `_make_database_name` to determine the name of the new database that will be created for the supplied model version.  
  * The unit test `TestPrepareDatabase.setUp` (in `pedsnetdcc/tests/prepdb_test.py`) also calls `_make_database_name` to generate a test database name based on a test version string.  

**Note**:  
- The function assumes that the version string is well‑formed; any malformed version will cause `_version_to_shorthand` to raise a `ValueError`.  
- The resulting database name always follows the pattern `pedsnet_dcc_vXY`, where `XY` is the concatenated major and minor version numbers. If the major version contains more than one digit (e.g., “10.2”), the shorthand will be longer than two characters (e.g., “102”), and the database name will reflect that (e.g., `pedsnet_dcc_v102`).  
- No additional validation of the input string is performed beyond what `_version_to_shorthand` provides.  

**Output Example**:  
```python
_make_database_name('2.1')      # returns 'pedsnet_dcc_v21'
_make_database_name('2.1.3')    # returns 'pedsnet_dcc_v21'
_make_database_name('10.2')     # returns 'pedsnet_dcc_v102'
```
## FunctionDef _make_database_name_alt(model_version, name)
**_make_database_name_alt**: The function of _make_database_name_alt is to produce the final database name for an alternate‑named PEDSnet database, based on the supplied model version and a user‑provided name.

**parameters**:  
· model_version: a string representing the PEDSnet model version (e.g., “2.1” or “2.1.3”).  
· name: the explicit name that the caller wants to assign to the new database.

**Code Description**:  
The function begins by converting the supplied *model_version* into a two‑character shorthand using the helper `_version_to_shorthand`. This conversion yields a string such as “21” for version “2.1”. The result of this conversion is stored in the local variable `short_version`, but the variable is not used thereafter. The function then returns the *name* argument unchanged. Consequently, the database name that will be used downstream is exactly the string passed in as *name*, regardless of the version information.  

From a functional perspective, `_make_database_name_alt` is invoked by `prepare_database_altname` (found in the same module). `prepare_database_altname` calls `_make_database_name_alt` to obtain the identifier that will be passed to the SQL statements that create the new database. Although the version shorthand is computed, it currently has no effect on the returned value; any future change that incorporates `short_version` into the name would need to modify this function.

**Note**:  
- The call to `_version_to_shorthand` is presently redundant; it incurs a small performance cost without influencing the output.  
- If the intention is to embed the version shorthand into the database name, the implementation must be updated to concatenate `short_version` with `name` (or otherwise incorporate it).  
- The function assumes that *name* is a valid PostgreSQL identifier; no validation is performed.  
- Errors raised by `_version_to_shorthand` (e.g., `ValueError` for malformed version strings) will propagate out of `_make_database_name_alt`, even though the shorthand is not used.

**Output Example**:  
```python
_make_database_name_alt('2.1', 'pedsnet_alt')
# returns 'pedsnet_alt'

_make_database_name_alt('3.0.1', 'my_custom_db')
# returns 'my_custom_db'
```
## FunctionDef _create_database_sql(database_name, owner)
**_create_database_sql**: The function of _create_database_sql is to generate the SQL statement required to create a PostgreSQL database with a specified name and owner.

**parameters**: The parameters of this Function.  
· database_name: The name of the database to be created. Must be a string that conforms to PostgreSQL identifier rules.  
· owner: The role that will own the newly created database. Defaults to the string `'dcc_owner'`. Must be a valid PostgreSQL role name.

**Code Description**:  
The function builds a single PostgreSQL `CREATE DATABASE` command using a template string.  
1. The template `tmpl` is defined as:  
   `"create database {0} with owner = {1} template = template0 encoding = 'UTF8' lc_collate = 'C' lc_ctype = 'C'"`  
   This template specifies:  
   * The database name (`{0}`) and owner (`{1}`).  
   * The use of `template0` as the base template, ensuring a clean database without locale‑specific defaults.  
   * UTF‑8 encoding and the `C` locale for both collation and character type, which provides deterministic sorting and comparison behavior.  
2. The function substitutes the supplied `database_name` and `owner` into the template via `tmpl.format(database_name, owner)`.  
3. It returns a one‑element tuple containing the formatted SQL string. The trailing comma after the expression creates the tuple, which matches the expected input type for the `StatementList` handling in the callers.

**Relationship with Callers**:  
- **prepare_database**: Calls `_create_database_sql(database_name, 'loading_user')` when the `update` flag is false. The returned tuple is unpacked into individual `Statement` objects, which are then executed against the initial connection string to actually create the database before any schema work begins.  
- **prepare_database_altname**: Calls `_create_database_sql(database_name, owner)` (or the newer `_create_database_sql_new`) when `update` is false. The same tuple‑based return format is used, allowing the caller to treat the result uniformly with other SQL‑generation helpers. In both callers, the generated statement is the first step in the overall database preparation workflow, establishing the target database that subsequent schema‑creation statements will operate on.

**Note**:  
- The function does not perform any validation of the input strings; callers must ensure that `database_name` and `owner` are safe and correctly quoted if they contain special characters.  
- Because the function returns a tuple, even though it contains only one element, callers must iterate over the tuple or unpack it as shown in the calling code.  
- The use of `template0` and the `C` locale is intentional to avoid locale‑dependent behavior in downstream data loading and queries.

**Output Example**:  
```python
sql_stmt, = _create_database_sql('pedsnet_2_0', 'dcc_owner')
# sql_stmt contains:
# "create database pedsnet_2_0 with owner = dcc_owner template = template0 encoding = 'UTF8' lc_collate = 'C' lc_ctype = 'C'"
```
## FunctionDef _create_database_sql_new(database_name, owner)
**_create_database_sql_new**: The function of _create_database_sql_new is to generate the SQL statement required to create a new PostgreSQL database with specific encoding, locale, and ownership settings, and return it as a tuple.

**parameters**:  
· database_name: The name of the database to be created (type: str).  
· owner: The role that will own the newly created database; defaults to `'dcc_owner'` (type: str).

**Code Description**:  
The function builds a single SQL command string using a template that specifies the creation of a database with the following characteristics:  

1. **Database name** – inserted at the first placeholder `{0}`.  
2. **Owner** – inserted at the second placeholder `{1}`.  
3. **Template** – fixed to `template0`, ensuring the new database starts from a clean base without locale or encoding inheritance from the default template.  
4. **Encoding** – set to `'UTF8'`.  
5. **Locale settings** – both `lc_collate` and `lc_ctype` are set to `'en_US.UTF-8'`, guaranteeing consistent sorting and character classification.  

The template string is:

```
create database {0} with owner = {1} template = template0 encoding = 'UTF8' lc_collate = 'en_US.UTF-8' lc_ctype = 'en_US.UTF-8'
```

The function formats this template with the supplied `database_name` and `owner`, producing the final SQL command. It then returns a one‑element tuple containing that command. The tuple format matches the expectations of the calling code, which iterates over the returned iterable to wrap each statement in a `Statement` object.

**Relationship with Callers**:  
`_create_database_sql_new` is invoked by `prepare_database_altname` (found in the same module) when the `new` flag is true and the database does not already exist (`update` is false). In that context, the caller builds a `StatementList`, extends it with `Statement` objects created from each element of the tuple returned by this function, and then executes the statements against the initial connection string. This step creates the physical database before any schema or permission statements are applied. If `new` is false, `prepare_database_altname` falls back to the older `_create_database_sql` function, but the overall workflow remains the same.

**Note**:  
- The function does not execute the SQL; it only returns the command string. Execution responsibility lies with the caller.  
- The returned value is a tuple, not a list; callers must iterate over it (as `prepare_database_altname` does) to handle the single statement.  
- The owner name is inserted directly into the SQL without quoting; callers should ensure the owner identifier is a valid PostgreSQL role name.  
- Because the template is fixed, any change to encoding, locale, or template requires modification of this function.

**Output Example**:  

Calling `_create_database_sql_new('pedsnet_v1', owner='dcc_owner')` returns:

```
('create database pedsnet_v1 with owner = dcc_owner template = template0 encoding = \'UTF8\' lc_collate = \'en_US.UTF-8\' lc_ctype = \'en_US.UTF-8\'',)
```
## FunctionDef _site_sql(site, owner, id_name, pedsnet_only)
**_site_sql**: The function of _site_sql is to generate a list of SQL statements required to create the database schemas for a specific site (or the central DCC) based on supplied options.

**parameters**: The parameters of this Function.  
· site: name of the site for which schemas are to be created (e.g., `'dcc'`, `'stlouis'`).  
· owner: database role that will own the created schemas; defaults to `'dcc_owner'`.  
· id_name: identifier name used for alternate ID schemas; defaults to `'dcc'`.  
· pedsnet_only: when True, only the PEDSnet‑specific schemas are generated; otherwise both PEDSnet and PCORnet schemas are created.

**Code Description**:  
1. **Template selection** – The function chooses a base SQL template according to the `id_name` and `pedsnet_only` flags:  
   * If `id_name` is `'dcc'` the function uses `SQL_SITE_PEDSNET_TEMPLATE` when `pedsnet_only` is True, otherwise `SQL_SITE_TEMPLATE`.  
   * If `id_name` differs from `'dcc'` the generic `SQL_SITE_ID_NAME_TEMPLATE` is used.  
   These templates contain placeholder tokens `{{.Site}}`, `{{.Owner}}`, and optionally `{{.IdName}}`.

2. **Placeholder substitution** – The selected template string (`tmpl`) is copied into `sql` and the placeholders are replaced with the actual argument values using `str.replace`.  
   * `{{.Site}}` → `site`  
   * `{{.Owner}}` → `owner`  
   * If `id_name` is not `'dcc'`, `{{.IdName}}` → `id_name`.

3. **Primary site statements** –  
   * When the site is `'dcc'` **or** the site name differs from `id_name`, the multi‑line `sql` string is split on newline characters. Each non‑empty line is passed through `_despace` (which collapses consecutive spaces to a single space) and collected into the `statements` list.  
   * If the site is `'dcc'` **and** an alternate `id_name` is supplied, a second round of template substitution is performed with `site` replaced by `id_name`. The resulting SQL block is also normalized with `_despace` and appended to `statements`. This creates the alternate ID schema for the central DCC when required.

4. **ID‑mapping schema** – When the site name and `id_name` are different, an additional set of statements is generated to create the ID‑mapping tables:  
   * The appropriate ID‑maps template (`SQL_ID_MAPS_TEMPLATE` or `SQL_ID_MAPS_ID_NAME_TEMPLATE`) is selected based on whether `id_name` equals `'dcc'`.  
   * Placeholders `{{.Site}}`, `{{.Owner}}`, and optionally `{{.IdName}}` are substituted.  
   * The final block is normalized with `_despace` and appended to `statements`.

5. **Return value** – The function returns the `statements` list, which may contain zero, one, or multiple SQL strings depending on the combination of arguments. The list is ready to be wrapped in `Statement` objects elsewhere in the code.

**Reference relationship**:  
* Callers – `prepare_database` and `prepare_database_altname` invoke `_site_sql` for each site (including the central DCC) to assemble the full set of schema‑creation statements before executing them against the target database.  
* Callees – `_site_sql` relies on `_despace` for whitespace normalization and on several module‑level SQL template constants (`SQL_SITE_TEMPLATE`, `SQL_SITE_PEDSNET_TEMPLATE`, `SQL_SITE_ID_NAME_TEMPLATE`, `SQL_ID_MAPS_TEMPLATE`, `SQL_ID_MAPS_ID_NAME_TEMPLATE`). No other functions are called directly.

**Note**:  
- The function assumes that the SQL template constants are defined and contain the required placeholder tokens; missing or malformed templates will cause a `ValueError` or incorrect SQL generation.  
- The special handling for the `'dcc'` site ensures that the central DCC schema is always created, and that an alternate ID schema is added only when `id_name` differs from `'dcc'`.  
- When `site` equals `id_name` (non‑DCC case), the primary schema block is omitted and only the ID‑mapping block is produced.  
- The returned statements are plain strings; they are not executed within `_site_sql`. Execution is performed by the caller using `Statement` objects and a `StatementList`.

**Output Example**:  
```python
>>> _site_sql('stlouis', owner='loading_user', id_name='dcc', pedsnet_only=False)
[
    "CREATE SCHEMA stlouis AUTHORIZATION loading_user;",
    "CREATE TABLE stlouis.patient ( ... );",
    "CREATE SCHEMA id_maps_stlouis AUTHORIZATION loading_user;",
    "CREATE TABLE id_maps_stlouis.patient_id_map ( ... );"
]
``` 
(Each string is a single‑line, space‑collapsed SQL statement ready for execution.)
## FunctionDef _other_sql(id_name, alt_id_only, owner)
**_other_sql**: The function of _other_sql is to generate a list of SQL statements that create the non‑site (global) schemas required for a PEDSnet database.

**parameters**: The parameters of this Function.  
· id_name: name of the identifier set to be used for the schemas; default is `'dcc'`.  
· alt_id_only: when *id_name* is not `'dcc'`, indicates whether only the alternate identifier’s schemas should be created (True) or both the alternate and the default `'dcc'` schemas should be created (False). Default is `False`.  
· owner: database role that will own the created schemas; default is `'dcc_owner'`.

**Code Description**:  
The function selects one of three SQL template strings that are defined elsewhere in the module:

1. If *id_name* equals `'dcc'`, the template `SQL_OTHER` is used.  
2. If *id_name* differs from `'dcc'` and *alt_id_only* is `True`, the template `SQL_OTHER_ID_ONLY` is used.  
3. If *id_name* differs from `'dcc'` and *alt_id_only* is `False`, the template `SQL_OTHER_ID` is used.

When a non‑default *id_name* is supplied, the chosen template contains the placeholder `{{.IdName}}`; the function replaces this placeholder with the actual *id_name* value. Regardless of the branch taken, the placeholder `{{.Owner}}` is always replaced with the supplied *owner* value.

After placeholder substitution, the resulting multi‑line SQL string is split on newline characters. Empty lines are discarded, and each remaining line is passed through the helper function `_despace`. `_despace` collapses any sequence of consecutive space characters into a single space, ensuring a clean, uniformly spaced statement. The list comprehension returns the cleaned statements as a list of strings.

The function is invoked by the high‑level database‑preparation routines:

* `prepare_database` – always calls `_other_sql('dcc', False, 'loading_user')` to create the standard global schemas for the DCC owner.  
* `prepare_database_altname` – calls `_other_sql(id_name, alt_id_only, owner)` so that alternative identifier sets (e.g., a partner site) can receive their own global schemas.

Thus, `_other_sql` supplies the non‑site portion of the schema‑creation workload, while site‑specific schemas are generated by the separate `_site_sql` helper.

**Note**:  
- The function does not explicitly raise `ValueError`; the docstring mentions it, but no validation code is present. Callers should ensure that *id_name* and *owner* are non‑empty strings and that *alt_id_only* is a boolean.  
- The placeholders `{{.IdName}}` and `{{.Owner}}` must exist in the selected SQL template; otherwise a `KeyError`‑like replacement failure will occur.  
- `_despace` only normalizes space characters; tabs or other whitespace remain unchanged.  
- The returned list contains each SQL statement as a single line without trailing newline characters.

**Output Example**:  
If `id_name='dcc'`, `alt_id_only=False`, and `owner='loading_user'`, a possible return value could be:

```
[
    "CREATE SCHEMA vocab AUTHORIZATION loading_user;",
    "CREATE SCHEMA vocab_common AUTHORIZATION loading_user;",
    "GRANT USAGE ON SCHEMA vocab TO loading_user;",
    "GRANT USAGE ON SCHEMA vocab_common TO loading_user;"
]
```
## FunctionDef _delete_external_schemas(conn_str, site)
**_delete_external_schemas**: The function of _delete_external_schemas is to remove the site‑specific PEDSnet and PCORnet schemas that are not needed for a newly prepared database.

**parameters**:  
· conn_str: a libpq‑style PostgreSQL connection string that identifies the target database where the schemas will be dropped.  
· site: a string containing the site identifier (e.g., “mdcd”, “mdcr”) whose schemas are to be removed.

**Code Description**:  
1. The function defines a multi‑line SQL template `delete_schemas_sql` that contains two `DROP SCHEMA … CASCADE;` statements. The placeholders `{0}` are replaced with the supplied `site` value, producing statements such as `DROP SCHEMA mdcd_pedsnet CASCADE;` and `DROP SCHEMA mdcd_pcornet CASCADE;`.  
2. A human‑readable log message template `delete_schemas_msg` is also defined, which will be formatted with the same `site` value to produce a description like “cleaning up unused mdcd schemas”.  
3. A `Statement` object is instantiated with the formatted SQL string and the formatted message. The `Statement` class encapsulates the SQL command, a descriptive message, and execution metadata.  
4. The `execute` method of the `Statement` instance is called with `conn_str`. This opens a new PostgreSQL connection, runs the two `DROP SCHEMA` commands in autocommit mode, and stores any execution error in `stmt.err`.  
5. Immediately after execution, the helper `check_stmt_err` is invoked with the statement and a context string `'clean up {site} schemas'`. If the statement encountered an error, `check_stmt_err` logs the failure and raises a `DatabaseError`. If no error occurred, execution proceeds.  
6. When the function reaches the end without raising an exception, it returns the boolean value `True` to indicate successful cleanup.

**Reference Relationships**:  
- **Callers**: The function is called from `prepare_database` and `prepare_database_altname` (both in `pedsnetdcc/prepdb.py`). After all required site and DCC schemas have been created, each external site listed in `EXTERNAL_SITES` is passed to `_delete_external_schemas` so that the unnecessary external schemas are removed from the new database.  
- **Callees**: The function uses the `Statement` class from `pedsnetdcc/db.py` to encapsulate and execute the SQL, and it calls `check_stmt_err` from `pedsnetdcc/utils.py` to enforce error handling. No other project components are directly involved.

**Note**:  
- The `CASCADE` option ensures that any objects (tables, views, functions, etc.) contained in the target schemas are also dropped automatically. Use this function only when you are certain that the schemas are not required, because the operation is irreversible.  
- Errors during the drop operation are not raised by the `Statement.execute` call itself; they are captured in `stmt.err` and subsequently turned into a `DatabaseError` by `check_stmt_err`. Callers should be prepared to handle this exception.  
- The function always returns `True` when it completes without raising; the return value does not convey additional information about the number of objects removed.  

**Output Example**:  
```python
result = _delete_external_schemas(conn_str, 'mdcd')
print(result)   # -> True
```
## FunctionDef prepare_database(model_version, conn_str, update, dcc_only)
**prepare_database**: The function of prepare_database is to create a new PEDSnet database, populate it with the required vocabulary and site schemas, and set the appropriate permissions.

**parameters**:  
· model_version: PEDSnet model version string (e.g., “2.1” or “2.1.3”).  
· conn_str: libpq‑style PostgreSQL connection string used for the initial CREATE DATABASE command and for subsequent connections.  
· update: Boolean flag indicating whether the database already exists; when True the CREATE DATABASE step is skipped.  
· dcc_only: Boolean flag indicating whether only the central DCC schema should be created (no site schemas).

**Code Description**:  
1. The function logs the start of the preparation step, including the supplied model version, and records the wall‑clock start time.  
2. It derives the target database name by calling `_make_database_name(model_version)`, which converts the version to a short “XY” form and prefixes it with “pedsnet_dcc_v”.  
3. A `StatementList` instance (`stmts`) is created to hold the SQL commands that will be executed.  
4. If `update` is False, the function generates a CREATE DATABASE statement via `_create_database_sql(database_name, 'loading_user')`, wraps the resulting SQL string in a `Statement` object, and adds it to `stmts`.  
5. `stmts.serial_execute(conn_str)` runs the CREATE DATABASE statement (or does nothing when `update` is True) using the original connection string, which points to a database that has permission to issue CREATE DATABASE.  
6. Immediately after the database is created, `grant_database_permissions(conn_str, database_name)` is called to grant CREATE privileges on the new database to the standard PEDSnet roles.  
7. The function prepares a second `StatementList` for schema creation. It iterates over the tuple returned by `_sites_and_dcc(dcc_only, True)`. For each site (including “dcc” and, unless `dcc_only` is True, all internal and external sites), it calls `_site_sql(site, 'loading_user')` to obtain a list of DDL statements for that site’s schemas and wraps each statement in a `Statement` object, extending the list.  
8. It also adds the global (non‑site) schema statements by calling `_other_sql('dcc', False, 'loading_user')` and wrapping the results in `Statement` objects.  
9. A new connection string that points to the freshly created database is built with `_conn_str_with_database(conn_str, database_name)`. This string replaces any existing “dbname=” clause or appends a new one.  
10. The complete set of schema‑creation statements is executed against the new database via `stmts.serial_execute(new_conn_str)`. Each statement runs in its own autocommit connection because the default `transaction=False` is used (required for many DDL commands).  
11. After the schemas exist, the function grants additional privileges:  
    – `grant_loading_user_permissions(new_conn_str, True)` grants the loading_user role rights on all site schemas, including external sites.  
    – `grant_schema_permissions(new_conn_str, True)` grants schema‑ and table‑level permissions for all sites.  
    – `grant_vocabulary_permissions(new_conn_str)` grants the loading_user role rights on the vocabulary schemas.  
12. Each statement in `stmts` is examined with `check_stmt_err(stmt, 'database preparation')`; any error raises a `DatabaseError` and aborts the process.  
13. The function then removes any external site schemas that are not needed for the new database by iterating over `EXTERNAL_SITES` and calling `_delete_external_schemas(new_conn_str, ext_site)`. This drops both the PEDSnet and PCORnet schemas for each external site.  
14. A final log entry records the completion of the preparation step, the model version, and the elapsed time calculated by `secs_since(starttime)`.  
15. The function returns `True` to indicate successful completion; any exception raised earlier prevents the return.

**Reference Relationships**:  
*Callers* – `prepare_database` is invoked by the CLI entry point `prepdb` in `pedsnetdcc/main.py` and by the unit tests in `pedsnetdcc/tests/prepdb_test.py`. Both callers expect a boolean success flag and will exit with an error code if the function returns `False` or raises an exception.  
*Callees* – The function relies on a number of helper utilities within the same module and across the project:  
- `_make_database_name` (creates the canonical database identifier).  
- `_create_database_sql` (generates the CREATE DATABASE command).  
- `grant_database_permissions` (grants CREATE privileges on the new database).  
- `_sites_and_dcc` (determines which site schemas to create).  
- `_site_sql` (produces the DDL for each site).  
- `_other_sql` (produces the DDL for global schemas).  
- `_conn_str_with_database` (produces a connection string that targets the new database).  
- `grant_loading_user_permissions`, `grant_schema_permissions`, `grant_vocabulary_permissions` (apply role‑based permissions).  
- `_delete_external_schemas` (cleans up unneeded external schemas).  
- `Statement`, `StatementList`, and `check_stmt_err` (encapsulate SQL execution, ordering, and error handling).  

**Note**:  
- The function assumes that the role “loading_user” exists and has sufficient rights to create databases and schemas.  
- All DDL statements are executed outside a transaction (`transaction=False`) because PostgreSQL does not allow many schema‑creation commands inside a transaction block.  
- If `update` is True, the CREATE DATABASE step is omitted, but the subsequent permission‑granting and schema‑creation steps still run against the existing database.  
- Errors in any individual statement are captured in the corresponding `Statement.err` attribute and cause an immediate `DatabaseError` via `check_stmt_err`. The function will not return `True` in that case.  
- External site cleanup is performed unconditionally for every site listed in `EXTERNAL_SITES`; callers must ensure that those schemas are present only when they are truly unwanted.  

**Output Example**:  
```python
result = prepare_database('2.1', 'host=localhost user=admin password=secret', 
                          update=False, dcc_only=False)
# result == True  (indicates that the database was created, schemas were built,
#                 permissions were granted, and external schemas were removed)
```
## FunctionDef prepare_database_altname(model_version, conn_str, name, addsites, skipsites, id_name, alt_id_only, owner, pedsnet_only, inc_ext, new, limit, update, dcc_only)
**prepare_database_altname**: The function of prepare_database_altname is to create a new PostgreSQL database (using an alternate name supplied by the user) and to populate it with the required PEDSnet vocabulary and site schemas, applying appropriate permissions and optional restrictions.

**parameters**:  
· model_version: PEDSnet model version string (e.g., “2.1.3”).  
· conn_str: libpq connection string used for the initial CREATE DATABASE command.  
· name: explicit name for the new database (alternate name).  
· addsites: comma‑separated list of site identifiers to be added in addition to the defaults.  
· skipsites: comma‑separated list of site identifiers to be omitted from the defaults.  
· id_name: identifier name for the ID namespace (e.g., “dcc”, “omop”).  
· alt_id_only: boolean flag; when True and id_name is not “dcc”, only the alternate ID schemas are created.  
· owner: database role that will own the created schemas and be granted permissions.  
· pedsnet_only: boolean flag; when True only PEDSnet schemas (no PCORnet) are created.  
· inc_ext: boolean flag; when True external site schemas are included in the creation process.  
· new: boolean flag; when True the database is created with the newer locale settings (uses _create_database_sql_new).  
· limit: boolean flag; when True database and schema permissions are granted in a limited‑privilege mode.  
· update: boolean flag (default False); when True the function assumes the database already exists and skips the CREATE DATABASE step.  
· dcc_only: boolean flag; when True only the central DCC schemas are created (no site schemas).

**Code Description**:  
1. The function logs the start of the preparation step and records the current time for elapsed‑time reporting.  
2. It selects the base set of sites: `SITES_AND_EXTERNAL` if `inc_ext` is True, otherwise `SITES`.  
3. Primary site list (`primary_sites`) is built:  
   * If `id_name` is “dcc” or `alt_id_only` is False, the list always includes “dcc”.  
   * When `dcc_only` is False, the list also includes the base sites and, if `inc_ext` is False, the external sites (`EXTERNAL_SITES`).  
   * If `dcc_only` is True, only (“dcc”,) is used.  
   * If `alt_id_only` is True and `id_name` is not “dcc”, `primary_sites` is empty.  
4. When `id_name` is not “dcc”, the function computes the set of sites to be added or skipped:  
   * `skip_sites` is derived from `skipsites`.  
   * `tmp_sites` starts as the base sites minus the skipped ones.  
   * `add_sites` is derived from `addsites` and merged into `tmp_sites`.  
   * Empty strings are filtered out, and `alt_sites` is built as a tuple beginning with `id_name` followed by the resulting site list.  
   * If `id_name` is “dcc”, `alt_sites` is an empty tuple.  
5. The target database name is generated by `_make_database_name_alt(model_version, name)`. Currently this returns the supplied `name` unchanged.  
6. A `StatementList` (`stmts`) is created to hold the CREATE DATABASE statements.  
   * If `update` is False, the appropriate CREATE DATABASE SQL is added:  
     – When `new` is True, `_create_database_sql_new(database_name, owner)` is used.  
     – Otherwise `_create_database_sql(database_name, owner)` is used.  
   * The statements are executed against the original `conn_str` (which points to a database that has permission to create other databases).  
   * After execution, each statement is checked with `check_stmt_err` to raise an exception on failure.  
7. Database‑level permissions are granted:  
   * If `limit` is True, `grant_database_permissions_limited` is called (granting only CREATE to the specified `owner`).  
   * Otherwise, `grant_database_permissions` is called (granting CREATE to the standard PEDSnet roles).  
8. A new connection string that points to the newly created database is built with `_conn_str_with_database(conn_str, database_name)`.  
9. Schema creation statements are assembled in a fresh `StatementList`:  
   * For each site in `primary_sites`, `_site_sql(site, owner, 'dcc', pedsnet_only)` generates the DCC or site schema DDL and the statements are added.  
   * For each site in `alt_sites`, `_site_sql(site, owner, id_name)` generates the alternate‑ID schema DDL.  
   * `_other_sql(id_name, alt_id_only, owner)` adds the global (non‑site) schemas.  
10. The assembled statements are executed serially against the new database using `new_conn_str`.  
11. If the database is not in limited‑privilege mode and `pedsnet_only` is False, `grant_loading_user_permissions` is called to grant the loading user full access to all site schemas (including external sites when `inc_ext` is True).  
12. Schema‑level permissions are then granted:  
    * When `limit` is True, `grant_schema_permissions_limited` is called separately for the DCC schemas (`primary_sites`) and the alternate ID schemas (`alt_sites`).  
    * When `limit` is False, `grant_schema_permissions` (full permissions) and `grant_vocabulary_permissions` (full vocabulary permissions) are called.  
13. After permission grants, each statement in the schema‑creation list is checked with `check_stmt_err`.  
14. If `id_name` is “dcc” or `alt_id_only` is False, external site schemas listed in `EXTERNAL_SITES` are removed from the new database by calling `_delete_external_schemas` for each external site.  
15. A final log entry records the completion of the preparation step together with the elapsed time, and the function returns `True` to indicate success.

**Reference Relationships**:  
*Callers* – The function is invoked by `prepdb_altname` in `pedsnetdcc/main.py`, which parses command‑line arguments, builds the connection string with `make_conn_str`, and forwards all user‑supplied options to `prepare_database_altname`.  
*Callees* – The function relies on a number of helper utilities within the same module and across the project:  
- `_make_database_name_alt`, `_create_database_sql`, `_create_database_sql_new`, `_conn_str_with_database` (prepdb.py) for name handling and database creation.  
- `Statement`, `StatementList`, and `check_stmt_err` (db.py, utils.py) for SQL encapsulation, execution, and error handling.  
- Permission‑granting functions: `grant_database_permissions`, `grant_database_permissions_limited`, `grant_schema_permissions`, `grant_schema_permissions_limited`, `grant_loading_user_permissions`, `grant_vocabulary_permissions`, `grant_vocabulary_permissions_limited`, `grant_vocabulary_only_permissions_limited`, `grant_ids_permissions_limited` (permissions.py).  
- Schema‑generation helpers: `_site_sql`, `_other_sql` (prepdb.py).  
- Cleanup helper: `_delete_external_schemas` (prepdb.py).  
These relationships form a linear workflow: create database → grant database‑level rights → create schemas → grant schema/vocabulary rights → optionally drop unwanted external schemas.

**Note**:  
- The `new` flag determines which CREATE DATABASE template is used; the newer template sets locale to “en_US.UTF‑8”.  
- When `limit` is True, only minimal CREATE privileges are granted to the `owner`, and schema permissions are restricted via the “limited” permission generators.  
- The function assumes that the caller has sufficient privileges on the server to create databases and to assign ownership to the specified `owner`.  
- If any SQL statement fails, `check_stmt_err` raises a `DatabaseError`, causing the function to abort and return `False` (the caller exits with a non‑zero status).  
- External site schemas are always dropped unless `inc_ext` is True and the external sites are needed; the drop operation uses `CASCADE` and is irreversible.  
- The function does not perform validation of the `addsites` or `skipsites` strings; malformed input may result in empty or unexpected site lists.

**Output Example**:  
When called with valid arguments and no errors, the function logs the start and finish messages and returns the boolean value `True`:

```python
result = prepare_database_altname(
    model_version='2.1.3',
    conn_str='host=localhost user=admin password=secret',
    name='pedsnet_custom',
    addsites='stlouis,nyc',
    skipsites='',
    id_name='dcc',
    alt_id_only=False,
    owner='loading_user',
    pedsnet_only=False,
    inc_ext=False,
    new=True,
    limit=False,
    update=False,
    dcc_only=False
)
# result == True
```
