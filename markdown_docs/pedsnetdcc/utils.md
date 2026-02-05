## FunctionDef _parse_options(options)
**_parse_options**: The function of _parse_options is to convert an “options” string from a PostgreSQL libpq connection string into a dictionary of individual option keys and their corresponding values.

**parameters**:  
· **options**: *str* – The raw options string as it appears in the connection URI or connection string. It may be surrounded by single quotes and contains one or more “‑c key=value” fragments separated by spaces.

**Code Description**:  
The function begins by creating an empty dictionary `o_dict` that will hold the parsed key‑value pairs.  
1. `options_val = options.strip("'")` removes a leading and trailing single‑quote character, if present. This step normalises the input because the `options` query parameter in a URI is often quoted.  
2. `options = [o.strip() for o in options_val.split('-c ')]` splits the cleaned string on the literal substring `-c `, which is the libpq flag used to pass a configuration command. The result is a list where each element corresponds to a single “key=value” fragment; any surrounding whitespace is stripped. The first element of the list may be an empty string when the original string started with `-c `.  
3. The function iterates over the list. Empty strings are ignored (`if not option: continue`). For each non‑empty fragment, `k, v = option.split('=', 1)` separates the key from the value at the first equals sign. The key `k` and value `v` are then stored in `o_dict`.  
4. After processing all fragments, the populated dictionary `o_dict` is returned.

**Relationship with Callers**:  
- **make_conn_str** calls `_parse_options` when the incoming URI contains an `options` query parameter. The returned dictionary is later merged with a possible `search_path` override and re‑encoded into a new options string via `_make_options`.  
- **conn_str_with_search_path** also invokes `_parse_options` to decode the existing `options` part of a libpq connection string, updates or adds the `search_path` entry, and then rebuilds the options string. In both contexts, `_parse_options` provides the essential step of turning the raw options text into a mutable Python mapping, enabling the higher‑level utilities to modify individual options safely.

**Note**:  
- The function assumes that each option fragment contains exactly one ‘=’. If a fragment lacks an equals sign, a `ValueError` will be raised by the `split` call.  
- Only single‑quote characters are stripped; double quotes remain untouched.  
- The split delimiter is the literal string `-c ` (including the trailing space). Options that are not prefixed with `-c ` will be treated as a single fragment and may not parse as intended.  
- Whitespace around keys or values is not removed beyond the initial `strip()` on each fragment; embedded spaces inside a value are preserved.

**Output Example**:  

```python
options_str = "'-c search_path=public -c application_name=myapp'"
result = _parse_options(options_str)
# result => {'search_path': 'public', 'application_name': 'myapp'}
```
## FunctionDef _make_options(o_dict)
**_make_options**: The function of _make_options is to convert a dictionary of PostgreSQL option key‑value pairs into a single space‑separated string that can be used as the value of the libpq `options` connection parameter.

**parameters**:  
· o_dict: A dictionary where each key is an option name (e.g., `"search_path"`) and each value is the corresponding option value. The dictionary may be empty.

**Code Description**:  
The function iterates over the keys of `o_dict` in alphabetical order (`sorted(o_dict)`). For each key `k` it formats a fragment `"-c {}={}".format(k, o_dict[k])`, which follows the libpq syntax for setting a run‑time configuration parameter (`-c name=value`). All formatted fragments are collected into a list comprehension, then joined together with a single space character using `' '.join(...)`. The resulting string is returned. If `o_dict` is empty, the list comprehension yields an empty list and the join operation returns an empty string.

**Relationship with Callers**:  
- **make_conn_str** builds a connection string from a URI. After extracting any existing `options` query parameter and optionally adding or overriding the `search_path`, it calls `_make_options` to turn the resulting options dictionary into the final `options` value that is inserted into the connection string.  
- **conn_str_with_search_path** patches an existing libpq connection string with a new `search_path`. It parses the current `options` value (if present) into a dictionary, updates the `search_path` entry, and then uses `_make_options` to re‑serialize the dictionary back into a proper `options` string. In both cases, `_make_options` guarantees a deterministic ordering of the options, which helps produce stable connection strings.

**Note**:  
- The function does not perform any quoting or escaping of the option values; callers must ensure that values are safe for inclusion in a libpq command‑line fragment.  
- Because the keys are sorted, the output order is predictable, which is useful for testing and caching.  
- An empty dictionary yields an empty string, allowing callers to omit the `options` parameter entirely when no options are required.

**Output Example**:  
```python
>>> _make_options({'search_path': 'public,extra', 'application_name': 'myapp'})
'-c application_name=myapp -c search_path=public,extra'

>>> _make_options({})
''
```
## FunctionDef _conn_str_sort_key(key)
**_conn_str_sort_key**: The function of _conn_str_sort_key is to provide a deterministic ordering key for components of a database connection string, primarily to make debugging output predictable.

**parameters**:  
· key: a string representing a single component name (e.g., “host”, “port”, “dbname”) from a connection‑string dictionary.

**Code Description**:  
The function examines the supplied *key* and returns a string that can be used by Python’s `sorted` function as a sort key.  
1. If the key equals `'host'`, the function returns the literal string `' 1'`.  
2. If the key equals `'port'`, it returns `' 2'`.  
3. If the key equals `'dbname'`, it returns `' 3'`.  
4. If the key equals `'user'`, it returns `' 4'`.  
5. If the key equals `'password'`, it returns `' 5'`.  
6. For any other key, the function returns the lower‑cased version of the original key (`key.lower()`).

The leading space in the returned strings (`' 1'` … `' 5'`) ensures that these prioritized keys sort before any alphabetic keys when the default lexical ordering is applied. The numeric suffix establishes a fixed relative order among the five most common connection‑string components. All other keys fall back to a case‑insensitive alphabetical order.

In the project, `_conn_str_sort_key` is used by the helper function `_make_conn_str`. `_make_conn_str` builds a connection string from a dictionary by iterating over `sorted(parts.keys(), key=_conn_str_sort_key)`. Because of the custom sort key, the resulting string consistently lists `host`, `port`, `dbname`, `user`, and `password` first (in that exact sequence), followed by any additional parameters sorted alphabetically. This deterministic ordering aids developers when inspecting or logging generated connection strings, but it does not affect the functional behavior of the database connection itself.

**Note**:  
- The ordering is intended solely for readability and debugging; it does not influence how the database driver interprets the connection string.  
- Adding new standard keys should be done by extending the conditional block if a specific order is desired; otherwise they will be sorted alphabetically after the predefined keys.  
- The function returns strings, not integers; the space prefix is required to keep the returned values comparable with the lower‑cased keys in the default sorting algorithm.

**Output Example**:  
```python
>>> _conn_str_sort_key('host')
' 1'
>>> _conn_str_sort_key('port')
' 2'
>>> _conn_str_sort_key('dbname')
' 3'
>>> _conn_str_sort_key('user')
' 4'
>>> _conn_str_sort_key('password')
' 5'
>>> _conn_str_sort_key('SSLMode')
'sslmode'
>>> sorted(['user', 'host', 'SSLMode', 'port', 'dbname', 'application_name'],
...        key=_conn_str_sort_key)
['host', 'port', 'dbname', 'user', 'application_name', 'sslmode']
```
## FunctionDef _make_conn_str(parts)
**_make_conn_str**: The function of _make_conn_str is to assemble a libpq‑compatible connection string from a dictionary of connection parameters.

**parameters**:  
· parts: a dictionary where each key is a connection‑string component name (e.g., “host”, “port”, “dbname”, “user”, “password”, “options”) and each value is the corresponding string value to be placed in the connection string.

**Code Description**:  
1. An empty list named *pairs* is created to hold the individual “key=value” fragments.  
2. The function iterates over the keys of *parts* in a deterministic order obtained by `sorted(parts.keys(), key=_conn_str_sort_key)`.  
   * The helper `_conn_str_sort_key` forces the common keys *host*, *port*, *dbname*, *user*, and *password* to appear first (in that exact sequence) and sorts any remaining keys alphabetically, which makes debugging output predictable.  
3. For each key (`parts_key`) the associated value (`parts_value`) is retrieved.  
   * If the value contains a space character, it is wrapped in single quotes (`'value'`). This quoting follows the libpq convention that values containing whitespace must be quoted.  
4. A string of the form `"{key}={value}"` is appended to *pairs*.  
5. After all keys have been processed, the list *pairs* is joined with a single space character, producing the final connection string, which is then returned.

**Reference Relationships**:  
- `_make_conn_str` is invoked by the public helper `make_conn_str`, which parses a PostgreSQL URI, extracts standard components, optional *search_path* and *password*, and passes the resulting dictionary to `_make_conn_str`.  
- It is also called by `conn_str_with_search_path`, which modifies an existing connection string’s `options` parameter and then rebuilds the full string via `_make_conn_str`.  
- The only internal dependency of `_make_conn_str` is the sorting helper `_conn_str_sort_key`; no other functions are called from within `_make_conn_str`.

**Note**:  
- The ordering of components is solely for readability; the PostgreSQL driver does not depend on any particular order.  
- Only values containing spaces are quoted; other characters are left unchanged.  
- All dictionary values should be strings; non‑string values will be converted to their string representation by the `format` operation.  
- If a key appears more than once in the input dictionary, the later entry will overwrite the earlier one before `_make_conn_str` is called (the function itself does not perform duplicate detection).  

**Output Example**:  
```python
parts = {
    'host': 'db.example.com',
    'port': '5432',
    'dbname': 'clinical',
    'user': 'alice',
    'password': 's3cr3t',
    'options': '-c search_path=public,raw',
    'application_name': 'pedsnet-dcc'
}
conn_str = _make_conn_str(parts)
# conn_str will be:
# "host=db.example.com port=5432 dbname=clinical user=alice password=s3cr3t options='-c search_path=public,raw' application_name=pedsnet-dcc"
```
## FunctionDef make_conn_str(uri, search_path, password)
**make_conn_str**: The function of make_conn_str is to build a libpq‑compatible connection string from a PostgreSQL URI, optionally overriding the search_path and password.

**parameters**  
· uri (str): The base PostgreSQL URI (e.g., “postgresql://user:pass@host:port/dbname”).  
· search_path (str or None, optional): A schema search_path to be inserted or to replace any existing search_path option. Default is None (no change).  
· password (str or None, optional): An explicit password that, if supplied, overrides any password present in the URI. Default is None.

**Code Description**  
1. The function parses the supplied URI with `urlparse`, extracting hostname, port, path, username, password and the query component.  
2. `parse_qs` converts the query string into a dictionary where each key maps to a list of values.  
3. An empty dictionary `parts` is prepared to hold the individual libpq parameters.  
4. Standard connection components are copied into `parts` when present:  
   * `host` ← hostname  
   * `port` ← port (converted to string)  
   * `dbname` ← path without the leading “/”  
   * `user` ← username  
   * `password` ← the explicit `password` argument if given; otherwise the password from the URI.  
5. The function looks for an `options` query parameter. If present, it must appear only once; otherwise a `ValueError` is raised. The single options string is parsed by the internal helper `_parse_options`, which returns a dictionary of option key/value pairs (e.g., `{'search_path': 'public'}`). If no `options` parameter exists, an empty dictionary is used.  
6. If a `search_path` argument is supplied, it overwrites (or adds) the `search_path` entry in the options dictionary.  
7. The updated options dictionary is turned back into a properly formatted libpq options string by `_make_options`. If the resulting string is non‑empty, it is stored in `parts['options']`.  
8. All remaining query parameters (those other than `options`) are copied into `parts`. Each key must have exactly one value; multiple values cause a `ValueError`.  
9. Finally, `_make_conn_str` receives the fully populated `parts` dictionary and assembles the final connection string. `_make_conn_str` orders the keys (host, port, dbname, user, password first, then alphabetically), quotes values containing spaces, and joins the “key=value” fragments with spaces. The resulting string is returned.

**Reference Relationships**  
*Callers*: `make_conn_str` is invoked throughout the command‑line interface in `pedsnetdcc/main.py` (e.g., `post_load`, `sync_observation_period`, `transform`, `run_derivations`, etc.) to obtain a connection string for every database‑related operation. It is also used in the test suite (`pedsnetdcc/tests/...`) to create temporary connections for unit tests.  
*Callees*: Internally it relies on `_parse_options` (to decode an existing `options` parameter), `_make_options` (to re‑encode the options dictionary) and `_make_conn_str` (to format the final string). These helpers are defined in the same module and are also used by `conn_str_with_search_path`, which modifies an existing connection string’s search_path.

**Note**  
- The function enforces a single occurrence for each query parameter; providing duplicate keys raises `ValueError`.  
- When both the URI and the function arguments specify a password, the explicit `password` argument takes precedence.  
- When both the URI and the function arguments specify a search_path via the `options` parameter, the supplied `search_path` argument overrides the existing one.  
- The `options` value is built from key/value pairs without any quoting or escaping; callers must ensure that option values do not contain characters that would break libpq parsing.  
- Values containing spaces are automatically quoted by `_make_conn_str`; other characters are left unchanged.  

**Output Example**  
```python
url = "postgresql://auser:apass@ahost:5433/adb?sslmode=disable&connect_timeout=30"
conn_str = make_conn_str(url, search_path='testschema')
# Result:
# "host=ahost port=5433 dbname=adb user=auser password=apass connect_timeout=30 options='-c search_path=testschema' sslmode=disable"
```
## FunctionDef conn_str_with_search_path(conn_str, search_path)
**conn_str_with_search_path**: The function of conn_str_with_search_path is to insert or replace the `search_path` setting in a libpq connection string by using the `options` parameter.

**parameters**  
· **conn_str** (str): The original libpq‑style connection string that may contain any combination of host, port, dbname, user, password, and optional query parameters.  
· **search_path** (str): The schema search path that should be applied, expressed as a comma‑separated list of schema names.

**Code Description**  
1. The function begins by splitting the incoming connection string into whitespace‑separated *key=value* fragments using `shlex.split`. This respects quoted values, ensuring that parameters containing spaces remain intact.  
2. Each fragment is further split on the first `'='` character, producing a dictionary `pair_dict` that maps parameter names (e.g., `host`, `port`, `options`) to their raw values.  
3. The function checks whether the dictionary already contains an `options` entry:  
   * **If `options` is absent**, a new entry is created with the value `-c search_path={search_path}`. The `-c` flag follows the libpq convention for passing runtime configuration commands.  
   * **If `options` is present**, the existing options string is parsed into a dictionary of individual `-c` settings by calling the internal helper `_parse_options`. The helper strips surrounding single quotes, splits on the literal `-c ` token, and builds a mapping of option names to values.  
   * The parsed dictionary is then updated (or added) with the key `'search_path'` set to the supplied `search_path`.  
   * The updated options dictionary is converted back to a properly formatted options string via `_make_options`, which joins the sorted key/value pairs into a space‑separated string of `-c name=value` fragments.  
4. After the `options` entry has been created or replaced, the function reconstructs a complete connection string from the modified `pair_dict` by invoking `_make_conn_str`. This helper orders the parameters deterministically (host, port, dbname, user, password first, then the rest alphabetically) and quotes any values that contain spaces.  
5. The resulting string is returned to the caller.

**Reference Relationships**  
* **Callers**:  
  * `pedsnetdcc/main.py/subset_by_cohort` uses `conn_str_with_search_path` to adjust the search path after the initial subset has been created, enabling subsequent split‑measurement and partition‑measurement steps.  
  * `pedsnetdcc/merge_site_data.py/merge_site_data`, `clear_dcc_data`, `merge_data_to_schema`, and `clear_schema_data` all call `conn_str_with_search_path` to ensure that the connection operates within the correct DCC or target schema before executing DDL statements.  
  * `pedsnetdcc/transform_runner.py` (various `run_*_transformation` functions) calls `conn_str_with_search_path` to build a connection string that includes the temporary transformed schema and the vocabulary schema for post‑transformation operations.  
  * Unit tests in `pedsnetdcc/tests/utils_test.py` verify the correct behaviour of the function under different URL configurations (no query, existing query, existing options, and overrides).  

* **Callees**:  
  * `_parse_options` – parses an existing `options` value into a dictionary of individual `-c` settings.  
  * `_make_options` – serialises a dictionary of options back into a single `options` string suitable for libpq.  
  * `_make_conn_str` – assembles the final connection string from the updated parameter dictionary.  

**Note**  
- The function raises `ValueError` only if the underlying `_parse_options` encounters more than one `options` parameter in the original URI; this situation is prevented by the earlier parsing logic that stores each key only once in `pair_dict`.  
- The `search_path` value overwrites any existing `search_path` option but leaves other options (e.g., `geqo=off`) untouched.  
- Because `_make_options` sorts keys alphabetically, the order of `-c` fragments in the resulting `options` string is deterministic, which aids testing and caching.  
- Values containing spaces are automatically quoted by `_make_conn_str`; therefore, the returned connection string is always a valid libpq connection string.  

**Output Example**  
```python
original = "host=ahost port=5433 dbname=adb user=auser password=apass connect_timeout=30 options='-c geqo=off' sslmode=disable"
new_conn_str = conn_str_with_search_path(original, "testschema")
# new_conn_str =>
# "host=ahost port=5433 dbname=adb user=auser password=apass connect_timeout=30 options='-c geqo=off -c search_path=testschema' sslmode=disable"
```
## FunctionDef get_conn_info_dict(conn_str)
**get_conn_info_dict**: The function of get_conn_info_dict is to parse a libpq‑compliant PostgreSQL connection string and return a dictionary containing the extracted connection parameters.

**parameters**  
· conn_str: a single string that follows the libpq connection string format (e.g., “host=… port=… dbname=… user=… password=… options='‑c search_path=myschema'”).

**Code Description**  
The function creates a result dictionary with keys `user`, `host`, `port`, `dbname`, and `search_path`, each initially set to `None`. It then applies five regular‑expression searches against the supplied `conn_str`:

1. `host_match = re.search(r"host=(\S*)", conn_str)` – captures the value following “host=”.  
2. `port_match = re.search(r"port=(\S*)", conn_str)` – captures the value following “port=”.  
3. `dbname_match = re.search(r"dbname=(\S*)", conn_str)` – captures the value following “dbname=”.  
4. `user_match = re.search(r"user=(\S*)", conn_str)` – captures the value following “user=”.  
5. `search_path_match = re.search(r"search_path=(.*?)[' ]", conn_str)` – captures the value of the `search_path` option inside the `options` parameter; it stops at the next single‑quote or space character.

If a match is found, the corresponding entry in the result dictionary is replaced with the captured group (`group(1)`). After processing all five patterns, the dictionary is returned.

The function does not raise exceptions for missing components; absent fields simply remain `None`. It also does not validate the extracted values beyond the regular‑expression match.

**Relationship with Callers**  
`get_conn_info_dict` is used throughout the pedsnetdcc package to enrich log messages with connection details. Typical callers include:

* `run_bmi_calc` (pedsnetdcc/bmi.py) – obtains connection info for logging the start of a BMI calculation and for combining site‑specific log data.  
* `run_r_dose`, `run_r_drug_era`, `run_r_obs_covid`, `run_r_obs_recover`, `run_r_query`, `run_r_pcornet_*` – each extracts connection info to include user, host, and search_path in log entries before executing R scripts or SQL statements.  
* Permission‑granting functions (e.g., `grant_loading_user_permissions`, `grant_schema_permissions`) – retrieve connection info to log the beginning and end of permission changes.  
* Database execution helpers (`Statement.execute`, `Statement.execute_on_conn`, `StatementSet.parallel_execute`, `StatementList.serial_execute`) – call `get_conn_info_dict` to embed connection metadata in debug logs for each SQL statement.  
* Transformation runners (`run_transformation`, `run_target_transformation`, etc.) – use the function to build consistent log dictionaries that contain connection context for each transformation step.

By providing a uniform dictionary, the function enables consistent, structured logging across the entire code base without requiring each caller to parse the connection string individually.

**Note**  
* The regular expression for `search_path` assumes the option appears as `options='-c search_path=...` and stops at the next single quote or space; if the connection string contains multiple `-c` options, only the first `search_path` occurrence is captured.  
* The function does not extract other libpq parameters such as `password`, `sslmode`, or `connect_timeout`; callers that need those values must parse them separately (as seen in several modules where `re.search(r"password=(\S*)", conn_str)` is used).  
* Because the function uses `\S*` for most fields, it will capture characters up to the next whitespace, which works for typical connection strings but may include trailing punctuation if the string is malformed.

**Output Example**  
For the connection string  

`host=ahost port=5432 dbname=mydb user=dbuser options='-c search_path=myschema'`  

the function returns the dictionary  

`{'user': 'dbuser', 'host': 'ahost', 'port': '5432', 'dbname': 'mydb', 'search_path': 'myschema'}`.
## FunctionDef combine_dicts
**combine_dicts**: The function of combine_dicts is to create and return a new dictionary that contains the combined key‑value pairs of all supplied dictionary arguments, applying later arguments over earlier ones when keys overlap.

**parameters**:  
· *args*: a variable‑length sequence of objects that must be of type `dict`. Each dictionary contributes its key‑value pairs to the final result.

**Code Description**:  
The implementation starts by initializing an empty dictionary named `result`. It then iterates over the supplied `args` in the order they were passed. For each argument `arg`, the method `result.update(arg)` is executed. The `update` operation inserts all key‑value pairs from `arg` into `result`; if a key already exists in `result`, the value from the current `arg` overwrites the previous value. After processing all arguments, the populated `result` dictionary is returned to the caller. The function does not modify any of the input dictionaries; it only builds a fresh dictionary that aggregates their contents.

**Relationship with Callers**:  
`combine_dicts` is used throughout the pedsnetdcc codebase as a utility for constructing logging dictionaries that combine contextual information (such as site identifiers, connection details, and operation‑specific messages). Typical call patterns are:

- In `run_bmi_calc`, `combine_dicts` merges `{'site': site}` with the connection‑info dictionary returned by `get_conn_info_dict(conn_str)` to form `log_dict`. The same merged dictionary is later combined with a message dictionary for each log entry.
- In `_add_measurement_ids`, `combine_dicts` builds a `log_dict` that contains both the site identifier and the connection information, which is then used for start‑up and error logging.
- In `grant_loading_user_permissions`, `combine_dicts` merges the connection‑info dictionary with a static message dictionary before logging the start and end of the permission‑granting process.
- Similar usage appears in many other modules (e.g., `run_r_drug_era`, `run_r_obs_covid`, `run_subset_by_cohort`, `run_transformation`, etc.), where a consistent logging structure is required across different functional areas.

By centralizing the merging logic, the code ensures that all log messages share a uniform set of metadata without each caller having to repeat the merging code.

**Note**:  
- The function expects each argument to be a dictionary; passing a non‑dictionary object will raise a `TypeError` at runtime because `dict.update` requires a mapping.  
- Because later arguments overwrite earlier ones for duplicate keys, callers should order their arguments from most general to most specific when they need precedence.  
- The returned dictionary is a new object; modifications to it do not affect the original input dictionaries.  
- This utility does not perform deep merging; nested dictionaries are not merged recursively—only the top‑level keys are considered.

**Output Example**:  
If the function is called as `combine_dicts({'site': 'stlouis'}, {'user': 'loader', 'host': 'db.example.com'}, {'host': 'db.internal'})`, the returned dictionary will be:  

`{'site': 'stlouis', 'user': 'loader', 'host': 'db.internal'}`  

The `'host'` key from the third argument overwrites the value from the second argument.
## ClassDef StatementError
**StatementError**: The function of StatementError is to serve as a custom base exception for errors related to database statements.  

**attributes**: This class does not define any additional attributes beyond those inherited from RuntimeError.  

**Code Description**:  
StatementError is defined as a subclass of the built‑in RuntimeError. The class body contains only the `pass` statement, meaning it does not add new methods, properties, or initialization logic. By inheriting from RuntimeError, StatementError behaves like a standard exception but can be caught separately from generic runtime errors, allowing developers to distinguish statement‑related failures from other runtime issues.  

In the project, two other exception classes derive from StatementError:  

- **MissingDataError** (pedsnetdcc/utils.py) inherits directly from StatementError and represents situations where required data is absent when executing a statement.  
- **DatabaseError** (pedsnetdcc/utils.py) also inherits from StatementError and signals broader database‑level problems encountered during statement execution.  

Both subclasses rely on StatementError as their common ancestor, enabling code that catches StatementError to handle either missing data or general database errors uniformly, while still allowing more specific handling when needed.  

**Note**:  
- Use StatementError (or its subclasses) to raise exceptions that clearly indicate a problem with a database statement rather than using generic RuntimeError.  
- When catching exceptions, consider catching StatementError to handle all statement‑related errors in one block, or catch MissingDataError and DatabaseError separately for finer‑grained control.  
- Because the class adds no custom initialization, any message passed when raising the exception will be stored in the standard RuntimeError `args` attribute.
## ClassDef MissingDataError
**MissingDataError**: The function of MissingDataError is to signal that required data is absent when executing a database statement.  

**attributes**: This class does not define any additional attributes beyond those inherited from RuntimeError via StatementError.  

**Code Description**:  
MissingDataError is defined as a subclass of `StatementError`, which itself inherits from the built‑in `RuntimeError`. The class body contains only the `pass` statement, meaning it introduces no new methods, properties, or initialization logic. By extending `StatementError`, MissingDataError inherits the standard exception behavior of `RuntimeError` while providing a distinct type that can be caught separately from other statement‑related errors.  

In the project, the primary usage of MissingDataError occurs in the `check_stmt_data` function (located in the same `utils.py` module). `check_stmt_data` validates that a `Statement` object contains non‑empty data. If `stmt.data` is `None` or has zero length, the function creates an instance of MissingDataError with a descriptive message, logs the error, and raises the exception. This pattern allows calling code to detect the specific condition of missing data and handle it explicitly, while still enabling broader handling by catching the parent `StatementError` if desired.  

**Note**:  
- Raise MissingDataError when a database statement returns no data, using a clear message to aid debugging.  
- Catch MissingDataError separately when you need to distinguish missing‑data situations from other statement errors; otherwise, catching `StatementError` will also capture MissingDataError.  
- Because the class adds no custom initialization, any message supplied at raise time is stored in the standard `RuntimeError` `args` attribute and can be accessed via the exception instance.
## ClassDef DatabaseError
**DatabaseError**: The function of DatabaseError is to represent a generic failure that occurs at the database level while executing a SQL statement.  

**attributes**: This class does not define any additional attributes; it inherits the standard `args` attribute from `RuntimeError`, which stores the error message passed at raise time.  

**Code Description**:  
`DatabaseError` is declared as a subclass of `StatementError` with an empty body (`pass`). Because `StatementError` itself subclasses the built‑in `RuntimeError`, `DatabaseError` behaves exactly like a normal exception but can be distinguished from other runtime errors by its type. No custom initializer, methods, or properties are added, so any message supplied when the exception is raised is stored in the inherited `args` tuple.

The class serves as a semantic marker for “something went wrong with the database” and is used throughout the **pedsnetdcc** package. Typical call sites include:

* **Schema management** – `create_schema`, `drop_schema`, `schema_exists`, and `tables_in_schema` raise `DatabaseError` when a SQL statement fails (e.g., duplicate schema, missing schema, or errors listing tables).  
* **Transformation pipeline** – In `pedsnetdcc/transform_runner.py`, each private transformation helper (`_transform`, `_transform_target`, `_transform_age`, `_transform_concept`, `_transform_site`, `_transform_id`, `_transform_index`) checks the result of parallel statement execution and raises `DatabaseError` for any statement error that is not suppressed by the `force` flag. The high‑level functions (`run_transformation`, `run_target_transformation`, `run_age_transformation`, etc.) also raise `DatabaseError` when moving tables or performing final cleanup fails.  
* **Utility functions** – `check_stmt_err` converts a failed `Statement` into a `DatabaseError` and logs the incident. `set_logged` and `vacuum` iterate over a set of `Statement` objects; any statement error triggers a `DatabaseError`. `undo_transformation` raises `DatabaseError` if any step in the rollback transaction encounters an error.  

Because `DatabaseError` shares a common ancestor with `MissingDataError` (both inherit from `StatementError`), code that wishes to handle any statement‑related problem can catch `StatementError`. Code that needs to differentiate between missing data and broader database failures can catch `MissingDataError` and `DatabaseError` separately.

**Note**:  
* Raise `DatabaseError` whenever a low‑level SQL operation fails and the failure should be reported to the caller as a database‑specific problem.  
* When handling exceptions, prefer catching `StatementError` to cover both `DatabaseError` and `MissingDataError` in a single block, or catch `DatabaseError` alone for finer‑grained control.  
* The exception carries only the message string; no additional context (such as SQL text or error codes) is stored automatically. Callers typically embed that information in the message when raising, e.g., `DatabaseError('failed to create schema `{0}`: {1}'.format(schema, stmt.err))`.  
* Because the class adds no custom initialization, the standard `RuntimeError` behavior applies: the message is accessible via `str(e)` or `e.args[0]`.  
## FunctionDef check_stmt_data(stmt, caller_name)
**check_stmt_data**: The function of check_stmt_data is to verify that a database Statement object contains result data and to raise an error if the data is missing.

**parameters**:  
· stmt: a Statement instance whose execution result is being inspected.  
· caller_name: optional string identifying the calling function; used only for logging context.

**Code Description**:  
The function receives a Statement object (`stmt`) that has already been executed. It checks two conditions: whether `stmt.data` is `None` and whether the length of `stmt.data` is zero. If either condition is true, the function constructs a `MissingDataError` with a message that incorporates `stmt.msg` (the descriptive message attached to the statement). It then logs an error entry using the module‑level `logger`, including the caller name (if supplied) and the exception instance. Finally, the function raises the `MissingDataError`, causing execution to abort unless the exception is caught by the caller.

If the data check passes, the function simply returns `None` (implicit), allowing the calling code to continue processing the statement’s results.

**Reference Relationships**:  
`check_stmt_data` is invoked by many higher‑level utilities that perform ID assignment and data validation throughout the project. Notable callers include:

* `pedsnetdcc/bmi.py/_add_measurement_ids` – after counting new measurement IDs and after updating the last‑ID tracking table.  
* `pedsnetdcc/check_fact_relationship.py/check_fact_relationship` – for each statement that is expected to return a count or other scalar value (e.g., total and bad record counts).  
* `pedsnetdcc/era.py/_add_era_ids` – after counting new era IDs and after the last‑ID update statement.  
* `pedsnetdcc/external_id_mapper.py/map_external_ids` – after the statement that updates the last‑ID tracking table.  
* `pedsnetdcc/id_mapping_transform.py/IDMappingTransform/pre_transform` – after counting new ID mappings and after the last‑ID update statement.  
* `pedsnetdcc/r_obs_covid.py/_add_observation_ids`, `pedsnetdcc/r_obs_recover.py/_add_observation_ids`, and similar observation‑ID assignment functions – after counting new observation IDs and after the last‑ID update statement.

All these callers first execute a `Statement`, then call `check_stmt_err` (which validates that no execution error occurred), and finally call `check_stmt_data` to ensure that the statement produced a non‑empty result set before the code accesses `stmt.data`.

`check_stmt_data` does not call any other functions; its only external dependency is the `MissingDataError` class defined in the same module and the `logger` object for structured logging.

**Note**:  
- The function does not return a value; its purpose is to raise an exception when data is absent.  
- Because it raises `MissingDataError`, callers can either catch this specific exception or catch the broader `StatementError`/`RuntimeError` hierarchy.  
- The logged error includes both the caller identifier (`caller_name`) and the exception object, which aids debugging in complex pipelines where many statements are executed in sequence.  
- Ensure that `stmt.msg` is set to a meaningful description when the `Statement` is created; this message will appear in the raised exception’s text.

**Output Example**:  
If a statement that should return a count yields no rows, the function raises:

```
MissingDataError: data not returned from counting new IDs needed for measurement
```

The logger records an entry similar to:

```
{'msg': 'exiting assign measurement ids', 'err': MissingDataError('data not returned from counting new IDs needed for measurement')}
```
## FunctionDef check_stmt_err(stmt, caller_name, start_time)
**check_stmt_err**: The function of check_stmt_err is to detect an error on a database statement, log detailed information about the failure, and raise a DatabaseError exception.

**parameters**  
· stmt: the Statement object that has been executed and whose error status is to be examined.  
· caller_name (optional): a string identifying the calling function, used in the log entry to indicate where the error was detected.  
· start_time (optional): a float representing the start time of the surrounding operation; if supplied, the elapsed time is calculated and included in the log.

**Code Description**  
The function begins by inspecting the `err` attribute of the supplied `stmt`. If `stmt.err` is not `None`, an error condition is present. In that case a `DatabaseError` instance is created with a message that incorporates three pieces of information from the statement: `stmt.msg` (a human‑readable description of the operation), `stmt.sql` (the SQL command that was executed), and `stmt.err` (the error message returned by the database).  

A dictionary named `log_dict` is assembled for logging. It always contains the key `'msg'` with a value indicating that the caller is exiting (e.g., `"exiting <caller_name>"`), and the key `'err'` with the newly created `DatabaseError` object. If a `start_time` value was provided, the function computes the elapsed time by calling `secs_since(start_time)` and adds this as the `'elapse'` entry in the dictionary.  

The function then writes the dictionary to the module‑level `logger` using `logger.error`, ensuring that the failure is recorded with the appropriate severity. Finally, the `DatabaseError` is raised, propagating the exception to the caller. If `stmt.err` is `None`, the function simply returns without performing any logging or raising an exception.

From a functional perspective, `check_stmt_err` is a central error‑handling utility used throughout the pedsnetdcc codebase. It is invoked after every execution of a `Statement` object in modules such as `bmi.py`, `era.py`, `r_obs_covid.py`, `split_measurement.py`, and many others. By standardising the error detection, logging, and exception raising, it provides consistent behaviour across all database operations. The only external objects it directly interacts with are the `DatabaseError` class (used to construct the exception) and the `logger` (used for error logging). It also relies on the helper function `secs_since` to compute elapsed time when timing information is supplied.

**Note**  
- The function assumes that the `stmt` object provides the attributes `err`, `msg`, and `sql`. If any of these are missing, an AttributeError will be raised before the intended DatabaseError.  
- The caller should supply a meaningful `caller_name` to make log entries clear; otherwise the default empty string will produce a generic log message.  
- Providing `start_time` is optional but recommended when the surrounding operation wants to record how long it took before the failure occurred.  
- Because the function raises `DatabaseError`, any code that calls `check_stmt_err` should be prepared to handle this exception, either by allowing it to propagate or by catching it and performing additional cleanup.  
- The function does not modify the original `Statement` object; it only reads its error state. Therefore, it can be safely called multiple times on the same statement, though repeated logging of the same error may be redundant.
## FunctionDef stock_metadata(model_version)
**stock_metadata**: The function of **stock_metadata** is to obtain a SQLAlchemy MetaData object that represents the PEDSnet data model for a specified model version.

**parameters**  
· model_version: a string indicating the PEDSnet model version (e.g., "2.2.0").

**Code Description**  
The function begins by creating an empty `MetaData` instance from SQLAlchemy:

```python
metadata = sqlalchemy.MetaData()
```

It then populates this `MetaData` with table, column, and constraint definitions by calling `dmsa.make_model_from_service`. The call supplies four arguments:

1. The name of the data service – the literal string `'pedsnet'`.  
2. The `model_version` supplied to the function.  
3. `DATA_MODELS_SERVICE`, a constant that identifies the remote service endpoint from which the model definition is retrieved.  
4. The previously created empty `metadata` object that will be filled.

The `make_model_from_service` routine contacts the PEDSnet data‑model service, fetches the schema definition for the requested version, and registers all tables, columns, primary‑key constraints, foreign‑key constraints, and other schema objects into the provided `MetaData`. Finally, the populated `MetaData` instance is returned to the caller.

**Relationship with Callers**  
A large portion of the codebase relies on **stock_metadata** to gain read‑only access to the canonical PEDSnet schema:

* In `pedsnetdcc/bmi.py/_add_measurement_ids`, `stock_metadata` supplies the table object for `"measurement"` and enables construction of template variables for ID‑generation SQL.
* In `pedsnetdcc/era.py/_add_era_ids`, it provides the era‑related tables needed to count unmapped IDs and to create sequences.
* In `pedsnetdcc/foreign_keys.py/_foreign_keys_from_model_version`, it is used to enumerate all foreign‑key constraints for a given version, optionally filtering vocabulary tables.
* In `pedsnetdcc/indexes.py/_indexes_sql` and related index‑generation helpers, it supplies the table definitions required to build CREATE/DROP INDEX statements.
* In transformation utilities (`pedsnetdcc/transform_runner.py` and its helper functions), it supplies the base model before any transformation objects modify it.
* In test suites (`tests/*`), each test creates a fresh `MetaData` via **stock_metadata** to verify that transformations, index creation, primary‑key handling, and other operations work against the expected schema.

Thus, **stock_metadata** is the single source of truth for the untransformed PEDSnet data model throughout the package. All higher‑level operations (ID assignment, constraint extraction, index generation, schema migration, and testing) depend on the accurate and version‑specific metadata returned by this function.

**Note**  
* The function does **not** cache the metadata; each call creates a new `MetaData` instance and re‑downloads the model definition. Callers that need to reuse the same metadata should store the returned object locally.  
* The function expects that `DATA_MODELS_SERVICE` and the `dmsa` module are correctly configured; otherwise, a runtime error will be raised when the service cannot be reached or the version is unknown.  
* The returned `MetaData` reflects the *stock* (i.e., untransformed) model. If transformation objects have been applied, callers typically invoke their `modify_metadata` methods on the returned object to obtain the transformed schema.

**Output Example**  
A typical call:

```python
metadata = stock_metadata('2.2.0')
```

returns an instance similar to:

```
MetaData(bind=None)
  Table('person', MetaData(...), Column('person_id', Integer, primary_key=True), Column('gender_concept_id', Integer), ...)
  Table('measurement', MetaData(...), Column('measurement_id', BigInteger), Column('person_id', Integer), ...)
  Table('condition_occurrence', MetaData(...), ...)
  ...
```

The object contains a dictionary `metadata.tables` mapping table names to `Table` objects, each of which holds column definitions and constraint collections. This `MetaData` can be inspected, iterated over, or passed to SQLAlchemy engines for further operations.
## FunctionDef set_logged(conn_str, model_version, vocabulary, tables)
**set_logged**: The function of set_logged is to change PostgreSQL tables from the UNLOGGED state back to the default LOGGED state.

**parameters**:  
· conn_str (str): PostgreSQL connection string used by the underlying Statement objects.  
· model_version (str): PEDSnet model version (e.g., “2.2.0”) that determines which tables belong to the schema.  
· vocabulary (bool, optional): When True, operate only on the predefined vocabulary tables (VOCAB_TABLES). When False (default), operate on all non‑vocabulary tables.  
· tables (list of str, optional): Explicit list of table names to set to logged. If supplied, it overrides the automatic selection based on model_version and the vocabulary flag.

**Code Description**:  
1. The function imports the Statement and StatementSet classes from *pedsnetdcc.db*.  
2. It determines the set of table names to process.  
   * If the *tables* argument is provided, that list is used directly.  
   * If *tables* is empty, the function obtains a fresh SQLAlchemy MetaData object for the requested *model_version* by calling **stock_metadata**.  
   * When *vocabulary* is True, the table list is set to the constant **VOCAB_TABLES**.  
   * When *vocabulary* is False, the list is built from all tables present in the metadata minus the vocabulary tables.  
3. A **StatementSet** instance (named *stmts*) is created to hold the DDL statements.  
4. For each table name in *table_names*, a SQL string `alter table <table> set logged` is formatted and wrapped in a **Statement** object together with a human‑readable message `setting table <table> to logged`. Each Statement is added to the StatementSet.  
5. The StatementSet’s **parallel_execute** method is invoked with *conn_str*. This method launches a pool of worker processes (up to 25) that execute each ALTER TABLE statement concurrently, logs progress, and returns the modified StatementSet.  
6. After parallel execution, the function iterates over the statements in *stmts*. If any statement has a non‑null *err* attribute, a **DatabaseError** is raised with a message that includes the offending SQL and the error text.  
7. The function does not return a value; successful completion means all specified tables are now LOGGED.

**Reference Relationships**:  
*Callers* – The function is invoked by many high‑level utilities that need to ensure tables are LOGGED after bulk operations:  
  • **merge_site_data** and **merge_data_to_schema** (both in *pedsnetdcc/merge_site_data.py*) call set_logged after merging site data.  
  • All transformation runners in *pedsnetdcc/transform_runner.py* (run_transformation, run_target_transformation, run_age_transformation, run_concept_transformation, run_concept_age_transformation, run_site_transformation, run_id_transformation, run_index_transformation) call set_logged to guarantee the target tables are LOGGED before applying constraints, indexes, or moving schemas.  

*Callees* – Within set_logged the following objects are used:  
  • **stock_metadata** (pedsnetdcc/utils.py) to retrieve the canonical table list for a given model version.  
  • **Statement** and **StatementSet** (pedsnetdcc/db.py) to build and execute the ALTER TABLE commands in parallel.  
  • **DatabaseError** (pedsnetdcc/utils.py) to propagate any execution failures to the caller.  

**Note**:  
- The function relies on PostgreSQL’s `ALTER TABLE … SET LOGGED` command; it will fail if the table does not exist in the current search_path or if the user lacks ALTER privileges.  
- Because the function uses **parallel_execute**, the order of execution is nondeterministic, but each statement is independent, so ordering does not affect the outcome.  
- If *tables* is supplied, the function does not validate that the names actually exist; any invalid name will cause the corresponding Statement to raise an error that is turned into a DatabaseError.  
- The *vocabulary* flag is mutually exclusive with an explicit *tables* list; providing both will cause the explicit list to win.  
- No explicit transaction is used; each ALTER TABLE runs in its own autocommit context inside the worker processes.  

**Output Example**:  

```python
# Example usage
conn_str = "postgresql://user:pwd@host/dbname"
model_version = "2.2.0"

# Set all non‑vocabulary tables to LOGGED
set_logged(conn_str, model_version)

# Set only the vocabulary tables to LOGGED
set_logged(conn_str, model_version, vocabulary=True)

# Set a specific subset of tables
set_logged(conn_str, model_version, tables=["person", "measurement"])
```

If all statements succeed, the function returns `None`. If any ALTER TABLE fails, a `DatabaseError` is raised, e.g.:

```
DatabaseError: setting tables to logged: alter table person set logged: permission denied for relation person
```
## FunctionDef vacuum(conn_str, model_version, analyze, vocabulary, tables)
**vacuum**: The function of vacuum is to execute a VACUUM (optionally VACUUM ANALYZE) on one or more tables in a PEDSnet database for a specified model version.

**parameters**:  
· **conn_str** (str): libpq connection string used to connect to the PostgreSQL database.  
· **model_version** (str): PEDSnet data model version (e.g., “2.2.0”). Determines which tables belong to the model.  
· **analyze** (bool, default False): If True, the generated SQL is “VACUUM ANALYZE <table>”; otherwise it is “VACUUM <table>”.  
· **vocabulary** (bool, default False): When True, the function operates only on the vocabulary tables defined in the constant VOCAB_TABLES; when False it operates on all non‑vocabulary tables.  
· **tables** (list of str, optional): Explicit list of table names to vacuum. If supplied it overrides the automatic selection based on the model version and the vocabulary flag.

**Code Description**:  
1. **Import dependencies** – The function imports the Statement and StatementSet classes from pedsnetdcc.db. These classes encapsulate individual SQL commands and provide a container that can execute many statements in parallel.  

2. **Determine target tables** –  
   * If the caller supplies a non‑empty tables list, that list is used directly.  
   * If tables is falsy, the function calls stock_metadata(model_version) to obtain a SQLAlchemy MetaData object that contains the full set of tables for the requested model version.  
   * When vocabulary is True, the function selects only the tables listed in the module‑level constant VOCAB_TABLES.  
   * When vocabulary is False, it selects every table defined in the metadata except those in VOCAB_TABLES. The resulting list is stored in tables.  

3. **Create a StatementSet** – An empty StatementSet instance stmts is created to hold the VACUUM statements.  

4. **Build SQL template** –  
   * Base template: `'VACUUM {0}'`.  
   * If analyze is True, the template becomes `'VACUUM ANALYZE {0}'`.  

5. **Add statements to the set** – For each table name in tables, a Statement object is instantiated with the formatted SQL (`sql_tpl.format(table)`) and a human‑readable message (`msg_tpl.format(table)`). Each statement is added to stmts.  

6. **Execute in parallel** – The method stmts.parallel_execute(conn_str) dispatches all VACUUM statements to worker processes. VACUUM cannot be run inside a transaction block; each worker executes its statement in autocommit mode, which is handled by the Statement implementation.  

7. **Error handling** – After parallel execution, the function iterates over the statements. If any statement has a non‑null err attribute, a DatabaseError is raised with a message that includes the offending SQL and the error text. This propagates the failure to the caller.  

8. **Return value** – The function does not return a value; successful completion implies that all requested tables have been vacuumed (and optionally analyzed) without error.

**Reference Relationships**:  
* **Callers** – The vacuum function is invoked by many high‑level utilities that need to clean up tables after data manipulation:  
  - `run_bmi_calc` (in pedsnetdcc/bmi.py) vacuums the BMI output table.  
  - `run_era` (in pedsnetdcc/era.py) vacuums the era output table unless novac is True.  
  - `merge_site_data` (and its variant merge_data_to_schema) vacuums all DCC tables after merging site data.  
  - `run_r_dose`, `run_r_drug_era`, `run_r_obs_covid`, `run_r_obs_recover` vacuum their respective derivation tables after R script execution.  
  - `split_measurement_table` vacuumes the three split measurement tables.  
  - `run_subset_by_cohort` and `run_subset_pcornet_by_cohort` vacuum the tables they create.  
  - `sync_observation_period` vacuumes the observation_period table after rebuilding it.  
  - Unit tests in pedsnetdcc/tests/utils_test.py verify basic vacuum behavior.  

* **Callees** – Within vacuum, the following objects are used:  
  - `stock_metadata` (to retrieve the model’s metadata when a table list is not supplied).  
  - `Statement` (to represent each VACUUM command).  
  - `StatementSet` (to batch‑execute the statements in parallel).  
  - `DatabaseError` (to raise a consistent exception when any statement fails).  

**Note**:  
- VACUUM is a maintenance command that reclaims storage and updates planner statistics. It must be run outside of an explicit transaction; the parallel workers handle this automatically.  
- The function respects the vocabulary flag, allowing callers to vacuum only the vocabulary schema (which is typically small) or the rest of the model.  
- Errors from individual VACUUM statements are not aggregated; the function raises on the first encountered error after all workers have finished.  
- Because the function uses StatementSet.parallel_execute without specifying a pool size, the number of worker processes defaults to the number of tables (capped at 25). This is sufficient for typical use cases where only a handful of tables are vacuumed.  
- The function does not return any data; callers should rely on the absence of an exception to confirm success.  

**Output Example**:  
```python
# Vacuum a single table without ANALYZE
vacuum(conn_str, '2.2.0', tables=['person'])

# Vacuum all non‑vocabulary tables with ANALYZE
vacuum(conn_str, '2.2.0', analyze=True)

# Vacuum only vocabulary tables
vacuum(conn_str, '2.2.0', vocabulary=True)
```  
In each call, successful execution completes silently; if a table cannot be vacuumed, a DatabaseError is raised, e.g.:

```python
DatabaseError: setting tables to logged: VACUUM person: permission denied
```
## FunctionDef pg_error(stmt)
**pg_error**: The function of pg_error is to return the PostgreSQL error condition name associated with a executed Statement, or an empty string when no error is present.  

**parameters**:  
· stmt: a Statement object that has been executed; the object is expected to have an attribute `err` representing the exception raised by psycopg2 (if any).  

**Code Description**:  
The function first checks whether the `stmt.err` attribute exists and whether it contains a non‑empty `pgcode` attribute. The `pgcode` attribute is the five‑character SQLSTATE code supplied by PostgreSQL for the error. If both conditions are satisfied, the function uses `psycopg2_errorcodes.lookup` to translate the SQLSTATE code into its symbolic name (for example, `'23505'` becomes `'UNIQUE_VIOLATION'`). If the lookup succeeds and a symbolic name is returned, that name is returned to the caller. If the statement has no error, the error object lacks a `pgcode`, or the lookup does not find a matching name, the function falls through to the final line and returns an empty string.  

In the project, `pg_error` is used by several higher‑level utilities to decide whether a particular PostgreSQL error can be safely ignored when the `force` flag is set. For example:  
- In `pedsnetdcc/schema.py/create_schema`, the function checks `pg_error(stmt) == 'DUPLICATE_SCHEMA'` to suppress the “schema already exists” error when `force=True`.  
- In `pedsnetdcc/transform_runner.py/_transform` and related transformation helpers, it checks `pg_error(stmt) == 'DUPLICATE_TABLE'` to skip duplicate‑table errors under `force=True`.  
- In the final cleanup phases of `run_transformation`, `run_target_transformation`, and other runner functions, it checks for `'IN_FAILED_SQL_TRANSACTION'` to ignore errors that are a side‑effect of a previously failed transaction.  

Thus, `pg_error` acts as a small utility that normalises PostgreSQL error handling across the codebase, allowing callers to write clear conditional logic based on symbolic error names rather than raw SQLSTATE codes.  

**Note**:  
- The function depends on the `psycopg2_errorcodes` module; if the module is not imported or the lookup fails, the function will simply return an empty string.  
- Only errors that expose a `pgcode` attribute (standard psycopg2 exceptions) are processed; other exception types will result in an empty string.  
- Callers should treat an empty string as “no recognizable PostgreSQL error” and handle it accordingly.  

**Output Example**:  
If a statement fails with SQLSTATE `42P06` (duplicate schema), `pg_error(stmt)` returns `'DUPLICATE_SCHEMA'`.  
If a statement succeeds or the error does not have a `pgcode`, the function returns `''` (empty string).
