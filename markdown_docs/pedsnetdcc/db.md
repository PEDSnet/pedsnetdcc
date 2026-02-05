## ClassDef Statement
**Statement**: The function of Statement is to encapsulate a single SQL command together with its execution metadata, allowing the command to be run against a PostgreSQL database (either by creating a new connection from a connection string or by using an existing DB‑API connection) and to be safely used in parallel execution collections.

**attributes**  
· sql: the raw SQL string that will be sent to the database.  
· msg: a human‑readable description of the purpose of the statement; defaults to “executing SQL”.  
· _id_: a unique identifier for the statement instance; generated with `uuid.uuid4()` when not supplied. The identifier is stored internally as `_id_` and exposed through a read‑only property `id_`.  
· data: the result set returned by the statement after execution; `None` when the statement does not produce rows.  
· fields: a list of column names extracted from the cursor description after execution.  
· rowcount: the number of rows affected by the statement (e.g., INSERT, UPDATE, DELETE) or the number of rows fetched for SELECT statements; `None` before execution.  
· err: any exception object raised during execution; `None` when execution succeeds.  

**Code Description**  
The `Statement` class is defined in `pedsnetdcc/db.py`. It provides a lightweight, immutable container for a SQL command and its associated metadata.  

*Construction* – The constructor (`__init__`) stores the supplied `sql` and `msg`, generates a UUID for `_id_` when not provided, and initializes execution‑related attributes (`data`, `fields`, `rowcount`, `err`) to empty defaults.  

*Identity & hashing* – Equality (`__eq__`) and hashing (`__hash__`) are based solely on the immutable `id_` value, enabling `Statement` objects to be placed in sets or used as keys in dictionaries (e.g., `StatementSet`).  

*String representations* – `__repr__` returns a detailed constructor‑like string useful for debugging, while `__str__` provides a concise description that includes the `msg`.  

*Immutable identifier* – The `id_` property getter returns the internal `_id_`. The setter deliberately raises `RuntimeError` to enforce immutability, preventing accidental changes that would break set membership or parallel tracking.  

*Logging* – A class‑level reference to the module logger (`logger = logger`) is kept. The private method `_get_logger` chooses between the module logger and a `QueueHandler` logger when a logging queue (`logq`) is supplied. This supports logging from child processes during parallel execution.  

*Execution flow* –  
1. `execute(self, conn_str, resq=None, logq=None)` creates a new `psycopg2` connection from `conn_str`, sets autocommit (`ISOLATION_LEVEL_AUTOCOMMIT`), and delegates to `execute_on_conn`. Connection errors are caught, stored in `self.err`, and logged. If a result queue (`resq`) is provided, the statement (including any error) is placed on the queue after handling. The connection is always closed.  
2. `execute_on_conn(self, conn, resq=None, logq=None)` performs the actual SQL work on an existing DB‑API connection. It first resets all execution‑related attributes to allow safe re‑execution. It logs the statement details, executes the SQL via a cursor, captures `rowcount`, extracts column names into `self.fields`, and attempts to fetch all rows into `self.data`. A `psycopg2.ProgrammingError` with the message “no results to fetch” is silently ignored for statements that do not return rows. Any other exception is stored in `self.err` and logged. If a result queue is supplied, the statement object is enqueued after processing. The method returns the statement instance for chaining.  

*Integration with the project* – Throughout the codebase, higher‑level functions (e.g., `_fill_age_in_months`, `_add_measurement_ids`, `run_bmi_calc`, `run_era`, `run_r_obs_covid`, etc.) instantiate `Statement` objects with specific SQL strings and descriptive messages. They then call `stmt.execute(conn_str)` to run the command. Collections such as `StatementSet` aggregate many `Statement` objects and invoke `parallel_execute`, which internally calls each statement’s `execute` method in separate worker processes. After execution, callers typically invoke helper functions like `check_stmt_err(stmt, context)` and `check_stmt_data(stmt, context)` to raise exceptions if `stmt.err` is set or if expected data is missing. This pattern centralizes error handling, logging, and result collection.  

**Note**  
- The `id_` attribute is immutable; attempting to assign a new value raises `RuntimeError`.  
- After each execution, `data`, `fields`, `rowcount`, and `err` are reset, so re‑executing a statement will produce fresh results.  
- For statements that do not return a result set (e.g., DDL or DML without RETURNING), `self.data` remains `None`; callers must check `self.rowcount` or rely on the absence of `self.err`.  
- When used in parallel contexts, provide a logging queue (`logq`) if you need child‑process logs to be collected centrally; otherwise the module logger is used.  
- Always inspect `stmt.err` after execution; the class logs errors at DEBUG level but does not raise them automatically.  

**Output Example**  
```python
stmt = Statement("SELECT person_id FROM dcc_pedsnet.person LIMIT 5", "fetch sample persons")
stmt.execute(conn_str)

print(stmt)                     # -> Statement for fetch sample persons
print(stmt.id_)                 # -> 550e8400-e29b-41d4-a716-446655440000 (example UUID)
print(stmt.rowcount)           # -> 5
print(stmt.fields)             # -> ['person_id']
print(stmt.data)               # -> [(12345,), (67890,), (13579,), (24680,), (11223,)]
print(stmt.err)                # -> None
```
### FunctionDef __init__(self, sql, msg, id_)
**__init__**: The function of __init__ is to initialize a new Statement object with default values and provided arguments.  

**parameters**: The parameters of this Function.  
· sql: a string containing the SQL statement to be executed.  
· msg: a string describing the purpose of the SQL; defaults to 'executing SQL' when not supplied.  
· id_: an optional unique identifier for the Statement; if omitted, a new UUID is generated.  

**Code Description**:  
The constructor assigns the incoming `sql` argument to the instance attribute `self.sql`, preserving the raw SQL text for later execution.  
The `msg` argument is stored in `self.msg`; this message is typically used for logging or user feedback to indicate what the statement is intended to do.  
For the identifier, the code evaluates `id_ or uuid.uuid4()`. If the caller supplies a value for `id_`, that value is used; otherwise, `uuid.uuid4()` creates a new universally unique identifier, which is stored in `self._id_`. This ensures every Statement instance can be distinguished even when the caller does not provide an explicit ID.  
The constructor also prepares several attributes that will be populated during the statement’s lifecycle:  
- `self.data` is initialized to `None` and will later hold fetched result rows.  
- `self.fields` is set to an empty list, intended to store column metadata after execution.  
- `self.rowcount` is set to `None` and will later reflect the number of rows affected or returned.  
- `self.err` is set to `None` and will be used to capture any exception information if execution fails.  

By establishing these defaults, the Statement object is ready for consistent handling throughout the data‑access layer, regardless of which optional arguments are supplied at creation time.  

**Note**:  
- The identifier attribute is named `_id_` (with leading and trailing underscores) to avoid clashing with Python’s built‑in `id` function.  
- The constructor does not perform any validation on the `sql` or `msg` strings; callers should ensure they are appropriate for the target database.  
- If a custom `id_` is provided, it should be hashable and comparable, as it may be used as a key in collections.  
- The `uuid` module must be imported in the module where this class is defined; otherwise, a `NameError` will occur.
***
### FunctionDef __eq__(self, other)
**__eq__**: The function of __eq__ is to determine logical equality between two Statement objects based on their unique identifiers.  
**parameters**:  
· other: the object to compare with the current Statement instance. It is expected to be another Statement object.  

**Code Description**:  
The method first checks whether *other* is an instance of the same class as *self* (`self.__class__`). If the type check fails, the special value `NotImplemented` is returned, allowing Python’s fallback mechanisms to handle the comparison (e.g., trying the reflected operation or defaulting to inequality). When the type check succeeds, the method retrieves the immutable identifier of each Statement by invoking the `id_` accessor (`self.id_` and `other.id_`). Equality is then evaluated by a simple comparison of these identifiers (`self.id_ == other.id_`). The result is a Boolean value: `True` if the identifiers match, otherwise `False`.  

From a functional perspective, `__eq__` is a core component of the Statement class’s identity contract:

- **Callees**: It calls the read‑only `id_` method to obtain each object’s identifier. The `id_` accessor returns the value stored in the hidden attribute `_id_`, which is immutable for the lifetime of the object.
- **Callers**: The `__ne__` method implements inequality by delegating to `__eq__` and negating its result (`return not self.__eq__(other)`). Other parts of the codebase that rely on object comparison (e.g., collections, testing frameworks, or custom logic) implicitly invoke `__eq__` when the `==` operator is used with Statement instances.
- **Related methods**: `__hash__` uses the same identifier (`self.id_`) to compute a stable hash value, ensuring that objects considered equal by `__eq__` also share the same hash, which is required for correct behavior in sets and dictionary keys. `__repr__` includes the identifier in its string representation, providing a human‑readable form that aligns with the equality semantics.

**Note**:  
- The identifier returned by `id_` must remain constant; any mutation would break the equality and hashing contracts.  
- Returning `NotImplemented` for mismatched types is intentional; it enables Python to attempt the reflected comparison or to fall back to default inequality handling.  
- Do not rely on `__eq__` to compare objects of different classes; it will not raise an error but will simply return `NotImplemented`.  

**Output Example**:  
```python
stmt1 = Statement(..., id_=42)
stmt2 = Statement(..., id_=42)
stmt3 = Statement(..., id_=99)

stmt1 == stmt2   # returns True
stmt1 == stmt3   # returns False
stmt1 == "foo"   # returns NotImplemented (treated as False in a boolean context)
```
***
### FunctionDef __ne__(self, other)
**__ne__**: The function of __ne__ is to determine logical inequality between two Statement objects by negating the result of their equality comparison.  

**parameters**:  
· self: the current Statement instance on which the inequality operator is invoked.  
· other: the object to compare against the current Statement instance; typically another Statement object.  

**Code Description**:  
The method consists of a single statement: `return not self.__eq__(other)`. It delegates the actual comparison work to the `__eq__` method of the same class. `__eq__` returns either a Boolean value (`True` if the two objects have identical identifiers, `False` otherwise) or the special sentinel `NotImplemented` when `other` is not an instance of the same class. The `not` operator then inverts the Boolean result. If `__eq__` yields `NotImplemented`, Python treats that value as truthy, so `not NotImplemented` evaluates to `False`. Consequently, `__ne__` returns a Boolean indicating whether the two objects are considered unequal according to the identifier‑based equality logic.  

From a functional perspective, `__ne__` provides the complementary behavior required by Python’s data model: when the `!=` operator is used with Statement instances, the interpreter calls this method, which in turn relies on the established identity contract implemented in `__eq__`. This ensures that inequality is consistent with equality and that both operators are based on the immutable `id_` attribute of each Statement.  

**Note**:  
- The method does not perform its own type checking; it depends entirely on `__eq__` for that responsibility.  
- Because `__eq__` may return `NotImplemented` for mismatched types, `__ne__` will return `False` in such cases, which aligns with Python’s fallback handling of inequality.  
- The correctness of `__ne__` is tied to the immutability of the identifier returned by `id_`; any change to that identifier would break the logical relationship between equality and inequality.  

**Output Example**:  
```python
stmt_a = Statement(..., id_=10)
stmt_b = Statement(..., id_=10)
stmt_c = Statement(..., id_=20)

stmt_a != stmt_b   # returns False (identifiers are equal)
stmt_a != stmt_c   # returns True  (identifiers differ)
stmt_a != "foo"    # returns False (stmt_a.__eq__("foo") returns NotImplemented, not NotImplemented -> False)
```
***
### FunctionDef __hash__(self)
**__hash__**: The function of __hash__ is to return a unique integer identifier derived from the statement’s immutable id, enabling Statement objects to be used in hash‑based collections.

**parameters**: This function takes no parameters.

**Code Description**:  
`__hash__` computes and returns the built‑in Python hash of the statement’s identifier by calling `self.id_`. The `id_` accessor retrieves the value stored in the private attribute `_id_`, which is assigned once when the `Statement` instance is created and never changes thereafter. By delegating to `self.id_`, `__hash__` guarantees that the hash value is stable for the lifetime of the object. This stability is essential for correct behavior when a `Statement` instance is placed in a `set` or used as a key in a `dict`. The method relies on the immutability enforcement provided by the `id_` setter (which raises a `RuntimeError` on any assignment attempt), ensuring that the hash cannot become inconsistent due to identifier mutation.

**Note**:  
- The returned hash is based solely on the immutable identifier; any change to `_id_` (which should never occur) would break the contract of hashability.  
- Because `__hash__` does not consider the SQL text or other attributes, two distinct `Statement` objects with the same identifier will have identical hash values and will be considered equal by `__eq__`.  
- Use `Statement` objects in sets or as dictionary keys only when you are certain that their identifiers remain constant.  

**Output Example**:  
If a `Statement` instance has an identifier of `42`, calling `hash(stmt)` (which invokes `stmt.__hash__()`) might produce a value such as:

```
1234567890123456789
```
***
### FunctionDef __repr__(self)
**__repr__**: The function of __repr__ is to return an unambiguous string that represents the current state of a Statement instance, including its SQL text, message, and unique identifier.  

**parameters**: This method takes no parameters.  

**Code Description**:  
The `__repr__` method builds a formatted string using Python’s `str.format` syntax. It accesses three attributes of the Statement object:

1. `self.sql` – the raw SQL command stored in the instance.  
2. `self.msg` – a human‑readable message that describes the purpose or context of the statement.  
3. `self.id_()` – the accessor method that returns the statement’s immutable identifier from the hidden attribute `_id_`.

The method concatenates these values into the pattern  

`Statement(sql={0}, msg={1}, id_={2})`  

and returns the resulting string. Because `id_` is a method, calling it inside `format` invokes the accessor, ensuring that the identifier is retrieved via the class’s public interface rather than directly accessing the private attribute. This design keeps the representation consistent with other parts of the class that rely on `id_`, such as equality (`__eq__`), hashing (`__hash__`), and logging (`_get_logger`). The returned string is intended for debugging, logging, or interactive sessions where a clear, reproducible depiction of the object is valuable.

**Note**:  
- The method does not perform any validation; it assumes that `self.sql`, `self.msg`, and `self.id_()` are already set and valid.  
- Because `__repr__` is used by the Python interpreter when printing objects in the console or logs, any change to the internal representation of these attributes will be reflected automatically in the output.  
- The identifier returned by `id_()` must remain immutable for the representation to stay reliable; attempts to modify the identifier will raise a `RuntimeError` via the `id_` setter.  
- Do not rely on `__repr__` for user‑facing messages; it is meant for developers and debugging purposes.  

**Output Example**:  

```
Statement(sql=SELECT * FROM patients WHERE age > 18, msg=Fetching adult patients, id_=42)
```
***
### FunctionDef __str__(self)
**__str__**: The function of __str__ is to return a human‑readable string that identifies the Statement instance.  
**parameters**: This method takes no parameters.  
· self: the Statement object whose string representation is being generated.  

**Code Description**:  
The method constructs a formatted string using Python’s `str.format` method. It inserts the value of the instance attribute `msg` into the placeholder `{0}` of the template `'Statement for {0}'`. The resulting string is then returned. This provides a concise description that can be used for logging, debugging, or any context where the object is implicitly converted to a string (e.g., `print(statement)` or `str(statement)`).  

**Note**:  
- The attribute `msg` must be defined on the Statement instance before `__str__` is called; otherwise, an `AttributeError` will be raised.  
- The method does not modify the object’s state; it only reads the `msg` attribute.  

**Output Example**:  
If a Statement object has `msg = "patient_data_load"`, then `str(statement)` will produce:  

`Statement for patient_data_load`
***
### FunctionDef id_(self)
**id_**: The function of id_ is to retrieve the statement’s unique identifier stored in the hidden internal attribute `_id_`.

**parameters**: This function takes no parameters.

**Code Description**:  
The `id_` method is a simple accessor that returns the value held in the private attribute `self._id_`. The identifier is assigned when a `Statement` object is created (outside the shown snippet) and remains immutable for the lifetime of the object. By exposing the identifier through a dedicated method, the class encapsulates the internal storage while still allowing external code to read the value.

The identifier returned by `id_` is central to several other methods in the `Statement` class:

- `__eq__` compares two `Statement` objects by checking whether their `id_` values are equal, establishing logical equality based on the identifier.
- `__hash__` computes a hash from `self.id_`, enabling `Statement` instances to be used as keys in dictionaries or members of sets.
- `__repr__` includes the identifier in the string representation, making debugging output more informative.
- `_get_logger` creates a logger named after the identifier (`logging.getLogger(str(self.id_))`) when a logging queue is supplied, ensuring that log messages can be traced back to the specific statement.
- Both `execute` and `execute_on_conn` embed the identifier in diagnostic dictionaries (`msg_dict`) that are logged at debug level. This provides clear traceability of which statement caused a connection or database error.
- In error handling paths, the identifier is added to the message dictionary (`{'id': self.id_}`) so that any logged error can be correlated with the originating statement.

Because `id_` is used extensively for equality, hashing, representation, and logging, it must reliably return the exact value stored in `_id_`. Any change to the internal attribute after object creation would break these contracts, so the design assumes the identifier is immutable.

**Note**:  
- The method does not perform any validation; it simply returns the stored value. Ensure that `_id_` is correctly initialized during object construction.  
- Do not modify the returned identifier; treat it as read‑only.  
- When using `Statement` objects in collections that rely on hashing (e.g., sets, dict keys), the identifier must remain constant to avoid inconsistent behavior.

**Output Example**:  
If a `Statement` instance was created with an identifier of `42`, calling `stmt.id_()` will return:

```
42
```
***
### FunctionDef id_(self, val)
**id_**: The function of id_ is to prevent any modification of the statement’s identifier by raising an error whenever a new value is assigned.

**parameters**:  
· val: the value that a caller attempts to assign to the statement’s identifier.

**Code Description**:  
The id_ method is defined to immediately raise a RuntimeError with the message 'Statement.id_ is immutable.' Whenever it is invoked, execution stops and the exception propagates to the caller. The method does not perform any checks, conversions, or assignments; its sole purpose is to enforce immutability of the identifier attribute of a Statement object.

Within the Statement class, the identifier id_ is accessed (read‑only) by several other methods:

- `__eq__` compares two Statement instances by evaluating `self.id_ == other.id_`.  
- `__hash__` returns `hash(self.id_)` to provide a stable hash value for set and dict membership.  
- `__repr__` includes the identifier in the string representation: `Statement(sql=…, msg=…, id_=…)`.  
- `_get_logger` creates a logger named after the identifier when a logging queue is supplied: `logging.getLogger(str(self.id_))`.  
- `execute` and `execute_on_conn` embed the identifier in diagnostic dictionaries that are logged at debug level, and they also include it in error messages.

All these callers rely on the identifier being constant throughout the lifetime of the Statement object. By defining id_ to raise an error on any assignment attempt, the class guarantees that the identifier cannot be altered after the object is created, preserving the correctness of equality checks, hashing, logging, and error reporting.

**Note**:  
- Do not call `Statement.id_(value)` directly; it is intended to act as a setter for a read‑only property.  
- Any code that attempts to assign to `statement.id_` will trigger a RuntimeError, ensuring the identifier remains immutable.  
- The method accepts a single argument `val` solely to match the signature of a typical property setter, but the argument is never used.  
- If immutability of the identifier is required for correct behavior of collections, logging, or debugging, this method enforces that contract automatically.
***
### FunctionDef _get_logger(self, logq)
**_get_logger**: The function of _get_logger is to obtain a logger instance appropriate for the current Statement, using either the module‑level logger or a dedicated QueueHandler logger when a logging queue is supplied.  

**parameters**:  
· logq: a multiprocessing.Queue (or compatible Queue) used for inter‑process logging, or None if no queue is required.  

**Code Description**:  
The method first checks whether a logging queue (`logq`) was provided.  
- If `logq` is falsy (e.g., None), the method returns the Statement’s shared module‑level logger stored in `self.logger`. This logger is configured elsewhere in the module and is used for normal, single‑process logging.  
- If `logq` is truthy, the method creates a logger that is unique to the Statement instance. It does this by calling `logging.getLogger(str(self.id_))`, where `self.id_` is the immutable identifier of the Statement. The logger’s level is set to `logging.DEBUG` to ensure all debug messages are captured.  
  - The method then checks whether the newly obtained logger already has any handlers attached. If no handlers exist, it constructs a `DictQueueHandler` with the supplied queue (`logq`) and adds this handler to the logger. `DictQueueHandler` is a custom subclass of `logging.handlers.QueueHandler` that prepares log records for pickling without converting dictionary messages to plain strings, preserving the structure required by the project’s `DictLogFilter`.  
  - Finally, the configured logger is returned.  

The returned logger is used by the `Statement.execute` and `Statement.execute_on_conn` methods. Both methods call `_get_logger(logq)` at the start of their execution to obtain a logger that will record diagnostic dictionaries (containing fields such as `msg`, `sql`, `id_`, and connection information) at the DEBUG level. When a queue is supplied, these log records are sent through the `DictQueueHandler` to the main process, enabling parallel execution of statements while maintaining coherent logging.  

**Note**:  
- The logger created for a Statement with a queue is named after the Statement’s identifier; therefore each parallel worker gets its own logger instance, avoiding handler duplication across workers.  
- The method sets the logger’s level to DEBUG unconditionally; callers should rely on the logger’s level rather than re‑configuring it.  
- If the same Statement instance calls `_get_logger` multiple times with a non‑None `logq`, the logger will be reused because the handler check (`if not local_logger.handlers`) prevents adding duplicate `DictQueueHandler` objects.  
- The method assumes that `self.id_` is hashable and convertible to a string; this is guaranteed by the immutable identifier implementation in the Statement class.  

**Output Example**:  
When called with a non‑None queue, `_get_logger` returns a logger similar to the following (conceptual representation):  

```
<logging.Logger "42">
    level: DEBUG
    handlers:
        [<DictQueueHandler queue=<multiprocessing.Queue object at 0x7f...>>]
```
***
### FunctionDef execute(self, conn_str, resq, logq)
**execute**: The function of execute is to run the stored SQL statement against a PostgreSQL database using a new connection derived from the supplied connection string.

**parameters**:  
· conn_str (str): the libpq‑style connection string that identifies the target database.  
· resq (Queue, optional): a multiprocessing‑compatible queue onto which the Statement object is placed after execution; used by parallel execution frameworks.  
· logq (Queue, optional): a queue used to route log records through a DictQueueHandler when the code runs in parallel processes.

**Code Description**:  
1. **Logger preparation** – The method obtains a logger appropriate for the current execution context by calling self._get_logger(logq). If logq is falsy, the module‑level logger stored in self.logger is returned; otherwise a logger named after the statement’s immutable identifier is created (or reused) and equipped with a DictQueueHandler that forwards structured log dictionaries to logq.  

2. **Connection information extraction** – get_conn_info_dict(conn_str) parses the connection string and returns a dictionary containing the user, host, port, database name, and search_path. This dictionary is later merged into diagnostic log messages.  

3. **Database connection** – A new connection is opened with psycopg2.connect(conn_str) inside a with block, guaranteeing that the connection is closed automatically when the block exits. Immediately after opening, the isolation level is set to ISOLATION_LEVEL_AUTOCOMMIT (0), which enables execution of statements that cannot run inside a transaction (e.g., CREATE DATABASE, DROP DATABASE, VACUUM).  

4. **Statement execution on the connection** – The method delegates the actual SQL work to self.execute_on_conn(conn, resq, logq). This helper creates a cursor, logs the execution request, runs the SQL, captures row count, field names, and result data, and stores any database‑level exception in self.err. It also puts the statement onto resq if a result queue is supplied.  

5. **Connection‑level error handling** – If any exception is raised while establishing the connection or while entering the with block (i.e., before execute_on_conn is called), the exception is caught by the outer except clause. The error object is stored in self.err. A diagnostic dictionary containing a message (“connection error while …”), the stringified error, the statement identifier, and the connection‑info dictionary is built with combine_dicts and logged at DEBUG level via local_logger. Because the connection itself failed, execute_on_conn has not run; therefore, if a result queue is present, the statement object is placed onto that queue so that the caller can still retrieve it.  

6. **Cleanup** – In the finally block, the code checks whether the conn variable was assigned (i.e., a connection was successfully created). If so, conn.close() is called explicitly, although the with context would also close it; this defensive step ensures the connection is released even if an unexpected path bypasses the context manager.  

7. **Return value** – The method returns self, the same Statement instance now enriched with execution results (or error information). This enables method chaining or direct inspection of attributes such as self.data, self.fields, self.rowcount, and self.err.  

**Reference relationships**:  
- **Callers** – execute is invoked throughout the code base wherever a single SQL command must be run, for example in _bmi._fill_age_in_months, _era._add_era_ids, _external_id_mapper.insert_mapping_row, _schema.create_schema, _schema.drop_schema, _schema.tables_in_schema, and many other transformation or derivation functions. In each case the caller supplies the connection string and optionally result or logging queues.  
- **Callee** – execute relies on self._get_logger to obtain a logger, get_conn_info_dict to parse connection metadata, combine_dicts to build log dictionaries, and most importantly self.execute_on_conn to perform the actual cursor‑level work. Errors from execute_on_conn are already stored in self.err; execute only handles connection‑level failures.  

**Note**:  
- The connection is opened with autocommit; therefore any DDL or VACUUM statements are applied immediately and cannot be rolled back by this method.  
- Errors occurring during connection establishment are stored in self.err and logged, but they are not re‑raised; callers must inspect stmt.err to detect failure.  
- When a logq is supplied, each parallel worker gets its own logger named after the statement identifier, preventing handler duplication.  
- The result queue resq is used only for bookkeeping; the statement object is always placed on the queue regardless of success or failure, enabling parallel orchestration code (e.g., StatementSet.parallel_execute) to collect results.  
- The explicit conn.close() in the finally block is safe even if the connection was already closed by the context manager.  

**Output Example**:  
```python
stmt = Statement('SELECT id, name FROM patients', msg='fetch patients')
stmt = stmt.execute(conn_str)   # returns the same Statement instance
print(stmt.id_)                 # => 42  (example identifier)
print(stmt.data)                # => [(1, 'Alice'), (2, 'Bob')]
print(stmt.fields)              # => ['id', 'name']
print(stmt.rowcount)            # => 2
print(stmt.err)                 # => None  (or an exception instance if a connection error occurred)
```
***
### FunctionDef execute_on_conn(self, conn, resq, logq)
**execute_on_conn**: The function of execute_on_conn is to run the stored SQL statement on a given DB‑API connection, capture results, handle errors, log diagnostic information, and optionally place the processed Statement object onto result and logging queues.

**parameters**:  
· conn (dbapi connection object): an open connection (e.g., a psycopg2 connection) that will be used to create a cursor and execute the statement.  
· resq (multiprocessing.Queue or similar, optional): a queue onto which the Statement instance is put after execution, enabling parallel workers to return results.  
· logq (multiprocessing.Queue or similar, optional): a queue used by DictQueueHandler to forward log records to a central logger when the code runs in parallel processes.

**Code Description**:  
1. **State reset** – The method begins by clearing any previous execution data: `self.data` is set to None, `self.fields` to an empty list, `self.rowcount` to None, and `self.err` to None. This guarantees that a re‑execution starts from a clean slate.  

2. **Logger selection** – `_get_logger(logq)` is called to obtain a logger appropriate for the current execution context. If logq is None, the module‑level logger (`self.logger`) is used; otherwise a dedicated logger named after the statement’s immutable identifier is created (or reused) with a DictQueueHandler attached, so that all debug messages are sent through logq.  

3. **Connection metadata** – `get_conn_info_dict(conn.dsn)` extracts user, host, port, database name, and search_path from the connection’s DSN string. The resulting dictionary (`conn_info`) is later merged into every log entry.  

4. **Cursor context** – A cursor is opened using a `with conn.cursor() as cursor:` block, ensuring automatic cleanup.  

5. **Logging the execution request** – A diagnostic dictionary containing the statement’s message (`self.msg`), raw SQL (`self.sql`), identifier (`self.id_`), and the connection metadata is built with `combine_dicts` and logged at DEBUG level via the selected logger.  

6. **SQL execution** – `cursor.execute(self.sql)` runs the statement.  

7. **Row count handling** – If `cursor.rowcount` is a non‑negative integer (i.e., not -1 or None), it is stored in `self.rowcount`. This provides callers with the number of rows affected by INSERT/UPDATE/DELETE or other DML statements.  

8. **Field name extraction** – When the cursor has a description (i.e., the statement produced a result set), each column’s name (`field[0]`) is appended to `self.fields`. This list is later used by callers to interpret the returned rows.  

9. **Data fetching** – The method attempts `self.data = cursor.fetchall()`. If the cursor raises a `psycopg2.ProgrammingError` with the message “no results to fetch”, the exception is ignored and `self.data` remains None (typical for DDL statements). Any other programming error is re‑raised, allowing it to be caught by the outer exception handler.  

10. **Error handling** – Any exception raised during the cursor block (including SQL syntax errors, constraint violations, etc.) is caught by the outer `except Exception as err:` clause. The exception object is stored in `self.err`. A diagnostic dictionary containing a generic message (`'database error while {self.msg}'`), the stringified error, the statement identifier, and connection metadata is logged at DEBUG level. The error is **not** re‑raised; execution continues so that the caller receives a fully populated Statement object indicating failure.  

11. **Result queue** – If a result queue (`resq`) was supplied, the method puts the current Statement instance onto that queue (`resq.put(self)`). This enables parallel execution frameworks (e.g., StatementSet.parallel_execute) to collect results from worker processes.  

12. **Return value** – The method returns `self`, i.e., the same Statement object now enriched with `data`, `fields`, `rowcount`, and possibly `err`.  

**Reference relationships**:  
- **Caller**: `Statement.execute` creates a new database connection from a connection string, sets autocommit, and then invokes `execute_on_conn(conn, resq, logq)`. If the connection itself fails, `execute` handles the error and may also place the Statement on the result queue.  
- **Caller (parallel execution)**: `StatementSet.parallel_execute` and similar utilities spawn separate processes, pass a connection object to each worker, and rely on `execute_on_conn` to perform the actual SQL work while sending back results via `resq` and logs via `logq`.  
- **Callee**: `_get_logger` (internal) to obtain the appropriate logger; `get_conn_info_dict` to parse connection DSN; `combine_dicts` to build log dictionaries; `psycopg2` cursor methods (`execute`, `fetchall`, `rowcount`, `description`).  

**Note**:  
- The method does not commit or roll back the transaction; the caller (`Statement.execute`) sets the connection’s isolation level to AUTOCOMMIT, making the statement immediately persistent.  
- Only DEBUG‑level log messages are emitted; ensure that the logger’s level is configured to capture DEBUG if detailed tracing is required.  
- When `logq` is provided, each worker must have a separate logger instance; the implementation guards against duplicate handlers by checking `if not local_logger.handlers`.  
- Errors are stored in `self.err` but not raised; callers should inspect this attribute to determine whether execution succeeded.  
- The method assumes the connection object provides a `dsn` attribute compatible with `get_conn_info_dict`.  

**Output Example**:  
```python
stmt = Statement('SELECT id, name FROM patients')
stmt.execute_on_conn(conn)   # conn is a psycopg2 connection
# After execution:
stmt.data      # => [(1, 'Alice'), (2, 'Bob'), ...]
stmt.fields    # => ['id', 'name']
stmt.rowcount  # => 2   (or None if not applicable)
stmt.err       # => None   (or an exception instance if an error occurred)
```
***
## ClassDef StatementSet
**StatementSet**: The function of StatementSet is to manage a mutable collection of executable statements and provide a method to run all of them concurrently in parallel worker processes.

**attributes**
· data: an internal Python `set` that stores the members of the collection. It is created in `__init__` and can be accessed directly if needed.

**methods**
· __init__(self, *data): constructs a new StatementSet, optionally pre‑populating it with any number of objects passed as positional arguments. The objects are stored in the `data` attribute.
· __contains__(self, elem): returns `True` if `elem` is present in the underlying set.
· __iter__(self): yields each element of the set, allowing the StatementSet to be iterated over.
· __len__(self): returns the number of elements currently stored.
· add(self, elem): inserts `elem` into the set.
· discard(self, elem): removes `elem` from the set if it exists; otherwise does nothing.
· parallel_execute(self, conn_str, pool_size=None, taskq=None, resq=None, logq=None): launches a pool of worker processes (up to a maximum of 25 workers, or one worker per statement if fewer statements exist) that each call the internal `_worker_process` function. All statements currently in the set are placed on a task queue, a dedicated logging thread receives log records from the workers, and the method blocks until every task finishes. After workers terminate, the method clears the set, retrieves the modified statements from the result queue, and repopulates the set with those results. The method returns `self` so that the caller can continue to use the updated collection.

**Code Description**
StatementSet inherits from `collections.MutableSet`, thereby satisfying the abstract set interface required by Python. Internally it stores its elements in the attribute `self.data`, which is a plain `set`. No type checking is performed when adding elements; any object that implements an `execute(conn_str, resq=None, logq=None) -> obj` method can be stored. This design allows the class to be used with the project's `Statement` objects, but it also works with any compatible custom objects.

The core capability of the class is the `parallel_execute` method. When invoked, the method determines an appropriate pool size: if the caller does not specify `pool_size`, the method uses the number of statements in the set, capped at 25 workers. It then creates three multiprocessing queues if they were not supplied: a `JoinableQueue` for tasks, a regular `Queue` for results, and another `Queue` for log records. Connection information extracted from `conn_str` is merged with a log message and emitted via the module‑level logger.

Worker processes are started by creating `multiprocessing.Process` instances that run the module function `_worker_process`. Each worker receives the connection string and the three queues, enabling it to fetch a statement, call its `execute` method, and place the possibly modified statement on the result queue. After all statements have been queued, a separate thread runs `_logger_thread` to consume log records from `logq` and forward them to the logging system, ensuring that log output from parallel workers does not interleave.

The main process then blocks on `taskq.join()`, which waits until every queued task has been marked as done by the workers. Once all work is complete, sentinel `None` values are placed on the task queue to signal each worker to exit, and the main process joins each worker process. The logging thread is similarly terminated by sending a `None` sentinel and joining the thread.

Result collection proceeds by first clearing the current set, then repeatedly pulling items from `resq` until a `None` sentinel is encountered. Each retrieved object (the original statement, now potentially enriched with result data, error information, etc.) is added back into the set. The method finally returns the updated StatementSet instance.

Throughout the project, StatementSet is used extensively to batch‑execute SQL statements that are generated by higher‑level functions. Examples include:
- In `pedsnetdcc/bmi.py/run_bmi_calc`, a StatementSet is created to hold table‑creation, index‑creation, and other DDL statements. After populating the set, `parallel_execute` is called (often with an explicit pool size such as 5) to run the statements concurrently, then the caller iterates over the set to inspect each statement for errors via `check_stmt_err`.
- In `pedsnetdcc/check_fact_relationship.py/check_fact_relationship`, a StatementSet aggregates dozens of SELECT statements that validate the fact_relationship table. The set is executed in parallel, and the resulting data is examined to compute percentages and sample rows.
- In `pedsnetdcc/concept_group_tables.py/create_index_replacement_tables`, two separate StatementSets (`table_stmts` and `pk_stmts`) are built to create replacement tables and then add primary keys, each executed with `parallel_execute`.
- In `pedsnetdcc/foreign_keys.py/add_foreign_keys` and `drop_foreign_keys`, a StatementSet holds the generated ALTER TABLE statements for adding or dropping foreign keys, respectively, and runs them in parallel.
- In many other modules (indexes, measurement partitioning, subset creation, etc.) the pattern is the same: build a set of `Statement` objects, call `parallel_execute`, then iterate over the set to verify success or to apply further processing.

Because `parallel_execute` returns the same StatementSet instance, callers can continue to use the set after execution, for example to clear it, add more statements, or re‑execute with a different pool size.

**Note**
- The maximum number of parallel workers is hard‑coded to 25; attempting to run more statements than this will still use only 25 processes unless a larger `pool_size` is explicitly supplied.
- The method expects the worker functions `_worker_process` and `_logger_thread` to be defined elsewhere in the module; they must correctly handle the task, result, and log queues.
- Statements that cannot be run inside a transaction block (e.g., `VACUUM`) are still supported because each worker invokes the statement’s own `execute` method, which can manage autocommit as needed.
- If any of the queues are supplied by the caller, they must be compatible with the multiprocessing API; otherwise fresh queues are created automatically.
- Errors from individual statements are stored on the statement object (`stmt.err`) and are not raised by `parallel_execute`. Callers are responsible for inspecting each statement after execution and handling errors as appropriate.

**Output Example**
When a caller creates a StatementSet, adds two simple SELECT statements, and calls `parallel_execute`, the returned object is the same StatementSet instance, now containing the executed statements with their result data populated:

```
stmts = StatementSet()
stmts.add(Statement('SELECT foo1 FROM test1'))
stmts.add(Statement('SELECT foo2 FROM test2'))
stmts.parallel_execute(conn_str)

for stmt in stmts:
    print(stmt.sql, stmt.data, stmt.err)
# Possible printed output:
# SELECT foo1 FROM test1  [(1,)]  None
# SELECT foo2 FROM test2  [(2,)]  None
```

In this example `stmt.data` holds the fetched rows, and `stmt.err` is `None` indicating successful execution.
### FunctionDef __init__(self)
**__init__**: The function of __init__ is to initialize a StatementSet instance with a collection of unique items.  
**parameters**: The parameters of this Function.  
· self: the instance of the StatementSet being created.  
· *data: a variable‑length tuple of items to be stored in the set; each item can be of any hashable type.  
**Code Description**: The constructor receives any number of positional arguments via the *data parameter. Inside the method, the tuple `data` is passed to the built‑in `set` constructor, which creates a new set containing each argument exactly once, automatically discarding duplicate values. This set is then assigned to the instance attribute `self.data`, making the collection of unique items accessible to other methods of the StatementSet class.  
**Note**: Because the arguments are stored in a Python `set`, all items must be hashable; attempting to include unhashable types (e.g., lists or dictionaries) will raise a `TypeError`. Duplicate arguments are silently removed, so the resulting `self.data` contains only distinct elements.
***
### FunctionDef __contains__(self, elem)
**__contains__**: The function of __contains__ is to determine whether a given element is present in the StatementSet’s internal collection.  

**parameters**:  
· self: the StatementSet instance on which the membership test is performed.  
· elem: the element whose presence is being checked within the StatementSet.  

**Code Description**:  
The method executes a single expression `elem in self.data`. Here, `self.data` is an attribute of the StatementSet class that stores the statements (typically as a list, set, or other iterable). The `in` operator triggers Python’s built‑in containment logic, iterating over `self.data` and returning `True` as soon as an element equal to `elem` is found, otherwise `False`. The result of this expression is directly returned to the caller, making the method a thin wrapper that exposes the standard containment semantics for the StatementSet object.  

**Note**:  
- The correctness of the containment check depends on `self.data` being an iterable that supports the `in` operation.  
- Equality comparison between `elem` and items in `self.data` follows the usual Python rules (`__eq__` implementation of the stored objects).  
- No type conversion or preprocessing of `elem` is performed; the caller must provide an element of a type compatible with the items stored in `self.data`.  

**Output Example**:  
```python
stmt_set = StatementSet()
stmt_set.data = ['SELECT * FROM patients', 'INSERT INTO visits ...']
result = stmt_set.__contains__('SELECT * FROM patients')  # result is True
```
***
### FunctionDef __iter__(self)
**__iter__**: The function of __iter__ is to provide an iterator over the elements stored in the StatementSet instance.  

**parameters**: This method does not accept any parameters.  

**Code Description**:  
- The method accesses the attribute `self.data`, which is expected to be an iterable collection (such as a list, tuple, or any object implementing the iterator protocol).  
- It enters a `for` loop that iterates over each element (`elem`) in `self.data`.  
- For every iteration, the method uses the `yield` statement to produce the current element to the caller.  
- Because `yield` is used, `__iter__` becomes a generator function, automatically returning a generator object that conforms to Python’s iterator protocol (`__next__` and `__iter__`).  
- This implementation enables a `StatementSet` object to be used directly in iteration contexts such as `for elem in statement_set:` or any function that consumes an iterable.  

**Note**:  
- The correctness of iteration depends on `self.data` being a properly initialized iterable; if `self.data` is `None` or not iterable, a `TypeError` will be raised at runtime.  
- Since the method yields elements directly without copying, modifications to the underlying `self.data` during iteration will be reflected in the iteration sequence.  
- No additional state is maintained by the iterator; each call to `__iter__` creates a fresh generator, allowing multiple independent iterations over the same `StatementSet`.
***
### FunctionDef __len__(self)
**__len__**: The function of __len__ is to return the number of items stored in the StatementSet instance.  
**parameters**: This method takes no external parameters.  
· self: the StatementSet object whose internal data collection size is being queried.  

**Code Description**: The __len__ method accesses the attribute `self.data`, which is expected to be an iterable container (such as a list, tuple, set, or any object that implements the built‑in `len()` protocol). It then calls Python’s built‑in `len()` function on this container and returns the resulting integer. This integer represents the total count of elements currently held in the StatementSet’s `data` attribute.  

**Note**:  
- The method assumes that `self.data` has been properly initialized and is a type that supports the `len()` operation. If `self.data` is `None` or an unsupported type, a `TypeError` will be raised.  
- Because the method does not modify any state, it is safe to call repeatedly without side effects.  

**Output Example**:  
If `self.data` contains five statement objects, calling `len(statement_set_instance)` will return `5`.
***
### FunctionDef add(self, elem)
**add**: The function of add is to insert a new element into the internal collection of a StatementSet instance.

**parameters**: The parameters of this function.
· self: the StatementSet instance on which the method is invoked.  
· elem: the element (typically a Statement object) to be added to the set.

**Code Description**:  
The StatementSet class maintains an attribute named `data` that holds a Python `set`. The `add` method is a thin wrapper that forwards the supplied element `elem` to the built‑in `set.add` method of `self.data`. The call `self.data.add(elem)` attempts to insert `elem` into the set; if the element is already present, the set remains unchanged. The method then returns the result of `set.add`, which is always `None`. Throughout the project, `StatementSet.add` is used to accumulate `Statement` objects before they are executed, for example in functions such as `run_bmi_calc`, `check_fact_relationship`, `add_foreign_keys`, `add_primary_keys`, and many others. Each of those callers creates a `StatementSet` instance, builds `Statement` objects representing SQL commands, and calls `stmts.add(statement)` to collect them for later parallel or sequential execution.

**Note**:  
- The method does not perform any type checking; it relies on the caller to supply appropriate objects (normally instances of the `Statement` class).  
- Because `set.add` returns `None`, the `add` method also returns `None`; callers should not expect a value indicating success.  
- Adding an element that is already present in the set has no effect and does not raise an error.

**Output Example**:  
```python
stmts = StatementSet()
result = stmts.add(Statement("SELECT 1"))
print(result)   # Output: None
```
***
### FunctionDef discard(self, elem)
**discard**: The function of discard is to remove a specified element from the internal collection of the `StatementSet` without raising an error if the element is absent.  

**parameters**: The parameters of this Function.  
· `self`: the instance of `StatementSet` on which the method is invoked.  
· `elem`: the element to be removed from the set stored in the instance.  

**Code Description**:  
The method forwards the removal request to the `discard` method of the `self.data` attribute. `self.data` is expected to be a mutable set‑like object that implements the standard Python `set.discard` interface. When `self.data.discard(elem)` is executed, the element `elem` is removed from the underlying set if it exists; otherwise, the operation has no effect. The result of `set.discard` is always `None`, and this value is returned directly by the `StatementSet.discard` method.  

**Note**:  
- The method does not raise a `KeyError` when `elem` is not present, unlike `remove`.  
- The return value is always `None`; it should not be used to determine whether the element was present.  
- Ensure that `self.data` is initialized as a set or a compatible container before calling `discard`.  

**Output Example**:  
```python
stmt_set = StatementSet()
stmt_set.data = {1, 2, 3}
result = stmt_set.discard(2)   # result is None, and the internal set becomes {1, 3}
```
***
### FunctionDef parallel_execute(self, conn_str, pool_size, taskq, resq, logq)
**parallel_execute**: The function of parallel_execute is to run all statements stored in a StatementSet concurrently using a pool of worker processes and a logger thread, then collect the results back into the set.

**parameters**:  
· **self**: the StatementSet instance on which the method is invoked.  
· **conn_str** (str): database connection string that will be passed unchanged to each statement’s `execute` method.  
· **pool_size** (int, optional): number of worker processes to create; if omitted the function chooses a size based on the number of statements (up to a maximum of 25).  
· **taskq** (Queue, optional): a multiprocessing.JoinableQueue used to feed tasks to workers; if omitted a new queue is created.  
· **resq** (Queue, optional): a multiprocessing.Queue used by workers to return modified Statement objects; if omitted a new queue is created.  
· **logq** (Queue, optional): a multiprocessing.Queue used by workers to send logging records to the logger thread; if omitted a new queue is created.  

**Code Description**:  
1. **Determine pool size** – If `pool_size` is not supplied, the method sets it to the number of statements in the set when that number is ≤ 25; otherwise it caps the size at 25. This prevents creating an excessive number of processes for small workloads.  
2. **Create queues** – When any of `taskq`, `resq`, or `logq` are `None`, fresh multiprocessing queues are instantiated: a `JoinableQueue` for tasks and regular `Queue`s for results and logs.  
3. **Log start of parallel execution** – Connection details are extracted with `get_conn_info_dict(conn_str)` and merged with a message dictionary via `combine_dicts`. The resulting dictionary is logged at INFO level.  
4. **Spawn worker processes** – For each worker (0 … `pool_size‑1`), a `multiprocessing.Process` is created with target function `_worker_process`. The worker receives the connection string and the three queues. Each process is started and stored in the `workers` list.  
5. **Enqueue tasks** – Every Statement object contained in the StatementSet (`for task in self`) is placed onto `taskq` using `taskq.put(task)`. Workers will retrieve these tasks, call `task.execute(conn_str, resq, logq)`, and then signal completion with `taskq.task_done()`.  
6. **Start logger thread** – A separate `threading.Thread` runs `_logger_thread(logq)`. This thread continuously reads logging records from `logq` and forwards them to the module‑level `logger`, preserving log level filtering. The thread runs until a sentinel `None` is placed on the queue.  
7. **Wait for all tasks** – `taskq.join()` blocks until every enqueued task has been marked as done by the workers. This ensures that all statements have finished execution before proceeding.  
8. **Graceful shutdown of workers** – After all real tasks are completed, a `None` sentinel is placed on `taskq` once for each worker (`for i in range(pool_size): taskq.put(None)`). Each worker receives the sentinel, breaks its loop, and exits. The main process then joins each worker (`wp.join()`).  
9. **Terminate logger thread** – A `None` sentinel is sent on `logq` and the logger thread is joined, guaranteeing that all log records have been processed.  
10. **Collect results** – The StatementSet is cleared (`self.clear()`). The result queue receives a `None` sentinel to mark the end of results. The method then repeatedly calls `resq.get()` until the sentinel is encountered, adding each returned Statement back into the set with `self.add(result)`. This restores the set with statements that now contain any data, error information, or other modifications produced by execution.  
11. **Return value** – The method returns `self`, i.e., the same StatementSet instance now populated with the executed statements and their outcomes.

**Reference relationships**:  
- **Callees** – `_worker_process` (internal function that executes each statement) and `_logger_thread` (internal function that forwards log records). Both are defined in the same module (`pedsnetdcc/db.py`).  
- **Callers** – `parallel_execute` is invoked by many high‑level functions that need to run multiple SQL statements concurrently, such as `run_bmi_calc`, `check_fact_relationship`, `create_index_replacement_tables`, `add_foreign_keys`, `drop_foreign_keys`, `add_primary_keys`, `set_not_nulls`, `drop_not_nulls`, `partition_measurement_table`, `split_measurement_table`, transformation runners (`_transform`, `_transform_target`, etc.), and the unit tests in `pedsnetdcc/tests/db_test.py`. In each case the caller builds a `StatementSet`, adds `Statement` objects (each representing a SQL command), and then calls `parallel_execute` to perform the work in parallel.  

**Note**:  
- The function assumes that every non‑None object placed on `taskq` implements an `execute(conn_str, resq, logq)` method (the `Statement` class satisfies this contract).  
- All objects transferred between processes must be picklable; `Statement` objects are designed for this purpose.  
- The sentinel values (`None`) must be enqueued exactly once per worker for `taskq` and once for the logger thread on `logq`; otherwise workers or the logger may block indefinitely.  
- The result queue is drained after workers finish; any statements that failed during execution will have their `err` attribute populated, allowing callers to inspect and handle errors.  
- The default maximum of 25 workers prevents resource exhaustion on systems with many statements but limited CPU cores. Callers can override this by supplying `pool_size`.  

**Output Example**:  
```python
stmts = StatementSet()
stmts.add(Statement('SELECT 1'))
stmts.add(Statement('SELECT 2'))
stmts.parallel_execute(conn_str)   # runs the two SELECTs in parallel
for stmt in stmts:
    print(stmt.sql, stmt.data)    # e.g. ('SELECT 1', [(1,)]), ('SELECT 2', [(2,)])
```
***
## ClassDef StatementList
**StatementList**: The function of StatementList is to hold an ordered collection of executable statement objects and provide a method to run them sequentially, optionally inside a single database transaction.

**attributes**  
· **data** – internal Python list that stores the statement objects supplied at construction or added later.  

**Code Description**  
StatementList is a subclass of `collections.MutableSequence`, which means it implements the full mutable sequence protocol (indexing, slicing, insertion, deletion, iteration, and length queries). The class stores its elements in the attribute `self.data`, a plain list created from any positional arguments passed to `__init__`. Because it follows the MutableSequence contract, the list can be manipulated with standard list operations (`append`, `extend`, `insert`, `pop`, etc.) while preserving order.

Key special‑method implementations:  

* `__contains__(self, elem)` – delegates the containment test to the underlying list.  
* `__iter__(self)` – yields each element in order, enabling `for stmt in stmt_list:` syntax.  
* `__len__(self)` – returns the number of stored statements.  
* `__getitem__(self, idx)` – provides indexed access (`stmt_list[i]`).  
* `__setitem__(self, idx, val)` – replaces the element at the given index.  
* `__delitem__(self, idx)` – removes the element at the given index.  
* `insert(self, idx, val)` – inserts a new element at the specified position using slice assignment (`self.data[idx:idx] = [val]`).  

The central feature is `serial_execute(self, conn_str, transaction=False)`. This method iterates over the stored statements and executes each one in the order they appear. Execution proceeds in two modes:

1. **Non‑transactional mode (`transaction=False`)** – each statement’s `execute(conn_str)` method is called directly. This is appropriate for statements that cannot be run inside a transaction block (e.g., `VACUUM`, `CREATE TABLE`, `DROP TABLE`). The method logs a start‑up message that includes the number of statements and whether a transaction is requested.

2. **Transactional mode (`transaction=True`)** – a single PostgreSQL connection is opened with `psycopg2.connect(conn_str)`. The connection is used as a context manager, so it automatically commits if all statements succeed or rolls back on any error. Inside the `with` block each statement’s `execute_on_conn(conn)` method is invoked, allowing the statement to reuse the same open connection. After the block the connection is explicitly closed in a `finally` clause.

The method returns `self`, enabling method chaining or inspection of the list after execution. Because statements modify themselves (e.g., they store query results in a `data` attribute), callers can examine the statements after `serial_execute` to retrieve results, row counts, or error information.

**Relationship with callers**  
Throughout the PEDSnet codebase, `StatementList` is used to assemble groups of `Statement` objects that represent individual SQL commands. Typical usage patterns include:

* **ID assignment functions** (`_add_measurement_ids`, `_add_era_ids`, `_add_observation_ids`, etc.) – they create a `StatementList`, append two statements (a `LOCK` and an `UPDATE` that reserves a block of IDs), and then call `serial_execute(conn_str, transaction=True)`. The transaction guarantees that the lock and update happen atomically, preventing race conditions when multiple processes allocate IDs concurrently.

* **Permission‑granting utilities** (`grant_loading_user_permissions`, `grant_database_permissions`, etc.) – they build a list of `Statement` objects for each GRANT command and execute them without a transaction (`transaction=False`). This avoids errors for statements that PostgreSQL does not allow inside a transaction block.

* **Transformation runners** (`run_transformation`, `run_site_transformation`, `undo_transformation`, etc.) – they construct large `StatementList` objects containing schema creation, table moves, and drops. These are executed with `transaction=True` so that the entire schema‑renaming operation is atomic; any failure rolls back all changes.

* **Test suite** (`StatementListTest`, `StatementListTestTransaction`, `StatementListTestVacuum`) – verifies both transactional and non‑transactional execution paths, including the special handling of `VACUUM`.

In all cases, the caller relies on the fact that `StatementList` preserves order, supports list‑like manipulation, and abstracts away the boilerplate of opening/closing connections and handling commit/rollback logic.

**Note**  
* The class does **not** enforce that stored objects are instances of a specific `Statement` class; any object providing an `execute(conn_str)` method (and, when `transaction=True`, an `execute_on_conn(conn)` method) will work. This flexibility is intentional but places the responsibility on the caller to supply compatible objects.  
* Statements that cannot be run inside a transaction (e.g., `VACUUM`, `CREATE INDEX CONCURRENTLY`) must be executed with `transaction=False`. Attempting to run them with `transaction=True` will cause the underlying PostgreSQL driver to raise an error, which is captured in the statement’s `err` attribute.  
* Because `serial_execute` returns the same `StatementList` instance, the caller can inspect the list after execution to retrieve results (`stmt.data`), row counts (`stmt.rowcount`), or error details (`stmt.err`).  
* The method logs a structured message using the project’s logger; the log includes the connection information, number of statements, and transaction flag, which aids debugging in production environments.  
* The connection is always closed in the `finally` block, even if an exception occurs during statement execution, preventing leaked database connections.

**Output Example**  
```python
stmts = StatementList()
stmts.append(Statement('SELECT 1 AS answer'))
result = stmts.serial_execute('postgresql://user:pw@host/db')
# result is the same StatementList instance
print(result[0].data)   # -> [(1,)]
print(result[0].rowcount)  # -> 1
print(result[0].err)    # -> None
```
### FunctionDef __init__(self)
**__init__**: The function of __init__ is to initialize a StatementList instance with an optional collection of items.  
**parameters**: The parameters of this Function.  
· self: the instance of the StatementList being created.  
· *data: a variable‑length tuple of positional arguments representing the initial elements to be stored in the list.  
**Code Description**: The constructor receives any number of positional arguments via the *data parameter. Inside the method, the tuple `data` is converted to a list using the built‑in `list()` constructor, and the resulting list is assigned to the instance attribute `self.data`. This attribute therefore holds all supplied elements in a mutable list, preserving the order in which they were passed. If no arguments are provided, `self.data` is initialized as an empty list.  
**Note**: The method does not perform type checking or validation on the supplied arguments; any objects can be stored. Because `self.data` is a list, it can be modified after initialization (e.g., items can be appended, removed, or reordered).
***
### FunctionDef __contains__(self, elem)
**__contains__**: The function of __contains__ is to determine whether a given element exists in the StatementList’s internal data collection.  

**parameters**:  
· self: the instance of StatementList on which the membership test is performed.  
· elem: the element whose presence is being checked within the StatementList.  

**Code Description**:  
The method executes a single expression `elem in self.data`. Here, `self.data` is assumed to be an iterable container (such as a list, set, or tuple) that stores the statements managed by the StatementList object. The Python `in` operator iterates over `self.data` and compares each stored item with `elem` using the equality operator (`==`). If a match is found, the expression evaluates to `True`; otherwise, it evaluates to `False`. The resulting Boolean value is then returned directly to the caller.  

**Note**:  
- The accuracy of the membership test depends on the type and equality semantics of the objects stored in `self.data`.  
- If `self.data` is a large list, the operation has linear time complexity O(n). Using a set for `self.data` would improve lookup to average O(1).  
- No type checking is performed; passing an incompatible type for `elem` will simply result in `False` unless the equality comparison raises an exception.  

**Output Example**:  
```python
stmt_list = StatementList()
stmt_list.data = ['SELECT * FROM patients', 'INSERT INTO visits ...']

# Check for an existing statement
result = 'SELECT * FROM patients' in stmt_list   # result is True

# Check for a non‑existing statement
result = 'DELETE FROM labs' in stmt_list         # result is False
```
***
### FunctionDef __iter__(self)
**__iter__**: The function of __iter__ is to provide an iterator that yields each element stored in the StatementList instance.  

**parameters**: The parameters of this Function.  
· *self*: the StatementList object whose internal data collection will be iterated over.  

**Code Description**:  
The method defines a generator using a `for` loop that iterates over `self.data`. For every `elem` encountered in `self.data`, the `yield` statement returns the element to the caller, allowing the StatementList to be used in any context that expects an iterable (e.g., in a `for` loop, list comprehension, or the `list()` constructor). The implementation relies on the underlying `self.data` attribute being an iterable container (such as a list, tuple, or any object that implements the iterator protocol). Because the method uses `yield`, it creates a lazy iterator that produces elements one at a time without constructing an intermediate list.  

**Note**:  
- The method does not accept any arguments besides `self`; attempting to pass additional arguments will raise a `TypeError`.  
- Modifying `self.data` while an iteration is in progress may lead to unexpected behavior or runtime errors, as the iterator reflects the state of `self.data` at the time of iteration.  
- If `self.data` is empty, the iterator will produce no elements, and the loop will terminate immediately.
***
### FunctionDef __len__(self)
**__len__**: The function of __len__ is to return the number of items stored in the StatementList instance.  
**parameters**: The parameters of this Function.  
· (none) – the method operates on the instance (`self`) and does not require additional arguments.  
**Code Description**: The method accesses the `data` attribute of the StatementList object, which is expected to be a sequence (e.g., list, tuple). It then calls Python’s built‑in `len()` function on that sequence and returns the resulting integer. This provides a convenient way to query the size of the collection directly via `len(statement_list_instance)`.  
**Note**: The correctness of the returned value depends on `self.data` being properly initialized as an iterable collection. If `self.data` is `None` or not a sequence, a `TypeError` will be raised. Ensure that the StatementList constructor or any mutating methods maintain `self.data` as a valid container.  
**Output Example**:  
```python
stmt_list = StatementList()
# assume stmt_list.data contains 3 SQL statements
print(len(stmt_list))   # Output: 3
```
***
### FunctionDef __getitem__(self, idx)
**__getitem__**: The function of __getitem__ is to retrieve an element from the internal data collection by its index.  
**parameters**: The parameters of this Function.  
· self: The instance of the StatementList class.  
· idx: The index (or slice) specifying which element(s) to retrieve from the internal list.  
**Code Description**: The method directly accesses the attribute `self.data`, which is expected to be a sequence (typically a list) containing the stored statements. By using the expression `self.data[idx]`, it delegates the indexing operation to the underlying sequence. This means that any index type supported by the underlying sequence—such as an integer for a single element or a slice object for a sub‑range—will behave identically. If `idx` is out of bounds, the underlying sequence will raise an `IndexError`, which propagates to the caller. The method returns the retrieved element without any additional processing.  
**Note**: Ensure that `self.data` is initialized as a sequence before using this method; otherwise, a `AttributeError` may occur. The method does not perform bounds checking or type validation beyond what the underlying sequence provides.  
**Output Example**:  
```python
stmt_list = StatementList()
stmt_list.data = ['SELECT * FROM patients', 'INSERT INTO visits ...']
first_statement = stmt_list[0]   # returns 'SELECT * FROM patients'
slice_of_statements = stmt_list[0:2]  # returns ['SELECT * FROM patients', 'INSERT INTO visits ...']
```
***
### FunctionDef __setitem__(self, idx, val)
**__setitem__**: The function of __setitem__ is to assign a value to a specific position in the internal data collection of a StatementList instance.  

**parameters**: The parameters of this Function.  
· idx: The index (integer or slice) indicating the position in the internal list where the new value should be stored.  
· val: The value (typically a SQL statement object) that will be placed at the specified index.  

**Code Description**: The method directly accesses the `data` attribute of the StatementList object, which is expected to be a mutable sequence (such as a Python list). By executing `self.data[idx] = val`, it replaces the element currently located at `idx` with `val`. This operation relies on Python’s built‑in sequence item assignment semantics, meaning that any valid index or slice accepted by the underlying sequence type is also accepted here. No additional validation, transformation, or side‑effects are performed within the method; it simply forwards the assignment to the underlying container.  

**Note**:  
- The caller must ensure that `idx` is a valid index or slice for the current state of `self.data`; otherwise, Python will raise an `IndexError` or `TypeError`.  
- Because the method does not perform type checking on `val`, inserting an inappropriate object may lead to errors later when the StatementList is used (e.g., when generating SQL).  
- This method enables StatementList instances to support the bracket‑assignment syntax (`statement_list[i] = new_statement`), making them behave like standard Python sequences.
***
### FunctionDef __delitem__(self, idx)
**__delitem__**: The function of __delitem__ is to delete an element at a specified index from the StatementList’s internal data collection.  

**parameters**:  
· self: the StatementList instance on which the operation is performed.  
· idx: the index (or slice) identifying the element(s) to be removed from the internal list.  

**Code Description**:  
The method receives an index (or slice) `idx` and directly applies Python’s `del` statement to the `self.data` attribute, which holds the underlying list of statements. `del self.data[idx]` removes the item at the given position, shifting subsequent elements left to fill the gap. The operation is performed in‑place; the method does not return a value. By implementing `__delitem__`, the StatementList class supports the built‑in `del` syntax, allowing callers to write `del statement_list[idx]` to remove entries.  

**Note**:  
- `idx` must be a valid index or slice for the internal list; otherwise Python will raise an `IndexError` (or `TypeError` for invalid types).  
- The deletion modifies the original StatementList; there is no copy created.  
- Because the method directly manipulates `self.data`, any external references to the same list will see the change.  
- No additional cleanup or side‑effects are performed beyond the list removal.
***
### FunctionDef insert(self, idx, val)
**insert**: The function of insert is to add a single element into the internal data list at a specified position.  
**parameters**: The parameters of this Function.  
· self: The instance of the StatementList class containing the internal list `data`.  
· idx: The integer index at which the new element should be placed.  
· val: The element to be inserted; it can be of any type.  
**Code Description**: The method performs insertion by using Python’s slice‑assignment syntax. The expression `self.data[idx:idx] = [val]` creates a slice that starts and ends at the same index `idx`, effectively representing an empty slice at that position. Assigning a one‑element list `[val]` to this empty slice inserts `val` into `self.data` without replacing any existing elements, shifting subsequent items to the right. This operation works in O(n) time where n is the number of elements after `idx`, as the underlying list must shift elements to accommodate the new entry.  
**Note**:  
- `idx` must be a valid index in the range `0` to `len(self.data)` inclusive; otherwise Python will raise an `IndexError`.  
- The method inserts `val` as a single list element. If `val` itself is a list, the entire list is inserted as one item, not unpacked.  
- This insertion does not return a value; it modifies `self.data` in place.
***
### FunctionDef serial_execute(self, conn_str, transaction)
**serial_execute**: The function of serial_execute is to execute each SQL statement contained in a StatementList sequentially, optionally within a single PostgreSQL transaction.

**parameters**:  
· conn_str (str): PostgreSQL libpq‑style connection string used to open a database connection.  
· transaction (bool, optional): When True, a single connection is opened and each statement is run on that connection via `execute_on_conn`; when False, each statement opens its own connection via `execute`. Default is False.

**Code Description**:  
1. **Connection metadata extraction** – The function calls `get_conn_info_dict(conn_str)` to parse the connection string and obtain a dictionary with keys `user`, `host`, `port`, `dbname`, and `search_path`.  
2. **Logging preparation** – It builds a log dictionary by merging a static message (`'executing sql statement list serially'`), the length of the StatementList (`len(self)`), the transaction flag, and the connection‑info dictionary using `combine_dicts`. The resulting dictionary is emitted with `logger.info`.  
3. **Non‑transactional path** – If `transaction` is False, the function iterates over the StatementList (`for each in self:`) and calls `each.execute(conn_str)`. The `Statement.execute` method creates its own connection, runs the SQL, captures results, and stores any error information inside the Statement object. The list is modified in place (each Statement may acquire `data`, `rowcount`, `err`, etc.).  
4. **Transactional path** – If `transaction` is True, the function:
   - Initializes `conn` to `None`.
   - Enters a `try` block and opens a connection with `psycopg2.connect(conn_str)` using a `with` statement. The context manager guarantees that `conn.commit()` is called automatically when the block exits without exception, or `conn.rollback()` is called if an exception propagates.
   - Inside the `with` block, it iterates over the StatementList and invokes `each.execute_on_conn(conn)` for every statement. `execute_on_conn` runs the SQL on the already‑opened connection, allowing all statements to share the same transaction.
   - The `finally` clause ensures that the connection is explicitly closed if it was successfully created.
5. **Return value** – After all statements have been processed, the function returns the StatementList instance (`self`). Because the list is mutated in place, callers receive the same object now populated with execution results.

**Reference Relationships**:  
- **Callers** – `serial_execute` is invoked by many high‑level modules that need to run a batch of SQL commands, for example:
  * `_add_measurement_ids` (pedsnetdcc/bmi.py) – updates last‑ID tracking tables inside a transaction.
  * `_add_era_ids` (pedsnetdcc/era.py and pedsnetdcc/r_drug_era.py) – reserves ID ranges atomically.
  * `map_external_ids` (pedsnetdcc/external_id_mapper.py) – updates external ID mapping tables.
  * `IDMappingTransform.pre_transform` – reserves IDs for all non‑vocabulary tables.
  * Permission‑granting functions (`grant_loading_user_permissions`, `grant_database_permissions`, `grant_schema_permissions`, `grant_vocabulary_permissions`, etc.) – execute a series of GRANT statements.
  * Transformation runners (`run_transformation`, `run_target_transformation`, `run_age_transformation`, `run_concept_transformation`, `run_site_transformation`, `run_id_transformation`, `run_index_transformation`) – move schemas and tables after a transformation.
  * `sync_observation_period` – runs a set of statements that must be atomic.
  * Unit tests in `pedsnetdcc/tests/db_test.py` verify both transactional and non‑transactional behavior.
- **Callees** – Within `serial_execute` the following utilities are used:
  * `get_conn_info_dict` – parses the connection string for logging.
  * `combine_dicts` – merges multiple dictionaries into a single log payload.
  * `psycopg2.connect` – creates the underlying PostgreSQL connection when a transaction is requested.
  * Each element of the StatementList must implement either `execute(conn_str)` or `execute_on_conn(conn)`. These methods belong to the `Statement` class defined elsewhere in the package and are responsible for actual SQL execution and result capture.

**Note**:  
- Statements that cannot be run inside a transaction block (e.g., `CREATE TABLE`, `DROP TABLE`, `VACUUM`) must be executed with `transaction=False`. Attempting to run them with `transaction=True` will cause PostgreSQL to raise an error, which is captured in the Statement’s `err` attribute (see the vacuum tests).  
- The function modifies the StatementList in place; callers should not reuse the same list for a different execution context without resetting or recreating it.  
- The `with psycopg2.connect(...) as conn:` construct automatically commits on successful exit and rolls back on any exception, ensuring atomicity of the whole batch when `transaction=True`.  
- If an exception occurs while opening the connection, the `finally` block will still attempt to close the connection only if it was successfully created, preventing a `NameError`.  
- Logging includes the length of the statement list and the transaction flag, which aids debugging of large batch operations.

**Output Example**:  
```python
stmts = StatementList()
stmts.append(Statement('SELECT 1'))
stmts.append(Statement('SELECT 2'))
result = stmts.serial_execute('host=localhost dbname=test user=loader', transaction=True)
# `result` is the same StatementList instance `stmts`
# After execution:
#   result[0].data == [(1,)]
#   result[1].data == [(2,)]
#   result[0].err is None
#   result[1].err is None
```
***
## FunctionDef _worker_process(conn_str, taskq, resq, logq)
**_worker_process**: The function of _worker_process is to continuously fetch tasks from a queue, execute each task with a given database connection string, optionally pass result and log queues, and signal task completion until a sentinel value (None) is received.

**parameters**: The parameters of this Function.  
· **conn_str**: a string containing the database connection information that will be passed unchanged to each task’s `execute` method.  
· **taskq**: a `queue.Queue` (specifically a `multiprocessing.JoinableQueue` in the calling code) from which tasks are retrieved. Each item is expected to implement an `execute(conn_str, resq, logq)` method.  
· **resq** (optional): a `queue.Queue` (a `multiprocessing.Queue` when created by the caller) that is forwarded to each task so the task can place its result object onto the queue. May be `None` if the caller does not need results.  
· **logq** (optional): a `queue.Queue` (also a `multiprocessing.Queue` when created by the caller) that is forwarded to each task for logging purposes. May be `None` if logging is not required.

**Code Description**:  
The function enters an infinite loop that repeatedly calls `taskq.get()` to obtain the next item from the task queue.  
1. **Sentinel detection** – If the retrieved item is `None`, the loop breaks, causing the worker process to exit. This sentinel is inserted by the caller (`StatementSet.parallel_execute`) after all real tasks have been processed.  
2. **Task execution** – For any non‑None item, the function assumes the object implements an `execute` method with the signature `execute(conn_str, resq, logq)`. It invokes this method, passing the supplied connection string and the optional result and log queues. In the test suite, `MockExecutable` objects fulfill this contract by placing a dictionary describing the call onto `resq`.  
3. **Task completion signalling** – After the task’s `execute` method returns, the worker calls `taskq.task_done()`. This informs the `JoinableQueue` that one unit of work has been finished, allowing the main thread to unblock when `taskq.join()` is called.  

The worker process is designed to be launched via `multiprocessing.Process` with `target=_worker_process`. It is used exclusively by `StatementSet.parallel_execute`, which creates a pool of such processes (up to a configurable `pool_size`). `parallel_execute` populates `taskq` with `Statement` objects (each a task), starts a logger thread that consumes `logq`, and finally inserts a `None` sentinel for each worker to trigger graceful shutdown. The result queue `resq` is later drained by `parallel_execute` to collect the modified statements.

The test cases in `pedsnetdcc/tests/db_test.py` verify the worker’s behavior under various configurations:
- Single task with a single worker.
- Single task with multiple workers.
- Multiple tasks with a single worker.
- Multiple tasks with multiple workers.  

All tests confirm that tasks are executed, results are placed on `resq`, and the worker exits cleanly after receiving the sentinel.

**Note**:  
- The function assumes that every non‑None queue item implements the required `execute` method; passing an object without this method will raise an `AttributeError`.  
- `taskq.task_done()` must be called for each retrieved task; failure to do so will cause `taskq.join()` in the caller to block indefinitely.  
- The sentinel (`None`) must be placed on the queue once for each worker process; otherwise some workers may wait forever for a termination signal.  
- If `resq` or `logq` are omitted (`None`), tasks must be prepared to handle missing queues; otherwise they may raise exceptions when attempting to put results or logs.  
- Because the function runs in a separate process, any objects placed on the queues must be picklable. This is satisfied by the simple dictionaries used in the test suite and by the `Statement` objects used in production.
## FunctionDef _logger_thread(logq)
**_logger_thread**: The function of _logger_thread is to continuously retrieve logging records from a queue and forward them to the module‑level logger, terminating when a sentinel value (None) is received.

**parameters**:  
· logq: a queue (typically `queue.Queue` or `multiprocessing.Queue`) from which log records are read.

**Code Description**:  
The function enters an infinite loop that repeatedly calls `logq.get()` to obtain the next item placed on the queue.  
1. If the retrieved item is `None`, the loop breaks, signalling that no further log records will be sent and allowing the thread to finish cleanly.  
2. For any other object, the function assumes it is a `logging.LogRecord`. Before handing the record to the global logger, it checks whether the logger is configured to handle the record’s severity level with `logger.isEnabledFor(record.levelno)`. This guard is necessary because `logger.handle()` does not perform its own level filtering.  
3. If the logger is enabled for that level, `logger.handle(record)` is called, which processes the record through the logger’s handlers, formatters, and filters exactly as if the record had been emitted in the main thread.

The function does not create or configure the logger; it relies on the module‑level `logger` that is imported elsewhere in `db.py`. It is deliberately lightweight and runs in its own thread, allowing other processes (started by `StatementSet.parallel_execute`) to place log records onto `logq` without blocking.

**Relationship with Callers**:  
- **StatementSet.parallel_execute**: This method spawns a separate thread that runs `_logger_thread`, passing it a `multiprocessing.Queue` (`logq`). Worker processes created by `parallel_execute` generate log records and put them onto the same queue. The logger thread ensures those records are emitted through the main process’s logger, preserving a unified logging output despite the parallel execution. After all tasks complete, `parallel_execute` places a `None` sentinel on `logq` and joins the logger thread, guaranteeing orderly shutdown.  
- **tests.db_test.LoggerThreadTest.test_log_passing**: The unit test constructs a dedicated logger (`test_logger`) with a custom `DictQueueHandler` that forwards log records into a queue. It then starts `_logger_thread` in a thread, logs messages via `test_logger`, and finally sends a `None` sentinel to stop the logger thread. The test verifies that messages placed on the queue are correctly handled by the main logger, confirming the thread’s behavior.

**Note**:  
- The queue must be thread‑safe; when used across processes, a `multiprocessing.Queue` should be supplied.  
- Always enqueue a `None` sentinel after all logging activity is finished; otherwise the logger thread will block indefinitely on `logq.get()`.  
- The global `logger` must be configured with appropriate handlers and level settings before the thread starts, otherwise records may be dropped or mis‑formatted.  
- Because the function checks `logger.isEnabledFor`, records below the logger’s level are ignored without invoking handlers, preserving expected logging semantics.  
- This utility is intended for internal use only; external callers should not invoke it directly unless they replicate the same queue‑based logging pattern.
