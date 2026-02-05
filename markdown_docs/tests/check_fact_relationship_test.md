## FunctionDef setUpModule
**setUpModule**: The function of setUpModule is to configure a global logger for the test suite so that log output is captured in a mock handler with JSON formatting.  

**parameters**: This function takes no parameters.  

**Code Description**:  
The function performs the following steps in order:  

1. Declares the names `logger` and `handler` as global variables, allowing the test module to access them from other functions or test cases.  
2. Retrieves (or creates) a logger instance named `'pedsnetdcc'` by calling `logging.getLogger('pedsnetdcc')`. This logger is the central point through which the code under test will emit log records.  
3. Instantiates a `MockLoggingHandler` (defined in the same test module) and assigns it to the global variable `handler`. The mock handler stores formatted log messages in an in‑memory dictionary (`handler.messages`) instead of writing them to a file or the console.  
4. Creates a `DictLogFilter` with the argument `'json'` and adds it to the handler via `handler.addFilter(...)`. The filter ensures that any log record whose `msg` attribute is a dictionary is transformed into a JSON string before the handler formats and stores it.  
5. Attaches the configured handler to the logger with `logger.addHandler(handler)`. From this point forward, every log record emitted by the `'pedsnetdcc'` logger will pass through the `DictLogFilter` and then be captured by `MockLoggingHandler`.  
6. Sets the logger’s level to `DEBUG` by calling `logger.setLevel(logging.getLevelName('DEBUG'))`. This guarantees that all messages of level DEBUG and higher are processed by the handler, providing maximal visibility for the tests.  

**Relationship with callees**:  
- `logging.getLogger` provides the logger object that the application code uses.  
- `MockLoggingHandler` is a test‑specific subclass of `logging.Handler` that records formatted messages in `handler.messages`. The test suite later inspects this dictionary to verify that expected log entries were produced.  
- `DictLogFilter('json')` is a logging filter that converts dictionary‑based log messages into JSON format. By adding this filter to the mock handler, the tests can assert against a deterministic JSON representation of log output.  

**Note**:  
- Because `logger`, `handler`, and the filter are global, they persist for the entire duration of the test run. If additional test modules also call `setUpModule`, they will reuse the same logger instance; ensure that the handler’s `reset()` method is called between independent test cases to avoid cross‑test contamination.  
- The logger level is set to DEBUG, which may produce a large volume of log records. If a test requires a more restrictive level, it must adjust `logger.setLevel` after `setUpModule` runs.  
- The filter only acts on records where `record.msg` is a dictionary; plain string messages will bypass the JSON conversion and be stored as‑is.  
- No explicit removal of the handler is performed; the logger will retain the mock handler for the lifetime of the process. This is acceptable for unit‑test environments but should be cleaned up in long‑running applications.
## ClassDef CheckFactRelationshipTest
**CheckFactRelationshipTest**: The function of CheckFactRelationshipTest is to verify that the `check_fact_relationship` function correctly identifies bad fact relationships and logs the appropriate messages.

**attributes**: This class does not define explicit instance attributes; it relies on the unittest framework and external utilities (`handler`, `make_conn_str`, `check_fact_relationship`).

**Code Description**:  
- The class inherits from `unittest.TestCase`, making it a unit‑test case that can be discovered and run by the Python unittest runner.  
- `setUp(self)`: Executed before each test method. It calls `handler.reset()` to clear any previously captured log messages, ensuring a clean state for each test.  
- `test_fact_relationship_results(self)`:  
  1. Retrieves the names of two required environment variables: `PEDSNETDCC_TEST_DBURI` and `PEDSNETDCC_TEST_SEARCH_PATH`.  
  2. If either variable is missing from `os.environ`, the test is skipped with a clear message indicating the missing variables.  
  3. Constructs a database connection string by calling `make_conn_str` with the values of the two environment variables.  
  4. Calls `check_fact_relationship(conn_str)` and stores the returned boolean in `success`.  
  5. Asserts that `success` is `False`, meaning the function should report a failure when bad fact relationships are present.  
  6. Prints the collected log messages from `handler.messages` (primarily for debugging).  
  7. Iterates over the `'info'` log entries captured by `handler`. Each entry is parsed from JSON. If the message text starts with `'fact relationship has bad'`, the test asserts that the associated `'percent'` field equals `100`, confirming that the log correctly reports the proportion of bad relationships.  

The test therefore checks both the return value of `check_fact_relationship` and the content of its informational log output.

**Note**:  
- The test depends on external configuration (environment variables) and on the behavior of `handler`, `make_conn_str`, and `check_fact_relationship`; these must be correctly implemented and available in the test environment.  
- The `print(handler.messages)` line is only for diagnostic purposes and does not affect test outcomes.  
- If the logging format or message text changes, the JSON parsing and string‑matching logic may need to be updated accordingly.
### FunctionDef setUp(self)
**setUp**: The function of setUp is to prepare a clean test environment by resetting the mock logging handler before each test case runs.  

**parameters**:  
· self: the instance of the test case class (automatically supplied by the unittest framework).  

**Code Description**:  
The method consists of a single operation: `handler.reset()`. This call invokes the `reset` method of the `MockLoggingHandler` object defined elsewhere in the test suite. The `reset` method acquires the handler’s internal lock, clears all stored log message lists while preserving the dictionary keys, and then releases the lock. By executing this call in `setUp`, the test framework guarantees that any log messages captured by the mock handler from previous tests are removed, ensuring that each test starts with an empty message store. This eliminates cross‑test contamination and provides deterministic test results.  

**Note**:  
- `setUp` is automatically executed by the unittest framework before each test method in `CheckFactRelationshipTest`.  
- The method does not return a value; its purpose is purely side‑effectual.  
- It relies on the thread‑safe implementation of `handler.reset()`, so no additional synchronization is required in `setUp`.
***
### FunctionDef test_fact_relationship_results(self)
**test_fact_relationship_results**: The function of test_fact_relationship_results is to verify that the check_fact_relationship routine correctly reports a completely invalid fact_relationship table and that the expected log messages are produced.

**parameters**: This test method does not accept any external parameters.

**Code Description**:
1. **Environment validation** – The method defines two environment‑variable names, `PEDSNETDCC_TEST_DBURI` and `PEDSNETDCC_TEST_SEARCH_PATH`.  
   - If **both** variables are absent from `os.environ`, the test is skipped by calling `self.skipTest` with a message that lists the missing variables.  
   - This ensures the test only runs when a database URI and a search‑path are supplied.

2. **Connection‑string construction** –  
   - The value of `PEDSNETDCC_TEST_DBURI` is read from the environment and passed, together with the value of `PEDSNETDCC_TEST_SEARCH_PATH`, to the utility function `make_conn_str`.  
   - `make_conn_str` returns a libpq‑compatible connection string (`conn_str`) that will be used for all subsequent SQL execution.

3. **Execution of the check** –  
   - `check_fact_relationship(conn_str)` is invoked. The function performs a series of SQL queries against the fact_relationship table and returns a Boolean (`True` if the table is valid, `False` otherwise).  
   - The test asserts that the returned value is `False` with `self.assertFalse(success)`, confirming that the test database deliberately contains invalid fact_relationship references.

4. **Log‑message inspection** –  
   - The test prints the contents of `handler.messages` (the in‑memory log handler that captures structured log output).  
   - It then iterates over every message stored under the `'info'` key of `handler.messages`. For each message string:
     * The string is parsed as JSON (`json.loads(msg)`).  
     * If the parsed JSON object’s `'msg'` field begins with the text `'fact relationship has bad'`, the test asserts that the associated `'percent'` field equals `100`. This checks that the warning generated by `check_fact_relationship` correctly reports a 100 % failure rate for the affected domain.

5. **Dependencies and relationships** –  
   - **Callees**: `os.getenv` (via `os.environ`), `make_conn_str` (from pedsnetdcc.utils), `check_fact_relationship` (from pedsnetdcc.check_fact_relationship), `json.loads`, and the `handler` object that records log messages.  
   - **Callers**: The unittest framework calls this method as part of the `CheckFactRelationshipTest` test case class.

**Note**:
- The test requires the environment variables `PEDSNETDCC_TEST_DBURI` and `PEDSNETDCC_TEST_SEARCH_PATH` to be defined; otherwise it is skipped.  
- The `handler` object must be configured elsewhere in the test suite to capture structured log output in the format `{'msg': ..., 'percent': ...}`; otherwise the loop that inspects `handler.messages['info']` will not find any entries.  
- The assertion on `msg['percent'] == 100` only runs for log entries whose message starts with `'fact relationship has bad'`; if the implementation of `check_fact_relationship` changes the wording of that message, the assertion will not be triggered.  
- The test does not examine warning‑level log entries; it only checks the `'info'` channel, which matches the current logging behavior of `check_fact_relationship`.
***
## ClassDef MockLoggingHandler
**MockLoggingHandler**: The function of MockLoggingHandler is to capture log records in memory for later inspection during tests.  

**attributes**:  
· **messages** – a dictionary that stores formatted log messages, keyed by the lowercase name of the log level (`'debug'`, `'info'`, `'warning'`, `'error'`, `'critical'`). Each key maps to a list that preserves the order in which messages were emitted.  

**Code Description**:  
MockLoggingHandler is a subclass of the standard library’s `logging.Handler`. Its purpose is to act as a test double for a real logging handler, allowing test code to verify that expected log output was produced without writing to files or the console.  

* **`__init__(self, *args, **kwargs)`** – Initializes the instance by creating the `messages` dictionary with empty lists for each log level. It then forwards any positional or keyword arguments to the base `logging.Handler` constructor via `super()`. No additional parameters are defined; the handler accepts the same arguments as its parent class.  

* **`emit(self, record)`** – This method is invoked automatically by the logging framework whenever a log record is emitted to the handler. The implementation performs the following steps:  
  1. Calls `self.acquire()` to obtain the internal lock, ensuring thread‑safe access to shared state.  
  2. Formats the incoming `record` using the handler’s formatter (`self.format(record)`).  
  3. Appends the formatted message to the appropriate list in `self.messages`, using `record.levelname.lower()` as the dictionary key.  
  4. Prints the formatted message to standard output (`print(msg)`). This side‑effect is useful during debugging but does not affect the stored messages.  
  5. If any exception occurs while formatting or storing the message, `self.handleError(record)` is called to follow the standard logging error handling policy.  
  6. Finally, `self.release()` is called to release the lock.  

* **`reset(self)`** – Provides a convenient way to clear all captured log messages between test cases. It acquires the lock, iterates over each list in `self.messages`, and deletes its contents in‑place (`del msg_list[:]`). The lock is released afterward, guaranteeing that no other thread can read or write the lists while they are being cleared.  

**Relationship with callers**:  
In the test module `tests/check_fact_relationship_test.py`, the function `setUpModule` creates a global logger named `'pedsnetdcc'` and attaches an instance of `MockLoggingHandler` to it. The handler is further configured with a `DictLogFilter('json')` filter before being added to the logger. The logger’s level is set to `DEBUG`, ensuring that all messages of level DEBUG and higher are passed to the handler. Consequently, any log statements executed by the code under test are captured by `MockLoggingHandler`, and test assertions can examine `handler.messages` to confirm that the correct messages were logged at the expected levels.  

**Note**:  
· The handler stores messages only after they have been formatted; therefore, the formatter attached to the handler (if any) determines the exact string representation stored in `messages`.  
· Because `emit` prints each message, test output may include duplicate log lines; this is intentional for visibility but can be suppressed by removing or overriding the `print` call.  
· The `reset` method must be called between independent test cases to avoid cross‑test contamination of captured logs.  
· Thread safety is provided by the internal lock (`acquire`/`release`), but the handler is not intended for high‑concurrency production use; it is optimized for deterministic unit‑test environments.  
· The handler does not implement `close` or other lifecycle methods; the default behavior from `logging.Handler` is sufficient for the test suite.
### FunctionDef __init__(self)
**__init__**: The function of __init__ is to initialize a MockLoggingHandler instance by preparing a container for captured log messages and delegating any additional initialization to its superclass.

**parameters**: The parameters of this Function.  
· *args: Variable positional arguments passed to the superclass initializer.  
· **kwargs: Variable keyword arguments passed to the superclass initializer.

**Code Description**:  
- The method begins by creating an instance attribute `self.messages`, which is a dictionary with five keys corresponding to standard logging levels: `'debug'`, `'info'`, `'warning'`, `'error'`, and `'critical'`. Each key is associated with an empty list, intended to store log records of that particular severity as they are emitted.  
- After establishing the message container, the method calls `super(MockLoggingHandler, self).__init__(*args, **kwargs)`. This invokes the `__init__` method of the parent class (typically `logging.Handler`), ensuring that any required setup defined in the base class—such as formatter configuration, level setting, or other internal state—is performed using the arguments supplied to the mock handler.

**Note**:  
- The `self.messages` dictionary is the primary interface for test code to inspect which messages have been logged at each level; tests should access these lists after logging operations to verify expected behavior.  
- Because the method forwards all received arguments to the superclass, it remains compatible with any custom initialization parameters that the original logging handler might accept.  
- No additional logic is performed in this constructor; all logging handling behavior must be implemented in other methods (e.g., `emit`).
***
### FunctionDef emit(self, record)
**emit**: The function of emit is to store a formatted log record’s message in the handler’s internal message store while ensuring thread‑safe operation.  

**parameters**:  
· self: the instance of the MockLoggingHandler class.  
· record: a logging.LogRecord object containing the log information to be processed.  

**Code Description**:  
The method begins by acquiring the handler’s internal lock (`self.acquire()`) to guarantee that concurrent threads do not interfere with each other while the log record is being processed. Inside the protected block, it attempts to format the incoming `record` using the handler’s formatter (`self.format(record)`). The resulting string `msg` is then appended to a list stored in `self.messages`, keyed by the lower‑cased log level name (`record.levelname.lower()`). This design groups messages by their severity (e.g., “info”, “warning”, “error”). After storing the message, the method prints the formatted message to standard output (`print(msg)`).  

If any exception occurs during formatting or storage, the `except` clause catches it and forwards the problematic record to the standard logging error handling mechanism via `self.handleError(record)`. Finally, regardless of success or failure, the `finally` block releases the lock (`self.release()`) to allow other threads to proceed.  

**Note**:  
- The handler assumes that `self.messages` already contains a list for each possible log level; otherwise, a `KeyError` will be raised.  
- Because the method prints each message, it may interfere with test output capture; consider redirecting `stdout` if silent operation is required.  
- The lock acquisition/release pattern ensures thread safety but may introduce contention in highly concurrent logging scenarios.
***
### FunctionDef reset(self)
**reset**: The function of reset is to clear all stored log messages in the handler while ensuring thread‑safety.  

**parameters**: This method does not accept any parameters.  

**Code Description**:  
- The method begins by acquiring the internal lock of the handler (`self.acquire()`). This guarantees exclusive access to the handler’s internal state, preventing race conditions when multiple threads might be logging simultaneously.  
- Inside a `try` block, it iterates over each list of messages stored in the dictionary `self.messages`. For each list (`msg_list`), the slice assignment `del msg_list[:]` removes all elements, effectively emptying the list without discarding the list object itself. This preserves the original dictionary keys while resetting their associated message collections.  
- The `finally` clause releases the lock (`self.release()`), ensuring that the lock is always released even if an unexpected exception occurs during the clearing process.  

**Relationship with Callers**:  
- In the test suite `tests/check_fact_relationship_test.py`, the `CheckFactRelationshipTest.setUp` method invokes `handler.reset()` before each test case runs. This call guarantees that the mock logging handler starts with an empty message store, providing a clean slate for each test and preventing cross‑test contamination of log data.  

**Note**:  
- Because the method manipulates shared mutable state, it must always be called while the handler is in a consistent state; the built‑in lock handling in this implementation ensures that requirement.  
- The method does not return a value; its purpose is solely side‑effectual (clearing stored messages).
***
