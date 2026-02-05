## FunctionDef secs_since(starttime)
**secs_since**: The function of secs_since is to calculate the number of whole seconds that have elapsed since a given start time and return that value as a zero‑padded four‑character string.

**parameters**: The parameters of this function.
· starttime: an integer or float representing the epoch time (seconds since 1970‑01‑01) from which the elapsed time should be measured.

**Code Description**: secs_since receives a start time value (typically obtained from time.time() at the beginning of a process). It calls time.time() again to obtain the current epoch time, subtracts the supplied starttime, and converts the resulting difference to an integer, discarding any fractional seconds. The integer is then formatted with Python’s string format specification '{0:0>4}', which left‑pads the number with zeros to ensure a minimum width of four characters. The formatted string is returned. Throughout the project, secs_since is used in logging statements to provide a consistent, human‑readable elapsed‑time field. For example, run_bmi_calc, _add_measurement_ids, run_era, run_r_dose, sync_observation_period, and many other functions call secs_since to embed the elapsed seconds in log messages that indicate the duration of a particular step or the total execution time of a function.

**Note**: 
- The function expects starttime to be comparable to the value returned by time.time(); passing a non‑numeric value will raise a TypeError.
- The returned string is always four characters long; if the elapsed time exceeds 9999 seconds, the string will contain the full number without truncation (e.g., '12345'), because the format pads only when the length is less than four.
- secs_since does not handle time zones or daylight‑saving adjustments; it works purely with epoch timestamps.

**Output Example**: If starttime corresponds to a moment 12 seconds before the current call, secs_since(starttime) will return the string '0012'.
## FunctionDef strtime
**strtime**: The function of strtime is to return the current local time as a string formatted according to RFC 3339.  

**parameters**: This function does not accept any parameters.  

**Code Description**:  
1. The function obtains the current epoch time in seconds using `time.time()` and stores it in `curr_time`.  
2. It formats the epoch time into a human‑readable date‑time string without timezone information using `time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(curr_time))`. The result, `fmtd_time`, follows the pattern `YYYY‑MM‑DDTHH:MM:SS`.  
3. To compute the local UTC offset, the function creates two `datetime` objects: one representing the local time (`datetime.datetime.fromtimestamp(curr_time)`) and one representing UTC (`datetime.datetime.utcfromtimestamp(curr_time)`). The difference between them yields a `timedelta` whose total seconds are stored in `utc_offset`.  
4. The offset is split into whole hours (`utc_hours`) and minutes (`utc_mins`). Hours are obtained by integer division of the offset by 3600. Minutes are derived from the remainder, taking the absolute value to handle negative offsets correctly.  
5. Finally, the function assembles the full RFC 3339 timestamp using `str.format`. The format string `'{0}{1:0=+3}:{2:0>2}'` inserts the base date‑time, the signed hour offset padded to three characters (including the sign), and the two‑digit minute offset. The resulting string includes the timezone offset in the form `±HH:MM`.  

**Relationship with Callers**:  
- In `DictLogFilter.json_filter`, `strtime()` is called to add a `"time"` field to the log record’s message dictionary before the record is converted to JSON. This ensures every JSON log entry contains an RFC 3339 timestamp.  
- In `DictLogFilter.text_filter`, `strtime()` is also used to insert a `"time"` field into the message dictionary prior to converting the dictionary into a space‑separated `key="value"` format. Both filters rely on `strtime()` to provide a consistent, standards‑compliant timestamp for log entries, regardless of the output format (JSON or plain text).  

**Note**:  
- The function uses the local system time and the corresponding local UTC offset; it does not force UTC or handle daylight‑saving transitions beyond what the operating system reports.  
- The returned string always includes a signed hour offset (`+` or `-`) followed by a colon and two‑digit minutes, matching RFC 3339 requirements.  
- Because the function imports `time` and `datetime` from the standard library, it has no external dependencies.  

**Output Example**:  
If the current local time is 2023‑08‑15 14:30:45 with a UTC offset of –04:00, `strtime()` will return:  

`2023-08-15T14:30:45-04:00`
## FunctionDef levelcolor(level)
**levelcolor**: The function of levelcolor is to return the terminal color code (SGR parameter) that corresponds to a given logging level.

**parameters**: The parameters of this Function.  
· level: an integer representing a logging level (e.g., logging.DEBUG, logging.WARNING, etc.)

**Code Description**:  
The function examines the supplied logging level and selects a predefined SGR color constant that will be used to colorize terminal output.  
- If the level equals `logging.DEBUG`, the function returns the constant `green`.  
- If the level equals `logging.WARNING`, the function returns the constant `yellow`.  
- If the level is either `logging.ERROR` or `logging.CRITICAL`, the function returns the constant `red`.  
- For any other level (including `logging.INFO` and custom levels), the function falls back to returning the constant `blue`.  

The returned value is an integer that represents the foreground text color in ANSI escape sequences (e.g., 32 for green, 33 for yellow, 31 for red, 34 for blue).  

In the project, `levelcolor` is invoked by `DictLogFilter.tty_filter` within `pedsnetdcc/dict_logging.py`. The filter builds a formatted log string for terminal display and calls `levelcolor(record.levelno)` to embed the appropriate color code for the log level in the ANSI escape sequence. This ensures that log messages appear in different colors (green for debug, yellow for warning, red for error/critical, blue for other levels) when printed to a TTY.

**Note**:  
- The function expects a standard logging level integer; passing a non‑integer or an undefined level will still trigger the default blue color.  
- The actual color constants (`green`, `yellow`, `red`, `blue`) must be defined elsewhere in the module as valid SGR numbers.  
- Changing the mapping requires editing the conditional branches; there is no external configuration.

**Output Example**:  
```python
import logging
color_code = levelcolor(logging.DEBUG)   # might return 32 (green)
color_code = levelcolor(logging.WARNING) # might return 33 (yellow)
color_code = levelcolor(logging.ERROR)   # might return 31 (red)
color_code = levelcolor(logging.INFO)    # falls back, might return 34 (blue)
```
## ClassDef DictLogFilter
**DictLogFilter**: The function of DictLogFilter is to act as a logging filter that transforms dictionary‑based log messages into a chosen textual representation (JSON, plain text, or colorized TTY) before they are emitted by a logger.

**attributes**:  
· **output** – a string that selects the output format. Acceptable values are `'json'`, `'text'`, `'tty'`, or `None`. When `None` or any unsupported value is supplied, the filter defaults to JSON formatting.

**Code Description**:  
The `DictLogFilter` class implements the standard logging filter interface (`filter(record) -> bool`). Its purpose is to detect when a `logging.LogRecord` carries a dictionary in its `msg` attribute and, if so, replace that dictionary with a formatted string or JSON payload appropriate for the configured `output` style.

* **__init__(self, output=None)** – stores the desired output format in the instance attribute `self.output`. No other initialization is required.

* **filter(self, record)** – the entry point invoked by the logging framework for each record.  
  – If `record.msg` is not a `dict`, the method returns `True` immediately, leaving the record unchanged.  
  – If the message is a dictionary, the method dispatches to one of three private helpers based on `self.output`:
    * `'text'` → `self.text_filter(record)`
    * `'tty'`  → `self.tty_filter(record)`
    * any other value (including `None`) → `self.json_filter(record)`  
  The method always returns `True` so that the record continues through the logging pipeline.

* **json_filter(self, record)** – converts the dictionary into a JSON string.  
  – Adds two standard keys: `'time'` (current timestamp from `strtime()`) and `'level'` (lower‑cased log level name).  
  – Calls `stringify()` to ensure every key and value is a string, which guarantees compatibility with `json.dumps()`.  
  – Replaces `record.msg` with the JSON‑encoded string. Returns `True`.

* **tty_filter(self, record)** – creates a human‑readable, color‑enhanced line suitable for interactive terminals.  
  – First stringifies the dictionary.  
  – Builds a prefix that includes a colored, truncated log‑level tag, the elapsed seconds since program start (`secs_since(starttime)`), and the value of the `'msg'` key if present.  
  – Pads or truncates the prefix to 80 characters.  
  – Appends each remaining key/value pair as colored `key=value` fragments.  
  – Stores the final assembled line back into `record.msg`. Returns `True`.

* **text_filter(self, record)** – produces a logfmt‑compatible plain‑text string.  
  – Adds `'time'` and `'level'` entries as in `json_filter`.  
  – Stringifies the dictionary, then formats each entry as `key="value"` (quoting the value).  
  – Joins the fragments with spaces and assigns the resulting string to `record.msg`. Returns `True`.

**Relationship with Callers**:  
- In `pedsnetdcc/main.py`, the CLI entry point creates a logger named `'pedsnetdcc'`. Depending on the command‑line option `logfmt`, it attaches a `logging.StreamHandler` and adds a `DictLogFilter` instance with the matching format (`'tty'`, `'text'`, or default `'json'`). This ensures that all log messages emitted by the application are automatically converted to the selected representation before reaching the console.  
- In the test suite (`pedsnetdcc/tests/db_test.py` and `tests/check_fact_relationship_test.py`), a `MockLoggingHandler` is configured with `DictLogFilter('json')`. The tests then inspect `handler.messages` to verify that log output contains correctly formatted JSON strings, confirming that the filter behaves as expected under unit‑test conditions.

**Note**:  
- The filter only processes records whose `msg` attribute is a dictionary; any other message type passes through unchanged.  
- The helper functions `strtime`, `stringify`, `levelcolor`, `secs_since`, and the module‑level variables `starttime` are assumed to be defined elsewhere in the package; they provide timestamp formatting, safe string conversion, ANSI colour codes, and program start time respectively.  
- Because the filter mutates `record.msg` in place, it must be added **before** any formatter that expects a string message. Adding it after a formatter that already consumes `record.msg` may result in lost or double‑formatted output.  
- The `'tty'` format pads the prefix to exactly 80 characters; if the terminal width is smaller, the line may wrap.  

**Output Example**:  

*JSON format (used when `output='json'`)*  
```json
{"time":"2023-07-21T14:32:10Z","level":"info","msg":"User created","user_id":"12345","status":"success"}
```

*Text format (used when `output='text'`)*  
```
time="2023-07-21T14:32:10Z" level="info" msg="User created" user_id="12345" status="success"
```

*TTY format (used when `output='tty'`)*  
```
[32mINFO[0m[12.34] User created                         [32muser_id[0m=12345 [32mstatus[0m=success
```
### FunctionDef __init__(self, output)
**__init__**: The function of __init__ is to create a new DictLogFilter instance and optionally store a desired output format.  

**parameters**: The parameters of this Function.  
· output: Optional. Specifies the output format for the filter. Acceptable values are None or a string equal to 'json', 'text', or 'tty'.  

**Code Description**:  
The constructor receives a single argument, output, which may be omitted (default None). Inside the method, the argument is assigned directly to the instance attribute self.output. No validation, transformation, or additional processing is performed; the method simply records the provided value for later use by other methods of the DictLogFilter class. The method does not return a value explicitly; Python implicitly returns None after object initialization.  

**Note**:  
- The caller is responsible for ensuring that output conforms to the allowed set ('json', 'text', 'tty') if a specific format is required. Supplying any other string will not raise an error at construction time, but may cause downstream failures when the filter attempts to format log records.  
- Because the attribute is stored unchanged, it can be inspected or modified after instantiation (e.g., filter_instance.output = 'json').  

**Output Example**:  
```python
# Creating a filter with JSON output
json_filter = DictLogFilter(output='json')
print(json_filter.output)   # -> 'json'

# Creating a filter with default (None) output
default_filter = DictLogFilter()
print(default_filter.output)   # -> None
```
***
### FunctionDef filter(self, record)
**filter**: The function of filter is to examine a logging record and, when the record’s message is a dictionary, reformat it according to the filter’s configured output style (text, tty, or json).  

**parameters**:  
· **self** – the instance of `DictLogFilter` on which the method is invoked.  
· **record** – a `logging.LogRecord` object that may contain a dictionary in its `msg` attribute.  

**Code Description**:  
1. The method first checks the type of `record.msg`. If `record.msg` is **not** an instance of `dict`, the method immediately returns `True`. In this case the record is left untouched and will continue through the logging pipeline.  
2. If `record.msg` **is** a dictionary, the method examines the value of the filter’s `output` attribute:  
   * When `self.output == 'text'`, the method calls `self.text_filter(record)`. This delegate adds timestamp and level fields, stringifies the dictionary, and converts it into a space‑separated `key="value"` string (logfmt style).  
   * When `self.output == 'tty'`, the method calls `self.tty_filter(record)`. This delegate produces a colour‑coded, padded string suitable for terminal display, including elapsed seconds, level abbreviation, and any additional key‑value pairs.  
   * For any other value of `self.output` (including the default case), the method calls `self.json_filter(record)`. This delegate adds timestamp and level fields, stringifies the dictionary, and serialises it to a JSON string.  
3. Each of the three delegate methods (`text_filter`, `tty_filter`, `json_filter`) returns `True`. The `filter` method propagates this return value unchanged, signalling to the logging framework that the record should not be filtered out.  

**Note**:  
- The method never suppresses a log record; it only reformats dictionary‑based messages.  
- The original dictionary stored in `record.msg` is mutated in‑place by the chosen delegate. After the call, `record.msg` will contain a string (text, tty, or JSON) rather than the original dictionary.  
- If `self.output` is set to an unexpected value, the method falls back to JSON formatting, ensuring a deterministic default behaviour.  
- Because the method returns `True` for both the “not a dict” path and the formatted paths, downstream handlers will always receive the record.  

**Output Example**:  
```python
# Assume a filter instance with output='text'
filter_instance = DictLogFilter(output='text')
record = logging.LogRecord(
    name='example',
    level=logging.INFO,
    pathname=__file__,
    lineno=10,
    msg={'event': 'login', 'user': 'alice'},
    args=(),
    exc_info=None
)

result = filter_instance.filter(record)   # result is True
# After the call, record.msg is a string such as:
# 'time="2023-08-15T14:30:45-04:00" level="info" event="login" user="alice"'
```
***
### FunctionDef json_filter(self, record)
**json_filter**: The function of json_filter is to transform a logging record whose message is a dictionary into a JSON‑encoded string while adding a timestamp and log level.

**parameters**:  
· **self** – the instance of `DictLogFilter` on which the method is invoked.  
· **record** – a `logging.LogRecord` object whose `msg` attribute is expected to be a dictionary containing the log payload.

**Code Description**:  
1. The method begins by inserting two standard fields into the dictionary stored in `record.msg`:  
   * `time` – populated with the current local time formatted according to RFC 3339 by calling `strtime()`.  
   * `level` – populated with the log level name in lower‑case (`record.levelname.lower()`).  
2. To guarantee that every key and value can be serialized by `json.dumps`, the dictionary is passed through `stringify(record.msg)`. `stringify` recursively converts all keys and values to plain strings while preserving the mapping structure.  
3. The resulting pure‑string dictionary is then encoded to a JSON text string with `json.dumps(record.msg)`. The JSON string replaces the original dictionary in `record.msg`.  
4. Finally, the method returns `True` unconditionally, signalling to the logging framework that the record should continue to be processed by subsequent handlers.

**Reference Relationships**:  
* **Caller** – `DictLogFilter.filter` invokes `json_filter` when the filter’s `output` attribute is neither `'text'` nor `'tty'` and when `record.msg` is a dictionary. This makes `json_filter` the default formatter for dictionary‑based log records.  
* **Callees** –  
  * `strtime()` supplies a standards‑compliant timestamp that is added to the log entry.  
  * `stringify()` sanitizes the dictionary so that non‑string values (e.g., numbers, objects) do not cause `json.dumps` to raise a `TypeError`.  
  * The standard library function `json.dumps()` performs the actual JSON serialization.  

**Note**:  
* The method mutates `record.msg` in‑place: the original dictionary is replaced by a JSON string. Subsequent filters or handlers that expect a dictionary will no longer see the original structure.  
* Because `json_filter` always returns `True`, it never suppresses a log record; it only reformats it.  
* The added `time` field reflects the local system time and includes the UTC offset as produced by `strtime()`. If the system clock is incorrect, the timestamp will be inaccurate.  
* All keys and values become strings; numeric values lose their native type after serialization, which is acceptable for pure JSON output but may affect downstream processing that expects original types.

**Output Example**:  
Assume a log record with `record.msg = {'event': 'user_login', 'user_id': 42}` and a log level of `INFO`. After `json_filter` runs, `record.msg` will contain a JSON string similar to:

```json
{"event":"user_login","user_id":"42","time":"2023-08-15T14:30:45-04:00","level":"info"}
```

The method returns `True`.
***
### FunctionDef tty_filter(self, record)
**tty_filter**: The function of tty_filter is to format a logging record that contains a dictionary into a human‑readable, color‑coded string suitable for display on a terminal (TTY) and always indicate that the record should be processed further.  

**parameters**: The parameters of this Function.  
· **record**: a `logging.LogRecord` instance whose `msg` attribute is expected to be a dictionary containing at least a `'msg'` key.  

**Code Description**:  
1. **Stringify the message dictionary** – The function calls `stringify(record.msg)`. This recursively converts every key and value in the dictionary to plain strings while preserving the mapping structure, guaranteeing that subsequent string operations will not raise type errors.  
2. **Build the leading part of the output** – An ANSI escape sequence is created with `'\x1b[{0}m{1}\x1b[0m'` where `{0}` is the color code returned by `levelcolor(record.levelno)` and `{1}` is the first four characters of the log level name (e.g., “INFO”, “WARN”). The elapsed seconds since the module‑level `starttime` are inserted with `secs_since(starttime)`. Finally the user‑supplied message text `record.msg.get('msg', '')` is appended. The resulting template is:  

   ```
   <color><lvl4></color>[<elapsed>] <msg>
   ```  

3. **Pad or truncate to 80 characters** – The string is left‑justified to a width of 80 characters using `'{0:<80}'.format(out)`. If the constructed prefix is shorter than 80 characters it is padded with spaces; if it is longer, it is truncated to the first 80 characters.  
4. **Append additional key‑value pairs** – The function iterates over every entry in the original dictionary except the `'msg'` key. For each pair it appends a space followed by another ANSI‑colored segment:  

   ```
   <color><key></color>=<value>
   ```  

   The same `levelcolor` code is reused so that all parts share the log‑level colour.  
5. **Replace the original message** – The fully assembled string is assigned back to `record.msg`, replacing the original dictionary. This transformed record will be emitted by the logging handler in its final form.  
6. **Return control flag** – The function returns `True` unconditionally, signalling to the logging framework that the record should not be filtered out.  

**Reference relationships**:  
- **Caller** – `DictLogFilter.filter` invokes `tty_filter` when the filter’s `output` attribute is set to `'tty'` and the log record’s `msg` is a dictionary.  
- **Callees** – `tty_filter` relies on three helper functions defined in the same module:  
  * `stringify` – ensures all dictionary contents are strings.  
  * `levelcolor` – maps the numeric log level to an ANSI colour code.  
  * `secs_since` – produces a zero‑padded four‑character string representing seconds elapsed since the module‑level `starttime`.  
- **Global dependency** – The variable `starttime` (defined elsewhere in the module, typically at import time via `time.time()`) provides the reference point for the elapsed‑time field.  

**Note**:  
- The function assumes `record.msg` is a dictionary; if it is not, the surrounding `filter` method bypasses `tty_filter`.  
- All keys and values are forced to strings; non‑string objects are converted via `str()`, which may affect readability for complex types.  
- The ANSI escape sequences work only on terminals that understand SGR codes; output directed to non‑TTY destinations will contain raw escape characters.  
- Padding to exactly 80 characters may truncate long prefixes, but additional key‑value pairs are appended after the padding step, potentially extending the final line beyond 80 characters.  
- The colour code is determined solely by the log level; custom levels will fall back to the default colour defined in `levelcolor`.  

**Output Example**:  
Assuming `record.levelno` corresponds to `logging.INFO`, `record.levelname` is `"INFO"`, `starttime` was set 12 seconds ago, and `record.msg` is `{'msg': 'User created', 'user_id': 42, 'status': 'active'}`, the resulting `record.msg` after `tty_filter` would be a string similar to:

```
\x1b[34mINFO\x1b[0m[0012] User created                     \x1b[34muser_id\x1b[0m=42 \x1b[34mstatus\x1b[0m=active
```

When printed on a compatible terminal, “INFO” and the keys `user_id` and `status` appear in the colour associated with the INFO level (blue in the default mapping), the elapsed time `[0012]` is shown, and the message is padded to 80 characters before the additional key‑value pairs.
***
### FunctionDef text_filter(self, record)
**text_filter**: The function of text_filter is to convert a logging record whose message is a dictionary into a single‑line, space‑separated string formatted as `key="value"` pairs (logfmt style) and always indicate that the record should be processed further.  

**parameters**:  
· record: a `logging.LogRecord` instance whose `msg` attribute is expected to be a dictionary containing the log fields to be formatted.  

**Code Description**:  
1. The function first enriches the message dictionary with two standard fields:  
   * `time` – obtained by calling `strtime()`, which returns the current local time as an RFC 3339‑compliant string.  
   * `level` – the log level name converted to lower case (`record.levelname.lower()`).  
   These entries guarantee that every formatted log line contains a timestamp and a severity indicator.  

2. The updated dictionary (`record.msg`) is passed to `stringify(record.msg)`. `stringify` recursively walks the dictionary (and any nested mappings or sequences) and converts every key and value to a plain Python `str`. This step ensures that non‑string objects (e.g., integers, dates, custom objects) will not cause type errors when later embedded in the formatted string.  

3. After all keys and values are guaranteed to be strings, the function builds a list of `key="value"` fragments. It iterates over the items of the dictionary and formats each pair with the expression `'{0}="{1}"'.format(k, v)`. The value is quoted to preserve spaces or special characters, matching the logfmt convention.  

4. The list of fragments is joined with a single space character, producing a flat string such as `time="2023-08-15T14:30:45-04:00" level="info" user="alice" action="login"`. This string replaces the original `record.msg` attribute, so downstream handlers see a ready‑to‑output text message.  

5. Finally, the function returns `True`. In the logging filter contract, returning `True` signals that the record should continue through the logging pipeline; the filter never discards a record.  

**Reference Relationships**:  
* Called by `DictLogFilter.filter`. When the filter’s `output` attribute is set to `'text'` and the record’s `msg` is a dictionary, `filter` delegates to `text_filter`.  
* Calls `strtime` to generate a timestamp and `stringify` to ensure all dictionary contents are strings. Both helpers are also used by other filter methods (`json_filter`, `tty_filter`), providing consistent preprocessing across output formats.  

**Note**:  
* The function assumes `record.msg` is a mutable dictionary; if it is not, the filter bypasses formatting and returns `True` unchanged.  
* All values are quoted, even if they already contain quotes; this may lead to escaped characters in the final output but preserves logfmt compatibility.  
* The order of key/value pairs in the resulting string follows the iteration order of the dictionary (in Python 3.7+ this is insertion order). Changing the order of insertion before the filter runs will affect the output order.  

**Output Example**:  
Given a log record where  

```python
record.msg = {
    'user': 'alice',
    'action': 'login',
    'status': 200
}
record.levelname = 'INFO'
```  

and assuming `strtime()` returns `2023-08-15T14:30:45-04:00`, after `text_filter(record)` the `record.msg` will be  

```
time="2023-08-15T14:30:45-04:00" level="info" user="alice" action="login" status="200"
```  

The function returns `True`.
***
## ClassDef DictQueueHandler
**DictQueueHandler**: The function of DictQueueHandler is to act as a logging QueueHandler that prepares log records for pickling without converting dictionary messages into plain strings.

**attributes**  
· **formatter** – a class‑level `logging.Formatter` instance used to format exception information when needed.  
· (Inherited) **queue** – the multiprocessing queue passed to the constructor of the base `QueueHandler`.  
· (Inherited) **level**, **filters**, **lock**, etc. – standard `logging.Handler` attributes provided by the parent class.

**Code Description**  
`DictQueueHandler` subclasses `logging.handlers.QueueHandler`. The standard `QueueHandler` calls its `prepare()` method before putting a `LogRecord` onto the queue; the default implementation invokes `self.format(record)`, which turns any dictionary stored in `record.msg` into a string via the logger’s formatter. This behavior interferes with the project's custom `DictLogFilter`, which expects the original dictionary structure to remain intact.

To avoid that conversion, `DictQueueHandler` overrides `prepare()`:

1. **Message handling** – `record.msg` is passed through the helper function `stringify()`. If the message is a mapping (e.g., a dict), `stringify` returns a new mapping where each value is converted to `str`. If the message is not a mapping, it is simply converted to `str`. This keeps the overall structure (mapping vs. non‑mapping) while ensuring every element is picklable.

2. **Argument handling** – `record.args` undergoes the same `stringify()` process, guaranteeing that any positional or keyword arguments attached to the log record are also safely serializable.

3. **Exception handling** – When `record.exc_info` is present, the method uses the class‑level `formatter` to format the exception (`self.formatter.formatException(record.exc_info)`). The resulting string is stored in `record.exc_text`, and `record.exc_info` is cleared (set to `None`). This mirrors the standard logging behavior that stores exception text separately, but avoids keeping the non‑picklable traceback tuple in the record.

4. The modified `record` is returned, and the base `QueueHandler.emit()` method subsequently enqueues it.

Because `DictQueueHandler` does not call `self.format(record)`, the original dictionary payload remains a dictionary (with stringified values) rather than a single formatted string. This enables downstream components—particularly the `DictLogFilter` used elsewhere in the project—to inspect the original keys and values.

**Relationship with callers**  
- In `pedsnetdcc/db.py`, the method `Statement._get_logger` creates a logger for each `Statement` instance. When a multiprocessing queue (`logq`) is supplied, it instantiates `DictQueueHandler(logq)` and attaches it to a logger named after the statement’s identifier. This logger is then used by parallel processes to forward log records back to the main process without losing dictionary structure.  
- The test suite `pedsnetdcc/tests/db_test.py/LoggerThreadTest/test_log_passing` explicitly constructs a `DictQueueHandler`, attaches it to a thread‑local logger, and sends dictionary messages (`{'msg': 'foo'}` and `{'msg': 'bar'}`). The test confirms that after passing through the queue and being processed by the main logger, the messages are still valid JSON objects with the original keys (`msg`) and appropriate log levels (`info`, `error`). This demonstrates that `DictQueueHandler.prepare()` correctly preserves the dictionary format while making the record picklable.

**Note**  
- `DictQueueHandler` relies on an external `stringify` utility; it must be imported or defined in the same module for the class to work.  
- The handler only ensures that the *values* of a dictionary are converted to strings; the keys remain unchanged. If a value is itself a non‑picklable object (e.g., a file handle), `str(value)` may still produce an unhelpful representation, but the record will be picklable.  
- Because `prepare()` clears `exc_info`, any downstream handler that expects the original traceback tuple will not receive it; only the formatted exception text (`exc_text`) is retained.  
- The class attribute `formatter` is a plain `logging.Formatter` without a specific format string; if custom exception formatting is required, replace or configure this formatter before creating the handler.

**Output Example**  
After a call to `DictQueueHandler.prepare()` with a record whose original `msg` is `{'msg': 'foo', 'count': 3}` and `exc_info` contains an exception, the returned `LogRecord` will have:

```
record.msg   -> {'msg': 'foo', 'count': '3'}
record.args  -> (converted arguments, if any)
record.exc_text -> 'Traceback (most recent call last):\n  ... formatted exception ...'
record.exc_info -> None
```

When this record is later retrieved from the queue and emitted by the main logger, the logged JSON string might look like:

```
{"msg":"foo","count":"3","level":"info","time":"2026-02-05T12:34:56.789012"}
```
### FunctionDef prepare(self, record)
**prepare**: The function of prepare is to transform a logging.LogRecord into a form that can be safely pickled and transmitted between processes.  

**parameters**:  
· record: the log record that needs to be prepared; expected type is `logging.LogRecord`.  

**Code Description**:  
1. The method receives a `LogRecord` instance.  
2. It calls the project‑wide helper `stringify` on `record.msg`. This converts the message payload—whether it is a plain string, a mapping, a sequence, or any other object—into a structure that contains only plain strings, while preserving the original container type (mappings stay dicts, sequences become lists).  
3. It calls `stringify` on `record.args` for the same reason, ensuring that any arguments supplied to the logging call are also fully stringified.  
4. If the record carries exception information (`record.exc_info` is not `None`), the method uses the handler’s formatter (`self.formatter`) to format the exception tuple into a readable string via `formatException`. The resulting string is stored in `record.exc_text`. The original `exc_info` tuple is then cleared (`record.exc_info = None`) because exception tuples are not pickle‑friendly.  
5. Finally, the modified `LogRecord` object is returned to the caller.  

From a functional perspective, `prepare` is the only place in the `DictQueueHandler` class that invokes `stringify`. By doing so, it guarantees that the log record can be serialized (pickled) before being placed onto a multiprocessing queue. The conversion of `exc_info` to `exc_text` also removes a non‑serializable component while preserving the human‑readable traceback.  

**Note**:  
- The method does not alter other attributes of the `LogRecord`; only `msg`, `args`, `exc_info`, and `exc_text` are touched.  
- If `record.msg` or `record.args` are already plain strings, `stringify` returns them unchanged, so the overhead is minimal.  
- The handler must have a valid `formatter` attribute; otherwise, calling `self.formatter.formatException` would raise an `AttributeError`.  
- After preparation, the record can be safely pickled and sent to another process, where it can be re‑logged or stored.  

**Output Example**:  

```python
import logging
from pedsnetdcc.dict_logging import DictQueueHandler

handler = DictQueueHandler()
record = logging.LogRecord(
    name='myapp',
    level=logging.ERROR,
    pathname='app.py',
    lineno=10,
    msg={'action': 'save', 'status': False},
    args=[{'detail': 123}],
    exc_info=(ValueError, ValueError('bad value'), None)
)

prepared = handler.prepare(record)

# prepared.msg  -> {'action': 'save', 'status': 'False'}
# prepared.args -> [{'detail': '123'}]
# prepared.exc_text -> 'Traceback (most recent call last): ... ValueError: bad value'
# prepared.exc_info -> None
```
***
## FunctionDef stringify(obj)
**stringify**: The function of stringify is to recursively convert an object’s contents to plain strings while preserving the original container types (mappings and sequences).  

**parameters**:  
· obj: The input value to be processed. It may be a scalar, a mapping (e.g., dict), a sequence (e.g., list, tuple), a Unicode string, or any other Python object.  

**Code Description**:  
The function begins by checking the type of *obj*:

1. If *obj* is already a `str`, it is returned unchanged (`new_obj = obj`).  
2. If *obj* is a `unicode` instance (Python 2), it is converted to a native `str` with `str(obj)`.  
3. If *obj* is a mapping (`collections.Mapping`), a new dictionary is built where each key is forced to a string (`str(k)`) and each value is processed recursively by calling `stringify(v)`. This ensures that nested dictionaries are also fully stringified.  
4. If *obj* is a sequence (`collections.Sequence`), a new list is created containing the recursively stringified elements (`[stringify(i) for i in obj]`). Tuples and other sequence types are converted to lists, preserving order but not the original type.  
5. For any other type, the object is simply cast to a string with `str(obj)`.

The resulting object (`new_obj`) is then returned. The recursion guarantees that any depth of nested mappings or sequences will be fully traversed, and every leaf value will be a plain string.

**Relationship with Callers**:  
- **DictLogFilter.json_filter** uses `stringify` to guarantee that all keys and values in the log record dictionary are strings before calling `json.dumps`. This prevents `json.dumps` from raising type errors on non‑string values.  
- **DictLogFilter.tty_filter** calls `stringify` so that the terminal‑style formatter can safely embed values in a colored output string.  
- **DictLogFilter.text_filter** also relies on `stringify` to produce a log‑fmt compatible string where each value is a string that can be quoted safely.  
- **DictQueueHandler.prepare** invokes `stringify` on both `record.msg` and `record.args` to make the log record fully serializable for pickling and transmission across processes.

In all these contexts, `stringify` acts as a defensive preprocessing step, ensuring that logging structures contain only string data, which simplifies formatting, JSON encoding, and inter‑process communication.

**Note**:  
- The check for `unicode` is only valid in Python 2; in Python 3 the name `unicode` is undefined, which would raise a `NameError`. If the code runs under Python 3, the `unicode` branch is never executed and can be removed or guarded with a try/except.  
- Sequences are always converted to lists, so original tuple types are not preserved.  
- Mapping keys are forced to strings using `str(k)`. If a key is a complex object, its string representation may not be unique, potentially causing key collisions.  

**Output Example**:  
```python
input_obj = {
    u'count': 42,
    'items': [1, u'two', {'nested': 3}]
}
result = stringify(input_obj)
# result => {'count': '42', 'items': ['1', 'two', {'nested': '3'}]}
```
