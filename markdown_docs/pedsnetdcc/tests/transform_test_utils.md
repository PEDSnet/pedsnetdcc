## FunctionDef clean(s)
**clean**: The function `clean` normalizes a string by trimming surrounding whitespace, collapsing consecutive spaces, removing literal newline characters, and expanding the placeholder `{NL}` into an actual newline.

**parameters**:  
· **s**: `str` – the input string to be cleaned.

**Code Description**:  
1. `s = s.strip()` – removes any leading and trailing whitespace characters (spaces, tabs, newlines) from the input.  
2. `s = re.sub(' +', ' ', s)` – uses a regular expression to replace every occurrence of one or more consecutive space characters with a single space, thereby compressing multiple spaces into one.  
3. `s = s.replace('\n', '')` – deletes all newline (`\n`) characters that may remain in the string after the previous steps.  
4. `s = s.replace('\r', '')` – deletes any carriage‑return (`\r`) characters.  
5. `s = s.replace('{NL}', '\n')` – substitutes the literal token `{NL}` with a real newline character, allowing test code to embed readable multiline strings without actual line breaks.  
6. `return s` – returns the cleaned string.

**Relationship with Callers**:  
`clean` is employed throughout the test suite (e.g., in `age_transform_test.py`, `concept_name_transform_test.py`, and `site_name_transform_test.py`). In each test, a multiline SQL statement is written as a triple‑quoted string containing the placeholder `{NL}` to indicate where a newline should appear. Before comparing the generated SQL with the expected SQL, the test calls `clean` on the expected string. This ensures that differences in indentation, extra spaces, or stray newline characters do not cause false test failures. By normalizing both the expected and actual SQL strings, the tests focus on structural correctness rather than formatting.

**Note**:  
- The function relies on the `re` module; ensure `import re` is present in the module where `clean` is defined.  
- Only space characters (`' '`) are collapsed; other whitespace such as tabs (`'\t'`) are left untouched.  
- The placeholder replacement occurs after all newline characters have been stripped, so any `{NL}` token will become the sole source of line breaks in the returned string.  
- The function does not modify the case of the text or perform any other transformations.

**Output Example**:  

```python
input_str = """
    SELECT  column1,   column2
    FROM    my_table
    WHERE   column1 = 'value' {NL}ORDER BY column2
"""
result = clean(input_str)
# result ==
# "SELECT column1, column2 FROM my_table WHERE column1 = 'value'\nORDER BY column2"
```
