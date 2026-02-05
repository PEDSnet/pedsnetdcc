## FunctionDef delete_csv_files(a_dir)
**delete_csv_files**: The function of delete_csv_files is to recursively delete all files with a ".csv" or ".CSV" extension within a given directory tree.  

**parameters**:  
· a_dir: The root directory (as a string or path‑like object) from which the recursive search for CSV files begins.  

**Code Description**:  
The function starts by walking the directory tree rooted at *a_dir* using `os.walk`. For each directory (`dir_name`) encountered, it receives a list of filenames (`file_list`). It iterates over this list and checks each filename with `fname.lower().endswith('csv')`. The case‑insensitive check ensures that files ending with ".csv", ".CSV", or any mixed‑case variant are selected. When a match is found, the full path to the file is constructed with `os.path.join(dir_name, fname)`. The file is then removed from the filesystem using `os.remove(path)`. After successful deletion, a debug‑level log entry is emitted via `logger.debug('Deleted %s', path)`, recording the exact path of the removed file. The process continues until every directory and subdirectory under *a_dir* has been examined, guaranteeing that no CSV files remain anywhere in the tree.  

**Relationship with Callers**:  
`delete_csv_files` is invoked by the `backup_directory` function in the same module. Within `backup_directory`, each candidate timestamp subdirectory (`c_dir`) is processed: first, `delete_csv_files(c_dir)` removes any CSV files that may be present, regardless of whether the subsequent backup succeeds. This step prevents CSV files from being copied into the backup location and ensures that the source directory can be safely deleted after verification. By cleaning the source directory of CSV files before `shutil.copytree` and later `delete_directory`, the function contributes to a clean backup workflow and avoids unnecessary data duplication.  

**Note**:  
- The function assumes that `logger` has been configured elsewhere in the module; if not, a `NameError` will be raised.  
- Only files whose names end with the literal string "csv" (case‑insensitive) are removed; files with extensions like ".csv.gz" will not be affected.  
- No error handling is performed for `os.remove`; if a file cannot be deleted (e.g., due to permission issues), an exception will propagate to the caller.  
- The function does not return any value; its effect is purely side‑effectual (file system modification and logging).
## FunctionDef candidate_dirs(site_dir)
**candidate_dirs**: The function of candidate_dirs is to return all timestamp‑formatted subdirectories of a given site directory, excluding the most recent one.

**parameters**:  
· site_dir: Path (string) to the site’s root directory that contains timestamp subdirectories.

**Code Description**:  
1. The function builds a list of all entries directly under *site_dir* using `glob.glob(os.path.join(site_dir, '*'))`. This includes files and directories.  
2. It iterates over each entry (`ts_dir`). For each entry it applies a regular‑expression match `r'.*/\d\d\d\d-\d\d-\d\dT\d\d:\d\d$'`. The pattern enforces the exact format **YYYY‑MM‑DDTHH:MM** at the end of the path (e.g., `/data/site/2023-07-15T14:30`).  
3. If an entry does **not** match the pattern, a warning is emitted via `logger.warn` and the entry is ignored. If it matches, the entry is added to `eligible_dirs`.  
4. After the loop, `eligible_dirs` is sorted lexicographically (`sorted(eligible_dirs)`). Because the timestamp format sorts naturally in chronological order, the last element of the sorted list represents the most recent timestamp directory.  
5. The slice `[:-1]` removes that last (most recent) element, producing `older_dirs`, which contains all earlier timestamp directories.  
6. The function returns `older_dirs` as a list of absolute (or relative) paths.

**Relationship with callers**:  
`candidate_dirs` is invoked by `backup_directory` in *pedsnetdcc/cleanup.py*. `backup_directory` uses the returned list to determine which timestamp directories should be backed up, verified, and then deleted. By excluding the newest directory, the backup process never touches the most recent data, preserving it for ongoing operations while cleaning up older snapshots.

**Note**:  
- The function relies on the directory naming convention; any deviation (e.g., missing “T” or using seconds) will cause the entry to be ignored.  
- `logger.warn` is deprecated in newer logging APIs; consider replacing it with `logger.warning`.  
- If there are zero or one eligible timestamp directories, the function returns an empty list, causing the caller to skip backup actions.  
- The sorting is lexical; it works correctly only because the timestamp format is ISO‑8601 without timezone or seconds.

**Output Example**:  
```python
['/data/site/2023-01-01T08:00',
 '/data/site/2023-01-02T08:00',
 '/data/site/2023-01-03T08:00']
``` 
(Assuming `/data/site/2023-01-04T08:00` is the most recent directory and therefore omitted.)
## FunctionDef test_target(backup_dir, site)
**test_target**: The function of test_target is to verify that the target backup directory for a given site is writable.  

**parameters**:  
· backup_dir: Path to the root backup directory (string).  
· site: Name of the site whose subdirectory under backup_dir will be checked (string).  

**Code Description**:  
1. The function builds the absolute path to the site‑specific directory by joining `backup_dir` and `site`.  
2. It checks whether this `site_dir` exists and is a directory. If the directory does not exist, an error is logged with the missing path and the function returns `False`.  
3. A temporary subdirectory named `test_target` is constructed inside `site_dir`. The function attempts to create this subdirectory using `os.mkdir`.  
4. If the creation fails (e.g., due to insufficient permissions, the directory already existing, or other OS‑level errors), the exception is caught, an error is logged containing the target path and the exception message, and the function returns `False`.  
5. When the temporary directory is successfully created, it is immediately removed with `os.rmdir`. This confirms that the process has both write and delete permissions in the target location.  
6. After successful creation and removal, the function returns `True`, indicating that the target directory is writable and ready for further backup operations.  

**Relationship with Callers**:  
`test_target` is invoked at the beginning of the `backup_directory` function (found in the same module). `backup_directory` uses the result of `test_target` to decide whether to proceed with backing up timestamp subdirectories for a site. If `test_target` returns `False`, `backup_directory` aborts the backup process for that site, preventing attempts to write into an unwritable or non‑existent location. Thus, `test_target` acts as a safeguard that ensures the backup workflow only runs when the destination directory is valid and has the necessary permissions.  

**Note**:  
- The function assumes that `backup_dir` and `site` are provided as valid string paths; it does not perform type checking.  
- If a directory named `test_target` already exists under the site directory, the function will treat this as an error and return `False`.  
- Logging is performed via a module‑level `logger`; ensure the logger is configured appropriately to capture error messages.  
- The function only checks write and delete permissions; it does not verify other filesystem attributes such as available space.  

**Output Example**:  
- Successful check: `True`  
- Failure due to missing site directory or permission issue: `False`
## FunctionDef are_dir_trees_equal(dir1, dir2)
**are_dir_trees_equal**: The function of are_dir_trees_equal is to determine whether two directory trees are identical in structure and file content, using file name and size as the equality criteria.

**parameters**: The parameters of this Function.  
· dir1: Path to the first directory to be compared.  
· dir2: Path to the second directory to be compared.

**Code Description**:  
The function begins by creating a `filecmp.dircmp` object (`dirs_cmp`) that compares the top‑level contents of `dir1` and `dir2`. It immediately checks three lists provided by `dircmp`:  

1. `left_only` – items present only in `dir1`.  
2. `right_only` – items present only in `dir2`.  
3. `funny_files` – items that could not be compared (e.g., permission errors).  

If any of these lists contain entries, the directories differ, and the function returns `False`.  

Next, the function calls `filecmp.cmpfiles` with `shallow=False` to perform a deep comparison of all files that appear in both directories (`dirs_cmp.common_files`). The call returns three sequences: a list of matching files (ignored), a list of mismatched files (`mismatch`), and a list of files that caused errors during comparison (`errors`). If either `mismatch` or `errors` is non‑empty, the directories are not identical, and the function returns `False`.  

If the top‑level checks pass, the function iterates over each subdirectory listed in `dirs_cmp.common_dirs`. For each common subdirectory, it builds the absolute paths (`new_dir1` and `new_dir2`) and recursively invokes `are_dir_trees_equal`. If any recursive call returns `False`, the overall result is `False`.  

Only when all top‑level and recursive checks succeed does the function return `True`, indicating that the two directory trees are structurally identical and contain files with matching names and sizes, without any access or comparison errors.

**Relationship with Callers**:  
`are_dir_trees_equal` is used by the `verify_directory_backup` function in `pedsnetdcc/cleanup.py`. `verify_directory_backup` calls this function to confirm that a backup directory (`dest_dir`) is an exact replica of the source directory (`src_dir`). If `are_dir_trees_equal` returns `False`, `verify_directory_backup` raises an exception, halting the backup verification process. Thus, `are_dir_trees_equal` serves as the core validation routine for directory backup integrity within the project.

**Note**:  
- The function relies on the standard library modules `os` and `filecmp`; ensure they are imported before use.  
- `shallow=False` forces a full content comparison; using the default `shallow=True` would compare only metadata (size and modification time).  
- Symbolic links are treated as regular files; if a link cannot be read, it will appear in `funny_files` and cause the function to return `False`.  
- Large directory trees may incur significant I/O overhead due to the recursive nature of the comparison.  
- The function does not raise exceptions for mismatches; it simply returns `False`. Callers must handle the boolean result accordingly.

**Output Example**:  
```python
result = are_dir_trees_equal('/data/source', '/data/backup')
# result will be either True (identical) or False (different)
```
## FunctionDef verify_directory_backup(src_dir, dest_dir)
**verify_directory_backup**: The function of verify_directory_backup is to confirm that a backup directory is an exact replica of the source directory and raise an exception if any mismatch is found.  

**parameters**: The parameters of this Function.  
· src_dir: Path to the source directory that was copied.  
· dest_dir: Path to the destination (backup) directory that should match the source.  

**Code Description**:  
The function performs a single logical step: it calls the helper function **are_dir_trees_equal(src_dir, dest_dir)**, which recursively compares the two directory trees for identical structure and file contents (using file name and size).  

- If **are_dir_trees_equal** returns **False**, the condition `if not are_dir_trees_equal(src_dir, dest_dir):` evaluates to true and the function raises a generic **Exception** with the message `'Directories %s and %s do not match'` and the two directory paths as arguments. This stops further processing and signals that the backup verification failed.  
- If the comparison returns **True**, the function reaches the `return True` statement, indicating that the backup is verified as identical.  

**Reference Relationships**:  
- **Caller**: The function is invoked by **backup_directory** (in the same module). After copying a timestamp subdirectory to the backup location, `backup_directory` calls `verify_directory_backup(c_dir, dest_dir)` to ensure the copy succeeded before deleting the original directory.  
- **Callee**: The only internal call is to **are_dir_trees_equal**, which implements the deep recursive comparison logic. The reliability of `verify_directory_backup` depends entirely on the correctness of this helper.  

**Note**:  
- The function does not perform any logging itself; logging is handled by the caller (`backup_directory`).  
- The raised exception is a generic `Exception`; callers should be prepared to catch it if they need graceful error handling.  
- Because the function returns a boolean only when the directories match, the return value is effectively redundant when the function completes without raising; the primary outcome is the side‑effect of raising on mismatch.  
- Ensure that both `src_dir` and `dest_dir` exist and are accessible before calling this function; otherwise, `are_dir_trees_equal` may return `False` and trigger the exception.  

**Output Example**:  
```python
# Successful verification
result = verify_directory_backup('/data/site/2023-01-01', '/backup/site/2023-01-01')
# result is True

# If the directories differ, an exception is raised:
# Exception: Directories /data/site/2023-01-01 and /backup/site/2023-01-01 do not match
```
## FunctionDef on_remove_error(function, path, excinfo)
**on_remove_error**: The function of on_remove_error is to log a warning when `shutil.rmtree` fails to remove a file or directory during a recursive delete operation.  

**parameters**:  
· function: The callback function that `shutil.rmtree` attempted to invoke; not used inside the body but required by the `onerror` signature.  
· path: The path of the file or directory that could not be removed.  
· excinfo: A tuple containing exception information `(exc_type, exc_value, traceback)`; the function extracts `excinfo[1]` to obtain the exception instance and its string representation.  

**Code Description**:  
The function is defined as an error‑handling callback for `shutil.rmtree`. When `shutil.rmtree` encounters an error while deleting a file or directory, it calls the supplied `onerror` function with the three arguments described above. `on_remove_error` ignores the `function` argument, extracts the problematic `path`, and formats a warning message. It uses the project‑wide `logger` to emit a warning dictionary containing:  
1. A human‑readable message instructing the user to manually delete the entire timestamp directory and retry the operation.  
2. The `file_or_dir` key holding the path that caused the failure.  
3. The `err` key holding the string representation of the exception that was raised.  

In the project, `on_remove_error` is passed to `shutil.rmtree` via the `delete_directory` function:

```python
def delete_directory(a_dir):
    """ Delete a directory tree recursively """
    shutil.rmtree(a_dir, onerror=on_remove_error)
```

Thus, whenever `delete_directory` is used to remove a directory tree, any removal error triggers `on_remove_error`, ensuring that the failure is logged with sufficient context for manual remediation.

**Note**:  
- The function only logs the issue; it does not attempt any recovery or retry. Users must manually remove the offending file or directory as suggested.  
- The use of `logger.warn` follows the project's logging conventions, but be aware that in newer logging libraries `logger.warning` is the preferred method.  
- The `function` argument is required by the `shutil.rmtree` callback signature but is not utilized; altering the signature may break compatibility with `shutil.rmtree`.
## FunctionDef delete_directory(a_dir)
**delete_directory**: The function of delete_directory is to remove an entire directory tree recursively.  

**parameters**:  
· a_dir: The absolute or relative path of the directory that should be deleted, including all of its sub‑directories and files.  

**Code Description**:  
The implementation consists of a single call to the standard library function `shutil.rmtree`.  
```python
shutil.rmtree(a_dir, onerror=on_remove_error)
```  
`shutil.rmtree` walks the directory tree rooted at `a_dir` and attempts to delete every file and sub‑directory it encounters. If any removal operation fails (for example, due to permission errors, read‑only files, or files being in use), `shutil.rmtree` invokes the callback supplied via the `onerror` argument. In this module the callback is the function `on_remove_error`, which logs a warning containing the problematic path and the exception message but does not attempt any recovery. Consequently, `delete_directory` either succeeds silently (the directory is gone) or logs a warning and leaves the offending file or sub‑directory in place.

From a functional perspective, `delete_directory` is used by higher‑level cleanup utilities. The primary caller is `backup_directory` in the same file. After a timestamped directory has been successfully copied to a backup location and verified, `backup_directory` calls `delete_directory(c_dir)` to remove the original source directory. The log entry generated by `backup_directory` after the call records the deletion event, while any error during the actual removal is captured by `on_remove_error`. No other functions in the project directly invoke `delete_directory`.  

**Note**:  
- The function does not perform any existence check; if `a_dir` does not exist, `shutil.rmtree` raises `FileNotFoundError`.  
- Errors are not re‑raised; they are only logged by `on_remove_error`. Users must manually delete the remaining files or directories if a warning is emitted.  
- The logging call inside `on_remove_error` uses `logger.warn`, which is deprecated in newer logging frameworks in favor of `logger.warning`.  
- Because `shutil.rmtree` follows symbolic links by default, any symlink inside `a_dir` will be removed, not the target it points to. This behavior is consistent with the intended cleanup semantics of the module.  
## FunctionDef backup_directory(site, site_dir, backup_dir)
**backup_directory**: The function of backup_directory is to back up eligible timestamp sub‑directories of a site, verify the copy, and then delete the original directories.

**parameters**:  
· **site**: Name of the site (string) whose directories are being processed.  
· **site_dir**: Path to the root directory for the given site that contains timestamp sub‑directories (string or path‑like).  
· **backup_dir**: Path to the root backup directory where site backups will be stored (string or path‑like).

**Code Description**:  
1. Logs an informational message indicating that a backup operation is starting for the supplied *site*, including the source (*site_dir*) and destination (*backup_dir*) paths.  
2. Calls **test_target(backup_dir, site)** to verify that the target backup location for the site exists, is a directory, and is writable. If the check fails, the function returns **False** immediately, aborting the backup for this site.  
3. Retrieves a list of candidate timestamp directories by invoking **candidate_dirs(site_dir)**. This function returns all timestamp‑formatted sub‑directories except the most recent one.  
4. If no candidate directories are found, an informational log records that there is “Nothing to do” for the site; the function then proceeds to the loop (which will be skipped).  
5. For each candidate directory **c_dir** in the list:  
   a. Calls **delete_csv_files(c_dir)** to recursively remove any “.csv” or “.CSV” files. This step is performed regardless of later success to avoid copying CSV files into the backup.  
   b. Constructs the destination path **dest_dir** as `os.path.join(backup_dir, site, base_name)`, where **base_name** is the leaf directory name of **c_dir**.  
   c. Attempts to copy the entire source directory tree to **dest_dir** using `shutil.copytree(c_dir, dest_dir, symlinks=True)`.  
      - If an `OSError` occurs because the destination already exists, an error is logged and the function returns **False** (no overwrite is performed).  
      - Any other `OSError` is re‑raised, allowing the caller to handle unexpected copy failures.  
   d. Logs a message confirming that the copy of **c_dir** to **dest_dir** succeeded.  
   e. Calls **verify_directory_backup(c_dir, dest_dir)** to ensure the backup is an exact replica of the source. This function raises an exception on mismatch; if it returns, the backup is considered verified.  
   f. Logs a verification success message.  
   g. Calls **delete_directory(c_dir)** to recursively remove the original source directory now that the backup is verified.  
   h. Logs a deletion confirmation message.  
6. After processing all candidate directories, the function returns **True** to indicate that the backup (and cleanup) completed without encountering the early‑return error conditions.

**Reference Relationships**:  
- **Caller**: `cleanup_site_directories` (in the same module) iterates over all configured sites and invokes `backup_directory(site, site_dir, backup_root)` for each. The return value of `backup_directory` determines whether `cleanup_site_directories` continues or aborts.  
- **Callees**:  
  * `test_target` – validates write access to the backup location.  
  * `candidate_dirs` – selects which timestamp directories should be processed.  
  * `delete_csv_files` – removes CSV files from each candidate before copying.  
  * `shutil.copytree` – performs the actual filesystem copy, preserving symlinks.  
  * `verify_directory_backup` – confirms that the copy is identical to the source.  
  * `delete_directory` – removes the original source directory after successful verification.  

**Note**:  
- The function relies on a module‑level `logger` being configured; all log calls use structured dictionaries for consistency.  
- If the backup destination already contains a directory with the same name, the function will not overwrite it and will return **False**.  
- CSV files are deleted before the copy; any failure to delete a CSV file will raise an exception and abort the current iteration.  
- Exceptions raised by `verify_directory_backup` (or any unexpected `OSError` from `shutil.copytree`) are not caught inside `backup_directory`; they propagate to the caller.  
- The function does not create the site‑specific subdirectory under `backup_dir`; it assumes `test_target` has already verified its existence.  

**Output Example**:  
```python
result = backup_directory('example_site',
                          '/data/example_site',
                          '/backup')
# result is True if all eligible directories were backed up, verified, and deleted;
# result is False if the target directory is not writable or a destination already exists.
```
## FunctionDef cleanup_site_directories(backup_root, site_root)
**cleanup_site_directories**: The function of cleanup_site_directories is to back up each configured site’s older data directories and delete the originals after successful backup.

**parameters**:  
· **backup_root**: Path to the root directory where backups should be stored. If falsy, the module‑level constant `BACKUP_ROOT` is used.  
· **site_root**: Path to the root directory that contains the site sub‑directories. If falsy, the module‑level constant `SITE_ROOT` is used.

**Code Description**:  
1. The function first resolves the effective backup and site roots. If the caller supplies `None` or an empty value, the defaults `BACKUP_ROOT` and `SITE_ROOT` are substituted.  
2. It verifies that `site_root` exists and is a directory using `os.path.isdir`. If the check fails, an error is logged with a structured message containing the missing directory path, and the function returns `False`.  
3. It verifies that `backup_root` exists and is a directory in the same way; on failure it logs an error and returns `False`.  
4. The function iterates over the global list `SITES`, which contains the names of all sites that should be processed. For each `site` it constructs the absolute path to that site’s data directory: `site_dir = os.path.join(site_root, site)`.  
5. For each site, it calls `backup_directory(site, site_dir, backup_root)`. This helper performs the actual backup of eligible timestamp sub‑directories, verifies the copy, and deletes the source directories.  
6. If any call to `backup_directory` returns `False` (indicating a failure such as an unwritable backup target or an existing destination directory), `cleanup_site_directories` aborts immediately and returns `False`.  
7. If all sites are processed without error, the function returns `True` to indicate overall success.

**Reference Relationships**:  
- **Caller**: `cleanup` in `pedsnetdcc/main.py` invokes `cleanup_site_directories` and exits the program with status 1 on failure or 0 on success.  
- **Callees**:  
  * `backup_directory` (defined in the same module) – performs per‑site backup, verification, and deletion.  
  * `os.path.isdir` – used for existence checks of the supplied roots.  
  * `logger` – used for structured error logging when a root directory is missing.  

**Note**:  
- The function does not create missing root directories; they must exist beforehand.  
- It relies on the module‑level constants `BACKUP_ROOT`, `SITE_ROOT`, and the list `SITES` being defined elsewhere in the module.  
- All logging is performed with a structured dictionary payload, which assumes a pre‑configured `logger` instance.  
- The function returns a simple Boolean; it does not raise exceptions for missing roots or backup failures, delegating error handling to the caller.  

**Output Example**:  
```python
result = cleanup_site_directories('/backup', '/data/sites')
# result is True if every site was backed up and cleaned successfully,
# False if a root directory is missing or any site backup fails.
```
