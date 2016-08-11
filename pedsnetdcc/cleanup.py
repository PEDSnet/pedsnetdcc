"""Clean up PEDSnet data directories, backing up to Isilon and deleting csv
files"""
import filecmp
import glob
import logging
import os
import re
import shutil

from pedsnetdcc import SITE_ROOT, SITES, BACKUP_ROOT

logger = logging.getLogger(__name__)

# For each site, find the timestamped directories, and leave the most
# recent one alone.  For the others, recursively delete all .csv and
# .CSV files.  Then copy the whole darn timestamped directory
# (recursively) to the Isilon at the specified location.  If those
# copies work without error (should probably do a fresh comparison
# rather than relying on error codes), delete the timestamped directory.


def delete_csv_files(a_dir):
    """Recursively delete csv or CSV files in a directory"""
    for dir_name, _, file_list in os.walk(a_dir):
        for fname in file_list:
            if fname.lower().endswith('csv'):
                path = os.path.join(dir_name, fname)
                os.remove(path)
                logger.debug('Deleted %s', path)


def candidate_dirs(site_dir):
    """Return timestamp subdirectories excluding the most recent"""
    timestamp_dirs = glob.glob(os.path.join(site_dir, '*'))
    eligible_dirs = []
    for ts_dir in timestamp_dirs:
        if not re.match(r'.*/\d\d\d\d-\d\d-\d\dT\d\d:\d\d$', ts_dir):
            logger.warn(
                'Ignoring %s because it does not look like YYYY-MM-DDTHH:MM',
                ts_dir)
        else:
            eligible_dirs.append(ts_dir)
    older_dirs = sorted(eligible_dirs)[:-1]
    return older_dirs


def test_target(backup_dir, site):
    """Verify that the target directory is writable"""
    site_dir = os.path.join(backup_dir, site)
    if not os.path.isdir(site_dir):
        logger.error({'msg': 'Site directory does not exist',
                      'dir': site_dir})
        return False

    test_dir = os.path.join(site_dir, 'test_target')
    try:
        os.mkdir(test_dir)
    except OSError as err:
        logger.error({
            'msg': 'Cannot create backup subdirectories, or test_target '
                   'exists',
            'dir': test_dir,
            'err': str(err)})
        return False
    os.rmdir(test_dir)
    return True


def are_dir_trees_equal(dir1, dir2):
    """
    Compare two directories recursively. Files in each directory are
    assumed to be equal if their names and filesize are equal.

    @param dir1: First directory path
    @param dir2: Second directory path

    @return: True if the directory trees are the same and
        there were no errors while accessing the directories or files,
        False otherwise.
   """

    dirs_cmp = filecmp.dircmp(dir1, dir2)
    if len(dirs_cmp.left_only) > 0 or \
            len(dirs_cmp.right_only) > 0 or \
            len(dirs_cmp.funny_files) > 0:
        return False
    (_, mismatch, errors) = filecmp.cmpfiles(
        dir1, dir2, dirs_cmp.common_files, shallow=False)
    if len(mismatch) > 0 or len(errors) > 0:
        return False
    for common_dir in dirs_cmp.common_dirs:
        new_dir1 = os.path.join(dir1, common_dir)
        new_dir2 = os.path.join(dir2, common_dir)
        if not are_dir_trees_equal(new_dir1, new_dir2):
            return False
    return True


def verify_directory_backup(src_dir, dest_dir):
    """ TBD """
    if not are_dir_trees_equal(src_dir, dest_dir):
        raise Exception('Directories %s and %s do not match', src_dir,
                        dest_dir)
    return True


def on_remove_error(function, path, excinfo):
    logger.warn({
        'msg': 'Failed to remove file or directory. Please manually remove '
               'the entire timestamp directory; then retry.',
        'file_or_dir': path,
        'err': str(excinfo[1])})


def delete_directory(a_dir):
    """ Delete a directory tree recursively """
    shutil.rmtree(a_dir, onerror=on_remove_error)


def backup_directory(site, site_dir, backup_dir):
    """Backup timestamp subdirs in a site directory"""
    logger.info({'msg': 'Backing up {0}'.format(site), 'site': site,
                 'site_dir': site_dir, 'backup_dir': backup_dir})

    if not test_target(backup_dir, site):
        return False

    c_dirs = candidate_dirs(site_dir)
    if not c_dirs:
        logger.info(
            {'msg': 'Nothing to do for {0}'.format(site), 'site_dir': site_dir,
             'site': site})
    for c_dir in c_dirs:
        # We want to delete csv files regardless of whether the backup
        # succeeds.
        delete_csv_files(c_dir)

        # Now copy the whole timestamp directory (c_dir)
        base_name = os.path.basename(c_dir)
        dest_dir = os.path.join(backup_dir, site, base_name)
        try:
            shutil.copytree(c_dir, dest_dir, symlinks=True)
        except OSError as err:
            if 'File exists' in str(err):
                logger.error(
                    {'msg': "Won't overwrite existing destination files",
                     'err': str(err), 'site': site})
                return False
            else:
                raise
        logger.info(
            {'msg': 'Backed up {0}'.format(c_dir), 'dir': c_dir, 'site': site})

        # Now verify the copy
        verify_directory_backup(c_dir, dest_dir)
        logger.info(
            {'msg': 'Verified backup of {0}'.format(c_dir), 'src_dir': c_dir,
             'dest_dir': dest_dir, 'site': site})

        # Now delete the source directory
        delete_directory(c_dir)
        logger.info(
            {'msg': 'Deleted {0}'.format(c_dir), 'dir': c_dir, 'site': site})

    return True


def cleanup_site_directories(backup_root, site_root):
    """Entry point: backup and delete older site data directories"""
    if not backup_root:
        backup_root = BACKUP_ROOT

    if not site_root:
        site_root = SITE_ROOT

    if not os.path.isdir(site_root):
        logger.error({'msg': 'site root dir does not exist',
                      'dir': site_root})
        return False

    if not os.path.isdir(backup_root):
        logger.error({'msg': 'backup root dir does not exist',
                      'dir': backup_root})
        return False

    for site in SITES:
        site_dir = os.path.join(site_root, site)
        if not backup_directory(site, site_dir, backup_root):
            return False

    return True
