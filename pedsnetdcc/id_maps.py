import logging
import re
import time
import os

from pedsnetdcc import SITES, ID_MAP_TABLES, CONSISTENT_ID_MAP_TABLES
from pedsnetdcc.db import (Statement, StatementList)
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_err

from sh import pg_dump, pg_restore

logger = logging.getLogger(__name__)

_dcc_ids_table_sql = """dcc_ids.dcc_{0}_id"""

_id_map_table_sql = """{0}_id_maps.{1}_ids"""

_create_id_map_table_sql = """CREATE TABLE IF NOT EXISTS """ + _id_map_table_sql + """(dcc_id INTEGER NOT NULL, site_id INTEGER NOT NULL)"""

_create_dcc_id_table_sql = """CREATE TABLE IF NOT EXISTS """ + _dcc_ids_table_sql + """(last_id INTEGER NOT NULL)"""
_initialize_dcc_id_table_sql = """INSERT INTO """ + _dcc_ids_table_sql + """(last_id) values(1)"""


_temp_dump_file_templ = "{0}_dump"
def _temp_dump_file(site):
    return _temp_dump_file_templ.format(site)

def _base_dump_args(conn_str, dump_path):
    return ('--dbname='+conn_str,
            '-Fc',
            '-Z',
            '9',
            '--data-only'
            '-f',
            dump_path)

def _dump_args(site, conn_str, dump_path):
    dump_args = _base_dump_args(conn_str, dump_path)

    for table in CONSISTENT_ID_MAP_TABLES:
        dump_args += ('-t', _id_map_table_sql.format(site, table))

    return dump_args + ('-f', dump_path)

def _dcc_dump_args(conn_str, dump_path):
    dump_args = _base_dump_args(conn_str, dump_path)

    for table in CONSISTENT_ID_MAP_TABLES:
        dump_args += ('-t', _dcc_ids_table_sql.format(table))

    return dump_args


def _restore_args(conn_str, dump_path):
    return ('--dbname=' + conn_str,
            '-Fc',
            '-j',
            '8',
            dump_path)

def create_dcc_ids_tables(conn_str):
    """Create tables (one per PEDSnet tables) for holding the last generated id for the dcc

    :param conn_str: connection string for target database
    :type: str
    """

    logger.info({'msg': 'starting dcc_ids table creation'})
    starttime = time.time()

    statements = StatementList()
    for table in ID_MAP_TABLES:
        statements.extend(
            [Statement(_create_dcc_id_table_sql.format(table))]
        )

        if table not in CONSISTENT_ID_MAP_TABLES:
            statements.extend(
                [Statement(_initialize_dcc_id_table_sql.format(table))]
            )

    statements.serial_execute(conn_str)

    for statement in statements:
        check_stmt_err(statement, 'dcc_ids table creation')

    logger.info({
        'msg', 'finished creation of dcc_ids tables',
        'elapsed', secs_since(starttime)
    })



def create_id_map_tables(conn_str):
    """Create a table (per site) for holding the id mappings between sites and the dcc

     :param conn_str: connection string for target database
     :type: str
     """

    logger.info({'msg': 'starting id_map table creation'})
    starttime = time.time()

    statements = StatementList()
    for site in SITES:
        for table in ID_MAP_TABLES:
            statements.extend(
                [Statement(_create_id_map_table_sql.format(site, table))]
            )

    statements.serial_execute(conn_str)

    for statement in statements:
        check_stmt_err(statement, 'id map table creation')

    logger.info({
        'msg', 'finished creation of id_maps tables',
        'elapsed', secs_since(starttime)
    })


def _dump_and_restore_dcc_ids(old_conn_str, new_conn_str, starttime):
    logger.info({
        'msg': 'dumping dcc_id tables from old database',
        'elapsed': secs_since(starttime)
    })

    dump_file_path = _temp_dump_file("dcc")
    pg_dump(_dcc_dump_args(old_conn_str, dump_file_path))

    pg_restore(_restore_args(new_conn_str, dump_file_path))

    os.remove(dump_file_path)
    logger.info({
        'msg': 'finished restoring dcc_id tables into new database',
        'elapsed': secs_since(starttime)
    })

def _dump_and_restore_id_maps(site, old_conn_str, new_conn_str, starttime):
    logger.info({
        'msg': 'dumping id_map tables from old database for ' + site + ' site.',
        'elapsed': secs_since(starttime)
    })

    dump_file_path = _temp_dump_file(site)
    pg_dump(_dump_args(site, old_conn_str, dump_file_path))

    logger.info({
        'msg': 'inserting id_map dumps into new database for ' + site + ' site.',
        'elapsed': secs_since(starttime)
    })

    pg_restore(_restore_args(new_conn_str, dump_file_path))

    os.remove(dump_file_path)
    logger.info({
        'msg': 'finished restoring id_map dumps into new database for ' + site + ' site.',
        'elapsed': secs_since(starttime)
    })

def copy_id_maps(old_conn_str, new_conn_str):
    """Using pg_dump, copy id_maps and dcc_ids tables from old database to new database

    :param old_conn_str: connection string for old target database
    :type: str
    :param new_conn_str: connection string for new target database
    :type: str
    """

    logger.info({'msg': 'starting id map copying'})
    starttime = time.time()

    _dump_and_restore_dcc_ids(old_conn_str, new_conn_str, starttime)
    for site in SITES:
        _dump_and_restore_id_maps(site, old_conn_str, new_conn_str, starttime)

    logger.info({
        'msg', 'finished copying of id map table data',
        'elapsed', secs_since(starttime)
    })
