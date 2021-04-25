import logging
import re
import time
import os

from pedsnetdcc import SITES_AND_EXTERNAL, ID_MAP_TABLES, CONSISTENT_ID_MAP_TABLES
from pedsnetdcc.db import (Statement, StatementList)
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_err

from sh import pg_dump, pg_restore

logger = logging.getLogger(__name__)

DCC_IDS_TABLE_SQL = """{1}_ids.{1}_{0}_id"""

ID_MAP_TABLE_SQL = """{0}{2}_id_maps.{1}_ids"""

CREATE_ID_MAP_TABLE_SQL = """CREATE TABLE IF NOT EXISTS {0}.{1}_ids({2}_id {3} NOT NULL, site_id {3} NOT NULL)"""

CREATE_DCC_ID_TABLE_SQL = """CREATE TABLE IF NOT EXISTS {0}.{1}(last_id {2} NOT NULL)"""
INITIALIZE_DCC_ID_TABLE_SQL = """INSERT INTO {0}.{1}(last_id) values(1)"""


_temp_dump_file_templ = "{0}_dump"
def _temp_dump_file(site):
    return _temp_dump_file_templ.format(site)

def _base_dump_args(conn_str, dump_path):
    return ('--dbname='+conn_str,
            '-Fc',
            '-Z',
            '9',
            '--data-only',
            '-f',
            dump_path)

def _dump_args(site, conn_str, dump_path, id_name):
    dump_args = _base_dump_args(conn_str, dump_path)

    if id_name != 'dcc':
        id_name = '_' + id_name
    else:
        id_name = ''

    for table in CONSISTENT_ID_MAP_TABLES:
        dump_args += ('-t', ID_MAP_TABLE_SQL.format(site, table, id_name))

    return dump_args + ('-f', dump_path)

def _dcc_dump_args(conn_str, dump_path, id_name):
    dump_args = _base_dump_args(conn_str, dump_path)

    for table in CONSISTENT_ID_MAP_TABLES:
        dump_args += ('-t', DCC_IDS_TABLE_SQL.format(table, id_name))

    return dump_args


def _restore_args(conn_str, dump_path):
    return ('--dbname=' + conn_str,
            '-Fc',
            '-j',
            '8',
            dump_path)


def _dump_and_restore_dcc_ids(old_conn_str, new_conn_str, starttime, id_name):
    logger.info({
        'msg': 'dumping id tables from old database',
        'elapsed': secs_since(starttime)
    })

    dump_file_path = _temp_dump_file(id_name)
    pg_dump(_dcc_dump_args(old_conn_str, dump_file_path, id_name))

    pg_restore(_restore_args(new_conn_str, dump_file_path))

    os.remove(dump_file_path)
    logger.info({
        'msg': 'finished restoring id tables into new database',
        'elapsed': secs_since(starttime)
    })

def _dump_and_restore_id_maps(site, old_conn_str, new_conn_str, starttime, id_name):
    logger.info({
        'msg': 'dumping id_map tables from old database for ' + site + ' site.',
        'elapsed': secs_since(starttime)
    })

    dump_file_path = _temp_dump_file(site)
    pg_dump(_dump_args(site, old_conn_str, dump_file_path, id_name))

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


def create_dcc_ids_tables(conn_str, id_name, id_type):
    """Create tables (one per PEDSnet tables) for holding the last generated id for the dcc

    :param str conn_str: connection string for target database
    :param str id_name: name of the id ex. dcc or onco
    :param str id_type: type of the id INTEGER or BIGINT
    """

    logger.info({'msg': 'starting ids table creation'})
    starttime = time.time()

    schema = id_name + '_ids'

    statements = StatementList()
    for table in ID_MAP_TABLES:
        table_name = id_name + '_' + table + '_id'
        statements.extend(
            [Statement(CREATE_DCC_ID_TABLE_SQL.format(schema, table_name, id_type))]
        )

        if table not in CONSISTENT_ID_MAP_TABLES:
            statements.extend(
                [Statement(INITIALIZE_DCC_ID_TABLE_SQL.format(schema, table_name))]
            )

    statements.serial_execute(conn_str)

    for statement in statements:
        check_stmt_err(statement, 'type_ids table creation')

    logger.info({
        'msg', 'finished creation of type_ids tables',
        'elapsed', secs_since(starttime)
    })


def create_id_map_tables(conn_str, skipsites, addsites, id_name, id_type):
    """Create a table (per site) for holding the id mappings between sites and the dcc

     :param str conn_str: connection string for target database
     :param str skipsites:      sites to skip
     :param str addsites:   `   sites to add
     :param str id_name:        name of the id ex. dcc or onco
     :param id_type:            type of id INTEGER or BIGINT
     """

    logger.info({'msg': 'starting id_map table creation'})
    starttime = time.time()

    # Get Sites to skip
    skip_sites = skipsites.split(",")

    id_sites = list(set(SITES_AND_EXTERNAL) - set(skip_sites))

    # Get Sites to add
    add_sites = addsites.split(",")

    id_sites = list(set(id_sites) | set(add_sites))

    id_sites = list(filter(None, id_sites))

    statements = StatementList()
    for site in id_sites:
        schema = site
        if id_name != 'dcc':
            schema = schema + '_' + id_name
        schema = schema + '_id_maps'
        for table in ID_MAP_TABLES:
            statements.extend(
                [Statement(CREATE_ID_MAP_TABLE_SQL.format(schema, table, id_name, id_type))])

    statements.serial_execute(conn_str)

    for statement in statements:
        check_stmt_err(statement, 'id map table creation')

    logger.info({
        'msg', 'finished creation of id_maps tables',
        'elapsed', secs_since(starttime)
    })

def copy_id_maps(old_conn_str, new_conn_str, id_name, skipsites, addsites,):
    """Using pg_dump, copy id_maps and dcc_ids tables from old database to new database

    :param old_conn_str: connection string for old target database
    :type: str
    :param new_conn_str: connection string for new target database
    :type: str
    :param id_name: name of id set ex: dcc or onco
    :param str skipsites:      sites to skip
    :param str addsites:   `   sites to add
    :type: str
    """

    logger.info({'msg': 'starting id map copying'})
    starttime = time.time()

    # Get Sites to skip
    skip_sites = skipsites.split(",")

    id_sites = list(set(SITES_AND_EXTERNAL) - set(skip_sites))

    # Get Sites to add
    add_sites = addsites.split(",")

    id_sites = list(set(id_sites) | set(add_sites))

    id_sites = list(filter(None, id_sites))

    _dump_and_restore_dcc_ids(old_conn_str, new_conn_str, starttime, id_name)
    for site in id_sites:
        _dump_and_restore_id_maps(site, old_conn_str, new_conn_str, starttime, id_name)

    logger.info({
        'msg', 'finished copying of id map table data',
        'elapsed', secs_since(starttime)
    })
