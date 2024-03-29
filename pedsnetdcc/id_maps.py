import logging
import re
import time
import os

from pedsnetdcc import SITES_AND_EXTERNAL, ID_MAP_TABLES, CONSISTENT_ID_MAP_TABLES
from pedsnetdcc.db import (Statement, StatementList)
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_err
from pedsnetdcc.schema import (primary_schema)

from sh import pg_dump, pg_restore

logger = logging.getLogger(__name__)

DCC_IDS_TABLE_SQL = """{1}_ids.{1}_{0}_id"""

ID_MAP_TABLE_SQL = """{0}{2}_id_maps.{1}_ids"""

CREATE_ID_MAP_TABLE_SQL = """CREATE TABLE IF NOT EXISTS {0}.{1}_ids({2}_id {3} NOT NULL, site_id {4} NOT NULL)"""

CREATE_DCC_ID_TABLE_SQL = """CREATE TABLE IF NOT EXISTS {0}.{1}(last_id {2} NOT NULL)"""
INITIALIZE_DCC_ID_TABLE_SQL = """INSERT INTO {0}.{1}(last_id) values(1)"""


def _populate_last_id(conn_str, schema, id_name):
    populate_last_id_sql = """
        create or replace function populate_last_id(schemanm text, mapmn text) returns void as $$
        declare
            tbl_array text[];
            count_tbl integer;
            sqlstr text;
            sel_stat text;
        begin
            select array(
                            SELECT tablename as table
                            FROM pg_tables pgt1
                            WHERE schemaname = 'dcc_pedsnet' AND
                            pgt1.tablename NOT IN ('dose_era', 'hash_token') AND
                            (SELECT EXISTS (
                                SELECT FROM pg_tables pgt2
                                WHERE schemaname = schemanm
                                AND tablename  = mapmn||'_'||pgt1.tablename||'_id')
                            )
                         ) into tbl_array;
            count_tbl = array_length(tbl_array, 1);
            <<table_loop>>
            for i in 1.. count_tbl  loop
                if tbl_array[i] = 'death' then
                    sqlstr = 'UPDATE '||schemanm||'.'||mapmn||'_'||tbl_array[i]||'_id SET last_id=(SELECT (MAX(death_cause_id)+1) FROM dcc_pedsnet.'||tbl_array[i]||')';
                elsif tbl_array[i] = 'measurement_organism' then
                    sqlstr = 'UPDATE '||schemanm||'.'||mapmn||'_'||tbl_array[i]||'_id SET last_id=(SELECT (MAX(meas_organism_id)+1) FROM dcc_pedsnet.'||tbl_array[i]||')';
                elsif tbl_array[i] IN ('care_site','person','provider','visit_occurrence') then
                    sqlstr = 'INSERT INTO '||schemanm||'.'||mapmn||'_'||tbl_array[i]||'_id(last_id) VALUES (0)';
                else
                    sqlstr = 'UPDATE '||schemanm||'.'||mapmn||'_'||tbl_array[i]||'_id SET last_id=(SELECT (MAX('||tbl_array[i]||'_id)+1) FROM dcc_pedsnet.'||tbl_array[i]||')';
                end if;
                execute sqlstr;
                sel_stat := null;
            end loop table_loop;
        end;
    $$ LANGUAGE plpgsql;
    
    select count(*) from populate_last_id('{0}', '{1}');
    
    """

    populate_last_id_msg = "populating last_ids"

    # Populate last_id
    populate_last_id_stmt = Statement(populate_last_id_sql.format(schema, id_name), populate_last_id_msg)

    # Execute the add concept names statement and ensure it didn't error
    populate_last_id_stmt.execute(conn_str)
    check_stmt_err(populate_last_id_stmt, 'populate last_ids')

    # If reached without error, then success!
    return True


_temp_dump_file_templ = "{0}_dump"


def _temp_dump_file(site):
    return _temp_dump_file_templ.format(site)


def _base_dump_args(conn_str, dump_path):
    return ('--dbname=' + conn_str,
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


def create_id_map_tables(conn_str, skipsites, addsites, id_name, id_type, site_type, site_len):
    """Create a table (per site) for holding the id mappings between sites and the dcc

     :param str conn_str: connection string for target database
     :param str skipsites:      sites to skip
     :param str addsites:   `   sites to add
     :param str id_name:        name of the id ex. dcc or onco
     :param id_type:            type of id INTEGER or BIGINT
     :param site_type:          type of site_id INTEGER, BIGINT, or VARCHAR
     :param site_len:           if site_type VARCHAR the length
     """

    logger.info({'msg': 'starting id_map table creation'})
    starttime = time.time()

    # Set site_type
    if site_type == 'VARCHAR':
        site_type = 'VARCHAR(' + site_len + ')'

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
                [Statement(CREATE_ID_MAP_TABLE_SQL.format(schema, table, id_name, id_type, site_type))])

    statements.serial_execute(conn_str)

    for statement in statements:
        check_stmt_err(statement, 'id map table creation')

    logger.info({
        'msg', 'finished creation of id_maps tables',
        'elapsed', secs_since(starttime)
    })


def copy_id_maps(old_conn_str, new_conn_str, id_name, skipsites, addsites, ):
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


def populate_last_id(conn_str, search_path, id_name):
    """Populate study id maps from dcc_pedsnet.

        :param str conn_str:      database connection string
        :param str search_path: PostgreSQL schema search path
        :param str id_name: name of the id
        :returns:                 True if the function succeeds
        :rtype:                   bool
        """

    logger.info({'msg': 'starting populating last_id'})
    starttime = time.time()

    schema = primary_schema(search_path)
    _populate_last_id(conn_str, schema, id_name)

    logger.info({
        'msg', 'finished populating last_id',
        'elapsed', secs_since(starttime)
    })

    return True
