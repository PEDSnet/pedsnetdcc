import logging
import time
import csv
import random
from pedsnetdcc.schema import (primary_schema)

from pedsnetdcc.db import Statement, StatementList, StatementSet
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_data, check_stmt_err, combine_dicts

logger = logging.getLogger(__name__)

UPDATE_LAST_ID_SQL = """UPDATE {last_id_table_name} AS new
SET last_id = new.last_id + '{new_id_count}'::bigint
FROM {last_id_table_name} AS old RETURNING old.last_id, new.last_id"""
UPDATE_LAST_ID_MSG = "updating {table_name} last ID tracking table to reserve new IDs"  # noqa

LOCK_LAST_ID_SQL = """LOCK {last_id_table_name}"""
LOCK_LAST_ID_MSG = "locking {table_name} last ID tracking table for update"

TABLE_EXISTS_SQL = """SELECT EXISTS ( SELECT FROM pg_tables WHERE schemaname = '{schema}' AND tablename = '{temp_table_name}');"""  # noqa
TABLE_EXISTS_MSG = "checking if temp table exists"

CREATE_TEMP_SQL = """CREATE UNLOGGED TABLE {schema}.{temp_table_name} (site_id bigint PRIMARY KEY, dcc_id integer);"""  # noqa
CREATE_TEMP_MSG = "create temp table"

INSERT_TEMP_SQL = """INSERT INTO {temp_table_name} (site_id) VALUES {site_id} ON CONFLICT (site_id) DO NOTHING"""  # noqa
INSERT_TEMP_MSG = "inserting site_ids into temp table"

SELECT_MAP_SQL = """SELECT t.site_id, m.dcc_id FROM {schema}.{temp_table_name} t LEFT JOIN {schema}.{map_table_name} m on m.site_id = t.site_id;""" # noqa
SELECT_MAP_MSG = "selecting current mapping"

UPDATE_DCC_SQL = """UPDATE {schema}.{temp_table_name} t SET dcc_id = m.dcc_id FROM (SELECT site_id, dcc_id FROM {schema}.{map_table_name}) m WHERE t.site_id = m.site_id;"""  # noqa
UPDATE_DCC_MSG = "inserting site_ids into temp table"

DROP_TEMP_SQL = """DROP TABLE {schema}.{temp_table_name} CASCADE;"""  # noqa
DROP_TEMP_MSG = "drop temp table"

INSERT_NEW_MAPS_SQL = """INSERT INTO {map_table_name} (site_id, dcc_id) VALUES({site_id}, {dcc_id}) ON CONFLICT (site_id) DO NOTHING"""  # noqa
INSERT_NEW_MAPS_MSG = "inserting new {table_name} ID mappings into map table"

MAP_TABLE_NAME_TMPL = "{table_name}_ids"
LAST_ID_TABLE_NAME_TMPL = "dcc_{table_name}_id"

SELECT_MAPPING_STATEMENT = """SELECT site_id, dcc_id FROM {map_table_name} WHERE site_id IN ({mapping_values})"""


def map_external_ids(conn_str, in_csv_file, out_csv_file, table_name, search_path):
    starttime = time.time()
    logger.info({
        'msg': 'starting external id mapping',
        'secs_elapsed': secs_since(starttime)
    })

    tpl_vars = {
        'table_name': table_name
    }

    tpl_vars['map_table_name'] = MAP_TABLE_NAME_TMPL.format(**tpl_vars)
    tpl_vars['last_id_table_name'] = LAST_ID_TABLE_NAME_TMPL.format(**tpl_vars)
    schema = primary_schema(search_path)

    with open(in_csv_file, 'rb') as f:
        reader = csv.reader(f)
        csv_data = list(reader)

    temp_table = ''
    while temp_table == '':
        temp_table = get_temp_table_name(conn_str, schema)

    logger.info({
        'msg': 'filling temp table',
        'secs_elapsed': secs_since(starttime)
    })
    fill_temp_table(conn_str, schema, temp_table, csv_data)

    logger.info({
        'msg': 'getting current mapping',
        'secs_elapsed': secs_since(starttime)
    })
    map_data = get_current_map_pairs(conn_str, schema, temp_table, tpl_vars['map_table_name'])

    logger.info({
        'msg': 'dropping temp table',
        'secs_elapsed': secs_since(starttime)
    })
    drop_temp_table(conn_str, schema, temp_table)

    unmapped = 0
    for map_pair in map_data:
        if map_pair["dcc_id"] is None:
            unmapped += 1

    tpl_vars['new_id_count'] = unmapped
    update_last_id_stmts = StatementList()
    update_last_id_stmts.append(Statement(
    LOCK_LAST_ID_SQL.format(**tpl_vars),
    LOCK_LAST_ID_MSG.format(**tpl_vars)))
    update_last_id_stmts.append(Statement(
        UPDATE_LAST_ID_SQL.format(**tpl_vars),
        UPDATE_LAST_ID_MSG.format(**tpl_vars)))

    # Execute last id table update statements and ensure they didn't
    # error and the second one returned results.
    update_last_id_stmts.serial_execute(conn_str, transaction=True)

    for stmt in update_last_id_stmts:
        check_stmt_err(stmt, 'ID mapping pre-transform')
    check_stmt_data(update_last_id_stmts[1],
                    'ID mapping pre-transform')

    # Get the old and new last IDs from the second update statement.
    tpl_vars['old_last_id'] = update_last_id_stmts[1].data[0][0]
    tpl_vars['new_last_id'] = update_last_id_stmts[1].data[0][1]
    logger.info({
        'msg': 'last ID tracking table updated',
        'table': table_name,
        'old_last_id': tpl_vars['old_last_id'],
        'new_last_id': tpl_vars['new_last_id']})

    dcc_id = int(tpl_vars['old_last_id']) + 1

    logger.info({
        'msg': 'mapping new ids',
        'secs_elapsed': secs_since(starttime)
    })
    for map_pair in map_data:
        if map_pair['dcc_id'] is None:
            map_pair['dcc_id'] = dcc_id
            insert_mapping_row(tpl_vars, map_pair['site_id'], dcc_id, conn_str)
            dcc_id += 1

    logger.info({
        'msg': 'writing output csv file',
        'secs_elapsed': secs_since(starttime)
    })
    with open(out_csv_file, 'wb') as out_csv:
        out_writer = csv.writer(out_csv, delimiter=',')
        out_writer.writerow(['site_id', 'dcc_id'])

        for map_pair in map_data:
            logger.info({
                'site_id': map_pair['site_id'],
                'dcc_id': map_pair['dcc_id']
            })
            out_writer.writerow([map_pair["site_id"], map_pair["dcc_id"]])

    logger.info({
        'msg': "Finished mapping external ids",
        'table': table_name,
        'secs_elapsed': secs_since(starttime)
    })

    # If reached without error, then success!
    return True


def insert_mapping_row(tpl_vars, site_id, dcc_id, conn_str):
    tpl_vars['site_id'] = site_id
    tpl_vars['dcc_id'] = dcc_id

    insert_statement = Statement(INSERT_NEW_MAPS_SQL.format(**tpl_vars))

    insert_statement.execute(conn_str)
    check_stmt_err(insert_statement, 'id mapping pre-transform')


def get_temp_table_name(conn_str, schema):
    tpl_vars = {
        'schema': schema,
        'temp_table_name': 't' + str(random.getrandbits(62))
    }

    exist_statement = Statement(TABLE_EXISTS_SQL.format(**tpl_vars))
    exist_statement.execute(conn_str)
    check_stmt_err(exist_statement, 'check temp table exists')
    exists = exist_statement.data
    if exists:
        return tpl_vars['temp_table_name']
    else:
        return ''


def fill_temp_table(conn_str, schema, table_name, csv_data):
    tpl_vars = {
        'schema': schema,
        'temp_table_name': table_name
    }

    create_statement = Statement(CREATE_TEMP_SQL.format(**tpl_vars))
    create_statement.execute(conn_str)
    check_stmt_err(create_statement, 'create temp table')

    site_id_list = []
    for site_id in csv_data:
        new_site_id = '(' + ''.join(site_id) + ')'
        site_id_list.append(new_site_id)

    # as list may be very long split into groups of 100k
    n = 100000
    split_site_id_list = [site_id_list[i * n:(i + 1) * n] for i in range((len(site_id_list) + n - 1) // n)]

    for site_id_list in split_site_id_list:
        tpl_vars['site_id'] = ', '.join(site_id_list)
        insert_statement = Statement(INSERT_TEMP_SQL.format(**tpl_vars))
        insert_statement.execute(conn_str)
        check_stmt_err(insert_statement, 'fill temp table')


def update_temp_table(conn_str, schema, table_name, map_table_name):
    tpl_vars = {
        'schema': schema,
        'temp_table_name': table_name,
        'map_table_name': map_table_name
    }

    update_statement = Statement(UPDATE_DCC_SQL.format(**tpl_vars))
    update_statement.execute(conn_str)
    check_stmt_err(update_statement, 'update available dcc_ids')


def get_current_map_pairs(conn_str, schema, table_name, map_table_name):
    tpl_vars = {
        'schema': schema,
        'temp_table_name': table_name,
        'map_table_name': map_table_name
    }

    mapping_statement = Statement(SELECT_MAP_SQL.format(**tpl_vars))
    mapping_statement.execute(conn_str)
    check_stmt_err(mapping_statement, 'select current mapping')

    map_data = []
    for result in mapping_statement.data:
        map_pair = {'site_id': result[0], 'dcc_id': result[1]}
        map_data.append(map_pair)

    return  map_data;


def drop_temp_table(conn_str, schema, table_name):
    tpl_vars = {
        'schema': schema,
        'temp_table_name': table_name
    }

    drop_statement = Statement(DROP_TEMP_SQL.format(**tpl_vars))
    drop_statement.execute(conn_str)
    check_stmt_err(drop_statement, 'drop temp table')
