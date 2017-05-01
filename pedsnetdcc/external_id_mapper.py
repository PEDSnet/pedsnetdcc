import logging
import time
import csv

from pedsnetdcc.db import Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_data, check_stmt_err, combine_dicts

logger = logging.getLogger(__name__)

UPDATE_LAST_ID_SQL = """UPDATE {last_id_table_name} AS new
SET last_id = new.last_id + '{new_id_count}'::integer
FROM {last_id_table_name} AS old RETURNING old.last_id, new.last_id"""
UPDATE_LAST_ID_MSG = "updating {table_name} last ID tracking table to reserve new IDs"  # noqa

LOCK_LAST_ID_SQL = """LOCK {last_id_table_name}"""
LOCK_LAST_ID_MSG = "locking {table_name} last ID tracking table for update"

INSERT_NEW_MAPS_SQL = """INSERT INTO {map_table_name} (site_id, dcc_id) VALUES({site_id}, {dcc_id}) ON CONFLICT (site_id) DO NOTHING"""  # noqa
INSERT_NEW_MAPS_MSG = "inserting new {table_name} ID mappings into map table"

MAP_TABLE_NAME_TMPL = "{table_name}_ids"
LAST_ID_TABLE_NAME_TMPL = "dcc_{table_name}_id"

SELECT_MAPPING_STATEMENT = """SELECT site_id, dcc_id FROM {map_table_name} WHERE site_id IN ({mapping_values})"""

def map_external_ids(conn_str, in_csv_file, out_csv_file, table_name):
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

    with open(in_csv_file, 'rb') as f:
        reader = csv.reader(f)

        num_of_rows = len(list(reader))

        tpl_vars['new_id_count'] = num_of_rows
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
        # Return to the beginning of the file after taking the count previously
        f.seek(0)

        site_mapping_values = []
        for row in reader:
            site_id = row[0]
            insert_mapping_row(tpl_vars, site_id, dcc_id, conn_str)

            site_mapping_values.append(str(site_id))

            dcc_id += 1

        ## Get mapping from database
        tpl_vars['mapping_values'] = ",".join(site_mapping_values)

        mapping_statement = Statement(SELECT_MAPPING_STATEMENT.format(**tpl_vars))

        mapping_statement.execute(conn_str)
        check_stmt_err(mapping_statement, 'id mapping select')

        with open(out_csv_file, 'wb') as out_csv:
            out_writer = csv.writer(out_csv, delimiter=',')
            out_writer.writerow(['site_id', 'dcc_id'])

            for result in mapping_statement.data:
                logger.info({
                    'site_id': result[0],
                    'dcc_id': result[1]
                })
                out_writer.writerow([result[0], result[1]])

    logger.info({
        'msg': "Finished mapping external ids",
        'table': table_name,
        'secs_elapsed': secs_since(starttime)
    })


def insert_mapping_row(tpl_vars, site_id, dcc_id, conn_str):
    tpl_vars['site_id'] = site_id
    tpl_vars['dcc_id'] = dcc_id

    insert_statement = Statement(INSERT_NEW_MAPS_SQL.format(**tpl_vars))

    insert_statement.execute(conn_str)
    check_stmt_err(insert_statement, 'id mapping pre-transform')
