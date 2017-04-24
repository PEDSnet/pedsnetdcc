import logging
import time
import csv

from pedsnetdcc.db import Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_data, check_stmt_err, combine_dicts

logger = logging.getLogger(__name__)

update_last_id_sql = """UPDATE {last_id_table_name} AS new
SET last_id = new.last_id + '{new_id_count}'::integer
FROM {last_id_table_name} AS old RETURNING old.last_id, new.last_id"""
update_last_id_msg = "updating {table_name} last ID tracking table to reserve new IDs"  # noqa

lock_last_id_sql = """LOCK {last_id_table_name}"""
lock_last_id_msg = "locking {table_name} last ID tracking table for update"

insert_new_maps_sql = """INSERT INTO {map_table_name} (site_id, dcc_id) VALUES({site_id}, {dcc_id})"""  # noqa
insert_new_maps_msg = "inserting new {table_name} ID mappings into map table"

map_table_name_tmpl = "{table_name}_ids"
last_id_table_name_tmpl = "dcc_{table_name}_id"

def map_external_ids(conn_str, in_csv_file, out_csv_file, table_name):

    starttime = time.time()
    logger.info({
        'msg': 'starting external id mapping',
        'secs_elapsed': secs_since(starttime)
    })

    tpl_vars = {
        'table_name': table_name
    }

    tpl_vars['map_table_name'] = map_table_name_tmpl.format(**tpl_vars)
    tpl_vars['last_id_table_name'] = last_id_table_name_tmpl.format(**tpl_vars)

    with open(in_csv_file, 'rb') as f:
        reader = csv.reader(f)

        num_of_rows = len(list(reader))

        tpl_vars['new_id_count'] = num_of_rows
        update_last_id_stmts = StatementList()
        update_last_id_stmts.append(Statement(
            lock_last_id_sql.format(**tpl_vars),
            lock_last_id_msg.format(**tpl_vars)))
        update_last_id_stmts.append(Statement(
            update_last_id_sql.format(**tpl_vars),
            update_last_id_msg.format(**tpl_vars)))

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
        print({'msg': 'last ID tracking table updated',
               'table': table_name,
               'old_last_id': tpl_vars['old_last_id'],
               'new_last_id': tpl_vars['new_last_id']})

        dcc_id = int(tpl_vars['old_last_id']) + 1
        f.seek(0)

        with open(out_csv_file, 'wb') as out_csv:
            out_writer = csv.writer(out_csv, delimiter=',')
            out_writer.writerow(['site_id', 'dcc_id'])
            for row in reader:
                site_id = row[0]

                tpl_vars['site_id'] = site_id
                tpl_vars['dcc_id'] = dcc_id

                insert_statement = Statement(insert_new_maps_sql.format(**tpl_vars))

                insert_statement.execute(conn_str)
                check_stmt_err(insert_statement, 'id mapping pre-transform')

                out_writer.writerow([site_id, dcc_id])
                dcc_id += 1

    print("new_id_count:" + str(num_of_rows))
