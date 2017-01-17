import logging
import re
import time

from pedsnetdcc import SITES, ID_MAP_TABLES, CONSISTENT_ID_MAP_TABLES
from pedsnetdcc.db import (Statement, StatementList)
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_err

from sh import pg_dump

logger = logging.getLogger(__name__)

_id_map_table_sql = """{0}_id_maps.{1}_ids"""

_create_id_map_table_sql = """CREATE TABLE """ + _id_map_table_sql + """(dcc_id INTEGER NOT NULL, site_id INTEGER NOT NULL)"""


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
        'msg', 'finished creation of id map tables',
        'elapsed', secs_since(starttime)
    })


def copy_id_maps(old_conn_str, new_conn_str):
    logger.info({'msg': 'starting id map copying'})
    starttime = time.time()

    statements = StatementList()
    for site in SITES:
        output = pg_dump('--dbname=' + old_conn_str,
                         '--data-only',
                         '-t',
                         _id_map_table_sql.format(site, 'person'),
                         '-t',
                         _id_map_table_sql.format(site, 'visit_occurrence'))

        statements.extend(Statement(output))

    statements.serial_execute(new_conn_str)

    for statement in statements:
        check_stmt_err(statement, 'id map data copying')

    logger.info({
        'msg', 'finished copying of id map table data',
        'elapsed', secs_since(starttime)
    })
