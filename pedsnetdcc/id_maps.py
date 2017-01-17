import logging
import re
import time

from pedsnetdcc import SITES, ID_MAP_TABLES
from pedsnetdcc.db import (Statement, StatementList)
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_err

logger = logging.getLogger(__name__)

_id_map_table_sql = """CREATE TABLE {0}_id_maps.{1}_ids (dcc_id INTEGER NOT NULL, site_id INTEGER NOT NULL)"""


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
                [Statement(_id_map_table_sql.format(site, table))]
            )

    statements.serial_execute(conn_str)

    for statement in statements:
        check_stmt_err(statement, 'id_map schema creation')

    logger.info({
        'msg', 'finished creation of id_map schemas',
        'elapsed', secs_since(starttime)
    })
