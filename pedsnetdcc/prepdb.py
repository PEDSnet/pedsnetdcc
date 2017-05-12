import logging
import re
import time

from pedsnetdcc import SITES
from pedsnetdcc.db import (Statement, StatementList)
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_err

from pedsnetdcc.permissions import (grant_database_permissions, grant_schema_permissions,
                                    grant_vocabulary_permissions, grant_loading_user_permissions)

logger = logging.getLogger(__name__)


def _despace(statement):
    """Return string with runs of spaces replaced with a single space"""
    return re.sub(r' +', ' ', statement)


def _conn_str_with_database(conn_str, dbname):
    """Adjust a connection string to specify the given database.

    This will override any existing database in the connection string.

    :param conn_str: libpq/psycopg2 connection string
    :type: str
    :param dbname: new database name
    :type: str
    :return: new connection string
    :rtype: str
    """
    dbname_clause = 'dbname={}'.format(dbname)
    if 'dbname=' in conn_str:
        new_conn_str = re.sub(r'dbname=([^ ]+)', dbname_clause, conn_str)
    else:
        new_conn_str = conn_str + ' ' + dbname_clause
    return new_conn_str


def _sites_and_dcc(dcc_only=False):
    """Return a tuple containing "dcc" and the site names, or just "dcc".
    :param dcc_only: return a tuple containing just 'dcc'
    :type: bool
    :rtype: tuple
    """
    if dcc_only:
        return 'dcc',
    else:
        return ('dcc',) + SITES


def _version_to_shorthand(version):
    """Given a version string such as 'X.Y.Z', return 'XY'.

    TODO: This is an awkward convention when X >= 10.

    :param version: PEDSnet version 'X.Y' or 'X.Y.Z'
    :type: str
    :rtype: str
    :raises: ValueError
    """
    parts = version.split('.')
    if len(parts) != 2 and len(parts) != 3:
        tmpl = 'Version string must be like X.Y or X.Y.Z, not `{}`'
        raise ValueError(tmpl.format(version))
    return parts[0] + parts[1]


def _make_database_name(model_version):
    """Return a database name, given a version string, e.g. '21' for '2.1'.
    :param model_version: PEDSnet model version: X.Y.Z or X.Y
    :type: str
    :rtype: str
    """
    short_version = _version_to_shorthand(model_version)
    return 'pedsnet_dcc_v{}'.format(short_version)


def _create_database_sql(database_name):
    """Return a tuple of statements to create the database with the given name.
    :param database_name: Database name
    :type: str
    :rtype: tuple
    """
    tmpl = "create database {} with owner = dcc_owner template = template0 " \
           "encoding = 'UTF8' lc_collate = 'C' lc_ctype = 'C'"
    return tmpl.format(database_name),

# SQL template for creating site schemas in an internal database instance.
SQL_SITE_TEMPLATE = """
create schema if not exists {{.Site}}_pedsnet  authorization dcc_owner;
create schema if not exists {{.Site}}_pcornet  authorization dcc_owner;
create schema if not exists {{.Site}}_harvest  authorization dcc_owner;
create schema if not exists {{.Site}}_achilles authorization dcc_owner;
"""

SQL_ID_MAPS_TEMPLATE = """create schema if not exists {{.Site}}_id_maps authorization dcc_owner;"""




def _site_sql(site):
    """Return a list of statements to create the schemas for a given site.

    :param site: site name, e.g. 'dcc' or 'stlouis'
    :type: str
    :return: SQL statements
    :rtype: list(str)
    :raises: ValueError
    """
    tmpl = SQL_SITE_TEMPLATE
    sql = tmpl.replace('{{.Site}}', site)

    statements = [_despace(x) for x in sql.split("\n") if x]

    if site != 'dcc':
        id_maps_tmpl = SQL_ID_MAPS_TEMPLATE
        id_maps_sql = id_maps_tmpl.replace('{{.Site}}', site)
        statements.append(_despace(id_maps_sql))

    return statements


SQL_OTHER = """
create schema if not exists vocabulary authorization dcc_owner;
create schema if not exists dcc_ids authorization dcc_owner;
"""


def _other_sql():
    """Return a list of statements to create non-site schemas.

    :rtype: list(str)
    :raises: ValueError
    """
    sql = SQL_OTHER
    return [_despace(x) for x in sql.split("\n") if x]


def prepare_database(model_version, conn_str, update=False, dcc_only=False):
    """Create a new database containing vocabulary and site schemas.

    The initial conn_str is used for issuing a CREATE DATABASE statement.
    statement.

    A subsequent connection to the newly created database is established
    in order to create schemas.

    :param model_version: PEDSnet model version, e.g. X.Y.Z
    :type: str
    :param conn_str: libpq connection string
    :type: str
    :param update: assume the database is already created
    :type: bool
    :param dcc_only: only create schemas for `dcc` (no sites)
    :return: True on success, False otherwise
    :raises RuntimeError: if any of the sql statements cause an error
    """
    logger.info({'msg': 'starting database preparation',
                 'model': model_version})
    starttime = time.time()

    database_name = _make_database_name(model_version)

    stmts = StatementList()

    if not update:
        stmts.extend(
            [Statement(x) for x in _create_database_sql(database_name)])

    stmts.serial_execute(conn_str)

    grant_database_permissions(conn_str, database_name)
    # Operate on the newly created database.
    stmts = StatementList()
    for site in _sites_and_dcc(dcc_only):
        stmts.extend([Statement(x) for x in _site_sql(site)])

    stmts.extend([Statement(x) for x in _other_sql()])

    # Create new_conn_str to target the new database
    new_conn_str = _conn_str_with_database(conn_str, database_name)

    stmts.serial_execute(new_conn_str)

    grant_loading_user_permissions(new_conn_str)
    grant_schema_permissions(new_conn_str)
    grant_vocabulary_permissions(new_conn_str)

    for stmt in stmts:
        check_stmt_err(stmt, 'database preparation')

    logger.info({
        'msg': 'finished database preparation',
        'model_version': model_version,
        'elapsed': secs_since(starttime)})

    return True
