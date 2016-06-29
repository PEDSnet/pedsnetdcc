import logging
import re
import psycopg2

logger = logging.getLogger(__name__)


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
        return ('dcc', 'chop', 'colorado', 'nationwide', 'nemours', 'seattle',
                'stlouis')


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


def _database_privileges_sql(database_name):
    """Return a tuple of statements granting privileges on the database.
    :param database_name: Database name
    :type: str
    :return: a tuple of statements
    :rtype: tuple(str)
    """
    return 'grant create on database {} to peds_staff'.format(database_name),


# SQL template for creating site schemas in an internal database instance.
_sql_site_internal_template = """
create schema if not exists {{.Site}}_pedsnet authorization dcc_owner;
create schema if not exists {{.Site}}_pcornet authorization dcc_owner;
create schema if not exists {{.Site}}_harvest authorization dcc_owner;
create schema if not exists {{.Site}}_achilles authorization dcc_owner;
grant usage  on               schema {{.Site}}_pedsnet  to harvest_user, achilles_user, dqa_user, pcor_et_user, peds_staff;
grant select on all tables in schema {{.Site}}_pedsnet  to harvest_user, achilles_user, dqa_user, pcor_et_user, peds_staff;
grant all    on               schema {{.Site}}_pedsnet  to loading_user;
grant all    on               schema {{.Site}}_pcornet  to pcor_et_user;
grant usage  on               schema {{.Site}}_pcornet  to peds_staff;
grant select on all tables in schema {{.Site}}_pcornet  to peds_staff;
grant all    on               schema {{.Site}}_harvest  to harvest_user;
grant all    on               schema {{.Site}}_achilles to achilles_user;
alter default privileges for role loading_user in schema {{.Site}}_pedsnet grant select on tables to harvest_user, achilles_user, dqa_user, pcor_et_user, peds_staff;
alter default privileges for role loading_user in schema {{.Site}}_pcornet grant select on tables to peds_staff;
"""

# SQL template for creating site schemas in a production database instance.
_sql_site_prod_template = """
create schema if not exists {{.Site}}_pedsnet authorization dcc_owner;
create schema if not exists {{.Site}}_pcornet authorization dcc_owner;
create schema if not exists {{.Site}}_harvest authorization dcc_owner;
grant usage  on               schema {{.Site}}_pedsnet  to harvest_user, peds_staff;
grant select on all tables in schema {{.Site}}_pedsnet  to harvest_user, peds_staff;
grant usage  on               schema {{.Site}}_pcornet  to pcornet_sas, peds_staff;
grant select on all tables in schema {{.Site}}_pcornet  to pcornet_sas, peds_staff;
grant all    on               schema {{.Site}}_harvest  to harvest_user;
alter default privileges for role dcc_owner in schema {{.Site}}_pedsnet grant select on tables to harvest_user, peds_staff;
alter default privileges for role dcc_owner in schema {{.Site}}_pcornet grant select on tables to pcornet_sas, peds_staff;
"""


def _site_sql(site, db_type):
    """Return a list of statements to create the schemas for a given site.

    :param site: site name, e.g. 'dcc' or 'stlouis'
    :type: str
    :param db_type: 'prod' or 'internal'
    :type: str
    :return: SQL statements
    :rtype: list(str)
    :raises: ValueError
    """
    if db_type == 'internal':
        tmpl = _sql_site_internal_template
    elif db_type == 'prod':
        tmpl = _sql_site_prod_template
    else:
        raise ValueError('Invalid db_type: {}'.format(db_type))

    sql = tmpl.replace('{{.Site}}', site)
    return [x for x in sql.split("\n") if x]


_sql_vocab_internal = """
grant all                  on schema vocabulary to loading_user;
grant usage                on schema vocabulary to achilles_user, dqa_user, pcor_et_user, harvest_user, peds_staff;
grant select on all tables in schema vocabulary to achilles_user, dqa_user, pcor_et_user, harvest_user, peds_staff;
alter default privileges for role loading_user in schema vocabulary grant select on tables to achilles_user, dqa_user, pcor_et_user, harvest_user, peds_staff;
"""

_sql_vocab_prod = """
grant usage                on schema vocabulary to harvest_user, peds_staff;
grant select on all tables in schema vocabulary to harvest_user, peds_staff;
alter default privileges for role dcc_owner in schema vocabulary grant select on tables to harvest_user, peds_staff;
"""


def _vocabulary_sql(db_type):
    """Return a list of statements to create the vocabulary schema.

    :param db_type: 'prod' or 'internal'
    :type: str
    :rtype: list(str)
    :raises: ValueError
    """
    sql = "create schema if not exists vocabulary authorization dcc_owner;\n"

    if db_type == 'internal':
        sql += _sql_vocab_internal
    elif db_type == 'prod':
        sql += _sql_vocab_prod
    else:
        raise ValueError('Invalid db_type: {}'.format(db_type))
    return [x for x in sql.split("\n") if x]


def prepare_database(model_version, conn_str, db_type, update=False,
                     dcc_only=False):
    """Create a new database containing vocabulary and site schemas.

    The initial conn_str is used for issuing a CREATE DATABASE statement.
    statement.

    A subsequent connection to the newly created database is established
    in order to create schemas.

    :param model_version: PEDSnet model version, e.g. X.Y.Z
    :type: str
    :param conn_str: libpq connection string
    :type: str
    :param db_type: 'internal' or 'prod'
    :type: str
    :param update: assume the database is already created
    :type: bool
    :param dcc_only: only create schemas for `dcc` (no sites)
    :return: None, or (if `psql` is True) a SQL string
    :rtype: str
    """

    stmts = []

    database_name = _make_database_name(model_version)

    if not update:
        stmts.extend(_create_database_sql(database_name))

    stmts.extend(_database_privileges_sql(database_name))

    logger.info({'msg': 'Starting database preparation.'})

    with psycopg2.connect(conn_str) as conn:
        conn.set_isolation_level(0)
        with conn.cursor() as cursor:

            for stmt in stmts:
                logger.debug({'msg': 'Executing SQL.', 'sql': stmt})
                cursor.execute(stmt)
    conn.close()

    # Operate on the newly created database.

    stmts = []

    for site in _sites_and_dcc(dcc_only):
        stmts.extend(_site_sql(site, db_type))

    stmts.extend(_vocabulary_sql(db_type))

    # Create new_conn_str to target the new database
    new_conn_str = _conn_str_with_database(conn_str, database_name)

    with psycopg2.connect(new_conn_str) as conn:
        with conn.cursor() as cursor:

            for stmt in stmts:
                logger.debug({'msg': 'Executing SQL.', 'sql': stmt})
                cursor.execute(stmt)

    conn.close()
    logger.info({'msg': 'Finished database preparation.'})
