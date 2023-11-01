import logging
import re
import time

from pedsnetdcc import SITES, SITES_AND_EXTERNAL, EXTERNAL_SITES
from pedsnetdcc.db import (Statement, StatementList)
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_err

from pedsnetdcc.permissions import (grant_database_permissions, grant_database_permissions_limited,
                                    grant_schema_permissions, grant_schema_permissions_limited,
                                    grant_vocabulary_permissions, grant_vocabulary_permissions_limited,
                                    grant_loading_user_permissions, grant_vocabulary_only_permissions_limited,
                                    grant_ids_permissions_limited)

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


def _sites_and_dcc(dcc_only=False, inc_external=False):
    """Return a tuple containing "dcc" and the site names, or just "dcc".
    :param dcc_only: return a tuple containing just 'dcc'
    :type: bool
    :parm
    :rtype: tuple
    """
    if dcc_only:
        return 'dcc',
    else:
        if inc_external:
            return ('dcc',) + SITES_AND_EXTERNAL
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


def _make_database_name_alt(model_version, name):
    """Return a database name, given a version string, e.g. '21' for '2.1'.
    :param model_version: PEDSnet model version: X.Y.Z or X.Y
    :type: str
    :rtype: str
    """
    short_version = _version_to_shorthand(model_version)
    return name


def _create_database_sql(database_name, owner = 'dcc_owner'):
    """Return a tuple of statements to create the database with the given name.
    :param database_name: Database name
    :type: str
    :param owner: Database owner default = 'dcc_owner'
    :type: str
    :rtype: tuple
    """
    tmpl = "create database {0} with owner = {1} template = template0 " \
           "encoding = 'UTF8' lc_collate = 'C' lc_ctype = 'C'"
    return tmpl.format(database_name, owner),


def _create_database_sql_new(database_name, owner='dcc_owner'):
    """Return a tuple of statements to create the database with the given name.
    :param database_name: Database name
    :type: str
    :param owner: Database owner default = 'dcc_owner'
    :type: str
    :rtype: tuple
    """
    tmpl = "create database {0} with owner = {1} template = template0 " \
           "encoding = 'UTF8' lc_collate = 'en_US.UTF-8' lc_ctype = 'en_US.UTF-8'"
    return tmpl.format(database_name, owner),


# SQL template for creating site schemas in an internal database instance.
SQL_SITE_TEMPLATE = """
create schema if not exists {{.Site}}_pedsnet  authorization {{.Owner}};
create schema if not exists {{.Site}}_pcornet  authorization {{.Owner}};
"""

SQL_SITE_PEDSNET_TEMPLATE = """
create schema if not exists {{.Site}}_pedsnet  authorization {{.Owner}};
"""

SQL_SITE_ID_NAME_TEMPLATE = """
create schema if not exists {{.Site}}_{{.IdName}}  authorization {{.Owner}};
"""

SQL_ID_MAPS_TEMPLATE = """create schema if not exists {{.Site}}_id_maps authorization {{.Owner}};"""

SQL_ID_MAPS_ID_NAME_TEMPLATE = """create schema if not exists {{.Site}}_{{.IdName}}_id_maps authorization {{.Owner}};"""


def _site_sql(site, owner='dcc_owner', id_name='dcc', pedsnet_only=False):
    """Return a list of statements to create the schemas for a given site.

    :param site: site name, e.g. 'dcc' or 'stlouis'
    :type: str
    :param owner: owner of schema, default = 'dcc_owner'
    :type: str
    :param id_name: name of ids used, default = dcc'
    :type: str
    :param pedsnet_only: should only PEDSnet schemas be created
    :type: bool
    :return: SQL statements
    :rtype: list(str)
    :raises: ValueError
    """

    if id_name == 'dcc':
        if pedsnet_only:
            tmpl = SQL_SITE_PEDSNET_TEMPLATE
        else:
            tmpl = SQL_SITE_TEMPLATE
    else:
        tmpl = SQL_SITE_ID_NAME_TEMPLATE

    sql = tmpl.replace('{{.Site}}', site)
    sql = sql.replace('{{.Owner}}', owner)

    if id_name != 'dcc':
        sql = sql.replace('{{.IdName}}', id_name)

    if site == 'dcc' or site != id_name:
        statements = [_despace(x) for x in sql.split("\n") if x]
    else:
        statements = []

    if site == 'dcc' and id_name != 'dcc':
        if pedsnet_only:
            tmpl = SQL_SITE_PEDSNET_TEMPLATE
        else:
            tmpl = SQL_SITE_TEMPLATE
        sql = tmpl.replace('{{.Site}}', id_name)
        sql = sql.replace('{{.Owner}}', owner)
        statements.append(_despace(sql))

    if site != id_name:
        if id_name == 'dcc':
            id_maps_tmpl = SQL_ID_MAPS_TEMPLATE
        else:
            id_maps_tmpl = SQL_ID_MAPS_ID_NAME_TEMPLATE

        id_maps_sql = id_maps_tmpl.replace('{{.Site}}', site)
        id_maps_sql = id_maps_sql.replace('{{.Owner}}', owner)

        if id_name != 'dcc':
            id_maps_sql = id_maps_sql.replace('{{.IdName}}', id_name)

        statements.append(_despace(id_maps_sql))

    return statements


SQL_OTHER = """
create schema if not exists vocabulary authorization {{.Owner}};
create schema if not exists dcc_ids authorization {{.Owner}};
"""

SQL_OTHER_ID_ONLY = """
create schema if not exists vocabulary authorization {{.Owner}};
create schema if not exists {{.IdName}}_ids authorization {{.Owner}};
"""

SQL_OTHER_ID = """
create schema if not exists vocabulary authorization {{.Owner}};
create schema if not exists dcc_ids authorization {{.Owner}};
create schema if not exists {{.IdName}}_ids authorization {{.Owner}};
"""


def _other_sql(id_name='dcc', alt_id_only=False, owner='dcc_owner'):
    """Return a list of statements to create non-site schemas.
    :param id_name: name of ids used, default = dcc'
    :type: str
    :param alt_id_only: if id_name not dcc, should only id_name be created
    :type: bool
    :param id_name: owner of schema, default = dcc_owner'
    :type: str
    :rtype: list(str)
    :raises: ValueError
    """
    if id_name == 'dcc':
        sql = SQL_OTHER
    else:
        if alt_id_only:
            sql = SQL_OTHER_ID_ONLY
        else:
            sql = SQL_OTHER_ID
        sql = sql.replace('{{.IdName}}', id_name)

    sql = sql.replace('{{.Owner}}', owner)
    return [_despace(x) for x in sql.split("\n") if x]


def _delete_external_schemas(conn_str, site):
    delete_schemas_sql = """
    DROP SCHEMA {0}_pedsnet CASCADE;
    DROP SCHEMA {0}_pcornet CASCADE;
    """
    delete_schemas_msg = "cleaning up unused {0} schemas"

    # Clean up unneeded schemas
    clean_schemas_stmt = Statement( delete_schemas_sql.format(site), delete_schemas_msg.format(site))

    # Execute the clean up statements and ensure they didn't error
    clean_schemas_stmt.execute(conn_str)
    check_stmt_err(clean_schemas_stmt, 'clean up {0} schemas'.format(site))

    # If reached without error, then success!
    return True


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
            [Statement(x) for x in _create_database_sql(database_name, 'loading_user')])

    stmts.serial_execute(conn_str)

    grant_database_permissions(conn_str, database_name)
    # Operate on the newly created database.
    stmts = StatementList()
    for site in _sites_and_dcc(dcc_only, True):
        stmts.extend([Statement(x) for x in _site_sql(site, 'loading_user')])

    stmts.extend([Statement(x) for x in _other_sql('dcc', False, 'loading_user')])

    # Create new_conn_str to target the new database
    new_conn_str = _conn_str_with_database(conn_str, database_name)

    stmts.serial_execute(new_conn_str)

    grant_loading_user_permissions(new_conn_str, True)
    grant_schema_permissions(new_conn_str, True)
    grant_vocabulary_permissions(new_conn_str)

    for stmt in stmts:
        check_stmt_err(stmt, 'database preparation')

    for ext_site in EXTERNAL_SITES:
        _delete_external_schemas(new_conn_str, ext_site)

    logger.info({
        'msg': 'finished database preparation',
        'model_version': model_version,
        'elapsed': secs_since(starttime)})

    return True


def prepare_database_altname(model_version, conn_str, name, addsites, skipsites, id_name,
                             alt_id_only, owner, pedsnet_only, inc_ext, new, limit, update=False, dcc_only=False):
    """Create a new database containing vocabulary and site schemas.

    The initial conn_str is used for issuing a CREATE DATABASE statement.
    statement.

    A subsequent connection to the newly created database is established
    in order to create schemas.

    :param model_version: PEDSnet model version, e.g. X.Y.Z
    :type: str
    :param conn_str: libpq connection string
    :type: str
    :param name: alternate db name
    :type: str
    :param addsites: sites to add
    :type: str
    :param skipsites: sites to ignore
    :type: str
    :param id_name: name of the id other than dcc
    :type: str
    :param alt_id_only: should only non dcc ids be created
    :type: bool
    :param owner: owner of db and schemas
    :type: str
    :param pedsnet_only: create only PEDSnet schemas (no PCORnet)
    :type: bool
    :param inc_ext: should external sites be included
    :type: bool
    :param new: db version > 10
    :type: bool
    :param limit: limit access to super users
    :type: bool
    :param update: assume the database is already created
    :type: bool
    :param dcc_only: only create schemas for `dcc` (no sites)
    :return: True on success, False otherwise
    :raises RuntimeError: if any of the sql statements cause an error
    """
    logger.info({'msg': 'starting database preparation',
                 'model': model_version})
    starttime = time.time()

    # Get base sites
    if inc_ext:
        base_sites = SITES_AND_EXTERNAL
    else:
        base_sites = SITES

    if id_name == 'dcc' or not alt_id_only:
        if dcc_only:
            primary_sites = ('dcc',)
        else:
            # need to create id_maps for external sites for dcc
            if not inc_ext:
                primary_sites = ('dcc',) + base_sites + EXTERNAL_SITES
            else:
                primary_sites = ('dcc',) + base_sites
    else:
        primary_sites = ()

    if id_name != 'dcc':
        # Get Sites to skip
        skip_sites = skipsites.split(",")
        tmp_sites = list(set(base_sites) - set(skip_sites))

        # Get Sites to add
        add_sites = addsites.split(",")
        tmp_sites = list(set(tmp_sites) | set(add_sites))

        tmp_sites = list(filter(None, tmp_sites))
        alt_sites = (id_name,) + tuple(tmp_sites)
    else:
        alt_sites = ()

    database_name = _make_database_name_alt(model_version, name)

    stmts = StatementList()

    if not update:
        if new:
            stmts.extend(
                [Statement(x) for x in _create_database_sql_new(database_name, owner)])
        else:
            stmts.extend(
                [Statement(x) for x in _create_database_sql(database_name, owner)])

    stmts.serial_execute(conn_str)

    if not update:
        for stmnt in stmts:
            check_stmt_err(stmnt, 'creating database')

    if limit:
        grant_database_permissions_limited(conn_str, database_name, owner)
    else:
        grant_database_permissions(conn_str, database_name)

    # Operate on the newly created database.
    stmts = StatementList()
    if id_name == 'dcc' or not alt_id_only:
        for site in primary_sites:
            stmts.extend([Statement(x) for x in _site_sql(site, owner, 'dcc', pedsnet_only)])
    if id_name != 'dcc':
        for site in alt_sites:
            stmts.extend([Statement(x) for x in _site_sql(site, owner, id_name)])

    stmts.extend([Statement(x) for x in _other_sql(id_name, alt_id_only, owner)])

    # Create new_conn_str to target the new database
    new_conn_str = _conn_str_with_database(conn_str, database_name)

    stmts.serial_execute(new_conn_str)

    if not alt_id_only and not pedsnet_only:
        grant_loading_user_permissions(new_conn_str, inc_ext, owner)

    if limit:
        if primary_sites:
            grant_schema_permissions_limited(new_conn_str, inc_ext, owner, 'dcc', primary_sites)
        if alt_sites:
            grant_schema_permissions_limited(new_conn_str, inc_ext, owner, id_name, alt_sites)
        if id_name == 'dcc':
            grant_vocabulary_permissions_limited(new_conn_str, owner)
        else:
            grant_vocabulary_only_permissions_limited(new_conn_str, owner)
            grant_ids_permissions_limited(new_conn_str, owner, id_name)
            if not alt_id_only:
                grant_ids_permissions_limited(new_conn_str, owner, 'dcc')
    else:
        grant_schema_permissions(new_conn_str, False)
        grant_vocabulary_permissions(new_conn_str)

    for stmt in stmts:
        check_stmt_err(stmt, 'database preparation')

    if id_name == 'dcc' or not alt_id_only:
        for ext_site in EXTERNAL_SITES:
            _delete_external_schemas(new_conn_str, ext_site)

    logger.info({
        'msg': 'finished database preparation',
        'model_version': model_version,
        'elapsed': secs_since(starttime)})

    return True
