import logging
import time
import re

from pedsnetdcc.db import Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc import SITES_AND_DCC, SITES_EXTERNAL_ADD_DCC
from pedsnetdcc.utils import check_stmt_err, combine_dicts, get_conn_info_dict


logger = logging.getLogger(__name__)

PREP_DB_SQL_TEMPLATE = """
grant all on schema {{.Site}}_pcornet to {{.Owner}} with grant option;
"""

# SQL template for creating site schemas in an internal database instance.
PERMISSIONS_SQL_TEMPLATE = """
grant usage  on               schema {{.Site}}_pedsnet  to pcor_et_user, peds_staff, dcc_analytics;
grant select on all tables in schema {{.Site}}_pedsnet  to pcor_et_user, peds_staff, dcc_analytics;
grant all    on               schema {{.Site}}_pedsnet  to loading_user;
grant all    on               schema {{.Site}}_pcornet  to pcor_et_user;
grant usage  on               schema {{.Site}}_pcornet  to peds_staff;
grant select on all tables in schema {{.Site}}_pcornet  to peds_staff;
alter default privileges for role loading_user in schema {{.Site}}_pedsnet grant select on tables to pcor_et_user, peds_staff, dcc_analytics;
alter default privileges for role loading_user in schema {{.Site}}_pcornet grant select on tables to peds_staff;
"""

# SQL template for creating site schemas in an internal database instance.
PERMISSIONS_SQL_TEMPLATE_LIMITED = """
grant all    on               schema {{.Site}}_pedsnet  to {{.Owner}};
"""

PERMISSIONS_SQL_TEMPLATE_ID_NAME_LIMITED = """
grant all    on               schema {{.Site}}_{{.IdName}}  to {{.Owner}};
"""

ID_MAPS_SQL_TEMPLATE = """
grant all    on               schema {{.Site}}_id_maps  to loading_user;
alter default privileges for role loading_user in schema {{.Site}}_id_maps grant select on tables to peds_staff;
"""

ID_MAPS_SQL_TEMPLATE_LIMITED = """
grant all    on               schema {{.Site}}_id_maps  to {{.Owner}};
"""

ID_MAPS_SQL_TEMPLATE_ID_NAME_LIMITED = """
grant all    on               schema {{.Site}}_{{.IdName}}_id_maps  to {{.Owner}};
"""

VOCABULARY_PERMISSIONS_SQL_TEMPL = """
grant all                  on schema dcc_ids    to loading_user;
grant all                  on schema vocabulary to loading_user;
grant usage                on schema vocabulary to pcor_et_user, peds_staff, dcc_analytics;
grant select on all tables in schema vocabulary to pcor_et_user, peds_staff, dcc_analytics;
alter default privileges for role loading_user in schema vocabulary grant select on tables to pcor_et_user, peds_staff, dcc_analytics;
"""

VOCABULARY_PERM_SQL_TEMPL_LIMITED = """
grant all                  on schema dcc_ids    to {{.Owner}};
grant all                  on schema vocabulary to {{.Owner}};
"""

VOCABULARY_ONLY_PERM_SQL_TEMPL_LIMITED = """
grant all                  on schema vocabulary to {{.Owner}};
"""

IDS_PERM_SQL_TEMPL_LIMITED = """
grant all                  on schema {{.IdName}}_ids    to {{.Owner}};
"""


def _loading_user_privileges_sql(site, owner='loading_user'):
    """Return a list of statements to set the correct permissions loading_user during prepdb.

    :param site: site name, e.g. 'dcc' or 'stlouis'
    :type: str
    :param owner: role that should own schema
    :type: str
    :return: SQL statements
    :rtype: list(str)
    :raises: ValueError
    """

    tmpl = PREP_DB_SQL_TEMPLATE
    sql = tmpl.replace('{{.Site}}', site)
    sql = sql.replace('{{.Owner}}', owner)

    return [_despace(x) for x in sql.split("\n") if x]


def _database_privileges_sql(database_name):
    """Return a tuple of statements granting privileges on the database.
    :param database_name: Database name
    :type: str
    :return: a tuple of statements
    :rtype: tuple(str)
    """
    tmpl = 'grant create on database {db} to {usr}'
    return (tmpl.format(db=database_name, usr='peds_staff'),
            tmpl.format(db=database_name, usr='loading_user'))


def _database_privileges_sql_limited(database_name, owner='loading_user'):
    """Return a tuple of statements granting privileges on the database.
    :param database_name: Database name
    :type: str
    :return: a tuple of statements
    :rtype: tuple(str)
    """
    tmpl = 'grant create on database {db} to {usr}'
    return (tmpl.format(db=database_name, usr=owner)),


def _despace(statement):
    """Return string with runs of spaces replaced with a single space"""
    return re.sub(r' +', ' ', statement)


def _permissions_sql(site):
    """Return a list of statements to set the correct permissions for a given site.

    :param site: site name, e.g. 'dcc' or 'stlouis'
    :type: str
    :return: SQL statements
    :rtype: list(str)
    :raises: ValueError
    """
    tmpl = PERMISSIONS_SQL_TEMPLATE
    sql = tmpl.replace('{{.Site}}', site)

    statements = [_despace(x) for x in sql.split("\n") if x]

    if site != 'dcc':
        id_maps_tmpl = ID_MAPS_SQL_TEMPLATE
        id_maps_sql = id_maps_tmpl.replace('{{.Site}}', site)

        statements = statements + [_despace(x) for x in id_maps_sql.split("\n") if x]

    return statements


def _permissions_sql_limited(site, owner='loading_user', id_name='dcc'):
    """Return a list of statements to set the correct permissions for a given site.

    :param site: site name, e.g. 'dcc' or 'stlouis'
    :type: str
    :param owner: owner to grant access to
    :type: str
    :param id_name: name of ids in use
    :type: str
    :return: SQL statements
    :rtype: list(str)
    :raises: ValueError
    """
    if id_name == 'dcc':
        tmpl = PERMISSIONS_SQL_TEMPLATE_LIMITED
        sql = tmpl.replace('{{.Site}}', site)
        sql = sql.replace('{{.Owner}}', owner)
    else:
        tmpl = ID_MAPS_SQL_TEMPLATE_ID_NAME_LIMITED
        sql = tmpl.replace('{{.Site}}', site)
        sql = sql.replace('{{.Owner}}', owner)
        sql = sql.replace('{{.IdName}}', id_name)

    if id_name == 'dcc' or site != id_name:
        statements = [_despace(x) for x in sql.split("\n") if x]
    else:
        statements = []

    if site != 'dcc' and site != id_name:
        if id_name == 'dcc':
            id_maps_tmpl = ID_MAPS_SQL_TEMPLATE_LIMITED
        else:
            id_maps_tmpl = ID_MAPS_SQL_TEMPLATE_ID_NAME_LIMITED

        id_maps_sql = id_maps_tmpl.replace('{{.Site}}', site)
        id_maps_sql = id_maps_sql.replace('{{.Owner}}', owner)

        if id_name != 'dcc':
            id_maps_sql = id_maps_sql.replace('{{.IdName}}', id_name)

        statements = statements + [_despace(x) for x in id_maps_sql.split("\n") if x]

    return statements


def _vocabulary_permissions_sql():
    sql = VOCABULARY_PERMISSIONS_SQL_TEMPL
    return [_despace(x) for x in sql.split("\n") if x]


def _vocabulary_permissions_sql_limited(owner='loading_user'):
    sql = VOCABULARY_PERM_SQL_TEMPL_LIMITED
    sql = sql.replace('{{.Owner}}', owner)
    return [_despace(x) for x in sql.split("\n") if x]


def _vocabulary_only_permissions_sql_limited(owner='loading_user'):
    sql = VOCABULARY_ONLY_PERM_SQL_TEMPL_LIMITED
    sql = sql.replace('{{.Owner}}', owner)
    return [_despace(x) for x in sql.split("\n") if x]


def _ids_permissions_sql_limited(owner='loading_user', id_name='dcc'):
    sql = IDS_PERM_SQL_TEMPL_LIMITED
    sql = sql.replace('{{.Owner}}', owner)
    sql = sql.replace('{{.IdName}}', id_name)
    return [_despace(x) for x in sql.split("\n") if x]


def grant_loading_user_permissions(conn_str, inc_external = False, owner='loading_user'):
    """Grant loading_user grant permissions for pcornet

    :param conn_str: connection string to database
    :type: str
    """

    log_dict = get_conn_info_dict(conn_str)
    logger.info(combine_dicts({'msg': 'starting granting of loading_user permissions'},
                              log_dict))
    start_time = time.time()

    stmnts = StatementList()

    if inc_external:
        site_list = SITES_EXTERNAL_ADD_DCC
    else:
        site_list = SITES_AND_DCC

    for site in site_list:
        stmnts.extend(
            [Statement(x) for x in _loading_user_privileges_sql(site, owner)]
        )

    stmnts.serial_execute(conn_str)

    for stmnt in stmnts:
        check_stmt_err(stmnt, 'granting loading permissions')

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished granting of loading permissions',
                               'elapsed': secs_since(start_time)}, log_dict))


def grant_database_permissions(conn_str, database_name):
    """Grant create permissions on a database for the appropriate PEDSnet users

    :param conn_str: connection string to database
    :type: str
    :param database_name: name of newly created database to grant permissions on
    :type: str
    """

    log_dict = get_conn_info_dict(conn_str)

    logger.info(combine_dicts({'msg': 'starting granting of database permissions'},
                              log_dict))
    start_time = time.time()

    stmnts = StatementList()

    stmnts.extend(
        [Statement(x) for x in _database_privileges_sql(database_name)])

    stmnts.serial_execute(conn_str)

    for stmnt in stmnts:
        check_stmt_err(stmnt, 'granting database permissions')

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished granting of database permissions',
                               'elapsed': secs_since(start_time)}, log_dict))


def grant_database_permissions_limited(conn_str, database_name, owner='loading_user'):
    """Grant create permissions on a database for the appropriate PEDSnet users

    :param conn_str: connection string to database
    :type: str
    :param owner: role to grant permission to
    :type: str
    :param database_name: name of newly created database to grant permissions on
    :type: str
    """

    log_dict = get_conn_info_dict(conn_str)

    logger.info(combine_dicts({'msg': 'starting granting of database permissions'},
                              log_dict))
    start_time = time.time()

    stmnts = StatementList()

    stmnts.extend(
        [Statement(x) for x in _database_privileges_sql_limited(database_name, owner)])

    stmnts.serial_execute(conn_str)

    for stmnt in stmnts:
        check_stmt_err(stmnt, 'granting database permissions')

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished granting of database permissions',
                               'elapsed': secs_since(start_time)}, log_dict))


def grant_schema_permissions(conn_str, inc_external=False):
    """Grant schema and table permissions on a database for the appropriate PEDSnet users

    :param conn_str: connection string to database
    :type: str
    """

    log_dict = get_conn_info_dict(conn_str)

    logger.info(combine_dicts({'msg': 'starting granting of schema permissions'},
                              log_dict))
    start_time = time.time()
    stmnts = StatementList()

    if inc_external:
        site_list = SITES_EXTERNAL_ADD_DCC
    else:
        site_list = SITES_AND_DCC

    for site in site_list:
        stmnts.extend([Statement(x) for x in _permissions_sql(site)])

    stmnts.serial_execute(conn_str)

    for stmnt in stmnts:
        check_stmt_err(stmnt, 'granting schema permissions')

     # Log end of function.
    logger.info(combine_dicts({'msg': 'finished granting of schema permissions',
                               'elapsed': secs_since(start_time)}, log_dict))


def grant_schema_permissions_limited(conn_str, inc_external=False, owner='loading_user', id_name='dcc', sites=()):
    """Grant schema and table permissions on a database for user

    :param conn_str: connection string to database
    :type: str
    :param inc_external: include external sites
    :type: str
    :param owner: role to grant permission to
    :type: str
    :param id_name: name of the id
    :type: str
    :param sites: include external sites
    :type: tuple
    """

    log_dict = get_conn_info_dict(conn_str)

    logger.info(combine_dicts({'msg': 'starting granting of schema permissions'},
                              log_dict))
    start_time = time.time()
    stmnts = StatementList()

    if not sites:
        if inc_external:
            site_list = SITES_EXTERNAL_ADD_DCC
        else:
            site_list = SITES_AND_DCC
    else:
        site_list = sites

    for site in site_list:
        stmnts.extend([Statement(x) for x in _permissions_sql_limited(site, owner, id_name)])

    stmnts.serial_execute(conn_str)

    for stmnt in stmnts:
        check_stmt_err(stmnt, 'granting schema permissions')

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished granting of schema permissions',
                               'elapsed': secs_since(start_time)}, log_dict))


def grant_vocabulary_permissions(conn_str):
    """FILL IN
    """

    log_dict = get_conn_info_dict(conn_str)

    logger.info(combine_dicts({'msg': 'starting granting of vocabulary permissions'},
                              log_dict))
    start_time = time.time()

    stmnts = StatementList()
    stmnts.extend([Statement(x) for x in _vocabulary_permissions_sql()])

    stmnts.serial_execute(conn_str)

    for stmnt in stmnts:
        check_stmt_err(stmnt, 'granting vocabulary permissions')

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished granting of vocabulary permissions',
                               'elapsed': secs_since(start_time)}, log_dict))


def grant_vocabulary_permissions_limited(conn_str, owner):
    """FILL IN
    """

    log_dict = get_conn_info_dict(conn_str)

    logger.info(combine_dicts({'msg': 'starting granting of vocabulary permissions'},
                              log_dict))
    start_time = time.time()

    stmnts = StatementList()
    stmnts.extend([Statement(x) for x in _vocabulary_permissions_sql_limited(owner)])

    stmnts.serial_execute(conn_str)

    for stmnt in stmnts:
        check_stmt_err(stmnt, 'granting vocabulary permissions')

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished granting of vocabulary permissions',
                               'elapsed': secs_since(start_time)}, log_dict))


def grant_vocabulary_only_permissions_limited(conn_str, owner):
    """FILL IN
    """

    log_dict = get_conn_info_dict(conn_str)

    logger.info(combine_dicts({'msg': 'starting granting of vocabulary permissions'},
                              log_dict))
    start_time = time.time()

    stmnts = StatementList()
    stmnts.extend([Statement(x) for x in _vocabulary_only_permissions_sql_limited(owner)])

    stmnts.serial_execute(conn_str)

    for stmnt in stmnts:
        check_stmt_err(stmnt, 'granting vocabulary permissions')

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished granting of vocabulary permissions',
                               'elapsed': secs_since(start_time)}, log_dict))


def grant_ids_permissions_limited(conn_str, owner, id_name):
    """FILL IN
    """

    log_dict = get_conn_info_dict(conn_str)

    logger.info(combine_dicts({'msg': 'starting granting of <id>_ids permissions'},
                              log_dict))
    start_time = time.time()

    stmnts = StatementList()
    stmnts.extend([Statement(x) for x in _ids_permissions_sql_limited(owner, id_name)])

    stmnts.serial_execute(conn_str)

    for stmnt in stmnts:
        check_stmt_err(stmnt, 'granting ids permissions')

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished granting of ids permissions',
                               'elapsed': secs_since(start_time)}, log_dict))
