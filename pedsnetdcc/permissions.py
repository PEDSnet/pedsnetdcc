import logging
import time
import re

from pedsnetdcc.db import Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc import SITES_AND_DCC, SITES_EXTERNAL_ADD_DCC
from pedsnetdcc.utils import check_stmt_err, combine_dicts, get_conn_info_dict


logger = logging.getLogger(__name__)

PREP_DB_SQL_TEMPLATE = """
grant all on schema {{.Site}}_pcornet to loading_user with grant option;
grant all on schema {{.Site}}_harvest to loading_user with grant option;
grant all on schema {{.Site}}_achilles to loading_user with grant option;
"""

# SQL template for creating site schemas in an internal database instance.
PERMISSIONS_SQL_TEMPLATE = """
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

ID_MAPS_SQL_TEMPLATE = """
grant all    on               schema {{.Site}}_id_maps  to loading_user;
alter default privileges for role loading_user in schema {{.Site}}_id_maps grant select on tables to peds_staff;
"""

VOCABULARY_PERMISSIONS_SQL_TEMPL = """
grant all                  on schema dcc_ids    to loading_user;
grant all                  on schema vocabulary to loading_user;
grant usage                on schema vocabulary to achilles_user, dqa_user, pcor_et_user, harvest_user, peds_staff;
grant select on all tables in schema vocabulary to achilles_user, dqa_user, pcor_et_user, harvest_user, peds_staff;
alter default privileges for role loading_user in schema vocabulary grant select on tables to achilles_user, dqa_user, pcor_et_user, harvest_user, peds_staff;
"""

def _loading_user_privileges_sql(site):
    """Return a list of statements to set the correct permissions loading_user during prepdb.

    :param site: site name, e.g. 'dcc' or 'stlouis'
    :type: str
    :return: SQL statements
    :rtype: list(str)
    :raises: ValueError
    """

    tmpl = PREP_DB_SQL_TEMPLATE
    sql = tmpl.replace('{{.Site}}', site)

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

def _vocabulary_permissions_sql():
    sql = VOCABULARY_PERMISSIONS_SQL_TEMPL
    return [_despace(x) for x in sql.split("\n") if x]

def grant_loading_user_permissions(conn_str, inc_external = False):
    """Grant loading_user grant permissions for pcornet, achilles, harvest

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
            [Statement(x) for x in _loading_user_privileges_sql(site)]
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




