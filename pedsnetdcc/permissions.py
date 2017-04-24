import logging
import time
import re

from pedsnetdcc.db import Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc import SITES_AND_DCC
from pedsnetdcc.utils import check_stmt_err


# SQL template for creating site schemas in an internal database instance.
_permissions_sql_template = """
grant usage  on               schema {{.Site}}_pedsnet  to harvest_user, achilles_user, dqa_user, pcor_et_user, peds_staff;
grant select on all tables in schema {{.Site}}_pedsnet  to harvest_user, achilles_user, dqa_user, pcor_et_user, peds_staff;
grant all    on               schema {{.Site}}_pedsnet  to loading_user;
grant all    on               schema {{.Site}}_pcornet  to pcor_et_user;
grant usage  on               schema {{.Site}}_pcornet  to peds_staff;
grant select on all tables in schema {{.Site}}_pcornet  to peds_staff;
grant all    on               schema {{.Site}}_harvest  to harvest_user;
grant all    on               schema {{.Site}}_achilles to achilles_user;
grant all    on               schema {{.Site}}_id_maps  to loading_user;
alter default privileges for role loading_user in schema {{.Site}}_pedsnet grant select on tables to harvest_user, achilles_user, dqa_user, pcor_et_user, peds_staff;
alter default privileges for role loading_user in schema {{.Site}}_pcornet grant select on tables to peds_staff;
alter default privileges for role loading_user in schema {{.Site}}_id_maps grant select on tables to peds_staff;
"""

_vocabulary_permissions_sql_templ = """
grant all                  on schema dcc_ids    to loading_user;
grant all                  on schema vocabulary to loading_user;
grant usage                on schema vocabulary to achilles_user, dqa_user, pcor_et_user, harvest_user, peds_staff;
grant select on all tables in schema vocabulary to achilles_user, dqa_user, pcor_et_user, harvest_user, peds_staff;
alter default privileges for role loading_user in schema vocabulary grant select on tables to achilles_user, dqa_user, pcor_et_user, harvest_user, peds_staff;
"""

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


def _despace(s):
    """Return string with runs of spaces replaced with a single space"""
    return re.sub(r' +', ' ', s)


def _permissions_sql(site):
    """Return a list of statements to set the correct permissions for a given site.

    :param site: site name, e.g. 'dcc' or 'stlouis'
    :type: str
    :return: SQL statements
    :rtype: list(str)
    :raises: ValueError
    """
    tmpl = _permissions_sql_template
    sql = tmpl.replace('{{.Site}}', site)
    return [_despace(x) for x in sql.split("\n") if x]

def _vocabulary_permissions_sql():
    sql = _vocabulary_permissions_sql_templ
    return [_despace(x) for x in sql.split("\n") if x]


def grant_database_permissions(conn_str, database_name):
    """FILL IN
    """

    stmnts = StatementList()

    stmnts.extend(
        [Statement(x) for x in _database_privileges_sql(database_name)])

    stmnts.serial_execute(conn_str)

    for stmnt in stmnts:
        check_stmt_err(stmnt, 'granting database permissions')


def grant_schema_permissions(conn_str):
    """FILL IN
    """

    stmnts = StatementList()
    for site in SITES_AND_DCC:
        stmnts.extend([Statement(x) for x in _permissions_sql(site)])

    stmnts.serial_execute(conn_str)

    for stmnt in stmnts:
        check_stmt_err(stmnt, 'granting schema permissions')

def grant_vocabulary_permissions(conn_str):
    """FILL IN
    """

    stmnts = StatementList()
    stmnts.extend([Statement(x) for x in _vocabulary_permissions_sql()])

    stmnts.serial_execute(conn_str)

    for stmnt in stmnts:
        check_stmt_err(stmnt, 'granting vocabulary permissions')
    


