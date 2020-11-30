import logging
import time

from psycopg2 import errorcodes as psycopg2_errorcodes

from pedsnetdcc import VOCAB_TABLES, SITES
from pedsnetdcc.transform_runner import TRANSFORMS
from pedsnetdcc.db import Statement, StatementSet
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.foreign_keys import add_foreign_keys
from pedsnetdcc.indexes import add_indexes, drop_unneeded_indexes
from pedsnetdcc.not_nulls import set_not_nulls
from pedsnetdcc.primary_keys import add_primary_keys
from pedsnetdcc.utils import (combine_dicts, get_conn_info_dict,
                              stock_metadata, conn_str_with_search_path,
                              set_logged, vacuum)
from pedsnetdcc.concept_group_tables import create_index_replacement_tables

logger = logging.getLogger(__name__)

DCC_SCHEMA = "dcc_pedsnet"
VOCAB_SCHEMA = "vocabulary"

_sql_schema_tmpl = "{site_name}_pedsnet"

_sql_schema_alt_tmpl = "{site_name}_{alt_name}"

_sql_select_tmpl = """
SELECT {fields} FROM {site_schema}.{table_name}
UNION ALL
"""

_sql_create_tmpl = """
CREATE UNLOGGED TABLE {table_name} AS
    {unions}
"""

_sql_drop_tmpl = """
DROP TABLE {table_name} CASCADE
"""


def _check_stmt_err(stmt, force):
    """Check statement for errors.

    If the error is benign and force is true, ignore the error.

    :param pedsnetdcc.db.Statement stmt:
    :param bool force:
    :return: None
    :raise: DatabaseError if error in a statement
    :raise: psycopg2.OperationalError if connection error
    """
    if stmt.err is None:
        return

    dropping = stmt.sql.startswith('DROP')
    creating = stmt.sql.startswith('CREATE')

    # Detect error 42P01, meaning the table doesn't exist yet.
    does_not_exist = (
        hasattr(stmt.err, 'pgcode')
        and stmt.err.pgcode
        and psycopg2_errorcodes.lookup(stmt.err.pgcode) == 'UNDEFINED_OBJECT')

    # Detect error 42P07, meaning the table already exists.
    already_exists = (
        hasattr(stmt.err, 'pgcode')
        and stmt.err.pgcode
        and psycopg2_errorcodes.lookup(stmt.err.pgcode) == 'DUPLICATE_TABLE')

    if dropping and force and does_not_exist:
        return

    if creating and force and already_exists:
        return

    raise stmt.err


def merge_site_data(model_version, conn_str, addsites, force=False, notable=False, nolog=False, nopk=False,
                    nonull=False, noidx=False, nodrop=False, norep=False, nofk=False, novac=False):
    """Merge data from site schemas into the DCC schema

    Any schema passed with the conn_str is ignored. The user and password must
    have the appropriate permissions. The only benign error is a table already
    exists error, which is not really benign unless you know exactly what's
    going on, but it does allow for restarting after some but not all DCC
    tables have been constituted.

    :param str model_version: PEDSnet model version, e.g. X.Y.Z
    :param str conn_str:      libpq connection string
    :param str addsites:      sites to add
    :param bool force:        ignore benign errors if true; see https://github.com/PEDSnet/pedsnetdcc/issues/10
    :param bool notable:      skip creating tables is they already exist
    :param bool nolog:        skip setting tables to logged if already done
    :param bool nopk:         skip primary keys if already exist
    :param bool nonull:       skip set not null if already done
    :param bool noidx:        skip ndexes if already exist
    :param bool nodrop:       skip drop unused indexes if already done
    :param bool norep:        skip index replacement tables if already exist
    :param bool nofk:         skip foreign keys if already exist
    :param bool novac:        skip vaccuum if already done
    :return:                  True on success, False otherwise
    :rtype:                   bool
    :raises RuntimeError:     If any of the sql statements cause an error
    """  # noqa

    # Make sure the search_path is set to the dcc schema.
    conn_str = conn_str_with_search_path(conn_str, DCC_SCHEMA)

    # Log function start and set starting time.
    log_dict = combine_dicts({'model_version': model_version, 'force': force},
                             get_conn_info_dict(conn_str))
    task = 'merge site data'
    logger.info(combine_dicts(log_dict, {'msg': 'starting {0}'.format(task)}))
    start_time = time.time()

    # Initialize set of statements for parallel execution.
    stmts = StatementSet()

    # Get metadata.
    metadata = stock_metadata(model_version)
    for t in TRANSFORMS:
        metadata = t.modify_metadata(metadata)

    # Get Sites to add
    add_sites = addsites.split(",")

    merge_sites = list(set(SITES) + set(add_sites))

    # Build a merge statement for each non-vocab table.
    for table_name in set(metadata.tables.keys()) - set(VOCAB_TABLES):

        table = metadata.tables[table_name]

        fields = ','.join(table.c.keys())

        unions = ''

        # Add a union statement for each site.
        for site_name in merge_sites:

            site_schema = _sql_schema_tmpl.format(site_name=site_name)

            select = _sql_select_tmpl.format(fields=fields,
                                             site_schema=site_schema,
                                             table_name=table_name)

            unions = unions + select

        # Strip the final, unneeded, UNION ALL.
        i = unions.rfind('UNION ALL')
        unions = unions[:i]

        # Build the final sql statement.
        sql = _sql_create_tmpl.format(table_name=table_name, unions=unions)

        # Add the statement to the set with an informative m})sage for logging.
        stmts.add(Statement(sql, 'merge site {0} data into dcc schema'.
                            format(table_name)))


    # Execute the merge statements in parallel if not alreadsy done.
    if not notable:
        stmts.parallel_execute(conn_str)
        # Check the statements for any errors and log and raise if found.
        for stmt in stmts:
           try:
                _check_stmt_err(stmt, force)
           except:
                logger.error(combine_dicts({'msg': 'fatal error in {0}'.
                                            format(task), 'sql': stmt.sql,
                                            'err': str(stmt.err)}, log_dict))
                logger.info(combine_dicts({'msg': '{0} failed'.format(task),
                                           'elapsed': secs_since(start_time)},
                                          log_dict))
                raise

    # Set tables logged if not already done.
    if not nolog:
        set_logged(conn_str, model_version)

    # Add primary keys, not nulls, indexes, drop unneeded indexes, add foreign keys.
    # Create new tables to replace concept name/source value indexes
    if not nopk:
        add_primary_keys(conn_str, model_version, force)
    if not nonull:
        set_not_nulls(conn_str, model_version)
    if not noidx:
        add_indexes(conn_str, model_version, force)
    if not nodrop:
        drop_unneeded_indexes(conn_str, model_version, force)
    if not norep:
        create_index_replacement_tables(conn_str, model_version)

    # Change search_path to include the vocabulary schema and add foreign keys.
    conn_str = conn_str_with_search_path(conn_str, DCC_SCHEMA + ',' +
                                         VOCAB_SCHEMA)
    if not nofk:
        add_foreign_keys(conn_str, model_version, force)

    # Vacuum analyze tables for piney freshness.
    if not novac:
        vacuum(conn_str, model_version, analyze=True)

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished {0}'.format(task),
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, success!
    return True


def clear_dcc_data(model_version, conn_str, force=False):
    """Clear any existing data from the DCC schema

    This operation is completed by simply dropping the non-vocab tables.
    The only benign error is a table does not exist error. Any schema passed
    with the conn_str is ignored.

    :param str model_version: PEDSnet model version, e.g. X.Y.Z
    :param str conn_str:      libpq connection string
    :param bool force:        ignore benign errors if true; see https://github.com/PEDSnet/pedsnetdcc/issues/10
    :return:                  True on success, False otherwise
    :rtype:                   bool
    :raises RuntimeError:     If any of the sql statements cause an error
    """  # noqa

    # Make sure the search_path is set to the dcc schema.
    conn_str = conn_str_with_search_path(conn_str, DCC_SCHEMA)

    # Log function start and set starting time.
    log_dict = combine_dicts({'model_version': model_version, 'force': force},
                             get_conn_info_dict(conn_str))
    task = 'clear dcc data'
    logger.info(combine_dicts(log_dict, {'msg': 'starting {0}'.format(task)}))
    start_time = time.time()

    # Initialize set of statements for parallel execution.
    stmts = StatementSet()

    # Get metadata. TODO: Make this the result of the transformations.
    metadata = stock_metadata(model_version)

    # Build a drop statement for each non-vocab table.
    for table_name in set(metadata.tables.keys()) - set(VOCAB_TABLES):
        stmts.add(Statement(_sql_drop_tmpl.format(table_name=table_name)))

    # Execute the drop statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check the statements for any errors and log and raise if found.
    for stmt in stmts:
        try:
            _check_stmt_err(stmt, force)
        except:
            logger.error(combine_dicts({'msg': 'fatal error in {0}'.
                                        format(task), 'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': '{0} failed'.format(task),
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished {0}'.format(task),
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, success!
    return True


def merge_data_to_schema(model_version, conn_str, schema, altname, skipsites, addsites, force=False, notable=False, nolog=False, nopk=False,
                    nonull=False, noidx=False, nodrop=False, norep=False, nofk=False, novac=False):
    """Merge data from site schemas into the DCC schema

    Any schema passed with the conn_str is ignored. The user and password must
    have the appropriate permissions. The only benign error is a table already
    exists error, which is not really benign unless you know exactly what's
    going on, but it does allow for restarting after some but not all DCC
    tables have been constituted.

    :param str model_version: PEDSnet model version, e.g. X.Y.Z
    :param str conn_str:      libpq connection string
    :param str schema:        schema to merge into
    :param str altname:       alternate site schema naming
    :param str skipsites:     sites to skip
    :param str addsites:      sites to add
    :param bool force:        ignore benign errors if true; see https://github.com/PEDSnet/pedsnetdcc/issues/10
    :param bool notable:      skip creating tables is they already exist
    :param bool nolog:        skip setting tables to logged if already done
    :param bool nopk:         skip primary keys if already exist
    :param bool nonull:       skip set not null if already done
    :param bool noidx:        skip ndexes if already exist
    :param bool nodrop:       skip drop unused indexes if already done
    :param bool norep:        skip index replacement tables if already exist
    :param bool nofk:         skip foreign keys if already exist
    :param bool novac:        skip vaccuum if already done
    :return:                  True on success, False otherwise
    :rtype:                   bool
    :raises RuntimeError:     If any of the sql statements cause an error
    """  # noqa

    # Make sure the search_path is set to the dcc schema.
    conn_str = conn_str_with_search_path(conn_str, schema)

    # Log function start and set starting time.
    log_dict = combine_dicts({'model_version': model_version, 'force': force},
                             get_conn_info_dict(conn_str))
    task = 'merge site data'
    logger.info(combine_dicts(log_dict, {'msg': 'starting {0}'.format(task)}))
    start_time = time.time()

    # Initialize set of statements for parallel execution.
    stmts = StatementSet()

    # Get metadata.
    metadata = stock_metadata(model_version)
    for t in TRANSFORMS:
        metadata = t.modify_metadata(metadata)

    # Get Sites to skip
    skip_sites = skipsites.split(",")

    merge_sites = list(set(SITES) - set(skip_sites))

    # Get Sites to add
    add_sites = addsites.split(",")

    merge_sites = list(set(SITES) + set(add_sites))

    # Build a merge statement for each non-vocab table.
    for table_name in set(metadata.tables.keys()) - set(VOCAB_TABLES):

        table = metadata.tables[table_name]

        fields = ','.join(table.c.keys())

        unions = ''

        # Add a union statement for each site.
        for site_name in merge_sites:

            if altname == '':
                site_schema = _sql_schema_tmpl.format(site_name=site_name)
            else:
                site_schema = _sql_schema_alt_tmpl.format(site_name=site_name, alt_name=altname)

            select = _sql_select_tmpl.format(fields=fields,
                                             site_schema=site_schema,
                                             table_name=table_name)

            unions = unions + select

        # Strip the final, unneeded, UNION ALL.
        i = unions.rfind('UNION ALL')
        unions = unions[:i]

        # Build the final sql statement.
        sql = _sql_create_tmpl.format(table_name=table_name, unions=unions)

        # Add the statement to the set with an informative m})sage for logging.
        stmts.add(Statement(sql, 'merge site {0} data into selected schema'.
                            format(table_name)))


    # Execute the merge statements in parallel if not already done.
    if not notable:
        stmts.parallel_execute(conn_str)
        # Check the statements for any errors and log and raise if found.
        for stmt in stmts:
           try:
                _check_stmt_err(stmt, force)
           except:
                logger.error(combine_dicts({'msg': 'fatal error in {0}'.
                                            format(task), 'sql': stmt.sql,
                                            'err': str(stmt.err)}, log_dict))
                logger.info(combine_dicts({'msg': '{0} failed'.format(task),
                                           'elapsed': secs_since(start_time)},
                                          log_dict))
                raise

    # Set tables logged if not already done.
    if not nolog:
        set_logged(conn_str, model_version)

    # Add primary keys, not nulls, indexes, drop unneeded indexes, add foreign keys.
    # Create new tables to replace concept name/source value indexes
    if not nopk:
        add_primary_keys(conn_str, model_version, force)
    if not nonull:
        set_not_nulls(conn_str, model_version)
    if not noidx:
        add_indexes(conn_str, model_version, force)
    if not nodrop:
        drop_unneeded_indexes(conn_str, model_version, force)
    if not norep:
        create_index_replacement_tables(conn_str, model_version)

    # Change search_path to include the vocabulary schema and add foreign keys.
    conn_str = conn_str_with_search_path(conn_str, schema + ',' +
                                         VOCAB_SCHEMA)
    if not nofk:
        add_foreign_keys(conn_str, model_version, force)

    # Vacuum analyze tables for piney freshness.
    if not novac:
        vacuum(conn_str, model_version, analyze=True)

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished {0}'.format(task),
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, success!
    return True


def clear_schema_data(model_version, conn_str, schema, force=False):
    """Clear any existing data from the DCC schema

    This operation is completed by simply dropping the non-vocab tables.
    The only benign error is a table does not exist error. Any schema passed
    with the conn_str is ignored.

    :param str model_version: PEDSnet model version, e.g. X.Y.Z
    :param str conn_str:      libpq connection string
    :param str schema:        schema to clear
    :param bool force:        ignore benign errors if true; see https://github.com/PEDSnet/pedsnetdcc/issues/10
    :return:                  True on success, False otherwise
    :rtype:                   bool
    :raises RuntimeError:     If any of the sql statements cause an error
    """  # noqa

    # Make sure the search_path is set to the dcc schema.
    conn_str = conn_str_with_search_path(conn_str, schema)

    # Log function start and set starting time.
    log_dict = combine_dicts({'model_version': model_version, 'force': force},
                             get_conn_info_dict(conn_str))
    task = 'clear schema data'
    logger.info(combine_dicts(log_dict, {'msg': 'starting {0}'.format(task)}))
    start_time = time.time()

    # Initialize set of statements for parallel execution.
    stmts = StatementSet()

    # Get metadata. TODO: Make this the result of the transformations.
    metadata = stock_metadata(model_version)

    # Build a drop statement for each non-vocab table.
    for table_name in set(metadata.tables.keys()) - set(VOCAB_TABLES):
        stmts.add(Statement(_sql_drop_tmpl.format(table_name=table_name)))

    # Execute the drop statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check the statements for any errors and log and raise if found.
    for stmt in stmts:
        try:
            _check_stmt_err(stmt, force)
        except:
            logger.error(combine_dicts({'msg': 'fatal error in {0}'.
                                        format(task), 'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': '{0} failed'.format(task),
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished {0}'.format(task),
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, success!
    return True
