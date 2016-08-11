import logging

import sqlalchemy
import sqlalchemy.dialects.postgresql

from pedsnetdcc import TRANSFORMS, VOCAB_TABLES
from pedsnetdcc.db import Statement, StatementSet, StatementList
from pedsnetdcc.indexes import add_indexes
from pedsnetdcc.foreign_keys import add_foreign_keys
from pedsnetdcc.primary_keys import add_primary_keys
from pedsnetdcc.not_nulls import set_not_nulls
from pedsnetdcc.schema import (create_schema_statement, create_schema,
                               drop_schema_statement, primary_schema)
from pedsnetdcc.utils import (DatabaseError, get_conn_info_dict, combine_dicts,
                              stock_metadata, conn_str_with_search_path,
                              pg_error)


logger = logging.getLogger(__name__)


def _transform_select_sql(model_version, site, target_schema):
    """Create SQL for `select` statement transformations.

    The `search_path` only needs to contain the source schema; the target
    schema is embedded in the SQL statements.

    The returned statements are not sufficient for the transformation;
    the `pre_transform` needs to be run beforehand.

    Returns a set of tuples of (sql_string, msg), where msg is a description
    for the operation to be carried out by the sql_string.

    :param model_version: PEDSnet model version, e.g. 2.3.0
    :param site: site name, e.g. 'stlouis'
    :param target_schema: schema in which to create the transformed tables
    :return: set of tuples of SQL statement strings and messages
    :rtype: set
    :raise: psycopg2.ProgrammingError (from the modify_select)
    """
    metadata = stock_metadata(model_version)
    metadata.info['site'] = site
    stmt_pairs = set()
    for table_name, table in metadata.tables.items():
        if table_name in VOCAB_TABLES:
            continue

        select_obj = sqlalchemy.select([table])
        join_obj = table

        for transform in TRANSFORMS:

            select_obj, join_obj = transform.modify_select(
                metadata,
                table_name,
                select_obj,
                join_obj)

        final_select_obj = select_obj.select_from(join_obj)

        table_sql = str(
            final_select_obj.compile(
                dialect=sqlalchemy.dialects.postgresql.dialect()))

        final_sql = 'CREATE UNLOGGED TABLE {0}.{1} AS {2}'.format(
            target_schema, table_name, table_sql)
        msg = 'creating transformed copy of table {}'.format(table_name)

        stmt_pairs.add((final_sql, msg))

    return stmt_pairs


def _set_logged(conn_str, model_version):
    """Set non-vocab pedsnet tables to logged.

    TODO: this should probably be in utils.py or utils/db.py or something.

    `Logged` is the default state of PostgreSQL tables. Presumably for
    performance reasons, tables are sometimes created as `unlogged` prior
    to batch load.

    :param conn_str: pq connection string
    :param model_version: pedsnet model version
    :return:
    """
    metadata = stock_metadata(model_version)
    stmts = StatementSet()
    sql_tpl = 'alter table {} set logged'
    msg_tpl = 'setting table {} to logged'
    for table in (set(metadata.tables.keys()) - set(VOCAB_TABLES)):
        stmts.add(Statement(sql_tpl.format(table), msg_tpl.format(table)))
    stmts.parallel_execute(conn_str)
    for stmt in stmts:
        if stmt.err:
            raise DatabaseError(
                'setting tables to logged: {}: {}'.format(stmt.sql, stmt.err))


def _transform(conn_str, model_version, site, target_schema, force=False):
    """Run transformations.

    TODO: Check whether exception handling is consistent e.g. DatabaseError.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version
    :param str target_schema: temporary schema to hold transformed tables
    :return: list of SQL statement strings
    :raise: psycopg2.ProgrammingError (from pre_transform)
    """

    for transform in TRANSFORMS:
        transform.pre_transform(conn_str)

    # TODO: revert to StatementSet/parallel below
    stmts = StatementList()
    for sql, msg in _transform_select_sql(model_version, site, target_schema):
        stmts.append(Statement(sql, msg))

    # Execute creation of transformed tables in parallel.
    # Note that the target schema is embedded in the SQL statements.
    stmts.serial_execute(conn_str)
    for stmt in stmts:
        # TODO: should we log all the individual errors at ERROR level?
        if stmt.err:
            if force and pg_error(stmt) == 'DUPLICATE_TABLE':
                return
            raise DatabaseError('{msg}: {err}'.format(msg=stmt.msg,
                                                      err=stmt.err))


def _move_tables_statements(model_version, from_schema, to_schema):
    """Return StatementList to move pedsnet tables from one schema to another.

    Vocabulary tables are ignored.

    :param str model_version: pedsnet model version
    :param str from_schema: source schema
    :param str to_schema: destination schema
    :return: list of statements
    :rtype: StatementList
    """
    stmts = StatementList()
    metadata = stock_metadata(model_version)
    move_tpl = 'ALTER TABLE {from_sch}.{tbl} SET SCHEMA {to_sch}'
    msg_tpl = 'moving {tbl} from {from_sch} to {to_sch}'

    for table_name in set(metadata.tables.keys()) - set(VOCAB_TABLES):
        tpl_vals = {'from_sch': from_schema, 'to_sch': to_schema,
                    'tbl': table_name}
        stmts.append(Statement(move_tpl.format(**tpl_vals),
                               msg_tpl.format(**tpl_vals)))
    return stmts


def run_transformation(conn_str, model_version, site, search_path,
                       force=False):
    """Run all transformations, backing up existing tables to a backup schema.

    * Create new schema FOO_transformed.
    * Create transformed tables in FOO_schema.
    * If there is a FOO_backup schema, drop it.
    * Create the FOO_backup schema.
    * Move the PEDSnet core tables from FOO into FOO_backup schema.
    * Move the transformed PEDSnet core tables from the FOO_transformed
    schema to the FOO schema.
    * The previous two steps can be done in a transaction.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str site: site label, e.g. 'stlouis'
    :param str search_path: PostgreSQL schema search path
    :param bool force: if True, ignore benign errors
    :return: True if no exception raised
    :rtype: bool
    """
    task = 'running transformation'
    log_dict = combine_dicts({'model_version': model_version,
                              'search_path': search_path, 'force': force},
                             get_conn_info_dict(conn_str))

    try:
        schema = primary_schema(search_path)
    except ValueError as err:
        logger.error(combine_dicts({'msg': 'error ' + task, 'err': err},
                                   log_dict))
        raise

    # Create the schema to hold the transformed tables.
    tmp_schema = schema + '_' + 'transformed'
    try:
        create_schema(conn_str, tmp_schema, force)
    except Exception as err:
        logger.error(combine_dicts({'msg': 'error ' + task, 'err': err},
                                   log_dict))
        raise

    # Perform the transformation.
    try:
        _transform(conn_str, model_version, site, tmp_schema, force)
    except Exception as err:
        logger.error(combine_dicts({'msg': 'error ' + task, 'err': err},
                                   log_dict))
        raise

    # TODO: should we catch all exceptions and perform logger.error?

    # Set up new connection string for manipulating the target schema
    new_search_path = ','.join((tmp_schema, schema))
    new_conn_str = conn_str_with_search_path(conn_str, new_search_path)

    # Set tables to logged
    _set_logged(new_conn_str, model_version)

    # Add primary keys to the transformed tables
    add_primary_keys(new_conn_str, model_version, force)

    # Add NOT NULL constraints to the transformed tables (no force option)
    set_not_nulls(new_conn_str, model_version)

    # Add indexes to the transformed tables
    add_indexes(new_conn_str, model_version, force)

    # Add constraints to the transformed tables
    add_foreign_keys(new_conn_str, model_version, force)

    # Move the old tables to a backup schema and move the new ones into
    # the original schema; then drop the temporary schema.
    backup_schema = schema + '_backup'

    stmts = StatementList()
    stmts.append(
        drop_schema_statement(backup_schema, if_exists=True, cascade=True))
    stmts.append(create_schema_statement(backup_schema))
    stmts.extend(_move_tables_statements(model_version, schema, backup_schema))
    stmts.extend(_move_tables_statements(model_version, tmp_schema, schema))
    stmts.append(
        drop_schema_statement(tmp_schema, if_exists=False, cascade=True))
    stmts.serial_execute(conn_str, transaction=True)
    for stmt in stmts:
        # Must check through all results to find the first (and last) real
        # error that caused the transaction to fail.
        if stmt.err:
            if pg_error(stmt) == 'IN_FAILED_SQL_TRANSACTION':
                continue
            logger.error(combine_dicts({'msg': 'error ' + task,
                                        'submsg': stmt.msg,
                                        'err': stmt.err,
                                        'sql': stmt.sql},
                                       log_dict))
            tpl = 'moving tables after transformation ({sql}): {err}'
            raise DatabaseError(tpl.format(sql=stmt.sql, err=stmt.err))

    return True
