import logging
import time

import sqlalchemy
import sqlalchemy.dialects.postgresql

from pedsnetdcc import VOCAB_TABLES
from pedsnetdcc.db import Statement, StatementSet, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.indexes import add_indexes, drop_indexes
from pedsnetdcc.foreign_keys import add_foreign_keys, drop_foreign_keys
from pedsnetdcc.primary_keys import add_primary_keys
from pedsnetdcc.not_nulls import set_not_nulls
from pedsnetdcc.schema import (create_schema_statement, create_schema,
                               drop_schema_statement, primary_schema,
                               schema_exists, tables_in_schema)
from pedsnetdcc.utils import (DatabaseError, get_conn_info_dict, combine_dicts,
                              stock_metadata, conn_str_with_search_path,
                              pg_error, set_logged)

from pedsnetdcc.age_transform import AgeTransform
from pedsnetdcc.concept_name_transform import ConceptNameTransform
from pedsnetdcc.site_name_transform import SiteNameTransform
from pedsnetdcc.id_mapping_transform import IDMappingTransform
from pedsnetdcc.permissions import grant_database_permissions, grant_schema_permissions, grant_vocabulary_permissions

logger = logging.getLogger(__name__)

TRANSFORMS = (AgeTransform, ConceptNameTransform, SiteNameTransform,
              IDMappingTransform)


def _transform_select_sql(model_version, site, target_schema, target_table, entity):
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

    table_list = metadata.tables.items()

    if target_table:
        table_list = [(target_table, metadata.tables.get(entity))]
        print(table_list)

    for table_name, table in table_list:
        if table_name in VOCAB_TABLES:
            continue

        select_obj = sqlalchemy.select([table])
        join_obj = table

        for transform in TRANSFORMS:
            select_obj, join_obj = transform.modify_select(
                metadata,
                entity,
                select_obj,
                join_obj)

        final_select_obj = select_obj.select_from(join_obj)

        table_sql_obj = final_select_obj.compile(
            dialect=sqlalchemy.dialects.postgresql.dialect())

        table_sql = str(table_sql_obj) % table_sql_obj.params

        final_sql = 'CREATE UNLOGGED TABLE {0}.{1} AS {2}'.format(
            target_schema, table_name, table_sql)
        msg = 'creating transformed copy of table {}'.format(table_name)

        stmt_pairs.add((final_sql, msg))

    return stmt_pairs


def _transform(conn_str, model_version, site, target_schema, force=False, target_table=None, entity=None):
    """Run transformations.

    TODO: Check whether exception handling is consistent e.g. DatabaseError.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version
    :param str target_schema: temporary schema to hold transformed tables
    :return: list of SQL statement strings
    :raise: psycopg2.ProgrammingError (from pre_transform)
    """

    logger.info({'target_table': target_table, 'entity': entity})
    for transform in TRANSFORMS:
        transform.pre_transform(conn_str, stock_metadata(model_version), target_table, entity)

    stmts = StatementSet()
    for sql, msg in _transform_select_sql(model_version, site, target_schema, target_table, entity):
        stmts.add(Statement(sql, msg))

    # Execute creation of transformed tables in parallel.
    # Note that the target schema is embedded in the SQL statements.
    stmts.parallel_execute(conn_str)
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


def _drop_tables_statements(model_version, schema, if_exists=False):
    """Return StatementList to drop pedsnet tables in the specified schema.

    Vocabulary tables are ignored.  If `if_exists` is true, then the
    `IF EXISTS` clause will be used to avoid errors if the tables don't
    exist.

    Foreign keys must have been dropped beforehand; otherwise, errors will
    occur.

    :param str model_version: pedsnet model version
    :param str schema: schema name
    :param bool if_exists: use IF EXISTS clause
    :return: list of statements
    :rtype: StatementList
    """
    stmts = StatementList()
    metadata = stock_metadata(model_version)
    move_tpl = 'DROP TABLE {if_exists} {sch}.{tbl}'
    msg_tpl = 'dropping {tbl} in {sch}'

    if_exists_clause = 'IF EXISTS' if if_exists else ''

    for table_name in set(metadata.tables.keys()) - set(VOCAB_TABLES):
        tpl_vals = {'sch': schema, 'tbl': table_name,
                    'if_exists': if_exists_clause}
        stmts.append(Statement(move_tpl.format(**tpl_vals),
                               msg_tpl.format(**tpl_vals)))
    return stmts


def run_transformation(conn_str, model_version, site, search_path,
                       force=False, target_table=None, entity=None):
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
    :raise: various possible exceptions ...
    """
    log_dict = combine_dicts({'model_version': model_version,
                              'search_path': search_path, 'force': force},
                             get_conn_info_dict(conn_str))

    task = 'running transformation'
    start_time = time.time()
    # TODO: define spec for computer readable log messages
    # E.g. we might want both 'task' and 'msg' keys, maybe 'submsg'
    logger.info(combine_dicts({'msg': 'started {}'.format(task)}, log_dict))
    logger.info({'target_table': target_table, 'entity': entity})

    # TODO: should we catch all exceptions and perform logger.error?
    # and a logger.info to record the elapsed time at abort.

    # TODO: do we need to validate the primary schema at all?
    schema = primary_schema(search_path)

    # Create the schema to hold the transformed tables.
    tmp_schema = schema + '_' + 'transformed'
    create_schema(conn_str, tmp_schema, force)

    # Perform the transformation.
    _transform(conn_str, model_version, site, tmp_schema, force, target_table, entity)

    # Set up new connection string for manipulating the target schema
    new_search_path = ','.join((tmp_schema, schema, 'vocabulary'))
    new_conn_str = conn_str_with_search_path(conn_str, new_search_path)

    # Set tables to logged
    set_logged(new_conn_str, model_version)

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
            logger.info(combine_dicts({'msg': 'aborted {}'.format(task),
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            tpl = 'moving tables after transformation ({sql}): {err}'
            raise DatabaseError(tpl.format(sql=stmt.sql, err=stmt.err))

    ## Regrant permissions after renaming schemas
    grant_schema_permissions(new_conn_str)
    grant_vocabulary_permissions(new_conn_str)

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    return True


def undo_transformation(conn_str, model_version, search_path):
    """Revert a transformation by restoring tables from the backup schema.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str search_path: PostgreSQL schema search path
    :return: True if no exception raised
    :rtype: bool
    """
    task = 'undoing transformation'
    log_dict = combine_dicts({'model_version': model_version,
                              'search_path': search_path},
                             get_conn_info_dict(conn_str))

    start_time = time.time()
    logger.info(combine_dicts({'msg': 'started {}'.format(task)}, log_dict))

    schema = primary_schema(search_path)

    backup_schema = schema + '_' + 'backup'

    # Verify existence of backup schema.
    if not schema_exists(conn_str, backup_schema):
        msg = 'Cannot undo transformation; schema `{}` missing'.format(
            backup_schema)
        raise RuntimeError(msg)

    # Verify existence of data model tables names in backup schema.
    metadata = stock_metadata(model_version)
    backup_tables = tables_in_schema(conn_str, backup_schema)
    expected_tables = set(metadata.tables.keys()) - set(VOCAB_TABLES)
    if backup_tables != expected_tables:
        msg = ('Cannot undo transformation; backup schema `{sch}` has '
               'different tables ({b_tbls}) from data model ({e_tbls})')
        raise RuntimeError(msg.format(sch=backup_schema, b_tbls=backup_tables,
                                      e_tbls=expected_tables))

    # A single transaction for everything would be better but we can't
    # do that yet.
    drop_foreign_keys(conn_str, model_version, force=True)
    drop_indexes(conn_str, model_version, force=True)

    stmts = StatementList()
    stmts.extend(_drop_tables_statements(model_version, schema))
    stmts.extend(_move_tables_statements(model_version, backup_schema, schema))
    stmts.append(
        drop_schema_statement(backup_schema, if_exists=False, cascade=True))
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
            logger.info(combine_dicts({'msg': 'aborted {}'.format(task),
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            tpl = 'undoing transformation ({sql}): {err}'
            raise DatabaseError(tpl.format(sql=stmt.sql, err=stmt.err))

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))
