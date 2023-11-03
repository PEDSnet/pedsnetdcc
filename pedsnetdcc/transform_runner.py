import logging
import time

import sqlalchemy
import sqlalchemy.dialects.postgresql

from pedsnetdcc import VOCAB_TABLES
from pedsnetdcc.db import Statement, StatementSet, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.indexes import add_indexes, drop_indexes, drop_unneeded_indexes, add_vocab_indexes, \
    drop_vocab_unneeded_indexes
from pedsnetdcc.foreign_keys import add_foreign_keys, drop_foreign_keys
from pedsnetdcc.primary_keys import add_primary_keys
from pedsnetdcc.not_nulls import set_not_nulls
from pedsnetdcc.schema import (create_schema_statement, create_schema,
                               drop_schema_statement, primary_schema,
                               schema_exists, tables_in_schema)
from pedsnetdcc.utils import (DatabaseError, get_conn_info_dict, combine_dicts,
                              stock_metadata, conn_str_with_search_path,
                              pg_error, set_logged, check_stmt_err, check_stmt_data)

from pedsnetdcc.age_transform import AgeTransform
from pedsnetdcc.concept_name_transform import ConceptNameTransform
from pedsnetdcc.site_name_transform import SiteNameTransform
from pedsnetdcc.id_mapping_transform import IDMappingTransform
from pedsnetdcc.add_index_transform import AddIndexTransform
from pedsnetdcc.permissions import grant_database_permissions, grant_schema_permissions, \
                                    grant_vocabulary_permissions, grant_schema_permissions_limited, \
                                    grant_vocabulary_only_permissions_limited
from pedsnetdcc.concept_group_tables import create_index_replacement_tables

logger = logging.getLogger(__name__)

TRANSFORMS = (AgeTransform, ConceptNameTransform, SiteNameTransform,
              IDMappingTransform, AddIndexTransform)


def _transform_select_sql(model_version, site, target_schema, id_name, id_type, logged):
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
    :param id_name: name of the id set
    :param id_type: type of the site id set
    :param logged: created logged table
    :return: set of tuples of SQL statement strings and messages
    :rtype: set
    :raise: psycopg2.ProgrammingError (from the modify_select)
    """
    metadata = stock_metadata(model_version)
    metadata.info['site'] = site

    stmt_pairs = set()x
    for table_name, table in metadata.tables.items():
        if table_name in VOCAB_TABLES:
            continue
            
        # temp fix for cohort table multi-column primary key
        if table_name == 'cohort':
            continue

        select_obj = sqlalchemy.select([table])
        join_obj = table

        for transform in TRANSFORMS:
                select_obj, join_obj = transform.modify_select(
                    metadata,
                    table_name,
                    select_obj,
                    join_obj,
                    id_name,
                    id_type)

        final_select_obj = select_obj.select_from(join_obj)

        table_sql_obj = final_select_obj.compile(
            dialect=sqlalchemy.dialects.postgresql.dialect())

        table_sql = str(table_sql_obj) % table_sql_obj.params

        if logged:
            final_sql = 'CREATE TABLE {0}.{1} AS {2}'.format(
                target_schema, table_name, table_sql)
        else:
            final_sql = 'CREATE UNLOGGED TABLE {0}.{1} AS {2}'.format(
                target_schema, table_name, table_sql)
        msg = 'creating transformed copy of table {}'.format(table_name)

        stmt_pairs.add((final_sql, msg))

    return stmt_pairs


def _transform_target_select_sql(model_version, site, target_schema, id_name, id_type, target_table):
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
    :param id_name: name of the id set
    :param id_type: type of the site id set
    :param target_table: table to use
    :return: set of tuples of SQL statement strings and messages
    :rtype: set
    :raise: psycopg2.ProgrammingError (from the modify_select)
    """
    target_table = target_table.split(",")
    metadata = stock_metadata(model_version)
    metadata.info['site'] = site
    stmt_pairs = set()
    for table_name, table in metadata.tables.items():
        if table_name in VOCAB_TABLES:
            continue
        if table_name not in target_table:
            continue

        select_obj = sqlalchemy.select([table])
        join_obj = table

        for transform in TRANSFORMS:
            select_obj, join_obj = transform.modify_select(
                metadata,
                table_name,
                select_obj,
                join_obj,
                id_name,
                id_type)

        final_select_obj = select_obj.select_from(join_obj)

        table_sql_obj = final_select_obj.compile(
            dialect=sqlalchemy.dialects.postgresql.dialect())

        table_sql = str(table_sql_obj) % table_sql_obj.params

        final_sql = 'CREATE UNLOGGED TABLE {0}.{1} AS {2}'.format(
            target_schema, table_name, table_sql)
        msg = 'creating transformed copy of table {}'.format(table_name)

        stmt_pairs.add((final_sql, msg))

    return stmt_pairs


def _transform_age_select_sql(model_version, site, target_schema, target_table):
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
    :param target_table: table to use
    :return: set of tuples of SQL statement strings and messages
    :rtype: set
    :raise: psycopg2.ProgrammingError (from the modify_select)
    """
    metadata = stock_metadata(model_version)
    metadata.info['site'] = site
    stmt_pairs = set()
    target_table = target_table.split(",")

    for table_name, table in metadata.tables.items():
        if table_name in target_table:

            select_obj = sqlalchemy.select([table])
            join_obj = table

            select_obj, join_obj = AgeTransform.modify_select(
                metadata,
                table_name,
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


def _transform_concept_select_sql(model_version, site, target_schema, target_table):
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
    :param target_table: table to use
    :return: set of tuples of SQL statement strings and messages
    :rtype: set
    :raise: psycopg2.ProgrammingError (from the modify_select)
    """
    metadata = stock_metadata(model_version)
    metadata.info['site'] = site
    stmt_pairs = set()
    target_table = target_table.split(",")

    for table_name, table in metadata.tables.items():
        if table_name in target_table:

            select_obj = sqlalchemy.select([table])
            join_obj = table

            select_obj, join_obj = ConceptNameTransform.modify_select(
                metadata,
                table_name,
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


def _transform_site_select_sql(model_version, site, target_schema, target_table):
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
    :param target_table: table to use
    :return: set of tuples of SQL statement strings and messages
    :rtype: set
    :raise: psycopg2.ProgrammingError (from the modify_select)
    """
    metadata = stock_metadata(model_version)
    metadata.info['site'] = site
    stmt_pairs = set()
    target_table = target_table.split(",")

    for table_name, table in metadata.tables.items():
        if table_name in target_table:

            select_obj = sqlalchemy.select([table])
            join_obj = table

            select_obj, join_obj = SiteNameTransform.modify_select(
                metadata,
                table_name,
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


def _transform_id_select_sql(model_version, site, target_schema, target_table, id_name, id_type):
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
    :param target_table: table to use
    :param id_name: name of the id set
    :param id_type: type of the site id (BigInteger vs String(256)
    :return: set of tuples of SQL statement strings and messages
    :rtype: set
    :raise: psycopg2.ProgrammingError (from the modify_select)
    """
    metadata = stock_metadata(model_version)
    metadata.info['site'] = site

    stmt_pairs = set()
    target_table = target_table.split(",")

    for table_name, table in metadata.tables.items():
        if table_name in target_table:

            select_obj = sqlalchemy.select([table])
            join_obj = table

            select_obj, join_obj = IDMappingTransform.modify_select(
                metadata,
                table_name,
                select_obj,
                join_obj,
                id_name,
                id_type)

            final_select_obj = select_obj.select_from(join_obj)

            table_sql_obj = final_select_obj.compile(
                dialect=sqlalchemy.dialects.postgresql.dialect())

            table_sql = str(table_sql_obj) % table_sql_obj.params

            final_sql = 'CREATE UNLOGGED TABLE {0}.{1} AS {2}'.format(
                target_schema, table_name, table_sql)
            msg = 'creating transformed copy of table {}'.format(table_name)

            stmt_pairs.add((final_sql, msg))

    return stmt_pairs


def _transform_index_select_sql(model_version, site, target_schema, target_table):
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
    :param target_table: table to use
    :return: set of tuples of SQL statement strings and messages
    :rtype: set
    :raise: psycopg2.ProgrammingError (from the modify_select)
    """
    metadata = stock_metadata(model_version)
    metadata.info['site'] = site
    stmt_pairs = set()
    target_table = target_table.split(",")

    for table_name, table in metadata.tables.items():
        if table_name in target_table:

            select_obj = sqlalchemy.select([table])
            join_obj = table

            select_obj, join_obj = AddIndexTransform.modify_select(
                metadata,
                table_name,
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


def _transform(conn_str, model_version, site, target_schema, id_name, id_type, logged, pool_limit, force=False):
    """Run transformations.

    TODO: Check whether exception handling is consistent e.g. DatabaseError.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version
    :param str target_schema: temporary schema to hold transformed tables
    :param str id_name: name of the id set
    :param str id_type: type of the site id set
    :param bool logged: if True create logged table
    :param bool pool_limit: if True limit pool to 1
    :return: list of SQL statement strings
    :raise: psycopg2.ProgrammingError (from pre_transform)
    """

    for transform in TRANSFORMS:
        transform.pre_transform(conn_str, stock_metadata(model_version), id_name, id_type)

    stmts = StatementSet()
    for sql, msg in _transform_select_sql(model_version, site, target_schema, id_name, id_type, logged):
        stmts.add(Statement(sql, msg))

    # Execute creation of transformed tables in parallel.
    # Note that the target schema is embedded in the SQL statements.
    if pool_limit:
        pool_size = 1
    else:
        pool_size = 25

    stmts.parallel_execute(conn_str, pool_size)
    for stmt in stmts:
        # TODO: should we log all the individual errors at ERROR level?
        if stmt.err:
            if force and pg_error(stmt) == 'DUPLICATE_TABLE':
                return
            raise DatabaseError('{msg}: {err}'.format(msg=stmt.msg,
                                                      err=stmt.err))


def _transform_target(conn_str, model_version, site, target_schema, id_name, id_type, target_table, force=False):
    """Run transformations.

    TODO: Check whether exception handling is consistent e.g. DatabaseError.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version
    :param str target_schema: temporary schema to hold transformed tables
    :param str id_name: name of the id set
    :param str id_type: type of the site id set
    :param target_table: table to use
    :return: list of SQL statement strings
    :raise: psycopg2.ProgrammingError (from pre_transform)
    """

    for transform in TRANSFORMS:
            transform.pre_transform(conn_str, stock_metadata(model_version), id_name, id_type)

    stmts = StatementSet()
    for sql, msg in _transform_target_select_sql(model_version, site, target_schema, id_name, id_type, target_table):
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


def _transform_age(conn_str, model_version, site, target_schema, target_table, force=False):
    """Run transformations.

    TODO: Check whether exception handling is consistent e.g. DatabaseError.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version
    :param str target_schema: temporary schema to hold transformed tables
    :param str target_table: transform table
    :return: list of SQL statement strings
    :raise: psycopg2.ProgrammingError (from pre_transform)
    """

    AgeTransform.pre_transform(conn_str, stock_metadata(model_version))

    stmts = StatementSet()
    for sql, msg in _transform_age_select_sql(model_version, site, target_schema, target_table):
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


def _transform_concept(conn_str, model_version, site, target_schema, target_table, force=False):
    """Run transformations.

    TODO: Check whether exception handling is consistent e.g. DatabaseError.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version
    :param str target_schema: temporary schema to hold transformed tables
    :param str target_table: transform table
    :return: list of SQL statement strings
    :raise: psycopg2.ProgrammingError (from pre_transform)
    """

    AgeTransform.pre_transform(conn_str, stock_metadata(model_version))

    stmts = StatementSet()
    for sql, msg in _transform_concept_select_sql(model_version, site, target_schema, target_table):
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


def _transform_site(conn_str, model_version, site, target_schema, target_table, force=False):
    """Run transformations.

    TODO: Check whether exception handling is consistent e.g. DatabaseError.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version
    :param str target_schema: temporary schema to hold transformed tables
    :param str target_table: transform table
    :return: list of SQL statement strings
    :raise: psycopg2.ProgrammingError (from pre_transform)
    """

    SiteNameTransform.pre_transform(conn_str, stock_metadata(model_version))

    stmts = StatementSet()
    for sql, msg in _transform_site_select_sql(model_version, site, target_schema, target_table):
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


def _transform_id(conn_str, model_version, site, target_schema, target_table, id_name, id_type, force=False):
    """Run transformations.

    TODO: Check whether exception handling is consistent e.g. DatabaseError.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version
    :param str target_schema: temporary schema to hold transformed tables
    :param str target_table: transform table
    :param str id_name: name of the id set
    :param str id_type: type of the site id set
    :return: list of SQL statement strings
    :raise: psycopg2.ProgrammingError (from pre_transform)
    """

    IDMappingTransform.pre_transform(conn_str, stock_metadata(model_version), id_name, id_type)

    stmts = StatementSet()
    for sql, msg in _transform_id_select_sql(model_version, site, target_schema, target_table, id_name, id_type):
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


def _transform_index(conn_str, model_version, site, target_schema, target_table, force=False):
    """Run transformations.

    TODO: Check whether exception handling is consistent e.g. DatabaseError.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version
    :param str target_schema: temporary schema to hold transformed tables
    :param str target_table: transform table
    :return: list of SQL statement strings
    :raise: psycopg2.ProgrammingError (from pre_transform)
    """

    AddIndexTransform.pre_transform(conn_str, stock_metadata(model_version))

    stmts = StatementSet()
    for sql, msg in _transform_index_select_sql(model_version, site, target_schema, target_table):
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


def _move_target_tables_statements(model_version, from_schema, to_schema, target_table):
    """Return StatementList to move pedsnet tables from one schema to another.

    Vocabulary tables are ignored.

    :param str model_version: pedsnet model version
    :param str from_schema: source schema
    :param str to_schema: destination schema
    :param str target_table: transform table
    :return: list of statements
    :rtype: StatementList
    """

    target_table = target_table.split(",")
    stmts = StatementList()
    metadata = stock_metadata(model_version)
    move_tpl = 'ALTER TABLE {from_sch}.{tbl} SET SCHEMA {to_sch}'
    msg_tpl = 'moving {tbl} from {from_sch} to {to_sch}'

    for table_name in set(metadata.tables.keys()) - set(VOCAB_TABLES):
        if table_name in target_table:
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


def _drop_target_tables_statements(model_version, schema, target_table, if_exists=False):
    """Return StatementList to drop pedsnet tables in the specified schema.

    Vocabulary tables are ignored.  If `if_exists` is true, then the
    `IF EXISTS` clause will be used to avoid errors if the tables don't
    exist.

    Foreign keys must have been dropped beforehand; otherwise, errors will
    occur.

    :param str model_version: pedsnet model version
    :param str schema: schema name
    :param str target_table: transform table
    :param bool if_exists: use IF EXISTS clause
    :return: list of statements
    :rtype: StatementList
    """

    target_table = target_table.split(",")
    stmts = StatementList()
    metadata = stock_metadata(model_version)
    move_tpl = 'DROP TABLE {if_exists} {sch}.{tbl}'
    msg_tpl = 'dropping {tbl} in {sch}'

    if_exists_clause = 'IF EXISTS' if if_exists else ''

    for table_name in set(metadata.tables.keys()) - set(VOCAB_TABLES):
        if table_name in target_table:
            tpl_vals = {'sch': schema, 'tbl': table_name,
                        'if_exists': if_exists_clause}
            stmts.append(Statement(move_tpl.format(**tpl_vals),
                                   msg_tpl.format(**tpl_vals)))
    return stmts


def _adjust_specialty_entity_ids(conn_str, schema):
    update_specialty_sql = """
        UPDATE {0}.specialty s
        SET entity_id=et.entity_id
        FROM (
            select site_id, care_site_id as entity_id, 'CARE_SITE' as entity_type
            from {0}.care_site
            union
            select site_id, provider_id as entity_id, 'PROVIDER' as entity_type
            from {0}.provider
        ) et
        WHERE s.entity_id::text = et.site_id::text and s.domain_id = et.entity_type;
    """
    update_specialty_msg = "updating {0}.specialty entity_id"

    # Update the entity_id
    update_specialty_stmt = Statement(update_specialty_sql.format(schema), update_specialty_msg.format(schema))

    # Execute the statement and ensure it didn't error
    update_specialty_stmt.execute(conn_str)
    check_stmt_err(update_specialty_stmt, 'update specialty entity_id')

    # If reached without error, then success!
    return True


def run_transformation(conn_str, model_version, site, search_path, id_name, id_type, logged, post_only, pool_limit,
                       nopk=False, nonull=False, noidx=False, nodrop=False, nofk=False,
                       limit=False, owner='loading_user',  force=False):
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
    :param str id_name: name of the id ex: dcc or onco
    :param str id_type: type of the site id: BigInteger or String
    :param bool logged: if True, create logged tables
    :param bool post_only: if True, do only post transform tasks
    :param bool pool_limit: if True, limit pool size to 1
    :param bool limit: if True, limit permissions to owner
    :param str owner: role to give permissions to if limited
    :param bool nopk:         skip primary keys if already exist
    :param bool nonull:       skip set not null if already done
    :param bool noidx:        skip ndexes if already exist
    :param bool nodrop:       skip drop unused indexes if already done
    :param bool nofk:         skip foreign keys if already exist
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

    # TODO: should we catch all exceptions and perform logger.error?
    # and a logger.info to record the elapsed time at abort.

    # TODO: do we need to validate the primary schema at all?
    schema = primary_schema(search_path)

    # Create the schema to hold the transformed tables.
    tmp_schema = schema + '_' + 'transformed'
    if not post_only:
        create_schema(conn_str, tmp_schema, force)

        # Perform the transformation.
        _transform(conn_str, model_version, site, tmp_schema, id_name, id_type, logged, pool_limit, force)

    # Set up new connection string for manipulating the target schema
    new_search_path = ','.join((tmp_schema, schema, 'vocabulary'))
    new_conn_str = conn_str_with_search_path(conn_str, new_search_path)

    # Set tables to logged
    if not logged:
        set_logged(new_conn_str, model_version)

    # Add primary keys to the transformed tables
    if not nopk:
        add_primary_keys(new_conn_str, model_version, force)

    # Update the speciality.entity_id based on domain_id
    _adjust_specialty_entity_ids(new_conn_str, tmp_schema)

    # Add NOT NULL constraints to the transformed tables (no force option)
    if not nonull:
        set_not_nulls(new_conn_str, model_version)

    # Add indexes to the transformed tables
    if not noidx:
        add_indexes(new_conn_str, model_version, force)

    # Drop unneeded indexes from the transformed tables
    if not nodrop:
        drop_unneeded_indexes(new_conn_str, model_version, force)

    # Add constraints to the transformed tables
    if not nofk:
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

    # Regrant permissions after renaming schemas

    if limit:
        grant_schema_permissions_limited(new_conn_str, False, owner, id_name, (site,))
        grant_vocabulary_only_permissions_limited(new_conn_str, owner)
    else:
        grant_schema_permissions(new_conn_str)
        grant_vocabulary_permissions(new_conn_str)

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    return True


def run_target_transformation(conn_str, model_version, site, search_path, target_table, id_name, id_type, force=False):
    """Run transformation on target tables.

    * Create new schema FOO_transformed.
    * Create transformed tables in FOO_schema.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str site: site label, e.g. 'stlouis'
    :param str search_path: PostgreSQL schema search path
    :param str target_table: table to transform
    :param str id_name: name of the id (ex. dcc or onco)
    :param str id_type: type of the id (BigInteger or String)
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

    # TODO: should we catch all exceptions and perform logger.error?
    # and a logger.info to record the elapsed time at abort.

    # TODO: do we need to validate the primary schema at all?
    schema = primary_schema(search_path)

    # Create the schema to hold the transformed tables.
    tmp_schema = schema + '_' + 'transformed'
    create_schema(conn_str, tmp_schema, force)

    # Perform the transformation.
    task = 'running transformations'
    _transform_target(conn_str, model_version, site, tmp_schema, id_name, id_type, target_table, force)

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    # Set up new connection string for manipulating the target schema
    new_search_path = ','.join((tmp_schema, schema, 'vocabulary'))
    new_conn_str = conn_str_with_search_path(conn_str, new_search_path)

    # Set tables to logged
    set_logged(new_conn_str, model_version, False, target_table.split(","))

    # Add primary keys to the transformed tables
    add_primary_keys(new_conn_str, model_version, True)

    # Move the old tables to a backup schema and move the new ones into
    # the original schema; then drop the temporary schema.
    backup_schema = schema + '_backup'

    stmts = StatementList()
    stmts.append(
        drop_schema_statement(backup_schema, if_exists=True, cascade=True))
    stmts.append(create_schema_statement(backup_schema))
    stmts.extend(_move_target_tables_statements(model_version, schema, backup_schema, target_table))
    stmts.extend(_move_target_tables_statements(model_version, tmp_schema, schema, target_table))
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

    task = 'running transformation'
    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    return True


def run_age_transformation(conn_str, model_version, site, search_path, target_table, force=False):
    """Run age transformation.

    * Create new schema FOO_transformed.
    * Create transformed tables in FOO_schema.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str site: site label, e.g. 'stlouis'
    :param str search_path: PostgreSQL schema search path
    :param str target_table: table to transform
    :param bool force: if True, ignore benign errors
    :return: True if no exception raised
    :rtype: bool
    :raise: various possible exceptions ...
    """
    log_dict = combine_dicts({'model_version': model_version,
                              'search_path': search_path, 'force': force},
                             get_conn_info_dict(conn_str))

    task = 'running age transformation'
    start_time = time.time()
    # TODO: define spec for computer readable log messages
    # E.g. we might want both 'task' and 'msg' keys, maybe 'submsg'
    logger.info(combine_dicts({'msg': 'started {}'.format(task)}, log_dict))

    # TODO: should we catch all exceptions and perform logger.error?
    # and a logger.info to record the elapsed time at abort.

    # TODO: do we need to validate the primary schema at all?
    schema = primary_schema(search_path)

    # Create the schema to hold the transformed tables.
    tmp_schema = schema + '_' + 'transformed'
    create_schema(conn_str, tmp_schema, force)

    # Perform the transformation.
    _transform_age(conn_str, model_version, site, tmp_schema, target_table, force)

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    # Set up new connection string for manipulating the target schema
    new_search_path = ','.join((tmp_schema, schema, 'vocabulary'))
    new_conn_str = conn_str_with_search_path(conn_str, new_search_path)

    # Set tables to logged
    set_logged(new_conn_str, model_version, False, target_table.split(","))

    # Move the old tables to a backup schema and move the new ones into
    # the original schema; then drop the temporary schema.
    backup_schema = schema + '_backup'

    stmts = StatementList()
    stmts.append(
        drop_schema_statement(backup_schema, if_exists=True, cascade=True))
    stmts.append(create_schema_statement(backup_schema))
    stmts.extend(_move_target_tables_statements(model_version, schema, backup_schema, target_table))
    stmts.extend(_move_target_tables_statements(model_version, tmp_schema, schema, target_table))
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

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    return True


def run_concept_transformation(conn_str, model_version, site, search_path, target_table, force=False):
    """Run concept transformation.

    * Create new schema FOO_transformed.
    * Create transformed tables in FOO_schema.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str site: site label, e.g. 'stlouis'
    :param str search_path: PostgreSQL schema search path
    :param str target_table: table to transform
    :param bool force: if True, ignore benign errors
    :return: True if no exception raised
    :rtype: bool
    :raise: various possible exceptions ...
    """
    log_dict = combine_dicts({'model_version': model_version,
                              'search_path': search_path, 'force': force},
                             get_conn_info_dict(conn_str))

    task = 'running concept transformation'
    start_time = time.time()
    # TODO: define spec for computer readable log messages
    # E.g. we might want both 'task' and 'msg' keys, maybe 'submsg'
    logger.info(combine_dicts({'msg': 'started {}'.format(task)}, log_dict))

    # TODO: should we catch all exceptions and perform logger.error?
    # and a logger.info to record the elapsed time at abort.

    # TODO: do we need to validate the primary schema at all?
    schema = primary_schema(search_path)

    # Create the schema to hold the transformed tables.
    tmp_schema = schema + '_' + 'transformed'
    create_schema(conn_str, tmp_schema, force)

    # Perform the transformation.
    _transform_concept(conn_str, model_version, site, tmp_schema, target_table, force)

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    # Set up new connection string for manipulating the target schema
    new_search_path = ','.join((tmp_schema, schema, 'vocabulary'))
    new_conn_str = conn_str_with_search_path(conn_str, new_search_path)

    # Set tables to logged
    set_logged(new_conn_str, model_version, False, target_table.split(","))

    # Move the old tables to a backup schema and move the new ones into
    # the original schema; then drop the temporary schema.
    backup_schema = schema + '_backup'

    stmts = StatementList()
    stmts.append(
        drop_schema_statement(backup_schema, if_exists=True, cascade=True))
    stmts.append(create_schema_statement(backup_schema))
    stmts.extend(_move_target_tables_statements(model_version, schema, backup_schema, target_table))
    stmts.extend(_move_target_tables_statements(model_version, tmp_schema, schema, target_table))
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

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    return True


def run_concept_age_transformation(conn_str, model_version, site, search_path, target_table, force=False):
    """Run concept transformation.

    * Create new schema FOO_transformed.
    * Create transformed tables in FOO_schema.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str site: site label, e.g. 'stlouis'
    :param str search_path: PostgreSQL schema search path
    :param str target_table: table to transform
    :param bool force: if True, ignore benign errors
    :return: True if no exception raised
    :rtype: bool
    :raise: various possible exceptions ...
    """
    log_dict = combine_dicts({'model_version': model_version,
                              'search_path': search_path, 'force': force},
                             get_conn_info_dict(conn_str))

    task = 'running concept transformation'
    start_time = time.time()
    # TODO: define spec for computer readable log messages
    # E.g. we might want both 'task' and 'msg' keys, maybe 'submsg'
    logger.info(combine_dicts({'msg': 'started {}'.format(task)}, log_dict))

    # TODO: should we catch all exceptions and perform logger.error?
    # and a logger.info to record the elapsed time at abort.

    # TODO: do we need to validate the primary schema at all?
    schema = primary_schema(search_path)

    # Create the schema to hold the transformed tables.
    tmp_schema = schema + '_' + 'transformed'
    create_schema(conn_str, tmp_schema, force)

    # Perform the transformation.
    _transform_concept(conn_str, model_version, site, tmp_schema, target_table, force)

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    # Set up new connection string for manipulating the target schema
    new_search_path = ','.join((tmp_schema, schema, 'vocabulary'))
    new_conn_str = conn_str_with_search_path(conn_str, new_search_path)

    # Set tables to logged
    set_logged(new_conn_str, model_version, False, target_table.split(","))

    # Move the old tables to a backup schema and move the new ones into
    # the original schema; then drop the temporary schema.
    backup_schema = schema + '_backup'

    stmts = StatementList()
    stmts.append(
        drop_schema_statement(backup_schema, if_exists=True, cascade=True))
    stmts.append(create_schema_statement(backup_schema))
    stmts.extend(_move_target_tables_statements(model_version, schema, backup_schema, target_table))
    stmts.extend(_move_target_tables_statements(model_version, tmp_schema, schema, target_table))
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

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    return True


def run_site_transformation(conn_str, model_version, site, search_path, target_table, force=False):
    """Run site transformation.

    * Create new schema FOO_transformed.
    * Create transformed tables in FOO_schema.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str site: site label, e.g. 'stlouis'
    :param str search_path: PostgreSQL schema search path
    :param str target_table: table to transform
    :param bool force: if True, ignore benign errors
    :return: True if no exception raised
    :rtype: bool
    :raise: various possible exceptions ...
    """
    log_dict = combine_dicts({'model_version': model_version,
                              'search_path': search_path, 'force': force},
                             get_conn_info_dict(conn_str))

    task = 'running site transformation'
    start_time = time.time()
    # TODO: define spec for computer readable log messages
    # E.g. we might want both 'task' and 'msg' keys, maybe 'submsg'
    logger.info(combine_dicts({'msg': 'started {}'.format(task)}, log_dict))

    # TODO: should we catch all exceptions and perform logger.error?
    # and a logger.info to record the elapsed time at abort.

    # TODO: do we need to validate the primary schema at all?
    schema = primary_schema(search_path)

    # Create the schema to hold the transformed tables.
    tmp_schema = schema + '_' + 'transformed'
    create_schema(conn_str, tmp_schema, force)

    # Perform the transformation.
    _transform_site(conn_str, model_version, site, tmp_schema, target_table, force)

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    # Set up new connection string for manipulating the target schema
    new_search_path = ','.join((tmp_schema, schema, 'vocabulary'))
    new_conn_str = conn_str_with_search_path(conn_str, new_search_path)

    # Set tables to logged
    set_logged(new_conn_str, model_version, False, target_table.split(","))

    # Move the old tables to a backup schema and move the new ones into
    # the original schema; then drop the temporary schema.
    backup_schema = schema + '_backup'

    stmts = StatementList()
    stmts.append(
        drop_schema_statement(backup_schema, if_exists=True, cascade=True))
    stmts.append(create_schema_statement(backup_schema))
    stmts.extend(_move_target_tables_statements(model_version, schema, backup_schema, target_table))
    stmts.extend(_move_target_tables_statements(model_version, tmp_schema, schema, target_table))
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

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    return True


def run_id_transformation(conn_str, model_version, site, search_path, target_table, id_name, id_type, force=False):
    """Run id transformation.

    * Create new schema FOO_transformed.
    * Create transformed tables in FOO_schema.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str site: site label, e.g. 'stlouis'
    :param str search_path: PostgreSQL schema search path
    :param str target_table: table to transform
    :param str id_name: name of the id (ex. dcc or onco)
    :param str id_type: type of the site id (BigInteger or String)
    :param bool force: if True, ignore benign errors
    :return: True if no exception raised
    :rtype: bool
    :raise: various possible exceptions ...
    """
    log_dict = combine_dicts({'model_version': model_version,
                              'search_path': search_path, 'force': force},
                             get_conn_info_dict(conn_str))

    task = 'running id map transformation'
    start_time = time.time()
    # TODO: define spec for computer readable log messages
    # E.g. we might want both 'task' and 'msg' keys, maybe 'submsg'
    logger.info(combine_dicts({'msg': 'started {}'.format(task)}, log_dict))

    # TODO: should we catch all exceptions and perform logger.error?
    # and a logger.info to record the elapsed time at abort.

    # TODO: do we need to validate the primary schema at all?
    schema = primary_schema(search_path)

    # Create the schema to hold the transformed tables.
    tmp_schema = schema + '_' + 'transformed'
    create_schema(conn_str, tmp_schema, force)

    # Perform the transformation.
    _transform_id(conn_str, model_version, site, tmp_schema, target_table, id_name, id_type, force)

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    # Set up new connection string for manipulating the target schema
    new_search_path = ','.join((tmp_schema, schema, 'vocabulary'))
    new_conn_str = conn_str_with_search_path(conn_str, new_search_path)

    # Set tables to logged
    set_logged(new_conn_str, model_version, False, target_table.split(","))

    # Move the old tables to a backup schema and move the new ones into
    # the original schema; then drop the temporary schema.
    backup_schema = schema + '_backup'

    stmts = StatementList()
    stmts.append(
        drop_schema_statement(backup_schema, if_exists=True, cascade=True))
    stmts.append(create_schema_statement(backup_schema))
    stmts.extend(_move_target_tables_statements(model_version, schema, backup_schema, target_table))
    stmts.extend(_move_target_tables_statements(model_version, tmp_schema, schema, target_table))
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

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    return True

def run_index_transformation(conn_str, model_version, site, search_path, target_table, force=False):
    """Run index transformation.

    * Create new schema FOO_transformed.
    * Create transformed tables in FOO_schema.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str site: site label, e.g. 'stlouis'
    :param str search_path: PostgreSQL schema search path
    :param str target_table: table to transform
    :param bool force: if True, ignore benign errors
    :return: True if no exception raised
    :rtype: bool
    :raise: various possible exceptions ...
    """
    log_dict = combine_dicts({'model_version': model_version,
                              'search_path': search_path, 'force': force},
                             get_conn_info_dict(conn_str))

    task = 'running index transformation'
    start_time = time.time()
    # TODO: define spec for computer readable log messages
    # E.g. we might want both 'task' and 'msg' keys, maybe 'submsg'
    logger.info(combine_dicts({'msg': 'started {}'.format(task)}, log_dict))

    # TODO: should we catch all exceptions and perform logger.error?
    # and a logger.info to record the elapsed time at abort.

    # TODO: do we need to validate the primary schema at all?
    schema = primary_schema(search_path)

    # Create the schema to hold the transformed tables.
    tmp_schema = schema + '_' + 'transformed'
    create_schema(conn_str, tmp_schema, force)

    # Perform the transformation.
    _transform_index(conn_str, model_version, site, tmp_schema, target_table, force)

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    # Set up new connection string for manipulating the target schema
    new_search_path = ','.join((tmp_schema, schema, 'vocabulary'))
    new_conn_str = conn_str_with_search_path(conn_str, new_search_path)

    # Set tables to logged
    set_logged(new_conn_str, model_version, False, target_table.split(","))

    # Move the old tables to a backup schema and move the new ones into
    # the original schema; then drop the temporary schema.
    backup_schema = schema + '_backup'

    stmts = StatementList()
    stmts.append(
        drop_schema_statement(backup_schema, if_exists=True, cascade=True))
    stmts.append(create_schema_statement(backup_schema))
    stmts.extend(_move_target_tables_statements(model_version, schema, backup_schema, target_table))
    stmts.extend(_move_target_tables_statements(model_version, tmp_schema, schema, target_table))
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


def run_vocab_indexes(conn_str, model_version, search_path,
                       force=False):
    """Adjust vocabulary indexes.

    :param str conn_str: pq connection string
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str search_path: PostgreSQL schema search path
    :param bool force: if True, ignore benign errors
    :return: True if no exception raised
    :rtype: bool
    :raise: various possible exceptions ...
    """
    log_dict = combine_dicts({'model_version': model_version,
                              'search_path': search_path, 'force': force},
                             get_conn_info_dict(conn_str))

    task = 'updating vocabulary indexes'
    start_time = time.time()
    # TODO: define spec for computer readable log messages
    # E.g. we might want both 'task' and 'msg' keys, maybe 'submsg'
    logger.info(combine_dicts({'msg': 'started {}'.format(task)}, log_dict))

    # TODO: should we catch all exceptions and perform logger.error?
    # and a logger.info to record the elapsed time at abort.

    # TODO: do we need to validate the primary schema at all?
    schema = primary_schema(search_path)

    # Add indexes to the vocabulary tables
    add_vocab_indexes(conn_str, model_version, force)

    # Drop unneeded indexes from the vocabulary tables
    drop_vocab_unneeded_indexes(conn_str, model_version, force)

    logger.info(combine_dicts(
        {'msg': 'finished {}'.format(task),
         'elapsed': secs_since(start_time)}, log_dict))

    return True