import logging

import sqlalchemy
import sqlalchemy.dialects.postgresql

from pedsnetdcc import TRANSFORMS
from pedsnetdcc.db import Statement
from pedsnetdcc.utils import (DatabaseError, get_conn_info_dict, combine_dicts,
                              stock_metadata)


logger = logging.getLogger(__name__)


def create_schema(conn_str, schema):
    """Create a schema in the database defined by conn_str.

    :param conn_str: pq/libpq connection string
    :param schema: database schema
    :return: None
    :raise: DatabaseError
    """
    sql = 'CREATE SCHEMA {0}'.format(schema)
    stmt = Statement(sql).execute(conn_str)
    if stmt.err:
        tpl = 'failed to create schema `{0}`: {1}'
        raise DatabaseError(tpl.format(schema, stmt.err))


def primary_schema(search_path):
    """Return the first schema in the search_path.

    :param str search_path: PostgreSQL search path of comma-separated schemas
    :return: first schema in path
    :raise: ValueError
    """
    search_path = search_path.strip()
    if not search_path:
        schemas = None
    else:
        schemas = search_path.split(',')
    if not schemas:
        raise ValueError('search_path must be non-empty')
    return schemas[0].strip()


def _transform(conn_str, model_version, schema, tmp_schema):
    """Run transformations.

    :param conn_str:
    :param model_version:
    :param schema:
    :param tmp_schema:
    :return: list of SQL statement strings
    :raise: psycopg2.ProgrammingError (from pre_transform)
    """
    old_metadata = stock_metadata(model_version)
    new_metadata = stock_metadata(model_version)
    for transform in TRANSFORMS:
        transform.pre_transform(conn_str)
        new_metadata = transform.modify_metadata(new_metadata)


def _transform_select_sql(model_version, site, target_schema):
    """Create SQL for `select` statement transformations.

    :param model_version: PEDSnet model version, e.g. 2.3.0
    :param site: site name, e.g. 'stlouis'
    :param target_schema: schema in which to create the transformed tables
    :return: set of SQL statement strings
    :raise: psycopg2.ProgrammingError (from pre_transform)
    """
    metadata = stock_metadata(model_version)
    metadata.info['site'] = site
    sql_strings = set()
    for table_name, table in metadata.tables.items():
        for transform in TRANSFORMS:
            select_obj = sqlalchemy.select([table])
            join_obj = table

            select_obj, join_obj = transform.modify_select(
                metadata,
                table_name,
                select_obj,
                join_obj)

            select_obj = select_obj.select_from(join_obj)

            table_sql = str(
                select_obj.compile(
                    dialect=sqlalchemy.dialects.postgresql.dialect()))

            final_sql = 'CREATE TABLE {0}.{1} AS {2}'.format(
                target_schema, table_name, table_sql)

            sql_strings.add(final_sql)

    return sql_strings


def run_transformation(conn_str, model_version, search_path, force):
    """Run all transformations, backing up existing tables to a backup schema.

    The backup schema will be named FOO_backup, where FOO is the primary
    schema in the `search_path`.  A pre-existing backup_schema is dropped.

    Details:
    * Run all transformations.

    * Execute `create table update_tmp_BAR as select ... from BAR` statements.

    * Move primary keys to the new tables. Note: if the original and
    transformed table are in the same schema, this leaves the original
    table in a somewhat compromised state, because the primary key has to be
    dropped on the original table before it can be recreated on the
    transformed table. Does it matter? Not really, as long
    as the transformation can be re-run on it.  However, if the transformed
    table is created in different schema, the original primary key does not
    need to be dropped.

    * Apply NOT NULL constraints to the new tables.

    * Optionally(?) add indexes and foreign key constraints to the new tables.
    This is theoretically not necessary, because after aggregation the tables
    will have to be re-indexed and re-constrained. However, since/if individual
    PCORnet schemas are created for each site, the PCORnet transformation will
    theoretically be sped up by having indexes around.

    * Archive off the original tables, and rename the transformed ones.

    Hmm, instead of farting around with `update_tmp_` prefix, how about
    creating a FOO_transform schema, create the new tables there. When done,
    any existing FOO_backup schema is dropped, the FOO schema is renamed to
    FOO_backup, and FOO_transform is renamed to FOO. The one problem with this
    is whatever other schemas exist in FOO will need to be moved back to FOO.
    It would be just as good to move individual tables between schemas.

    Revised approach:

    * Create new schema FOO_transformed.
    * Create transformed tables in FOO_schema.
    * If there is a FOO_backup schema, drop it.
    * Create the FOO_backup schema.
    * Move the PEDSnet core tables from FOO into FOO_backup schema.
    * Move the transformed PEDSnet core tables from the FOO_transformed schema to the FOO schema.
    * The previous two steps can be done in a transaction.

    :return: True if successful
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
        create_schema(conn_str, tmp_schema)
    except DatabaseError as err:
        logger.error(combine_dicts({'msg': 'error ' + task, 'err': err},
                                   log_dict))
        raise

    # Perform the transformation.
    try:
        _transform(conn_str, model_version, schema, tmp_schema)
    except DatabaseError as err:
        logger.error(combine_dicts({'msg': 'error ' + task, 'err': err},
                                   log_dict))
        raise

    stmt = Statement('DROP SCHEMA IF EXISTS ' + schema).execute(conn_str)
    if stmt.err:
        logger.error(combine_dicts({'msg': 'error ' + task, 'err': stmt.err},
                                   log_dict))
        raise
