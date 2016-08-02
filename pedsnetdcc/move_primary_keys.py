import logging
import re
import sqlalchemy
import sqlalchemy.dialects.postgresql
import time

from psycopg2 import errorcodes as psycopg2_errorcodes

from pedsnetdcc import VOCAB_TABLES
from pedsnetdcc.db import StatementList, Statement
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import (stock_metadata, combine_dicts,
                              get_conn_info_dict)


logger = logging.getLogger(__name__)


def _primary_keys_from_model_version(model_version):
    """Return list of SQLAlchemy primary key objects for the model version.

    Relevant attributes are `name` and `table.name`

    :param str model_version: pedsnet model version
    :return:                  list of primary keys objects
    :rtype:                   list(sqlalchemy.PrimaryKey)
    """

    # TODO: should ideally be based on the transformed data model
    # in case a transform modifies a primary key.
    metadata = stock_metadata(model_version)

    primary_keys = []
    for name, table in metadata.tables.items():
        if name not in VOCAB_TABLES:
            for con in table.constraints:
                if con and isinstance(con, sqlalchemy.PrimaryKeyConstraint):
                    primary_keys.append(con)

    return primary_keys


def _check_stmt_err(stmt, force):
    """Validate results of an executed Statement object relative to force flag

    Creating a constraint can produce a 'benign' error if it already exists.
    If `force` is true, such an error is ignored.

    Dropping cannot produce benign errors because of the 'if exists'
    clause, so we don't need to check for that.

    :param Statement stmt:
    :param bool force:
    :return: None
    :raise: DatabaseError
    """
    if stmt.err is None:
        return

    creating = 'add constraint' in stmt.sql.lower()

    # Detect error 42P16: multiple primary keys ... are not allowed
    already_exists = (
        hasattr(stmt.err, 'pgcode')
        and stmt.err.pgcode
        and psycopg2_errorcodes.lookup(
            stmt.err.pgcode) == 'INVALID_TABLE_DEFINITION')

    if creating and force and already_exists:
        return

    raise stmt.err


UPDATE_TABLE_PREFIX = 'update_tmp_'


def move_primary_keys(conn_str, model_version, force=False):
    """Move primary keys to post-transformation tables.

    This will move the primary keys from the pre-transformation tables to the
    post-transformation tables. The post-transformation tables are named
    by prepending UPDATE_TABLE_PREFIX to the original table name.


    :param str conn_str:      database connection string
    :param str model_version: the model version of the PEDSnet database
    :param bool force: ignore benign errors if true
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'model_version': model_version, 'force': force},
                             get_conn_info_dict(conn_str))
    task = 'moving primary keys to transformed tables'
    logger.info({'msg': task}, log_dict)
    start_time = time.time()

    # Get list of primary keys for that need creation.
    primary_keys = _primary_keys_from_model_version(model_version)

    # Make a set of statements for parallel execution.
    stmts = StatementList()

    pg = sqlalchemy.dialects.postgresql.dialect()

    # Add statements for each pair of tables
    for pk in primary_keys:
        orig_table = pk.table.name
        tmp_table = UPDATE_TABLE_PREFIX + orig_table
        drop_tpl = 'alter table {0} drop constraint if exists {1} cascade'
        drop_sql = drop_tpl.format(orig_table, pk.name)
        stmts.append(Statement(drop_sql))
        add_sql = str(sqlalchemy.schema.AddConstraint(pk).compile(dialect=pg))
        add_sql = re.sub('ALTER TABLE ' + orig_table,
                         'ALTER TABLE ' + tmp_table,
                         add_sql)
        stmts.append(Statement(add_sql))

    # Execute the statements in parallel.
    stmts.serial_execute(conn_str)

    # Check statements for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            _check_stmt_err(stmt, force)
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': '{0} failed'.format(task),
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished {0}'.format(task),
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True
