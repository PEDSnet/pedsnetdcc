import logging
import sqlalchemy
import sqlalchemy.dialects.postgresql
import time

from psycopg2 import errorcodes as psycopg2_errorcodes

from pedsnetdcc import VOCAB_TABLES
from pedsnetdcc.db import StatementSet, Statement
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import (stock_metadata, combine_dicts,
                              get_conn_info_dict)


logger = logging.getLogger(__name__)


def _foreign_keys_from_model_version(model_version, vocabulary=False):
    """Return list of SQLAlchemy foreign key objects for the model version.

    :param str model_version: pedsnet model version
    :param bool vocabulary:   whether to return vocab or non-vocab foreign keys
    :return:                  list of foreign keys objects
    :rtype:                   list(sqlalchemy.ForeignKey)
    """
    metadata = stock_metadata(model_version)

    foreign_keys = []
    for name, table in metadata.tables.items():
        if vocabulary:
            if name in VOCAB_TABLES:
                for con in table.constraints:
                    if isinstance(con, sqlalchemy.ForeignKeyConstraint):
                        foreign_keys.append(con)
        else:
            if name not in VOCAB_TABLES:
                for con in table.constraints:
                    if isinstance(con, sqlalchemy.ForeignKeyConstraint):
                        foreign_keys.append(con)

    return foreign_keys


def _check_stmt_err(stmt, error_mode):
    if stmt.err is None:
        return

    dropping = 'DROP CONSTRAINT' in stmt.sql
    creating = 'ADD CONSTRAINT' in stmt.sql

    # Detect error 42704
    does_not_exist = (
        hasattr(stmt.err, 'pgcode')
        and stmt.err.pgcode
        and psycopg2_errorcodes.lookup(stmt.err.pgcode) == 'UNDEFINED_OBJECT')

    # Detect error 42710
    already_exists = (
        hasattr(stmt.err, 'pgcode')
        and stmt.err.pgcode
        and psycopg2_errorcodes.lookup(stmt.err.pgcode) == 'DUPLICATE_OBJECT')

    if dropping and error_mode == 'normal' and does_not_exist:
        return

    if creating and error_mode == 'normal' and already_exists:
        return

    if error_mode == 'force' and type(stmt.err).__name__ == 'ProgrammingError':
        # ProgrammingError encompasses most post-connection errors
        return

    raise stmt.err


def add_foreign_keys(conn_str, model_version, error_mode='normal',
                        vocabulary=False):
    """Create foreign keys in the database.

    Execute CREATE statements to add foreign keys (on the vocabulary or data
    tables) in a PEDSnet database of a specific model version.

    :param str conn_str:      database connection string
    :param str model_version: the model version of the PEDSnet database
    :param error_mode: error sensitivity: 'normal', 'strict', 'or 'force'
    See https://github.com/PEDSnet/pedsnetdcc/issues/10
    :param bool vocabulary:   whether to create foreign keys on vocab tables
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """
    # Log start of the function and set the starting time.
    logger.info({'msg': 'starting foreign key creation',
                 'model_version': model_version, 'vocabulary': vocabulary,
                 'error_mode': error_mode})
    start_time = time.time()

    # Get list of foreign keys for that need creation.
    foreign_keys = _foreign_keys_from_model_version(model_version, vocabulary)

    # Make a set of statements for parallel execution.
    stmts = StatementSet()

    # Add a creation statement to the set for each foreign key.
    for fkey in foreign_keys:
        pg = sqlalchemy.dialects.postgresql.dialect()
        sql = str(sqlalchemy.schema.AddConstraint(fkey).compile(dialect=pg))
        stmts.add(Statement(sql))

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check statements for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            _check_stmt_err(stmt, error_mode)
        except:
            conn_info = get_conn_info_dict(conn_str)
            logger.error(combine_dicts({'msg': 'Fatal error for this '
                                               'error_mode',
                                        'error_mode': error_mode,
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, conn_info))
            logger.info({'msg': 'aborted foreign key creation',
                         'elapsed': secs_since(start_time)})
            raise

    # Log end of function.
    logger.info({'msg': 'finished foreign key creation',
                 'elapsed': secs_since(start_time)})

    # If reached without error, then success!
    return True


def drop_foreign_keys(conn_str, model_version, error_mode='normal',
                      vocabulary=False):
    """Drop foreign keys from the database.

    Execute DROP statements to remove foreign keys (on the vocabulary or data
    tables) from a PEDSnet database of a specific model version.

    :param str conn_str:      database connection string
    :param str model_version: the model version of the PEDSnet database
    :param error_mode: error sensitivity: 'normal', 'strict', 'or 'force'
    See https://github.com/PEDSnet/pedsnetdcc/issues/10
    :param bool vocabulary:   whether to drop foreign keys from vocab tables
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """
    # Log start of the function and set the starting time.
    logger.info({'msg': 'starting foreign key removal',
                 'model_version': model_version, 'vocabulary': vocabulary,
                 'error_mode': error_mode})
    start_time = time.time()

    # Get list of foreign keys for that need creation.
    foreign_keys = _foreign_keys_from_model_version(model_version, vocabulary)

    # Make a set of statements for parallel execution.
    stmts = StatementSet()

    # Add a creation statement to the set for each foreign key.
    for fkey in foreign_keys:
        pg = sqlalchemy.dialects.postgresql.dialect()
        sql = str(sqlalchemy.schema.DropConstraint(fkey).compile(dialect=pg))
        stmts.add(Statement(sql))

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check statements for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            _check_stmt_err(stmt, error_mode)
        except:
            conn_info = get_conn_info_dict(conn_str)
            logger.error(combine_dicts({'msg': 'Fatal error for this '
                                               'error_mode',
                                        'error_mode': error_mode,
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, conn_info))
            logger.info({'msg': 'aborted foreign key removal',
                         'elapsed': secs_since(start_time)})
            raise

    # Log end of function.
    logger.info({'msg': 'finished foreign key removal',
                 'elapsed': secs_since(start_time)})

    # If reached without error, then success!
    return True
