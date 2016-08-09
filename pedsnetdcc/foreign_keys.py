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

    if dropping and force and does_not_exist:
        return

    if creating and force and already_exists:
        return

    raise stmt.err


def add_foreign_keys(conn_str, model_version, force=False, vocabulary=False):
    """Create foreign keys in the database.

    Execute CREATE statements to add foreign keys (on the vocabulary or data
    tables) in a PEDSnet database of a specific model version.

    :param str conn_str:      database connection string
    :param str model_version: the model version of the PEDSnet database
    :param bool force: ignore benign errors if true; see
    https://github.com/PEDSnet/pedsnetdcc/issues/10
    :param bool vocabulary:   whether to create foreign keys on vocab tables
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """
    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'model_version': model_version,
                             'vocabulary': vocabulary, 'force': force},
                             get_conn_info_dict(conn_str))
    logger.info(combine_dicts({'msg': 'starting foreign key creation'},
                              log_dict))
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
            _check_stmt_err(stmt, force)
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'foreign key creation failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished foreign key creation',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True


def drop_foreign_keys(conn_str, model_version, force=False, vocabulary=False):
    """Drop foreign keys from the database.

    Execute DROP statements to remove foreign keys (on the vocabulary or data
    tables) from a PEDSnet database of a specific model version.

    :param str conn_str:      database connection string
    :param str model_version: the model version of the PEDSnet database
    :param bool force: ignore benign errors if true; see
    https://github.com/PEDSnet/pedsnetdcc/issues/10
    :param bool vocabulary:   whether to drop foreign keys from vocab tables
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """
    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'model_version': model_version,
                             'vocabulary': vocabulary, 'force': force},
                             get_conn_info_dict(conn_str))
    logger.info(combine_dicts({'msg': 'starting foreign key removal'},
                              log_dict))
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
            _check_stmt_err(stmt, force)
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'foreign key removal failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished foreign key removal',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True
