import logging
import sqlalchemy
import sqlalchemy.dialects.postgresql
import time

from pedsnetdcc import VOCAB_TABLES
from pedsnetdcc.db import StatementSet
from pedsnetdcc.utils import stock_metadata, check_stmt_err, secs_since

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


def create_foreign_keys(conn_str, model_version, vocabulary=False):
    """Create foreign keys in the database.

    Execute CREATE statements to add foreign keys (on the vocabulary or data
    tables) in a PEDSnet database of a specific model version.

    :param str conn_str:      database connection string
    :param str model_version: the model version of the PEDSnet database
    :param bool vocabulary:   whether to create foreign keys on vocab tables
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """
    # Log start of the function and set the starting time.
    logger.info({'msg': 'starting foreign key creation',
                 'model_version': model_version, 'vocabulary': vocabulary})
    starttime = time.time()

    # Get list of foreign keys for that need creation.
    foreign_keys = _foreign_keys_from_model_version(model_version, vocabulary)

    # Make a set of statements for parallel execution.
    stmts = StatementSet()

    # Add a creation statement to the set for each foreign key.
    for fkey in foreign_keys:
        stmts.add(str(sqlalchemy.schema.AddConstraint(fkey).compile(
            dialect=sqlalchemy.dialects.postgresql.dialect())).lstrip())

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check statements for any errors and raise exception if they are found.
    for stmt in stmts:
        check_stmt_err(stmt, 'foreign key creation')

    # Log end of function.
    logger.info({'msg': 'finished foreign key creation',
                 'elapsed': secs_since(starttime)})

    # If reached without error, then success!
    return True


def drop_foreign_keys(conn_str, model_version, vocabulary=False):
    """Drop foreign keys from the database.

    Execute DROP statements to remove foreign keys (on the vocabulary or data
    tables) from a PEDSnet database of a specific model version.

    :param str conn_str:      database connection string
    :param str model_version: the model version of the PEDSnet database
    :param bool vocabulary:   whether to drop foreign keys from vocab tables
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """
    # Log start of the function and set the starting time.
    logger.info({'msg': 'starting foreign key removal',
                 'model_version': model_version, 'vocabulary': vocabulary})
    starttime = time.time()

    # Get list of foreign keys for that need creation.
    foreign_keys = _foreign_keys_from_model_version(model_version, vocabulary)

    # Make a set of statements for parallel execution.
    stmts = StatementSet()

    # Add a creation statement to the set for each foreign key.
    for fkey in foreign_keys:
        stmts.add(str(sqlalchemy.schema.DropConstraint(fkey).compile(
            dialect=sqlalchemy.dialects.postgresql.dialect())).lstrip())

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check statements for any errors and raise exception if they are found.
    for stmt in stmts:
        check_stmt_err(stmt, 'foreign key removal')

    # Log end of function.
    logger.info({'msg': 'finished foreign key removal',
                 'elapsed': secs_since(starttime)})

    # If reached without error, then success!
    return True
