import logging
import time

from pedsnetdcc import VOCAB_TABLES
from pedsnetdcc.db import StatementSet
from pedsnetdcc.utils import stock_metadata, check_stmt_err, secs_since

logger = logging.getLogger(__name__)

set_not_null = "ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"
drop_not_null = "ALTER TABLE {table} ALTER COLUMN {column} DROP NOT NULL"


def _not_null_columns_from_model_version(model_version, vocabulary=False):
    """Return list of not nullable SQLAlchemy columns for the model version.

    :param str model_version: pedsnet model version
    :param bool vocabulary:   whether to return vocab or non-vocab columns
    :return:                  list of columns
    :rtype:                   list(sqlalchemy.Column)
    """
    metadata = stock_metadata(model_version)

    columns = []
    for name, table in metadata.tables.items():
        if vocabulary:
            if name in VOCAB_TABLES:
                for column in table.c.values():
                    if not column.nullable:
                        columns.append(column)
        else:
            if name not in VOCAB_TABLES:
                for column in table.c.values():
                    if not column.nullable:
                        columns.append(column)

    return columns


def set_not_nulls(conn_str, model_version, vocabulary=False):
    """Set the appropriate columns to not null for the model version.

    Execute ALTER TABLE statements to set the appropriate columns to not null
    in a PEDSnet database of a specific model version.

    :param str conn_str:      database connection string
    :param str model_version: the model version of the PEDSnet database
    :param bool vocabulary:   set not nulls on vocab or non-vocab tables
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """
    # Log start of the function and set the starting time.
    logger.info({'msg': 'starting setting columns not null',
                 'model_version': model_version, 'vocabulary': vocabulary})
    starttime = time.time()

    # Get list of columns that need setting not null.
    columns = _not_null_columns_from_model_version(model_version, vocabulary)

    # Make a set of statements for parallel execution.
    stmts = StatementSet()

    # Add a not null setting statement to the set for each column.
    for column in columns:
        stmts.add(set_not_null.format(table=column.table, column=column.name))

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check statements for any errors and raise exception if they are found.
    for stmt in stmts:
        check_stmt_err(stmt, 'setting columns not null')

    # Log end of function.
    logger.info({'msg': 'finished setting columns not null',
                 'elapsed': secs_since(starttime)})

    # If reached without error, then success!
    return True


def drop_not_nulls(conn_str, model_version, vocabulary=False):
    """Drop not nulls from the appropriate columns for the model version.

    Execute ALTER TABLE statements to drop not nulls from the appropriate
    columns in a PEDSnet database of a specific model version.

    :param str conn_str:      database connection string
    :param str model_version: the model version of the PEDSnet database
    :param bool vocabulary:   drop not nulls from vocab or non-vocab tables
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """
    # Log start of the function and set the starting time.
    logger.info({'msg': 'starting dropping column not nulls',
                 'model_version': model_version, 'vocabulary': vocabulary})
    starttime = time.time()

    # Get list of columns that need not nulls dropped.
    columns = _not_null_columns_from_model_version(model_version, vocabulary)

    # Make a set of statements for parallel execution.
    stmts = StatementSet()

    # Add a not null dropping statement to the set for each column.
    for column in columns:
        stmts.add(drop_not_null.format(table=column.table, column=column.name))

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check statements for any errors and raise exception if they are found.
    for stmt in stmts:
        check_stmt_err(stmt, 'dropping column not nulls')

    # Log end of function.
    logger.info({'msg': 'finished dropping column not nulls',
                 'elapsed': secs_since(starttime)})

    # If reached without error, then success!
    return True
