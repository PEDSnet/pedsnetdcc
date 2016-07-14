import logging
import time

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, DropIndex

from pedsnetdcc import VOCAB_TABLES, TRANSFORMS
from pedsnetdcc.utils import stock_metadata, check_stmt_err
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.db import Statement, StatementSet

logger = logging.getLogger(__name__)


def _indexes_from_metadata(metadata, transforms, vocabulary=False):
    """Return list of SQLAlchemy index objects for the transformed metadata.

    Given the stock metadata, for each transform `T` we invoke:

        new_metadata = T.modify_metadata(metadata)

    and at the end, we extract the indexes.

    :param metadata: SQLAlchemy metadata for PEDSnet
    :type: sqlalchemy.schema.MetaData
    :param transforms: list of Transform classes
    :type: list(type)
    :param vocabulary: whether to return indexes for vocabulary tables or
    non-vocabulary tables
    :type: bool
    :return: list of index objects
    :rtype: list(sqlalchemy.Index)
    """
    for t in transforms:
        metadata = t.modify_metadata(metadata)

    indexes = []
    for name, table in metadata.tables.items():
        if vocabulary:
            if name in VOCAB_TABLES:
                indexes.extend(table.indexes)
        else:
            if name not in VOCAB_TABLES:
                indexes.extend(table.indexes)

    return indexes


def _indexes_sql(conn_str, model_version, vocabulary=False, drop=False):
    """Return ADD or DROP INDEX statements for a transformed PEDSnet schema.

    Depending on the value of the `drop` parameter, either ADD or DROP
    statements are produced.

    Depending on the value of the `vocabulary` parameter, statements are
    produced either for a site schema (i.e. non-vocabulary tables in the
    `pedsnet` data model) or for the vocabulary schema (vocabulary tables in
    the `pedsnet` data model).

    :param model_version: pedsnet model version
    :type: str
    :param vocabulary: whether to make statements for vocabulary tables or
    non-vocabulary tables
    :type: bool
    :param drop: whether to generate ADD or DROP statements
    :type: bool
    :return: list of SQL statements
    :type: list(str)
    """

    func = DropIndex if drop else CreateIndex

    indexes = _indexes_from_metadata(stock_metadata(model_version), TRANSFORMS,
                                     vocabulary=vocabulary)
    return [str(func(x).compile(
            dialect=postgresql.dialect())).lstrip() for x in indexes]


def _process_indexes(conn_str, model_version, vocabulary=False, drop=False):
    """Execute ADD or DROP INDEX statements for a transformed PEDSnet schema.

    Depending on the value of the `drop` parameter, either ADD or DROP
    statements are executed.

    Depending on the value of the `vocabulary` parameter, statements are
    executed either for a site schema (i.e. non-vocabulary tables in the
    `pedsnet` data model) or for the vocabulary schema (vocabulary tables in
    the `pedsnet` data model).

    :param conn_str: database connection string
    :type: str
    :param model_version: pedsnet model version
    :type: str
    :param vocabulary: whether to make statements for vocabulary tables or
    non-vocabulary tables
    :type: bool
    :param drop: whether to generate ADD or DROP statements
    :type: bool
    :return: True (returns only if statements succeed)
    :type: bool
    :raises DatabaseError: if any of the statement executions cause errors
    """

    if not drop:
        func = CreateIndex
        operation = 'creation'
    else:
        func = DropIndex
        operation = 'removal'

    # Log start of the function and set the starting time.
    logger.info({'msg': 'starting index {op}'.format(op=operation),
                 'model_version': model_version, 'vocabulary': vocabulary})
    start_time = time.time()

    indexes = _indexes_from_metadata(stock_metadata(model_version), TRANSFORMS,
                                     vocabulary=vocabulary)

    stmts = StatementSet()

    for stmt in [str(func(x).compile(
            dialect=postgresql.dialect())).lstrip() for x in indexes]:
        stmts.add(Statement(stmt))

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check statements for any errors and raise exception if they are found.
    for stmt in stmts:
        check_stmt_err(stmt, 'foreign key creation')

    # Log end of function.
    logger.info({'msg': 'finished foreign key {op}'.format(op=operation),
                 'elapsed': secs_since(start_time)})

    # If reached without error, then success!
    return True


def add_indexes(conn_str, model_version, vocabulary=False):
    """Execute ADD INDEX statements for a transformed PEDSnet schema.

    Depending on the value of the `vocabulary` parameter, statements are
    executed either for a site schema (i.e. non-vocabulary tables in the
    `pedsnet` data model) or for the vocabulary schema (vocabulary tables in
    the `pedsnet` data model).

    :param conn_str: database connection string
    :type: str
    :param model_version: pedsnet model version
    :type: str
    :param vocabulary: whether to make statements for vocabulary tables or
    non-vocabulary tables
    :type: bool
    :return: True
    :type: bool
    :raises DatabaseError: if any of the statement executions cause errors
    """

    return _process_indexes(conn_str, model_version, vocabulary, drop=False)


def drop_indexes(conn_str, model_version, vocabulary=False):
    """Execute ADD or DROP INDEX statements for a transformed PEDSnet schema.

    Depending on the value of the `vocabulary` parameter, statements are
    executed either for a site schema (i.e. non-vocabulary tables in the
    `pedsnet` data model) or for the vocabulary schema (vocabulary tables in
    the `pedsnet` data model).

    :param conn_str: database connection string
    :type: str
    :param model_version: pedsnet model version
    :type: str
    :param vocabulary: whether to make statements for vocabulary tables or
    non-vocabulary tables
    :type: bool
    :return: True
    :type: bool
    :raises DatabaseError: if any of the statement executions cause errors
    """

    return _process_indexes(conn_str, model_version, vocabulary, drop=True)
