import logging
import time

from psycopg2 import errorcodes as psycopg2_errorcodes
import sqlalchemy

from pedsnetdcc import VOCAB_TABLES, TRANSFORMS
from pedsnetdcc.utils import stock_metadata, get_conn_info_dict, combine_dicts
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


def _indexes_sql(model_version, vocabulary=False, drop=False):
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

    if drop:
        func = sqlalchemy.schema.DropIndex
    else:
        func = sqlalchemy.schema.CreateIndex

    indexes = _indexes_from_metadata(stock_metadata(model_version), TRANSFORMS,
                                     vocabulary=vocabulary)
    return [str(func(x).compile(
        dialect=sqlalchemy.dialects.postgresql.dialect())).lstrip()
        for x in indexes]


def _check_stmt_err(stmt, error_mode):
    if stmt.err is None:
        return

    dropping = stmt.sql.startswith('DROP')
    creating = stmt.sql.startswith('CREATE')

    does_not_exist = (
        hasattr(stmt.err, 'pgcode')
        and stmt.err.pgcode
        and psycopg2_errorcodes.lookup(stmt.err.pgcode) == 'UNDEFINED_OBJECT')

    # Detect error 42P07 (already exists); btw, an index is a table in PG
    already_exists = (
        hasattr(stmt.err, 'pgcode')
        and stmt.err.pgcode
        and psycopg2_errorcodes.lookup(stmt.err.pgcode) == 'DUPLICATE_TABLE')

    if dropping and error_mode == 'normal' and does_not_exist:
        return

    if creating and error_mode == 'normal' and already_exists:
        return

    if error_mode == 'force' and type(stmt.err).__name__ == 'ProgrammingError':
        # ProgrammingError encompasses most post-connection errors
        return

    raise stmt.err


def _process_indexes(conn_str, model_version, error_mode, vocabulary=False,
                     drop=False,):
    """Execute ADD or DROP INDEX statements for a transformed PEDSnet schema.

    Depending on the value of the `drop` parameter, either ADD or DROP
    statements are executed.

    Depending on the value of the `vocabulary` parameter, statements are
    executed either for a site schema (i.e. non-vocabulary tables in the
    `pedsnet` data model) or for the vocabulary schema (vocabulary tables in
    the `pedsnet` data model).

    Errors are handled as per https://github.com/PEDSnet/pedsnetdcc/issues/10
    depending on the value of `error_mode`.

    :param conn_str: database connection string
    :type: str
    :param model_version: pedsnet model version
    :type: str
    :param error_mode: error sensitivity: 'normal', 'strict', 'or 'force'
    See https://github.com/PEDSnet/pedsnetdcc/issues/10
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
        func = sqlalchemy.schema.CreateIndex
        operation = 'creation'
    else:
        func = sqlalchemy.schema.DropIndex
        operation = 'removal'

    # Log start of the function and set the starting time.
    logger.info({'msg': 'starting index {op}'.format(op=operation),
                 'model_version': model_version, 'vocabulary': vocabulary})
    start_time = time.time()

    stmts = StatementSet()

    for stmt in _indexes_sql(model_version, vocabulary, drop):
        stmts.add(Statement(stmt))

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check statements for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            _check_stmt_err(stmt, error_mode)
        except Exception:
            conn_info = get_conn_info_dict(conn_str)
            logger.error(combine_dicts({'msg': 'Fatal error for this '
                                               'error_mode',
                                        'error_mode': error_mode,
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, conn_info))
            logger.info({'msg': 'aborted index {op}'.format(op=operation),
                         'elapsed': secs_since(start_time)})
            raise

    # Log end of function.
    logger.info({'msg': 'finished index {op}'.format(op=operation),
                 'elapsed': secs_since(start_time)})

    # If reached without error, then success!
    return True


def add_indexes(conn_str, model_version, error_mode='normal',
                vocabulary=False):
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
    :param error_mode: error sensitivity: 'normal', 'strict', 'or 'force'
    See https://github.com/PEDSnet/pedsnetdcc/issues/10
    :type: str
    :return: True
    :type: bool
    :raises DatabaseError: if any of the statement executions cause errors
    """

    return _process_indexes(conn_str, model_version, error_mode, vocabulary,
                            drop=False)


def drop_indexes(conn_str, model_version, vocabulary=False,
                 error_mode='normal'):
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
    :param error_mode: error sensitivity: 'normal', 'strict', 'or 'force'
    See https://github.com/PEDSnet/pedsnetdcc/issues/10
    :type: str
    :return: True
    :type: bool
    :raises DatabaseError: if any of the statement executions cause errors
    """

    return _process_indexes(conn_str, model_version, error_mode, vocabulary,
                            drop=True)
