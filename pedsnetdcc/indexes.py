from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, DropIndex

from pedsnetdcc import VOCAB_TABLES, TRANSFORMS
from pedsnetdcc.utils import stock_metadata


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


def _indexes_from_model_version(model_version, vocabulary=False):
    """Return list of SQLAlchemy index objects for the transformed metadata.

    Given the stock metadata, for each transform `T` we invoke:

        new_metadata = T.modify_metadata(metadata)

    and at the end, we extract the indexes.

    :param model_version: pedsnet model version
    :type: str
    :param vocabulary: whether to return indexes for vocabulary tables or
    non-vocabulary tables
    :type: bool
    :return: list of index objects
    :rtype: list(sqlalchemy.Index)
    """
    return _indexes_from_metadata(stock_metadata(model_version), TRANSFORMS,
                                  vocabulary=vocabulary)


def indexes_sql(model_version, drop=False, vocabulary=False):
    """Create ADD or DROP INDEX statements for a transformed PEDSnet schema.

    Depending on the value of the `drop` parameter, either ADD or DROP
    statements are produced.

    Depending on the value of the `vocabulary` parameter, statements are
    provided for either for a site schema (i.e. non-vocabulary tables in the
    `pedsnet` data model) or for the vocabulary schema (vocabulary tables in
    the `pedsnet` data model).

    :param model_version: pedsnet model version
    :type: str
    :param drop: whether to generate ADD or DROP statements
    :type: bool
    :param vocabulary: whether to make statements for vocabulary tables or
    non-vocabulary tables
    :type: bool
    :return: list of SQL ADD or DROP INDEX statements
    :type: list(str)
    """
    indexes = _indexes_from_model_version(model_version, vocabulary=vocabulary)
    if not drop:
        func = CreateIndex
    else:
        func = DropIndex
    return [str(func(x).compile(
                dialect=postgresql.dialect())).lstrip() for x in indexes]
