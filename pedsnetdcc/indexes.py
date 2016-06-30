from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, DropIndex

from pedsnetdcc import VOCAB_TABLES

def indexes(metadata, transforms, vocabulary=False):
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


def add_indexes_sql(indexes):
    """Create generic ADD INDEX statements.
    :param indexes: list of indexes
    :type: list(sqlalchemy.Index)
    :return: list of SQL ADD INDEX statements
    :type: list(str)
    """
    return [str(CreateIndex(x).compile(
                dialect=postgresql.dialect())) for x in indexes]


def drop_indexes_sql(indexes):
    """Create generic DROP INDEX statements.
    :param indexes: list of indexes
    :type: list(sqlalchemy.Index)
    :return: list of SQL DROP INDEX statements
    :type: list(str)
    """
    return [str(DropIndex(x).compile(
                dialect=postgresql.dialect())).lstrip() for x in indexes]
