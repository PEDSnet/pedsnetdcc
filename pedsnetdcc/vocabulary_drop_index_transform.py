from sqlalchemy.schema import Column, Index
from pedsnetdcc import VOCAB_TABLES

from pedsnetdcc.abstract_transform import Transform


class VocabularyDropIndexTransform(Transform):
    drop_by_table = {
        'concept': ('domain_id',),
    }
    idx_by_column = {
        'domain_id': 'idx_concept_domain_id',
    }

    @classmethod
    def modify_metadata(cls, metadata):
        """Modify SQLAlchemy metadata for all appropriate tables.

        Iterate over all non-vocabulary tables and run `modify_table`.

        The only current use of this is to allow the user to iterate over
        modified tables and generate indexes and constraints.

        :param sqlalchemy.MetaData metadata: SQLAlchemy Metadata object
        describing tables and columns
        :rtype: sqlalchemy.MetaData

        """
        indexes = []

        for table_name, table in metadata.tables.items():
            if table_name not in VOCAB_TABLES:
                continue

            new_indexs = cls.modify_table(metadata, table)
            if len(new_indexs) > 0:
                indexes.extend(cls.modify_table(metadata, table))

        return indexes

    @classmethod
    def modify_select(cls, metadata, table_name, select, join):
        """
        No transform for columns needed
        """
        return select, join


    @classmethod
    def modify_table(cls, metadata, table):
        """Helper function to apply the transformation to a table in place.
        See Transform.modify_table for signature.
        """

        indexes = []
        if not table.name in cls.drop_by_table:
            return indexes
        for col_name in cls.drop_by_table[table.name]:
            col = table.columns[col_name]
            if not col_name in cls.idx_by_column:
                continue
            index_name = cls.idx_by_column.get(col_name, "none")
            indexes.append(Index(index_name, col))

        return indexes
