from pedsnetdcc import VOCAB_TABLES
from pedsnetdcc.abstract_transform import Transform


def modify_table(table):
    """
    pass
    :param table:
    :return:
    """
    pass


class ConceptNameTransform(Transform):
    @classmethod
    def modify_select(cls, metadata, table_name, select, join):
        """Add a _concept_name column for each _concept_id column in a table.

        Requirements: `concept` table exists.

        :param sqlalchemy.MetaData metadata:
        :param str table_name:
        :param sqlalchemy.Select select:
        :rtype: sqlalchemy.Select
        """
        concept = metadata.tables['concept']

        for col in select.c:
            if col.name.endswith('_concept_id'):
                new_name = col.name.replace('_concept_id', '_concept_name')
                concept_alias = concept.alias()
                new_col = concept_alias.c.concept_name.label(new_name)
                join = join.outerjoin(concept_alias,
                                                concept_alias.c.concept_id
                                                == col)
                select = select.column(new_col)

        return select, join

    @classmethod
    def modify_metadata(cls, metadata):
        """Add a _concept_name column for each _concept_id column in a table

        Requirements: `concept` table exists.

        :param sqlalchemy.MetaData metadata:
        :param str table_name:
        :param sqlalchemy.Select select:
        :rtype: sqlalchemy.Select
        """
        concept = metadata.tables['concept']

        for table in metadata.tables:
            if table in VOCAB_TABLES:
                continue

            modify_table(table)

        return metadata
