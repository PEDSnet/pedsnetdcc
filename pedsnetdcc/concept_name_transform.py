import hashlib

from sqlalchemy.schema import Column, Index

from pedsnetdcc import VOCAB_TABLES
from pedsnetdcc.abstract_transform import Transform


# Oracle's identifier length maximum is 30 bytes, so we must limit
# object identifier length (e.g. for columns and indexes).
NAME_LIMIT = 30


def make_index_name(table_name, column_name):
    """
    Create an index name for a given table/column combination with
    a NAME_LIMIT-character (Oracle) limit.  The table/column combination
    `provider.gender_source_concept_name` results in the index name
    `pro_gscn_ae1fd5b22b92397ca9_ix`.  We opt for a not particularly
    human-readable name in order to avoid collisions, which are all too
    possible with columns like provider.gender_source_concept_name and
    person.gender_source_concept_name.
    """
    table_abbrev = table_name[:3]
    column_abbrev = ''.join([x[0] for x in column_name.split('_')])
    md5 = hashlib.md5(
        '{}.{}'.format(table_name, column_name).encode('utf-8')).hexdigest()
    hashlen = NAME_LIMIT - (len(table_abbrev) + len(column_abbrev) +
                            3 * len('_') + len('ix'))
    return '_'.join([table_abbrev, column_abbrev, md5[:hashlen], 'ix'])


class ConceptNameTransform(Transform):
    @classmethod
    def modify_select(cls, metadata, table_name, select, join):
        """Add a _concept_name column for each _concept_id column in a table.

        Requirements: `concept` table exists.

        See Transform.modify_select for signature.
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

        print("modify_select: ", hasattr(cls, 'modify_select'))
        print("nonexistent: ", hasattr(cls, 'nonexistent'))

        return select, join

    @classmethod
    def modify_table(cls, metadata, table):
        """Helper function to apply the transformation to a table in place.
        :param sqlalchemy.schema.MetaData metadata:
        :param sqlalchemy.schema.Table table:
        :return: None
        """
        concept_name = metadata.tables['concept'].c.concept_name

        orig_columns = table.c
        for col in orig_columns:
            if col.name.endswith('_concept_id'):
                new_col_name = col.name.replace('_concept_id', '_concept_name')
                new_col = Column(new_col_name, concept_name.type)
                table.append_column(new_col)
                Index(make_index_name(table.name, new_col_name), new_col)

    @classmethod
    def modify_metadata(cls, metadata):
        """Add a _concept_name column for each _concept_id column in a table

        Requirements: `concept` table exists.

        See Transform.modify_metadata for signature.
        """
        for table in metadata.tables.values():
            if table in VOCAB_TABLES:
                continue

            cls.modify_table(metadata, table)

        return metadata
