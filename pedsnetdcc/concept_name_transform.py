from sqlalchemy.schema import Column, Index

from pedsnetdcc.abstract_transform import Transform


class ConceptNameTransform(Transform):
    @classmethod
    def modify_select(cls, metadata, table_name, select, join, target_table):
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

        return select, join

    @classmethod
    def modify_table(cls, metadata, table):
        """Helper function to apply the transformation to a table in place.
        See Transform.modify_table for signature.
        """
        concept_name = metadata.tables['concept'].c.concept_name

        orig_columns = table.c
        for col in orig_columns:
            if col.name.endswith('_concept_id'):
                new_col_name = col.name.replace('_concept_id', '_concept_name')
                new_col = Column(new_col_name, concept_name.type)
                table.append_column(new_col)
                Index(Transform.make_index_name(table.name, new_col_name),
                      new_col)
