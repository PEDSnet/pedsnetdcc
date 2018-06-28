from sqlalchemy.schema import Column, Index

from pedsnetdcc.abstract_transform import Transform


class ConceptNameTransform(Transform):
    ignore_index_by_table = {
        'adt_occurrence': ('adt_type_concept_name', 'service_concept_name',),
        'care_site': ('specialty_concept_name','place_of_service_concept_name',),
        'condition_occurrence': ('condition_source_concept_name', 'condition_status_concept_name',
                                 'condition_type_concept_name', 'poa_concept_name',),
        'drug_exposure': ('dispense_as_written_concept_name', 'dose_unit_concept_name',
                          'drug_source_concept_name', 'drug_type_concept_name', 'route_concept_name',),
        'fact_relationship': ('relationship_concept_name',),
        'measurement': ('measurement_source_concept_name', 'measurement_type_concept_name',
                        'operator_concept_name', 'priority_concept_name', 'range_high_operator_concept_name',
                        'range_low_operator_concept_name', 'specimen_concept_name', 'unit_concept_name',
                        'value_as_concept_name',),
        'measurement_organism': ('organism_concept_name',),
        'observation': ('observation_concept_name', 'observation_source_concept_name', 'qualifier_concept_name',
                        'observation_type_concept_name', 'unit_concept_name', 'value_as_concept_name',),
        'observation_period': ('period_type_concept_name',),
        'person': ('ethnicity_source_concept_name', 'gender_source_concept_name', 'language_concept_name',
                   'language_source_concept_name', 'race_source_concept_name',),
        'procedure_occurrence': ('modifier_concept_name', 'procedure_source_concept_name',
                                 'procedure_type_concept_name',),
        'provider': ('gender_concept_name', 'gender_source_concept_name', 'specialty_concept_name',
                     'specialty_source_concept_name',),
        'visit_occurrence': ('admitting_source_concept_name', 'discharge_to_concept_name', 'visit_concept_name',
                             'visit_source_concept_name','visit_type_concept_name',),
        'visit_payer': ('visit_payer_type_concept_name',),
    }

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
                # Don't add indexes that are no longer needed
                if table.name in cls.ignore_index_by_table:
                    if new_col_name in cls.ignore_index_by_table[table.name]:
                        continue
                Index(Transform.make_index_name(table.name, new_col_name),
                      new_col)
