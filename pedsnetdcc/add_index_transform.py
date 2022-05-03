from sqlalchemy.schema import Column, Index
from pedsnetdcc.abstract_transform import Transform


class AddIndexTransform(Transform):
    create_by_table = {
        'adt_occurrence': ('person_id', 'adt_date',),
        'care_site': ('place_of_service_concept_id', 'specialty_concept_id',),
        'condition_occurrence': ('condition_start_date', 'condition_type_concept_id',),
        'device_exposure': ('device_type_concept_id', 'device_exposure_start_date',),
        'drug_exposure': ('drug_type_concept_id', 'drug_exposure_start_date',),
        'fact_relationship': ('fact_id_1', 'fact_id_2',),
        'location': ('zip', 'state',),
        'measurement': ('measurement_date', 'measurement_type_concept_id',
                        'value_as_concept_id', 'value_as_number',),
        'measurement_organism': ('organism_concept_id', 'person_id', 'visit_occurrence_id', 'measurement_id',),
        'observation_period': ('observation_period_start_date', 'observation_period_end_date',),
        'person': ('birth_datetime', 'ethnicity_concept_id', 'race_concept_id', 'gender_concept_id',),
        'procedure_occurrence': ('procedure_date',),
        'provider': ('specialty_concept_id',),
        'visit_occurrence': ('care_site_id', 'provider_id', 'visit_start_date',),
        'visit_payer': ('plan_type',),
    }

    @classmethod
    def modify_select(cls, metadata, table_name, select, join, id_name='dcc'):
        """
        No transform for columns needed
        """
        return select, join

    @classmethod
    def modify_table(cls, metadata, table, id_type='BigInteger'):
        """Helper function to apply the transformation to a table in place.
        See Transform.modify_table for signature.
        """

        if not table.name in cls.create_by_table:
            return
        for col_name in cls.create_by_table[table.name]:
            col = table.columns[col_name]
            Index(Transform.make_index_name(table.name, col_name), col)
