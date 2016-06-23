import psycopg2
from sqlalchemy import text, literal_column
from sqlalchemy.schema import Column, Index
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION

from pedsnetdcc.abstract_transform import Transform

AGE_FUNCTIONS_SQL = (
"""create or replace function last_month_of_interval(timestamp, timestamp)
     returns timestamp strict immutable language sql as $$
       select $1 + interval '1 year' * extract(years from age($2, $1)) + interval '1 month' * extract(months from age($2, $1))
$$""",
"""comment on function last_month_of_interval(timestamp, timestamp) is
'Return the timestamp of the last month of the interval between two timestamps'""",

"""create or replace function month_after_last_month_of_interval(timestamp, timestamp)
     returns timestamp strict immutable language sql as $$
       select $1 + interval '1 year' * extract(years from age($2, $1)) + interval '1 month' * (extract(months from age($2, $1)) + 1)
$$""",
"""comment on function month_after_last_month_of_interval(timestamp, timestamp) is
'Return the timestamp of the month AFTER the last month of the interval between two timestamps'""",

"""create or replace function days_in_last_month_of_interval(timestamp, timestamp)
     returns double precision strict immutable language sql as $$
       select extract(days from month_after_last_month_of_interval($1, $2) - last_month_of_interval($1, $2))
$$""",
"""comment on function days_in_last_month_of_interval(timestamp, timestamp) is
'Return the number of days in the last month of the interval between two timestamps'""",

"""create or replace function months_in_interval(timestamp, timestamp)
 returns double precision strict immutable language sql as $$
  select extract(years from age($2, $1)) * 12 + extract(months from age($2, $1)) + extract(days from age($2, $1))/days_in_last_month_of_interval($1, $2)
$$""",
"""comment on function months_in_interval(timestamp, timestamp) is
   'Return the number of months (double precision) between two timestamps.
    The number of years/months/days is computed by PostgreSQL''s
    extract/date_part function.  The fractional months value is
    computed by dividing the extracted number of days by the total
    number of days in the last month overlapping the interval; note
    that this is not a calendar month but, say, the number of days
    between Feb 2, 2001 and Mar 2, 2001.  You should be able to obtain
    the original timestamp from the resulting value, albeit with great
    difficulty.'"""
)

class AgeTransform(Transform):
    # Caller may override `columns`
    columns = [
        ('condition_occurrence', 'condition_start_time'),
        ('death', 'death_time'),
        ('drug_exposure', 'drug_exposure_start_time'),
        ('measurement', 'measurement_time'),
        ('measurement', 'measurement_result_time'),
        ('procedure_occurrence', 'procedure_time'),
        ('visit_occurrence', 'visit_start_time'),
        ('observation', 'observation_time')
    ]
    AGE_COLUMN_TYPE = 'float'

    @classmethod
    def is_age_column(cls, column_name, table_name):
        # Almost makes me want to change the data structure
        for age_table, age_col in cls.columns:
            if age_table == table_name and age_col == column_name:
                return True
        return False

    @classmethod
    def pre_transform(cls, dburi, search_path):
        """Define PL/SQL functions needed for the age transform
        See also Transform.pre_transform.
        """
        with psycopg2.connect(dburi) as conn:
            with conn.cursor() as cursor:

                if search_path:
                    # SQL injection vulnerability?
                    cursor.execute(
                        'SET search_path TO {0}'.format(search_path))

                for stmt in AGE_FUNCTIONS_SQL:
                    cursor.execute(stmt)

    @classmethod
    def modify_select(cls, metadata, table_name, select, join):
        """Add age columns for some time columns.

        Not all time columns in the PEDSnet model are transformed. There is
        a built-in list (AgeTransform.columns) of (table, column) tuples
        which may be overridden by the user before invoking
        AgeTransform.modify_select().
        """
        person = metadata.tables['person']

        # Don't join person twice to the same table
        joined_already_for_table_name = {}

        for table_name, col_name in AgeTransform.columns:

            # Make sure table/column pair is valid
            if (not table_name in metadata.tables
                    or not col_name in metadata.tables[table_name].c):
                raise ValueError(
                    'Invalid column: {0}.{1}'.format(table_name, col_name))

            table = metadata.tables[table_name]
            # Make sure table has `person_id` column
            if not 'person_id' in table.c:
                raise ValueError(
                    "Table {0} has no `person_id` column".format(table_name))

            col = table.c[col_name]
            new_col_name = col_name.replace('_time', '_age_in_months')

            new_col = literal_column(
                'months_in_interval(person.time_of_birth, {tbl}.{col})'.format(
                    tbl=table_name, col=col_name
                )).label(new_col_name)

            if not joined_already_for_table_name.get(table_name, False):
                join = join.join(person,
                                 person.c.person_id
                                 == table.c.person_id)
                joined_already_for_table_name[table_name] = True

            select = select.column(new_col)

        return select, join

    @classmethod
    def modify_table(cls, metadata, table):
        """Helper function to apply the transformation to a table in place.
        See Transform.modify_table for signature.
        """
        orig_columns = table.c
        for col in orig_columns:
            if cls.is_age_column(col.name, table.name):
                new_col_name = col.name.replace('_time', '_age_in_months')
                new_col = Column(new_col_name, DOUBLE_PRECISION)
                table.append_column(new_col)
                Index(Transform.make_index_name(table.name, new_col_name),
                      new_col)
