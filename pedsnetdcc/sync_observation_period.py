import logging
import psycopg2
import time

from pedsnetdcc.dict_logging import secs_since

sql_create_date_table = '''
CREATE TEMP TABLE date_limit
    (person_id, table_name, min_datetime, max_datetime)
AS
    SELECT person_id, 'visit_occurrence',
           min(coalesce(visit_start_time, visit_start_date)),
           max(coalesce(visit_end_time, visit_end_date))
    FROM visit_occurrence
    GROUP BY person_id
UNION ALL
    SELECT person_id, 'procedure_occurrence',
           min(coalesce(procedure_time, procedure_date)),
           max(coalesce(procedure_time, procedure_date))
    FROM procedure_occurrence
    GROUP BY person_id
UNION ALL
    SELECT person_id, 'condition_occurrence',
           min(coalesce(condition_start_time, condition_start_date)),
           max(coalesce(condition_end_time, condition_end_date))
    FROM condition_occurrence
    GROUP BY person_id
UNION ALL
    SELECT person_id, 'drug_exposure',
           min(coalesce(drug_exposure_start_time, drug_exposure_start_date)),
           max(coalesce(drug_exposure_end_time, drug_exposure_end_date))
    FROM drug_exposure
    GROUP BY person_id
UNION ALL
    SELECT person_id, 'observation',
           min(coalesce(observation_time, observation_date)),
           max(coalesce(observation_time, observation_date))
    FROM observation
    GROUP BY person_id
UNION ALL
    SELECT person_id, 'measurement',
           min(coalesce(measurement_time, measurement_date)),
           max(coalesce(measurement_time, measurement_date))
    FROM measurement
    GROUP BY person_id
UNION ALL
    SELECT person_id, 'death',
           min(coalesce(death_time, death_date)),
           max(coalesce(death_time, death_date))
    FROM death
    GROUP BY person_id
'''

sql_fill_null_maxes = '''
UPDATE date_limit SET (max_datetime) = (min_datetime)
    WHERE max_datetime IS NULL
'''

sql_delete_obs_period = '''
DELETE FROM observation_period
'''

sql_fill_obs_period = '''
INSERT INTO observation_period (
    person_id, observation_period_start_date, observation_period_end_date,
    observation_period_start_time, observation_period_end_time,
    period_type_concept_id, observation_period_id
) SELECT
    person_id, min(min_datetime), coalesce(max(max_datetime),
    max(min_datetime)), min(min_datetime), coalesce(max(max_datetime),
    max(min_datetime)), 44814724, row_number() over (range unbounded preceding)
FROM date_limit
GROUP BY person_id
'''

logger = logging.getLogger(__name__)


def sync_observation_period(conn_str):

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cursor:

            logger.info({'msg': 'Starting observation period sync.'})
            starttime = time.time()

            logger.debug({'msg': 'Creating temporary date table.',
                          'sql': sql_create_date_table})
            cursor.execute(sql_create_date_table)

            logger.debug({'msg': 'Filling in null max dates.',
                          'sql': sql_fill_null_maxes})
            cursor.execute(sql_fill_null_maxes)

            logger.debug({'msg': 'Deleting existing observation period rows.',
                          'sql': sql_delete_obs_period})
            cursor.execute(sql_delete_obs_period)
            deleted = cursor.rowcount

            logger.debug({'msg': ('Populating observation period with new'
                          ' rows.'), 'sql': sql_fill_obs_period})
            cursor.execute(sql_fill_obs_period)

            logger.info({'msg': 'Finished observation period sync.',
                         'deleted': deleted, 'created': cursor.rowcount,
                         'seconds': secs_since(starttime)})

    conn.close()
