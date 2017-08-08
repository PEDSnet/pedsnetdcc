import logging
import time

from pedsnetdcc.db import Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_err, vacuum

create_date_table_sql = '''
CREATE TEMP TABLE date_limit
    (person_id, table_name, min_datetime, max_datetime)
AS
    SELECT person_id, 'visit_occurrence',
           min(coalesce(visit_start_datetime, visit_start_date)),
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
create_date_table_msg = 'creating the temporary domain date limits table'

fill_null_maxes_sql = '''
UPDATE date_limit SET (max_datetime) = (min_datetime)
    WHERE max_datetime IS NULL
'''
fill_null_maxes_msg = 'filling null max dates with mins in date limit table'

delete_obs_period_sql = '''
DELETE FROM observation_period
'''
delete_obs_period_msg = 'deleting all existing observation period rows'

fill_obs_period_sql = '''
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
fill_obs_period_msg = 'filling observation period with newly calculated rows'

logger = logging.getLogger(__name__)


def sync_observation_period(conn_str):
    """Sync the observation period table to the fact data.

    Delete any existing records in the observation period table and calculate a
    completely new set of records from the fact data in the database. Log the
    number of new records and return True if the process completes without
    error.

    :param str conn_str:  the connection string for the database
    :returns:             True if the function completes without error
    :rtype:               bool
    :raises RuntimeError: if any of the sql statements cause an error
    """

    logger.info({'msg': 'starting observation period sync'})
    starttime = time.time()

    # Build appropriate set of statements.
    stmts = StatementList()
    stmts.append(Statement(create_date_table_sql, create_date_table_msg))
    stmts.append(Statement(fill_null_maxes_sql, fill_null_maxes_msg))
    stmts.append(Statement(delete_obs_period_sql, delete_obs_period_msg))
    stmts.append(Statement(fill_obs_period_sql, fill_obs_period_msg))

    # Execute the statements serially in a single transaction.
    stmts.serial_execute(conn_str, True)

    for stmt in stmts:
        # Will raise RuntimeError if stmt.err is not None.
        check_stmt_err(stmt, 'observation period sync')

    # Vacuum tables. (The model_version argument is required...)
    vacuum(conn_str, '2.3.0', analyze=True, tables=['observation_period'])

    logger.info({'msg': 'finished observation period sync.',
                 'rowcount': stmts[3].rowcount,
                 'elapsed': secs_since(starttime)})

    # If reached without error, then success!
    return True
