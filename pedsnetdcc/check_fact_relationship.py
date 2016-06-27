from __future__ import division
import logging

from pedsnetdcc.parallel_db_exec import parallel_db_exec

logger = logging.getLogger('__name__')

# The below total fact_relationship record count statements assume that
# domain_concept_id_1 defines a record as being in a domain.

sql_tot_obs_count = '''
SELECT COUNT(*) FROM fact_relationship WHERE domain_concept_id_1 = 27
'''

sql_tot_meas_count = '''
SELECT COUNT(*) FROM fact_relationship WHERE domain_concept_id_1 = 21
'''

sql_tot_visit_count = '''
SELECT COUNT(*) FROM fact_relationship WHERE domain_concept_id_1 = 8
'''

# The below bad fact_relationship record count statements return the
# number of bad *records* not the number of bad *references*. There may be
# more than one (two) bad references per record.

sql_bad_obs_count = '''
SELECT COUNT(*) FROM (
    SELECT f.*
    FROM fact_relationship f
        LEFT JOIN observation o ON f.fact_id_1 = o.observation_id
    WHERE domain_concept_id_1 = 27 AND o.observation_id IS NULL
UNION
    SELECT f.*
    FROM fact_relationship f
        LEFT JOIN observation o ON f.fact_id_2 = o.observation_id
    WHERE domain_concept_id_2 = 27 AND o.observation_id IS NULL
) q
'''

sql_bad_meas_count = '''
SELECT COUNT(*) FROM (
    SELECT f.*
    FROM fact_relationship f
        LEFT JOIN measurement m ON f.fact_id_1 = m.measurement_id
    WHERE domain_concept_id_1 = 21 AND m.measurement_id IS NULL
UNION
    SELECT f.*
    FROM fact_relationship f
        LEFT JOIN measurement m ON f.fact_id_2 = m.measurement_id
    WHERE domain_concept_id_2 = 21 AND m.measurement_id IS NULL
) q
'''

sql_bad_visit_count = '''
SELECT COUNT(*) FROM (
    SELECT f.*
    FROM fact_relationship f
        LEFT JOIN visit_occurrence v ON f.fact_id_1 = v.visit_occurrence_id
    WHERE domain_concept_id_1 = 8 AND v.visit_occurrence_id IS NULL
UNION
    SELECT f.*
    FROM fact_relationship f
        LEFT JOIN visit_occurrence v ON f.fact_id_2 = v.visit_occurrence_id
    WHERE domain_concept_id_2 = 8 AND v.visit_occurrence_id IS NULL
) q
'''

sql_bad_obs_1_sample = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN observation o ON f.fact_id_1 = o.observation_id
WHERE domain_concept_id_1 = 27 AND o.observation_id IS NULL
LIMIT 1
'''

sql_bad_obs_2_sample = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN observation o ON f.fact_id_2 = o.observation_id
WHERE domain_concept_id_2 = 27 AND o.observation_id IS NULL
LIMIT 1
'''

sql_bad_meas_1_sample = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN measurement m ON f.fact_id_1 = m.measurement_id
WHERE domain_concept_id_1 = 21 AND m.measurement_id IS NULL
LIMIT 1
'''

sql_bad_meas_2_sample = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN measurement m ON f.fact_id_2 = m.measurement_id
WHERE domain_concept_id_2 = 21 AND m.measurement_id IS NULL
LIMIT 1
'''

sql_bad_visit_1_sample = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN visit_occurrence v ON f.fact_id_1 = v.visit_occurrence_id
WHERE domain_concept_id_1 = 8 AND v.visit_occurrence_id IS NULL
LIMIT 1
'''

sql_bad_visit_2_sample = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN visit_occurrence v ON f.fact_id_2 = v.visit_occurrence_id
WHERE domain_concept_id_2 = 8 AND v.visit_occurrence_id IS NULL
LIMIT 1
'''

logger = logging.getLogger(__name__)


def check_fact_relationship(conn_str, output='percent'):

    if output == 'percent':

        sqls = {
            'tot_obs': sql_tot_obs_count,
            'tot_meas': sql_tot_meas_count,
            'tot_visit': sql_tot_visit_count,
            'bad_obs': sql_bad_obs_count,
            'bad_meas': sql_bad_meas_count,
            'bad_visit': sql_bad_visit_count
        }

        results = parallel_db_exec(conn_str, sqls, count=1)

        bad_obs_percent = (results['bad_obs']['data'][0] * 100 //
                           results['tot_obs']['data'][0])
        bad_meas_percent = (results['bad_meas']['data'][0] * 100 //
                            results['tot_meas']['data'][0])
        bad_visit_percent = (results['bad_visit']['data'][0] * 100 //
                             results['tot_visit']['data'][0])

        print('percent bad observation records: {0}'.format(bad_obs_percent))
        print('percent bad measurement records: {0}'.format(bad_meas_percent))
        print('percent bad visit records: {0}'.format(bad_visit_percent))

    elif output == 'samples':

        sql_list = [
            sql_bad_obs_1_sample,
            sql_bad_obs_2_sample,
            sql_bad_meas_1_sample,
            sql_bad_meas_2_sample,
            sql_bad_visit_1_sample,
            sql_bad_visit_2_sample
        ]

        results = parallel_db_exec(conn_str, sql_list, count=1)

        print('bad observation in fact_id_1 sample: {0}'.format(results[0][0]))
        print('bad observation in fact_id_2 sample: {0}'.format(results[1][0]))
        print('bad measurement in fact_id_1 sample: {0}'.format(results[2][0]))
        print('bad measurement in fact_id_2 sample: {0}'.format(results[3][0]))
        print('bad visit in fact_id_1 sample: {0}'.format(results[4][0]))
        print('bad visit in fact_id_2 sample: {0}'.format(results[5][0]))
