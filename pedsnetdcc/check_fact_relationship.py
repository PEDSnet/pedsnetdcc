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
            'tot_fr_obs': sql_tot_obs_count,
            'tot_fr_meas': sql_tot_meas_count,
            'tot_fr_visit': sql_tot_visit_count,
            'bad_fr_obs': sql_bad_obs_count,
            'bad_fr_meas': sql_bad_meas_count,
            'bad_fr_visit': sql_bad_visit_count
        }

        results = parallel_db_exec(conn_str, sqls, count=1)

        for k, v in results.items():
            if 'data' not in v:
                logger.critical({'msg': 'Data not returned.', 'process': k})
                raise RuntimeError('Data not returned from'
                                   ' check_fact_relationship {0} query.'
                                   .format(k))

        if results['bad_fr_obs']['data'][0] != 0:
            bad = results['bad_fr_obs']['data'][0]
            total = results['tot_fr_obs']['data'][0]
            percent = (bad * 100 // total)
            logger.warn({
                'msg': 'Fact relationship table has bad observation refs.',
                'percent': percent,
                'bad': bad,
                'total': total
            })
        else:
            logger.info({
                'msg': 'Fact relationship observation refs are valid.'
            })

        if results['bad_fr_meas']['data'][0] != 0:
            bad = results['bad_fr_meas']['data'][0]
            total = results['tot_fr_meas']['data'][0]
            percent = (bad * 100 // total)
            logger.warn({
                'msg': 'Fact relationship table has bad measurement refs.',
                'percent': percent,
                'bad': bad,
                'total': total
            })
        else:
            logger.info({
                'msg': 'Fact relationship measurement refs are valid.'
            })

        if results['bad_fr_visit']['data'][0] != 0:
            bad = results['bad_fr_visit']['data'][0]
            total = results['tot_fr_visit']['data'][0]
            percent = (bad * 100 // total)
            logger.warn({
                'msg': 'Fact relationship table has bad visit refs.',
                'percent': percent,
                'bad': bad,
                'total': total
            })
        else:
            logger.info({
                'msg': 'Fact relationship visit refs are valid.'
            })

    elif output == 'samples':

        sqls = {
            'bad_fr_obs_id1_sample': sql_bad_obs_1_sample,
            'bad_fr_obs_id2_sample': sql_bad_obs_2_sample,
            'bad_fr_meas_id1_sample': sql_bad_meas_1_sample,
            'bad_fr_meas_id2_sample': sql_bad_meas_2_sample,
            'bad_fr_visit_id1_sample': sql_bad_visit_1_sample,
            'bad_fr_visit_id2_sample': sql_bad_visit_2_sample
        }

        results = parallel_db_exec(conn_str, sqls, count=1)

        for k, v in results.items():
            if 'data' not in v:
                logger.critical({'msg': 'Data not returned.', 'process': k})
                raise RuntimeError('Data not returned from'
                                   ' check_fact_relationship {0} query.'
                                   .format(k))

        if results['bad_fr_obs_id1_sample']['data']:
            names = results['bad_fr_obs_id1_sample']['field_names']
            data = results['bad_fr_obs_id1_sample']['data']
            sample = dict(zip(names, data))
            logger.warn({
                'msg': 'Fact relationship table has bad observation refs.',
                'field': 'fact_id_1',
                'sample': sample
            })

        if results['bad_fr_obs_id2_sample']['data']:
            names = results['bad_fr_obs_id2_sample']['field_names']
            data = results['bad_fr_obs_id2_sample']['data']
            sample = dict(zip(names, data))
            logger.warn({
                'msg': 'Fact relationship table has bad observation refs.',
                'field': 'fact_id_2',
                'sample': sample
            })

        if not results['bad_fr_obs_id1_sample']['data'] and \
                not results['bad_fr_obs_id2_sample']['data']:
            logger.info({
                'msg': 'Fact relationship table observation refs are valid.'
            })

        if results['bad_fr_meas_id1_sample']['data']:
            names = results['bad_fr_meas_id1_sample']['field_names']
            data = results['bad_fr_meas_id1_sample']['data']
            sample = dict(zip(names, data))
            logger.warn({
                'msg': 'Fact relationship table has bad measurement refs.',
                'field': 'fact_id_1',
                'sample': sample
            })

        if results['bad_fr_meas_id2_sample']['data']:
            names = results['bad_fr_meas_id2_sample']['field_names']
            data = results['bad_fr_meas_id2_sample']['data']
            sample = dict(zip(names, data))
            logger.warn({
                'msg': 'Fact relationship table has bad measurement refs.',
                'field': 'fact_id_2',
                'sample': sample
            })

        if not results['bad_fr_meas_id1_sample']['data'] and \
                not results['bad_fr_meas_id2_sample']['data']:
            logger.info({
                'msg': 'Fact relationship table measurement refs are valid.'
            })

        if results['bad_fr_visit_id1_sample']['data']:
            names = results['bad_fr_visit_id1_sample']['field_names']
            data = results['bad_fr_visit_id1_sample']['data']
            sample = dict(zip(names, data))
            logger.warn({
                'msg': 'Fact relationship table has bad visit refs.',
                'field': 'fact_id_1',
                'sample': sample
            })

        if results['bad_fr_visit_id2_sample']['data']:
            names = results['bad_fr_visit_id2_sample']['field_names']
            data = results['bad_fr_visit_id2_sample']['data']
            sample = dict(zip(names, data))
            logger.warn({
                'msg': 'Fact relationship table has bad visit refs.',
                'field': 'fact_id_2',
                'sample': sample
            })

        if not results['bad_fr_visit_id1_sample']['data'] and \
                not results['bad_fr_visit_id2_sample']['data']:
            logger.info({
                'msg': 'Fact relationship table visit refs are valid.'
            })
