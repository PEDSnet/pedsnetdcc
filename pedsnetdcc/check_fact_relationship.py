from __future__ import division
import logging
import time

from pedsnetdcc.db import Statement, StatementSet
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_data, check_stmt_err, combine_dicts

# TODO: Make below code dependent on the pedsnetdcc.FACT_RELATIONSHIP_DOMAINS
# dictionary, allowing for easy addition of new domains to the table.

# The below total fact_relationship record count statements assume that
# domain_concept_id_1 defines a record as being in a domain.

tot_obs_sql = '''
SELECT COUNT(*) FROM fact_relationship WHERE domain_concept_id_1 = 27
'''
tot_obs_msg = 'counting total observation records in fact relationship'

tot_meas_sql = '''
SELECT COUNT(*) FROM fact_relationship WHERE domain_concept_id_1 = 21
'''
tot_meas_msg = 'counting total measurement records in fact relationship'

tot_visit_sql = '''
SELECT COUNT(*) FROM fact_relationship WHERE domain_concept_id_1 = 8
'''
tot_visit_msg = 'counting total visit records in fact relationship'

tot_drug_sql = '''
SELECT COUNT(*) FROM fact_relationship WHERE domain_concept_id_1 = 13
'''
tot_drug_msg = 'counting total drug records in fact relationship'

tot_dev_sql = '''
SELECT COUNT(*) FROM fact_relationship WHERE domain_concept_id_1 = 17
'''
tot_dev_msg = 'counting total device records in fact relationship'

tot_cond_sql = '''
SELECT COUNT(*) FROM fact_relationship WHERE domain_concept_id_1 = 19
'''
tot_cond_msg = 'counting total condition records in fact relationship'

# The below bad fact_relationship record count statements return the
# number of bad *records* not the number of bad *references*. There may be
# more than one (two) bad references per record.

bad_obs_sql = '''
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
bad_obs_msg = 'counting invalid observation records in fact relationship'

bad_meas_sql = '''
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
bad_meas_msg = 'counting invalid measurement records in fact relationship'

bad_visit_sql = '''
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
bad_visit_msg = 'counting invalid visit records in fact relationship'

bad_drug_sql = '''
SELECT COUNT(*) FROM (
    SELECT f.*
    FROM fact_relationship f
        LEFT JOIN drug_exposure d ON f.fact_id_1 = d.drug_exposure_id
    WHERE domain_concept_id_1 = 13 AND d.drug_exposure_id IS NULL
UNION
    SELECT f.*
    FROM fact_relationship f
        LEFT JOIN  drug_exposure d ON f.fact_id_2 = d.drug_exposure_id
    WHERE domain_concept_id_2 = 13 AND d.drug_exposure_id IS NULL
) q
'''
bad_drug_msg = 'counting invalid drug records in fact relationship'

bad_dev_sql = '''
SELECT COUNT(*) FROM (
    SELECT f.*
    FROM fact_relationship f
        LEFT JOIN device_exposure d ON f.fact_id_1 = d.device_exposure_id
    WHERE domain_concept_id_1 = 17 AND d.device_exposure_id IS NULL
UNION
    SELECT f.*
    FROM fact_relationship f
        LEFT JOIN  device_exposure d ON f.fact_id_2 = d.device_exposure_id
    WHERE domain_concept_id_2 = 17 AND d.device_exposure_id IS NULL
) q
'''
bad_dev_msg = 'counting invalid device records in fact relationship'

bad_cond_sql = '''
SELECT COUNT(*) FROM (
    SELECT f.*
    FROM fact_relationship f
        LEFT JOIN condition_occurrence c ON f.fact_id_1 = c.condition_occurrence_id
    WHERE domain_concept_id_1 = 19 AND c.condition_occurrence_id IS NULL
UNION
    SELECT f.*
    FROM fact_relationship f
        LEFT JOIN condition_occurrence c ON f.fact_id_2 = c.condition_occurrence_id
    WHERE domain_concept_id_2 = 19 AND c.condition_occurrence_id IS NULL
) q
'''
bad_cond_msg = 'counting invalid condition records in fact relationship'

# The below bad fact_relationship sample statements check each fact_id field
# independently for each domain.

bad_obs_1_sql = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN observation o ON f.fact_id_1 = o.observation_id
WHERE domain_concept_id_1 = 27 AND o.observation_id IS NULL
LIMIT 1
'''
bad_obs_1_msg = 'searching for invalid observation ref in fact_id_1'

bad_obs_2_sql = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN observation o ON f.fact_id_2 = o.observation_id
WHERE domain_concept_id_2 = 27 AND o.observation_id IS NULL
LIMIT 1
'''
bad_obs_2_msg = 'searching for invalid observation ref in fact_id_2'

bad_meas_1_sql = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN measurement m ON f.fact_id_1 = m.measurement_id
WHERE domain_concept_id_1 = 21 AND m.measurement_id IS NULL
LIMIT 1
'''
bad_meas_1_msg = 'searching for invalid measurement ref in fact_id_1'

bad_meas_2_sql = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN measurement m ON f.fact_id_2 = m.measurement_id
WHERE domain_concept_id_2 = 21 AND m.measurement_id IS NULL
LIMIT 1
'''
bad_meas_2_msg = 'searching for invalid measurement ref in fact_id_2'

bad_visit_1_sql = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN visit_occurrence v ON f.fact_id_1 = v.visit_occurrence_id
WHERE domain_concept_id_1 = 8 AND v.visit_occurrence_id IS NULL
LIMIT 1
'''
bad_visit_1_msg = 'searching for invalid visit ref in fact_id_1'

bad_visit_2_sql = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN visit_occurrence v ON f.fact_id_2 = v.visit_occurrence_id
WHERE domain_concept_id_2 = 8 AND v.visit_occurrence_id IS NULL
LIMIT 1
'''
bad_visit_2_msg = 'searching for invalid visit ref in fact_id_2'

bad_drug_1_sql = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN drug_exposure d ON f.fact_id_1 = d.drug_exposure_id
WHERE domain_concept_id_1 = 13 AND d.drug_exposure_id IS NULL
LIMIT 1
'''
bad_drug_1_msg = 'searching for invalid drug ref in fact_id_1'

bad_drug_2_sql = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN drug_exposure d ON f.fact_id_2 = d.drug_exposure_id
WHERE domain_concept_id_2 = 13 AND d.drug_exposure_id IS NULL
LIMIT 1
'''
bad_drug_2_msg = 'searching for invalid drug ref in fact_id_2'

bad_dev_1_sql = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN device_exposure d ON f.fact_id_1 = d.device_exposure_id
WHERE domain_concept_id_1 = 17 AND d.device_exposure_id IS NULL
LIMIT 1
'''
bad_dev_1_msg = 'searching for invalid device ref in fact_id_1'

bad_dev_2_sql = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN device_exposure d ON f.fact_id_2 = d.device_exposure_id
WHERE domain_concept_id_2 = 17 AND d.device_exposure_id IS NULL
LIMIT 1
'''
bad_dev_2_msg = 'searching for invalid device ref in fact_id_2'

bad_cond_1_sql = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN condition_occurrence c ON f.fact_id_1 = c.condition_occurrence_id
WHERE domain_concept_id_1 = 19 AND c.condition_occurrence_id IS NULL
LIMIT 1
'''
bad_cond_1_msg = 'searching for invalid condition ref in fact_id_1'

bad_cond_2_sql = '''
SELECT f.*
FROM fact_relationship f
    LEFT JOIN condition_occurrence c ON f.fact_id_2 = c.condition_occurrence_id
WHERE domain_concept_id_2 = 19 AND c.condition_occurrence_id IS NULL
LIMIT 1
'''
bad_cond_2_msg = 'searching for invalid condition ref in fact_id_2'

logger = logging.getLogger(__name__)


def check_fact_relationship(conn_str, output='both', pool_size=None):
    """Check the referential integrity of the fact relationship table.

    Execute sql statements, in parallel, to inspect the fact relationship table
    for validity and format and log the results for user inspection. Return a
    boolean to indicate whether the table is valid or not. The `pool_size`
    parameter can optionally be used to control the number of parallel workers
    to use when executing the sql statements.

    If `output` is 'percent', count the total records and bad records in each
    of the three current fact domains. If no bad records are found, emit an
    info-level log message for each domain saying so. If any bad records are
    found, emit a warning-level log message with the results, for each domain.

    If `output` is 'samples', try to find one sample of a bad record in each
    domain for each fact id field and either emit an info-level log message
    saying that none were found or emit a warning-level log message with the
    results.

    If `output` is 'both', both sets of checks are run. This is the default.

    :param str conn_str:  the connection string for the database
    :param str output:    the type of checks to run and output to produce
    :param pool_size:     the number of parallel workers to use
    :type pool_size:      None or int
    :returns:             whether the table is valid
    :rtype:               bool
    :raises RuntimeError: if data is not returned from one of the sql queries
    """


    logger.info({'msg': 'starting fact relationship check'})
    starttime = time.time()

    stmts = StatementSet()

    if pool_size is None:
        pool_size = 5;

    # Build appropriate set of statements based on output type.
    if output in ['percent', 'both']:

        stmts.add(Statement(tot_obs_sql, tot_obs_msg))
        stmts.add(Statement(tot_meas_sql, tot_meas_msg))
        stmts.add(Statement(tot_visit_sql, tot_visit_msg))
        stmts.add(Statement(tot_drug_sql, tot_drug_msg))
        stmts.add(Statement(tot_dev_sql, tot_dev_msg))
        stmts.add(Statement(tot_cond_sql, tot_cond_msg))
        stmts.add(Statement(bad_obs_sql, bad_obs_msg))
        stmts.add(Statement(bad_meas_sql, bad_meas_msg))
        stmts.add(Statement(bad_visit_sql, bad_visit_msg))
        stmts.add(Statement(bad_drug_sql, bad_drug_msg))
        stmts.add(Statement(bad_dev_sql, bad_dev_msg))
        stmts.add(Statement(bad_cond_sql, bad_cond_msg))

    if output in ['samples', 'both']:

        stmts.add(Statement(bad_obs_1_sql, bad_obs_1_msg))
        stmts.add(Statement(bad_obs_2_sql, bad_obs_2_msg))
        stmts.add(Statement(bad_meas_1_sql, bad_meas_1_msg))
        stmts.add(Statement(bad_meas_2_sql, bad_meas_2_msg))
        stmts.add(Statement(bad_visit_1_sql, bad_visit_1_msg))
        stmts.add(Statement(bad_visit_2_sql, bad_visit_2_msg))
        stmts.add(Statement(bad_drug_1_sql, bad_drug_1_msg))
        stmts.add(Statement(bad_drug_2_sql, bad_drug_2_msg))
        stmts.add(Statement(bad_dev_1_sql, bad_dev_1_msg))
        stmts.add(Statement(bad_dev_2_sql, bad_dev_2_msg))
        stmts.add(Statement(bad_cond_1_sql, bad_cond_1_msg))
        stmts.add(Statement(bad_cond_2_sql, bad_cond_2_msg))

    stmts.parallel_execute(conn_str, pool_size)

    results = {
        'obs': {'total': None, 'bad': None, 'percent': None,
                'samples': [], 'valid': True},
        'meas': {'total': None, 'bad': None, 'percent': None,
                 'samples': [], 'valid': True},
        'visit': {'total': None, 'bad': None, 'percent': None,
                  'samples': [], 'valid': True},
        'drug': {'total': None, 'bad': None, 'percent': None,
                  'samples': [], 'valid': True},
        'dev': {'total': None, 'bad': None, 'percent': None,
                 'samples': [], 'valid': True},
        'cond': {'total': None, 'bad': None, 'percent': None,
                'samples': [], 'valid': True},

    }

    for stmt in stmts:

        # Will raise RuntimeError if stmt.err is not None.
        check_stmt_err(stmt, 'fact relationship check')

        # Place results into intermediate data structure. Note that only the
        # data relevant to the output type will be populated and that only
        # statements which are expected to produce data are checked for it,
        # raising a RuntimeError if stmt.data is None or 0 length.

        sample = None

        if stmt.msg == tot_obs_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['obs']['total'] = stmt.data[0][0]

        elif stmt.msg == tot_meas_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['meas']['total'] = stmt.data[0][0]

        elif stmt.msg == tot_visit_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['visit']['total'] = stmt.data[0][0]

        elif stmt.msg == tot_drug_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['drug']['total'] = stmt.data[0][0]

        elif stmt.msg == tot_dev_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['dev']['total'] = stmt.data[0][0]

        elif stmt.msg == tot_cond_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['cond']['total'] = stmt.data[0][0]

        elif stmt.msg == bad_obs_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['obs']['bad'] = stmt.data[0][0]

        elif stmt.msg == bad_meas_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['meas']['bad'] = stmt.data[0][0]

        elif stmt.msg == bad_visit_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['visit']['bad'] = stmt.data[0][0]

        elif stmt.msg == bad_drug_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['drug']['bad'] = stmt.data[0][0]

        elif stmt.msg == bad_dev_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['dev']['bad'] = stmt.data[0][0]

        elif stmt.msg == bad_cond_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['cond']['bad'] = stmt.data[0][0]

        elif stmt.msg == bad_obs_1_msg:
            if len(stmt.data) > 0:
                domain = 'obs'
                field = 'fact_id_1'
                sample = dict(zip(stmt.fields, stmt.data[0]))

        elif stmt.msg == bad_obs_2_msg:
            if len(stmt.data) > 0:
                domain = 'obs'
                field = 'fact_id_2'
                sample = dict(zip(stmt.fields, stmt.data[0]))

        elif stmt.msg == bad_meas_1_msg:
            if len(stmt.data) > 0:
                domain = 'meas'
                field = 'fact_id_1'
                sample = dict(zip(stmt.fields, stmt.data[0]))

        elif stmt.msg == bad_meas_2_msg:
            if len(stmt.data) > 0:
                domain = 'meas'
                field = 'fact_id_2'
                sample = dict(zip(stmt.fields, stmt.data[0]))

        elif stmt.msg == bad_visit_1_msg:
            if len(stmt.data) > 0:
                domain = 'visit'
                field = 'fact_id_1'
                sample = dict(zip(stmt.fields, stmt.data[0]))

        elif stmt.msg == bad_visit_2_msg:
            if len(stmt.data) > 0:
                domain = 'visit'
                field = 'fact_id_2'
                sample = dict(zip(stmt.fields, stmt.data[0]))

        elif stmt.msg == bad_drug_1_msg:
            if len(stmt.data) > 0:
                domain = 'drug'
                field = 'fact_id_1'
                sample = dict(zip(stmt.fields, stmt.data[0]))

        elif stmt.msg == bad_drug_2_msg:
            if len(stmt.data) > 0:
                domain = 'drug'
                field = 'fact_id_2'
                sample = dict(zip(stmt.fields, stmt.data[0]))

        elif stmt.msg == bad_dev_1_msg:
            if len(stmt.data) > 0:
                domain = 'dev'
                field = 'fact_id_1'
                sample = dict(zip(stmt.fields, stmt.data[0]))

        elif stmt.msg == bad_dev_2_msg:
            if len(stmt.data) > 0:
                domain = 'dev'
                field = 'fact_id_2'
                sample = dict(zip(stmt.fields, stmt.data[0]))

        elif stmt.msg == bad_cond_1_msg:
            if len(stmt.data) > 0:
                domain = 'cond'
                field = 'fact_id_1'
                sample = dict(zip(stmt.fields, stmt.data[0]))

        elif stmt.msg == bad_cond_2_msg:
            if len(stmt.data) > 0:
                domain = 'cond'
                field = 'fact_id_2'
                sample = dict(zip(stmt.fields, stmt.data[0]))

        if sample:
            results[domain]['samples'].append({'field': field,
                                               'row': sample})

    # Calculate derived results.
    for domain, result in results.items():
        if result['bad'] and result['total']:
            result['percent'] = (result['bad'] * 100 // result['total'])

        if ((result['bad'] and result['bad'] > 0) or
                len(result['samples']) > 0):
            result['valid'] = False

    # Output messages based on results.
    if results['obs']['valid']:
        logger.info({
            'msg': 'all fact relationship observation references are valid'
        })
    else:
        logger.warn(combine_dicts({'msg': 'fact relationship table has bad'
                                   ' observation references'},
                                  results['obs']))

    if results['meas']['valid']:
        logger.info({
            'msg': 'all fact relationship measurement references are valid'
        })
    else:
        logger.warn(combine_dicts({'msg': 'fact relationship table has bad'
                                   ' measurement references'},
                                  results['meas']))

    if results['visit']['valid']:
        logger.info({
            'msg': 'all fact relationship visit references are valid'
        })
    else:
        logger.warn(combine_dicts({'msg': 'fact relationship table has bad'
                                   ' visit references'},
                                  results['visit']))

    if results['drug']['valid']:
        logger.info({
            'msg': 'all fact relationship drug references are valid'
        })
    else:
        logger.warn(combine_dicts({'msg': 'fact relationship table has bad'
                                   ' drug references'},
                                  results['drug']))

    if results['dev']['valid']:
        logger.info({
            'msg': 'all fact relationship device references are valid'
        })
    else:
        logger.warn(combine_dicts({'msg': 'fact relationship table has bad'
                                   ' device references'},
                                  results['dev']))

    if results['cond']['valid']:
        logger.info({
            'msg': 'all fact relationship condition references are valid'
        })
    else:
        logger.warn(combine_dicts({'msg': 'fact relationship table has bad'
                                   ' condition references'},
                                  results['cond']))

    # Overall fact relationship validity.
    valid = (results['obs']['valid'] and results['meas']['valid'] and
             results['visit']['valid'] and results['drug']['valid'] and
             results['dev']['valid'] and results['cond']['valid'])

    # Output summary message.
    logger.info({'msg': 'finished fact relationship check', 'valid': valid,
                 'elapsed': secs_since(starttime)})

    return valid
