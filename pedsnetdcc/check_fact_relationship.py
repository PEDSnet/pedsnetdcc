from __future__ import division
import logging
import time

from pedsnetdcc.db import Statement, StatementSet
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_data, check_stmt_err, combine_dicts

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

logger = logging.getLogger(__name__)


def check_fact_relationship(conn_str, output, pool_size=None):
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

    If `output` is 'both', both sets of checks are run.

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

    # Build appropriate set of statements based on output type.
    if output in ['percent', 'both']:

        stmts.add(Statement(tot_obs_sql, tot_obs_msg))
        stmts.add(Statement(tot_meas_sql, tot_meas_msg))
        stmts.add(Statement(tot_visit_sql, tot_visit_msg))
        stmts.add(Statement(bad_obs_sql, bad_obs_msg))
        stmts.add(Statement(bad_meas_sql, bad_meas_msg))
        stmts.add(Statement(bad_visit_sql, bad_visit_msg))

    if output in ['samples', 'both']:

        stmts.add(Statement(bad_obs_1_sql, bad_obs_1_msg))
        stmts.add(Statement(bad_obs_2_sql, bad_obs_2_msg))
        stmts.add(Statement(bad_meas_1_sql, bad_meas_1_msg))
        stmts.add(Statement(bad_meas_2_sql, bad_meas_2_msg))
        stmts.add(Statement(bad_visit_1_sql, bad_visit_1_msg))
        stmts.add(Statement(bad_visit_2_sql, bad_visit_2_msg))

    stmts.parallel_execute(conn_str, pool_size)

    results = {
        'obs': {'total': None, 'bad': None, 'percent': None,
                'samples': [], 'valid': True},
        'meas': {'total': None, 'bad': None, 'percent': None,
                 'samples': [], 'valid': True},
        'visit': {'total': None, 'bad': None, 'percent': None,
                  'samples': [], 'valid': True}
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

        elif stmt.msg == bad_obs_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['obs']['bad'] = stmt.data[0][0]

        elif stmt.msg == bad_meas_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['meas']['bad'] = stmt.data[0][0]

        elif stmt.msg == bad_visit_msg:
            check_stmt_data(stmt, 'fact relationship check')
            results['visit']['bad'] = stmt.data[0][0]

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

    # Overall fact relationship validity.
    valid = (results['obs']['valid'] and results['meas']['valid'] and
             results['visit']['valid'])

    # Output summary message.
    logger.info({'msg': 'finished fact relationship check', 'valid': valid,
                 'elapsed': secs_since(starttime)})

    return valid
