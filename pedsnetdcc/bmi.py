import logging
import time
import hashlib
import os
import re
import subprocess

from pedsnetdcc.db import StatementSet, Statement
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, combine_dicts, get_conn_info_dict, vacuum)
#from sh import docker

logger = logging.getLogger(__name__)
NAME_LIMIT = 30
CREATE_MEASURE_LIKE_TABLE_SQL = 'create table measurement_bmi (like measurement)'
DROP_NULL_BMI_TABLE_SQL = 'alter table measurement_bmi alter column measurement_id drop not null;'
IDX_MEASURE_LIKE_TABLE_SQL = 'create index {0} on measurement_bmi ({1})'


def _create_config_file(config_file, schema, password, conn_info_dict):
    with open(config_file, 'wb') as out_config:
        out_config.write('ht_measurement_concept_ids = 3023540,3036277' + os.linesep)
        out_config.write('wt_measurement_concept_ids = 3013762' + os.linesep)
        out_config.write('bmi_measurement_concept_id = 3038553' + os.linesep)
        out_config.write('bmi_measurement_type_concept_id = 45754907'+ os.linesep)
        out_config.write('bmi_unit_concept_id = 9531' + os.linesep)
        out_config.write('input_measurement_table = measurement_anthro' + os.linesep)
        out_config.write('output_chunk_size = 1000' + os.linesep)
        out_config.write('person_chunk_size = 1000' + os.linesep)
        out_config.write('clone_bmi_measurement = 1' + os.linesep)
        out_config.write('verbose = 1' + os.linesep)
        out_config.write('<src_rdb>' + os.linesep)
        out_config.write('driver = Pg' + os.linesep)
        out_config.write('host = ' + conn_info_dict.get('host') + os.linesep)
        out_config.write('database = ' + conn_info_dict.get('dbname') + os.linesep)
        out_config.write('schema = ' + schema + os.linesep)
        out_config.write('username = ' + conn_info_dict.get('user') + os.linesep)
        out_config.write('password = ' + password + os.linesep)
        out_config.write('domain = stage' + os.linesep)
        out_config.write('type = dcc' + os.linesep)
        out_config.write('post_connect_sql = set search_path to ' + schema + ',vocabulary;' + os.linesep)
        out_config.write('</src_rdb>' + os.linesep)
        out_config.write('output_measurement_table = measurement_bmi'+ os.linesep)
        out_config.write('person_finder_sql = select distinct person_id from measurement_anthro ')
        out_config.write('where measurement_concept_id in (3013762, 3023540, 3036277)' + os.linesep)


def _make_index_name(table_name, column_name):
    """
    Create an index name for a given table/column combination with
    a NAME_LIMIT-character (Oracle) limit.  The table/column combination
    `provider.gender_source_concept_name` results in the index name
    `pro_gscn_ae1fd5b22b92397ca9_ix`.  We opt for a not particularly
    human-readable name in order to avoid collisions, which are all too
    possible with columns like provider.gender_source_concept_name and
    person.gender_source_concept_name.
    :param str table_name:
    :param str column_name:
    :rtype: str
    """
    table_abbrev = "mea_" + table_name[:3]
    column_abbrev = ''.join([x[0] for x in column_name.split('_')])
    md5 = hashlib.md5(
        '{}.{}'.format(table_name, column_name).encode('utf-8')). \
        hexdigest()
    hashlen = NAME_LIMIT - (len(table_abbrev) + len(column_abbrev) +
                            3 * len('_') + len('ix'))
    return '_'.join([table_abbrev, column_abbrev, md5[:hashlen], 'ix'])


def run_bmi_calc(config_file, conn_str, site, password, search_path, model_version):
    """Run the BMI tool.

    * Create config file
    * Create output table
    * Run BMI
    * Index output table
    * Vacuum output table

    :param str config_file:   config file name
    :param str conn_str:      database connection string
    :param str site:    site to run BMI for
    :param str password:    user's password
    :param str search_path: PostgreSQL schema search path
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    conn_info_dict = get_conn_info_dict(conn_str)

    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'site': site, },
                             conn_info_dict)
    logger.info(combine_dicts({'msg': 'starting BMI calculation'},
                              log_dict))
    start_time = time.time()
    schema = primary_schema(search_path)

    if password == None:
        pass_match = re.search(r"password=(\S*)", conn_str)
        password = pass_match.group(1)

    # create the congig file
    _create_config_file(config_file, schema, password, conn_info_dict)

    # create measurement_bmi table

    # Add a creation statement.
    stmts = StatementSet()
    drop_stmt = Statement(DROP_NULL_BMI_TABLE_SQL)
    stmts.add(drop_stmt)
    create_stmt = Statement(CREATE_MEASURE_LIKE_TABLE_SQL)
    stmts.add(create_stmt)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            stmt.execute(conn_str)
            check_stmt_err(stmt, 'Run BMI calculation')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'create BMI table failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise

    # Run BMI tool
    cwd = os.getcwd()
    command = 'docker run -v {0}:/working -v /var/run/docker.sock:/var/run/docker.sock ' \
              '--rm pedsnet-derivation-bmi derive_bmi {1}_temp --verbose=2'.format(cwd, site)
    process = subprocess.Popen([command], shell=True, stdout=subprocess.PIPE)
    out, err = process.communicate()
    print(out)

    #docker.run('-v {0}:/working --rm -it pedsnet-derivation-bmi derive_bmi {1}_temp --verbose=2'.format(cwd, site))

    # Add indexes to measurement_bmi (same as measurement)
    stmts.clear()
    col_index = ('measurement_age_in_months', 'measurement_concept_id', 'measurement_date',
                 'measurement_type_concept_id', 'person_id', 'site', 'visit_occurrence_id',
                 'measurement_source_value', 'value_as_concept_id', 'value_as_number',)

    for col in col_index:
        idx_name = _make_index_name('bmi', col)
        idx_stmt = Statement(IDX_MEASURE_LIKE_TABLE_SQL.
                                 format(idx_name, col))
        stmts.add(idx_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Run BMI calculation')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding indexes failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise

    # Vacuum analyze tables for piney freshness.
    vacuum(conn_str, model_version, analyze=True, tables=['measurement_bmi'])

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished BMI calculation',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True
