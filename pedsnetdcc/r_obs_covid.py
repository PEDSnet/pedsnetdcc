import logging
import time
import shutil
import os
import re
import hashlib

from pedsnetdcc.db import StatementSet, Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, check_stmt_data, combine_dicts,
                              get_conn_info_dict, vacuum, stock_metadata)
from sh import Rscript

logger = logging.getLogger(__name__)
NAME_LIMIT = 30
PK_OBS_LIKE_TABLE_SQL = 'alter table {0}.observation_derivation_covid add primary key(observation_id)'
IDX_OBS_LIKE_TABLE_SQL = 'create index {0} on {1}.observation_derivation_covid ({2})'
FK_OBS_LIKE_TABLE_SQL = 'alter table {0}.observation_derivation_covid add constraint {1} foreign key ({2}) references {3}({4})'
GRANT_OBS_LIKE_TABLE_SQL = 'grant select on table {0}.observation_derivation_covid to {1}'

def _create_argos_file(config_path, config_file, schema, password, conn_info_dict):
    with open(os.path.join(config_path, config_file), 'wb') as out_config:
        out_config.write('{' + os.linesep)
        out_config.write('"src_name": "Postgres",' + os.linesep)
        out_config.write('"src_args": {' + os.linesep)
        out_config.write('"host": "' + conn_info_dict.get('host') + '",' + os.linesep)
        out_config.write('"port": 5432,' + os.linesep)
        out_config.write('"dbname": "' + conn_info_dict.get('dbname') + '",' + os.linesep)
        out_config.write('"user": "' + conn_info_dict.get('user') + '",' + os.linesep)
        out_config.write('"password": "' + password + '",' + os.linesep)
        out_config.write('"bigint": "numeric",' + os.linesep)
        out_config.write('"options": "-c search_path=' + schema + ',vocabulary"' + os.linesep)
        out_config.write('}' + os.linesep)
        out_config.write('}' + os.linesep)


def _fix_site_info(file_path, site):
    try:
        with open(os.path.join(file_path,'site','site_info.R'), 'r') as site_file:
            file_data = site_file.read()
        file_data = file_data.replace('<SITE>', site)
        with open(os.path.join(file_path,'site','site_info.R'), 'w') as site_file:
            site_file.write(file_data)
    except:
        # this query package may not have this file
        return False

    return True


def _fix_run(file_path, site):
    try:
        with open(os.path.join(file_path,'site','run.R'), 'r') as site_file:
            file_data = site_file.read()
        file_data = file_data.replace('<SITE>', site)
        with open(os.path.join(file_path,'site','run.R'), 'w') as site_file:
            site_file.write(file_data)
    except:
        # this query package may not have this file
        return False

    return True


def _make_index_name(column_name):
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
    table_name = 'covid_derivation'
    table_abbrev = "obs_" + table_name[:3]
    column_abbrev = ''.join([x[0] for x in column_name.split('_')])
    md5 = hashlib.md5(
        '{}.{}'.format(table_name, column_name).encode('utf-8')). \
        hexdigest()
    hashlen = NAME_LIMIT - (len(table_abbrev) + len(column_abbrev) +
                            3 * len('_') + len('ix'))
    return '_'.join([table_abbrev, column_abbrev, md5[:hashlen], 'ix'])


def run_r_obs_covid(conn_str, site, password, search_path, model_version, copy):
    """Run an R script.

    * Create argos file
    * Run R Script

    :param str conn_str:      database connection string
    :param str site:    site to run script for
    :param str password:    user's password
    :param str search_path: PostgreSQL schema search path
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param bool copy: if True, copy results to output directory
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    package = 'covid19_observation_derivations'
    config_file = site + "_" + package + "_argos_temp.json";
    conn_info_dict = get_conn_info_dict(conn_str)

    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'site': site, },
                             conn_info_dict)
    logger.info(combine_dicts({'msg': 'starting R Script'},
                              log_dict))
    start_time = time.time()
    schema = primary_schema(search_path)

    if password == None:
        pass_match = re.search(r"password=(\S*)", conn_str)
        password = pass_match.group(1)

    source_path = os.path.join(os.sep,'app', package)
    dest_path = os.path.join(source_path, site)
    # delete any old versions
    if os.path.isdir(dest_path):
        shutil.rmtree(dest_path)
    # copy base files to site specific
    shutil.copytree(source_path, dest_path)
    # create the Argos congig file
    _create_argos_file(dest_path, config_file, schema, password, conn_info_dict)
    # modify site_info and run.R to add actual site
    _fix_site_info(dest_path, site)
    _fix_run(dest_path, site)

    query_path = os.path.join(os.sep,'app', package, site, 'site', 'run.R')
    # Run R script
    Rscript(query_path, '--verbose=1', _cwd='/app', _fg=True)

    if copy:
        results_path = os.path.join(dest_path, 'results')
        if os.path.isdir(results_path):
            output_path = os.path.join(os.sep,'output', package, site)
            if os.path.exists(output_path):
                shutil.rmtree(output_path)
            shutil.copytree(results_path, output_path)
            logger.info({'msg': 'results copied to output'})
        else:
            logger.info({'msg': 'no results found'})

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished R Script',
                               'elapsed': secs_since(start_time)}, log_dict))

    # Set primary key
    stmts = StatementSet()
    logger.info({'msg': 'setting primary key'})
    pk_stmt = Statement(PK_OBS_LIKE_TABLE_SQL.format(schema))
    stmts.add(pk_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Covid Observation Derivation')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding primary key failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'primary keys set'})

    # Add indexes (same as observation)
    stmts.clear()
    logger.info({'msg': 'adding indexes'})
    col_index = ('observation_concept_id', 'observation_date', 'person_id',
                 'visit_occurrence_id', 'observation_age_in_months', 'site',)

    for col in col_index:
        idx_name = _make_index_name(col)
        idx_stmt = Statement(IDX_OBS_LIKE_TABLE_SQL.format(idx_name, schema, col))
        stmts.add(idx_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Covid Observation Derivation')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding indexes failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'indexes added'})

    # Add foreign keys (same as observation)
    stmts.clear()
    logger.info({'msg': 'adding foreign keys'})
    col_fk = ('observation_concept_id', 'person_id', 'provider_id',
              'observation_source_concept_id', 'qualifier_concept_id',
              'observation_type_concept_id', 'unit_concept_id', 'value_as_concept_id',
              'visit_occurrence_id',)

    for fk in col_fk:
        fk_len = fk.count('_')
        if "concept_id" in fk:
            base_name = '_'.join(fk.split('_')[:fk_len - 1])
            ref_table = "vocabulary.concept"
            ref_col = "concept_id"
        else:
            base_name = ''.join(fk.split('_')[:1])
            ref_table = '_'.join(fk.split('_')[:fk_len])
            ref_col = fk
        fk_name = "fk_obs_" + base_name + "_covid"
        fk_stmt = Statement(FK_OBS_LIKE_TABLE_SQL.format(schema,
                                                             fk_name, fk, ref_table,
                                                             ref_col))
        stmts.add(fk_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Execute statements and check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Covid Observation Derivation')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding foreign keys failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'foreign keys added'})

    # Set permissions
    stmts.clear()
    logger.info({'msg': 'setting permissions'})
    users = ('achilles_user', 'dqa_user', 'pcor_et_user', 'peds_staff', 'dcc_analytics')

    for usr in users:
        grant_stmt = Statement(GRANT_OBS_LIKE_TABLE_SQL.format(schema, usr))
        stmts.add(grant_stmt)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            stmt.execute(conn_str)
            check_stmt_err(stmt, 'Covid Observation Derivation')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'granting permissions failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'permissions set'})

    # Vacuum analyze tables for piney freshness.
    logger.info({'msg': 'begin vacuum'})
    vacuum(conn_str, model_version, analyze=True, tables=['observation_derivation_covid'])
    logger.info({'msg': 'vacuum finished'})

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished Covid Observation Derivation',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True