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
GRANT_OBS_MGKG_DERIVATION_SQL = 'grant select on table {0}.drug_exposures_mgkg_derivations to {1}'
GRANT_OBS_MGKG_METADATA_SQL = 'grant select on table {0}.drug_exposures_mgkg_metadata to {1}'

def _update_drug_exposure(conn_str, schema):
    update_drug_exposure_sql = """UPDATE {0}.drug_exposure de
        SET dose_unit_concept_id = de_dev.dose_unit_concept_id,
        effective_drug_dose = de_dev.effective_drug_dose,
        dose_unit_concept_name = de_dev.dose_unit_concept_name 
        FROM ( SELECT drug_exposure_id, person_id, dose_unit_concept_id, 
            effective_drug_dose, dose_unit_concept_name
            FROM {0}.drug_exposures_mgkg_derivations) AS de_dev
        WHERE de.person_id = de_dev.person_id AND 
        de.drug_exposure_id = de_dev.drug_exposure_id"""

    update_drug_exposure_msg = "updating drug_exposure"

    # Update_drug_exposure
    update_drug_exposure_stmt = Statement(update_drug_exposure_sql.format(schema), update_drug_exposure_msg)

    # Execute the add concept names statement and ensure it didn't error
    update_drug_exposure_stmt.execute(conn_str)
    check_stmt_err(update_drug_exposure_stmt, 'update drug_exposure')

    # If reached without error, then success!
    return True


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


def _fix_site_info(file_path, site, schema):
    try:
        with open(os.path.join(file_path, 'site', 'site_info.R'), 'r') as site_file:
            file_data = site_file.read()
        file_data = file_data.replace('<SITE>', site)
        file_data = file_data.replace('<SCHEMA>', schema)
        with open(os.path.join(file_path, 'site', 'site_info.R'), 'w') as site_file:
            site_file.write(file_data)
    except:
        # this query package may not have this file
        return False

    return True


def _fix_run(file_path, site):
    try:
        with open(os.path.join(file_path, 'site', 'run.R'), 'r') as site_file:
            file_data = site_file.read()
        file_data = file_data.replace('<SITE>', site)
        with open(os.path.join(file_path, 'site', 'run.R'), 'w') as site_file:
            site_file.write(file_data)
    except:
        # this query package may not have this file
        return False

    return True


def run_r_dose(conn_str, site, password, search_path, model_version, copy):
    """Run an R script.

    * Create argos file
    * Run R Script

    :param str conn_str:      database connection string
    :param str site:    site to run script for
    :param str password:    user's password
    :param str search_path: PostgreSQL schema search path
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param bool copy: if True, copy results to drug exposure
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    package = 'dose_derivations'
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

    source_path = os.path.join(os.sep, 'app', package)
    dest_path = os.path.join(source_path, site)
    # delete any old versions
    if os.path.isdir(dest_path):
        shutil.rmtree(dest_path)
    # copy base files to site specific
    shutil.copytree(source_path, dest_path)
    # create the Argos congig file
    _create_argos_file(dest_path, config_file, schema, password, conn_info_dict)
    # modify site_info and run.R to add actual site
    _fix_site_info(dest_path, site, schema)
    _fix_run(dest_path, site)

    query_path = os.path.join(os.sep, 'app', package, site, 'site', 'run.R')
    # Run R script
    Rscript(query_path, '--verbose=1', _cwd='/app', _fg=True)

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished R Script',
                               'elapsed': secs_since(start_time)}, log_dict))

    if copy:
        # update drug_exposure
        logger.info({'msg': 'Update drug_exposure'})
        okay = _update_drug_exposure(conn_str, schema)
        if not okay:
            return False
        logger.info({'msg': 'drug_exposure updated'})

    # Set permissions
    stmts = StatementSet()
    logger.info({'msg': 'setting permissions'})
    users = ('achilles_user', 'dqa_user', 'pcor_et_user', 'peds_staff', 'dcc_analytics')

    for usr in users:
        grant_stmt = Statement(GRANT_OBS_MGKG_DERIVATION_SQL.format(schema, usr))
        stmts.add(grant_stmt)

    for usr in users:
        grant_stmt = Statement(GRANT_OBS_MGKG_METADATA_SQL.format(schema, usr))
        stmts.add(grant_stmt)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            stmt.execute(conn_str)
            check_stmt_err(stmt, 'Dose Derivations')
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
    vacuum(conn_str, model_version, analyze=True, tables=['drug_exposures_mgkg_derivations', 'drug_exposures_mgkg_metadata'])
    logger.info({'msg': 'vacuum finished'})

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished Dose Derivations',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True