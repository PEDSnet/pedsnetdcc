import logging
import time
import shutil
import os
import re

from pedsnetdcc.db import StatementSet, Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, check_stmt_data, combine_dicts,
                              get_conn_info_dict, vacuum, stock_metadata)
from sh import Rscript

logger = logging.getLogger(__name__)
NAME_LIMIT = 30
ADD_FOREIGN_KEYS_SQL = """ALTER TABLE {0}.encounter ADD CONSTRAINT fk_encounter_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.procedures ADD CONSTRAINT fk_procedures_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.procedures ADD CONSTRAINT fk_procedures_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.procedures ADD CONSTRAINT fk_procedures_providerid FOREIGN KEY(providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.pro_cm ADD CONSTRAINT fk_pro_cm_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.pro_cm ADD CONSTRAINT fk_pro_cm_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.obs_clin ADD CONSTRAINT fk_obsclin_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.obs_clin ADD CONSTRAINT fk_obsclin_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.obs_clin ADD CONSTRAINT fk_obsclin_providerid FOREIGN KEY(obsclin_providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.obs_gen ADD CONSTRAINT fk_obsgen_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.obs_gen ADD CONSTRAINT fk_obsgen_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.obs_gen ADD CONSTRAINT fk_obsgen_providerid FOREIGN KEY(obsgen_providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.pcornet_trial ADD CONSTRAINT fk_pcornet_trial_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.enrollment ADD CONSTRAINT fk_enrollment_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.death ADD CONSTRAINT fk_death_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.death_cause ADD CONSTRAINT fk_death_cause_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.condition ADD CONSTRAINT fk_condition_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.diagnosis ADD CONSTRAINT fk_diagnosis_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.dispensing ADD CONSTRAINT fk_dispensing_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.med_admin ADD CONSTRAINT fk_medadmin_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.prescribing ADD CONSTRAINT fk_prescribing_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.prescribing ADD CONSTRAINT fk_prescribing_providerid FOREIGN KEY(rx_providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.dispensing ADD CONSTRAINT fk_dispensing_prescribingid FOREIGN KEY(prescribingid) REFERENCES {0}.prescribing (prescribingid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.diagnosis ADD CONSTRAINT fk_diagnosis_providerid FOREIGN KEY(providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.med_admin ADD CONSTRAINT fk_medadmin_providerid FOREIGN KEY(medadmin_providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.condition ADD CONSTRAINT fk_condition_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.diagnosis ADD CONSTRAINT fk_diagnosis_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.prescribing ADD CONSTRAINT fk_prescribing_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.med_admin ADD CONSTRAINT fk_medadmin_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.lab_result_cm ADD CONSTRAINT fk_lab_result_cm_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.vital ADD CONSTRAINT fk_vital_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.lab_result_cm ADD CONSTRAINT fk_lab_result_cm_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.vital ADD CONSTRAINT fk_vital_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.encounter ADD CONSTRAINT fk_encounter_providerid FOREIGN KEY(providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.hash_token ADD CONSTRAINT fk_hash_token_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.immunization ADD CONSTRAINT fk_immun_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.immunization ADD CONSTRAINT fk_immun_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.immunization ADD CONSTRAINT fk_immun_procedureid FOREIGN KEY(proceduresid) REFERENCES {0}.procedures (proceduresid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.immunization ADD CONSTRAINT fk_immun_providerid FOREIGN KEY(vx_providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.lds_address_history ADD CONSTRAINT fk_lds_addhist_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.private_address_geocode ADD CONSTRAINT fk_gecode_addressid FOREIGN KEY(addressid) REFERENCES {0}.lds_address_history (addressid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.private_address_history ADD CONSTRAINT fk_add_history_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE {0}.private_demographic ADD CONSTRAINT fk_priv_demographic_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;
"""

def _create_argos_file(config_path, config_file, source_schema, target_schema, password, conn_info_dict):
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
        out_config.write('"options": "-c search_path=' + source_schema + ',' + target_schema + '"' + os.linesep)
        out_config.write('}' + os.linesep)
        out_config.write('}' + os.linesep)


def _fix_site_info(file_path, site, source_schema, target_schema):
    try:
        with open(os.path.join(file_path,'site','site_info.R'), 'r') as site_file:
            file_data = site_file.read()
        file_data = file_data.replace('<SITE>', site)
        file_data = file_data.replace('<SOURCE_SCHEMA>', source_schema)
        file_data = file_data.replace('<TARGET_SCHEMA>', target_schema)
        with open(os.path.join(file_path,'site','site_info.R'), 'w') as site_file:
            site_file.write(file_data)
    except:
        # this query package may not have this file
        return False

    return True


def _fix_run(file_path, site, source_schema, target_schema):
    try:
        with open(os.path.join(file_path,'site','run.R'), 'r') as site_file:
            file_data = site_file.read()
        file_data = file_data.replace('<SITE>', site)
        file_data = file_data.replace('<SOURCE_SCHEMA>', source_schema)
        file_data = file_data.replace('<TARGET_SCHEMA>', target_schema)
        with open(os.path.join(file_path,'site','run.R'), 'w') as site_file:
            site_file.write(file_data)
    except:
        # this query package may not have this file
        return False

    return True


def run_r_pcornet_covid_slice(config_file, conn_str, site, password, source_schema, target_schema):
    """Run an R script.

    * Create argos file
    * Run R Script

    :param str config_file:   config file name
    :param str conn_str:      database connection string
    :param str site:    site to run script for
    :param str password:    user's password
    :param str source_schema: schema to use as source (full schema)
    :param str target_schema: schema to put results (covid schema)
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    conn_info_dict = get_conn_info_dict(conn_str)

    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'site': site, },
                             conn_info_dict)
    logger.info(combine_dicts({'msg': 'starting R Script'},
                              log_dict))
    start_time = time.time()
    source_schema = source_schema
    target_schema = target_schema

    if password == None:
        pass_match = re.search(r"password=(\S*)", conn_str)
        password = pass_match.group(1)

    package = 'pcornet_covid_slice'
    source_path = os.path.join(os.sep,'app', package)
    dest_path = os.path.join(source_path, site)
    # delete any old versions
    if os.path.isdir(dest_path):
        shutil.rmtree(dest_path)
    # copy base files to site specific
    shutil.copytree(source_path, dest_path)
    # create the Argos congig file
    _create_argos_file(dest_path, config_file, source_schema, target_schema, password, conn_info_dict)
    # modify site_info and run.R to add actual site
    _fix_site_info(dest_path, site, source_schema, target_schema)
    _fix_run(dest_path, site, source_schema, target_schema)

    query_path = os.path.join(os.sep,'app', package, site, 'site', 'run.R')
    # Run R script
    Rscript(query_path, '--verbose=1', _cwd='/app', _fg=True)

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished R Script',
                               'elapsed': secs_since(start_time)}, log_dict))

    stmts = StatementSet()

    # Add Foreign keys.
    logger.info({'msg': 'begin adding FKs'})
    stmts.clear()
    add_fk_stmt = Statement(ADD_FOREIGN_KEYS_SQL.format(target_schema))
    stmts.add(add_fk_stmt)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            stmt.execute(conn_str)
            check_stmt_err(stmt, logger_msg.format('Run'))
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'add FKs failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise

    logger.info({'msg': 'finished adding FKs'})

    # If reached without error, then success!
    return True