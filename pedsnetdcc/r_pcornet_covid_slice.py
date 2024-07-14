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
ADD_PRIMARY_KEYS_SQL = [None] * 25
ADD_PRIMARY_KEYS_SQL[0] = """ALTER TABLE IF EXISTS {0}.vital
    ADD CONSTRAINT xpk_vitalid PRIMARY KEY (vitalid);"""
ADD_PRIMARY_KEYS_SQL[1] = """ALTER TABLE IF EXISTS {0}.condition
    ADD CONSTRAINT xpk_condition PRIMARY KEY (conditionid);"""
ADD_PRIMARY_KEYS_SQL[2] = """ALTER TABLE IF EXISTS {0}.death
    ADD CONSTRAINT xpk_death PRIMARY KEY (patid, death_source);"""
ADD_PRIMARY_KEYS_SQL[3] = """ALTER TABLE IF EXISTS {0}.death_cause
    ADD CONSTRAINT xpk_death_cause PRIMARY KEY (patid, death_cause, death_cause_code, death_cause_type, death_cause_source);"""
ADD_PRIMARY_KEYS_SQL[4] = """ALTER TABLE IF EXISTS {0}.demographic
    ADD CONSTRAINT xpk_demographic PRIMARY KEY (patid);"""
ADD_PRIMARY_KEYS_SQL[5] = """ALTER TABLE IF EXISTS {0}.diagnosis
    ADD CONSTRAINT xpk_diagnosis PRIMARY KEY (diagnosisid);"""
ADD_PRIMARY_KEYS_SQL[6] = """ALTER TABLE IF EXISTS {0}.dispensing
    ADD CONSTRAINT xpk_dispensing PRIMARY KEY (dispensingid);"""
ADD_PRIMARY_KEYS_SQL[7] = """ALTER TABLE IF EXISTS {0}.encounter
    ADD CONSTRAINT xpk_encounter PRIMARY KEY (encounterid);"""
ADD_PRIMARY_KEYS_SQL[8] = """ALTER TABLE IF EXISTS {0}.enrollment
    ADD CONSTRAINT xpk_enrollment PRIMARY KEY (patid, enr_start_date, enr_basis);"""
ADD_PRIMARY_KEYS_SQL[9] = """ALTER TABLE IF EXISTS {0}.harvest
    ADD CONSTRAINT xpk_harvest PRIMARY KEY (networkid, datamartid);"""
ADD_PRIMARY_KEYS_SQL[10] = """ALTER TABLE IF EXISTS {0}.immunization
    ADD CONSTRAINT xpk_immunization PRIMARY KEY (immunizationid);"""
ADD_PRIMARY_KEYS_SQL[11] = """ALTER TABLE IF EXISTS {0}.lab_history
    ADD CONSTRAINT xpk_labhist PRIMARY KEY (labhistoryid);"""
ADD_PRIMARY_KEYS_SQL[12] = """ALTER TABLE IF EXISTS {0}.lab_result_cm
    ADD CONSTRAINT xpk_lab_result_cm PRIMARY KEY (lab_result_cm_id);"""
ADD_PRIMARY_KEYS_SQL[13] = """ALTER TABLE IF EXISTS {0}.lds_address_history
    ADD CONSTRAINT xpk_ldsaddhist PRIMARY KEY (addressid);"""
ADD_PRIMARY_KEYS_SQL[14] = """ALTER TABLE IF EXISTS {0}.med_admin
    ADD CONSTRAINT xpk_medadminid PRIMARY KEY (medadminid);"""
ADD_PRIMARY_KEYS_SQL[15] = """ALTER TABLE IF EXISTS {0}.obs_clin
    ADD CONSTRAINT xpk_obsclinid PRIMARY KEY (obsclinid);"""
ADD_PRIMARY_KEYS_SQL[16] = """ALTER TABLE IF EXISTS {0}.obs_gen
    ADD CONSTRAINT xpk_obsgenid PRIMARY KEY (obsgenid);"""
ADD_PRIMARY_KEYS_SQL[17] = """ALTER TABLE IF EXISTS {0}.pcornet_trial
    ADD CONSTRAINT xpk_pcornet_trial PRIMARY KEY (patid, trialid, participantid);"""
ADD_PRIMARY_KEYS_SQL[18] = """ALTER TABLE IF EXISTS {0}.prescribing
    ADD CONSTRAINT xpk_prescribing PRIMARY KEY (prescribingid);"""
ADD_PRIMARY_KEYS_SQL[19] = """ALTER TABLE IF EXISTS {0}.private_address_geocode
    ADD CONSTRAINT xpk_private_address_geo PRIMARY KEY (geocodeid);"""
ADD_PRIMARY_KEYS_SQL[20] = """ALTER TABLE IF EXISTS {0}.private_address_history
    ADD CONSTRAINT xpk_private_address_hist PRIMARY KEY (addressid);"""
ADD_PRIMARY_KEYS_SQL[21] = """ALTER TABLE IF EXISTS {0}.private_demographic
    ADD CONSTRAINT xpk_private_demographic PRIMARY KEY (patid);"""
ADD_PRIMARY_KEYS_SQL[22] = """ALTER TABLE IF EXISTS {0}.pro_cm
    ADD CONSTRAINT xpk_pro_cm PRIMARY KEY (pro_cm_id);"""
ADD_PRIMARY_KEYS_SQL[23] = """ALTER TABLE IF EXISTS {0}.procedures
    ADD CONSTRAINT xpk_procedures PRIMARY KEY (proceduresid);"""
ADD_PRIMARY_KEYS_SQL[24] = """ALTER TABLE IF EXISTS {0}.provider
    ADD CONSTRAINT xpk_providerid PRIMARY KEY (providerid);"""
ADD_FOREIGN_KEYS_SQL = [None] * 43
ADD_FOREIGN_KEYS_SQL[0] = """ALTER TABLE {0}.private_demographic ADD CONSTRAINT fk_priv_demographic_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[1] = """ALTER TABLE {0}.encounter ADD CONSTRAINT fk_encounter_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[2] = """ALTER TABLE {0}.procedures ADD CONSTRAINT fk_procedures_encounterid FOREIGN KEY(encounterid) 
REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[3] = """ALTER TABLE {0}.procedures ADD CONSTRAINT fk_procedures_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[4] = """ALTER TABLE {0}.procedures ADD CONSTRAINT fk_procedures_providerid FOREIGN KEY(providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[5] = """ALTER TABLE {0}.pro_cm ADD CONSTRAINT fk_pro_cm_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[6] = """ALTER TABLE {0}.pro_cm ADD CONSTRAINT fk_pro_cm_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[7] = """ALTER TABLE {0}.obs_clin ADD CONSTRAINT fk_obsclin_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[8] = """ALTER TABLE {0}.obs_clin ADD CONSTRAINT fk_obsclin_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[9] = """ALTER TABLE {0}.obs_clin ADD CONSTRAINT fk_obsclin_providerid FOREIGN KEY(obsclin_providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[10] = """ALTER TABLE {0}.obs_gen ADD CONSTRAINT fk_obsgen_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[11] = """ALTER TABLE {0}.obs_gen ADD CONSTRAINT fk_obsgen_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[12] = """ALTER TABLE {0}.obs_gen ADD CONSTRAINT fk_obsgen_providerid FOREIGN KEY(obsgen_providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[13] = """ALTER TABLE {0}.pcornet_trial ADD CONSTRAINT fk_pcornet_trial_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[14] = """ALTER TABLE {0}.enrollment ADD CONSTRAINT fk_enrollment_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[15] = """ALTER TABLE {0}.death ADD CONSTRAINT fk_death_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[16] = """ALTER TABLE {0}.death_cause ADD CONSTRAINT fk_death_cause_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[17] = """ALTER TABLE {0}.condition ADD CONSTRAINT fk_condition_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[18] = """ALTER TABLE {0}.diagnosis ADD CONSTRAINT fk_diagnosis_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[19] = """ALTER TABLE {0}.dispensing ADD CONSTRAINT fk_dispensing_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[20] = """ALTER TABLE {0}.med_admin ADD CONSTRAINT fk_medadmin_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[21] = """ALTER TABLE {0}.prescribing ADD CONSTRAINT fk_prescribing_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[22] = """ALTER TABLE {0}.prescribing ADD CONSTRAINT fk_prescribing_providerid FOREIGN KEY(rx_providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[23] = """ALTER TABLE {0}.dispensing ADD CONSTRAINT fk_dispensing_prescribingid FOREIGN KEY(prescribingid) 
REFERENCES {0}.prescribing (prescribingid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[24] = """ALTER TABLE {0}.diagnosis ADD CONSTRAINT fk_diagnosis_providerid FOREIGN KEY(providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[25] = """ALTER TABLE {0}.med_admin ADD CONSTRAINT fk_medadmin_providerid FOREIGN KEY(medadmin_providerid) 
REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[26] = """ALTER TABLE {0}.condition ADD CONSTRAINT fk_condition_encounterid FOREIGN KEY(encounterid) 
REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[27] = """ALTER TABLE {0}.diagnosis ADD CONSTRAINT fk_diagnosis_encounterid FOREIGN KEY(encounterid) 
REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[28] = """ALTER TABLE {0}.prescribing ADD CONSTRAINT fk_prescribing_encounterid FOREIGN KEY(encounterid) 
REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[29] = """ALTER TABLE {0}.med_admin ADD CONSTRAINT fk_medadmin_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[30] = """ALTER TABLE {0}.lab_result_cm ADD CONSTRAINT fk_lab_result_cm_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[31] = """ALTER TABLE {0}.vital ADD CONSTRAINT fk_vital_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[32] = """ALTER TABLE {0}.lab_result_cm ADD CONSTRAINT fk_lab_result_cm_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[33] = """ALTER TABLE {0}.vital ADD CONSTRAINT fk_vital_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[34] = """ALTER TABLE {0}.encounter ADD CONSTRAINT fk_encounter_providerid FOREIGN KEY(providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[35] = """ALTER TABLE {0}.hash_token ADD CONSTRAINT fk_hash_token_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[36] = """ALTER TABLE {0}.immunization ADD CONSTRAINT fk_immun_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[37] = """ALTER TABLE {0}.immunization ADD CONSTRAINT fk_immun_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[38] = """ALTER TABLE {0}.immunization ADD CONSTRAINT fk_immun_procedureid FOREIGN KEY(proceduresid) REFERENCES {0}.procedures (proceduresid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[39] = """ALTER TABLE {0}.immunization ADD CONSTRAINT fk_immun_providerid FOREIGN KEY(vx_providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[40] = """ALTER TABLE {0}.lds_address_history ADD CONSTRAINT fk_lds_addhist_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[41] = """ALTER TABLE {0}.private_address_geocode ADD CONSTRAINT fk_gecode_addressid FOREIGN KEY(addressid) REFERENCES {0}.lds_address_history (addressid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[42] = """ALTER TABLE {0}.private_address_history ADD CONSTRAINT fk_add_history_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""

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

    # Add Primary keys.
    logger.info(combine_dicts({'msg': 'Start adding PKs',
                               'elapsed': secs_since(start_time)}, log_dict))
    stmts.clear()

    for pk in ADD_PRIMARY_KEYS_SQL:
        add_pk_stmt = Statement(pk.format(target_schema))
        stmts.add(add_pk_stmt)

    # Execute the statements in parallel.
     stmts.parallel_execute(conn_str, 5)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'add PKs')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'add PKs failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise

    logger.info(combine_dicts({'msg': 'Finished adding PKs',
                               'elapsed': secs_since(start_time)}, log_dict))

    # Add Foreign keys.
    logger.info(combine_dicts({'msg': 'Start adding FKs',
                               'elapsed': secs_since(start_time)}, log_dict))
    stmts.clear()

    for fk in ADD_FOREIGN_KEYS_SQL:
        add_fk_stmt = Statement(fk.format(target_schema))
        stmts.add(add_fk_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str, 5)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'add FKs')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'add FKs failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise

    logger.info(combine_dicts({'msg': 'Finished adding FKs',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True