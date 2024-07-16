import logging
import time

from pedsnetdcc.db import StatementSet, Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import (check_stmt_err, check_stmt_data, combine_dicts,
                              get_conn_info_dict, vacuum, conn_str_with_search_path)

logger = logging.getLogger(__name__)
NAME_LIMIT = 30
ALTER_OWNER_SQL = 'alter table {0}.{1} owner to dcc_owner;'
GRANT_TABLE_SQL = 'grant select on table {0}.{1} to {2}'
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
ADD_INDEXES_SQL = [None] * 34
ADD_INDEXES_SQL[0]="""CREATE INDEX idx_enrol_patid ON {0}.enrollment (patid);"""
ADD_INDEXES_SQL[1]="""CREATE INDEX idx_death_patid ON {0}.death (patid);"""
ADD_INDEXES_SQL[2]="""CREATE INDEX idx_death_cause_patid ON {0}.death_cause (patid);"""
ADD_INDEXES_SQL[3]="""CREATE INDEX idx_encounter_patid ON {0}.encounter (patid);"""
ADD_INDEXES_SQL[4]="""CREATE INDEX idx_encounter_enctype ON {0}.encounter (enc_type);"""
ADD_INDEXES_SQL[5]="""CREATE INDEX idx_cond_encid ON {0}.condition (encounterid);"""
ADD_INDEXES_SQL[6]="""CREATE INDEX idx_cond_patid ON {0}.condition (patid);"""
ADD_INDEXES_SQL[7]="""CREATE INDEX idx_condition_ccode ON {0}.condition (condition);"""
ADD_INDEXES_SQL[8]="""CREATE INDEX idx_diag_patid ON {0}.diagnosis (patid);"""
ADD_INDEXES_SQL[9]="""CREATE INDEX idx_diag_encid ON {0}.diagnosis (encounterid);"""
ADD_INDEXES_SQL[10]="""CREATE INDEX idx_diag_code ON {0}.diagnosis (dx);"""
ADD_INDEXES_SQL[11]="""CREATE INDEX idx_proc_encid ON {0}.procedures (encounterid);"""
ADD_INDEXES_SQL[12]="""CREATE INDEX idx_proc_patid ON {0}.procedures (patid);"""
ADD_INDEXES_SQL[13]="""CREATE INDEX idx_proc_px ON {0}.procedures (px);"""
ADD_INDEXES_SQL[14]="""CREATE INDEX idx_disp_patid ON {0}.dispensing (patid);"""
ADD_INDEXES_SQL[15]="""CREATE INDEX idx_disp_ndc ON {0}.dispensing (ndc);"""
ADD_INDEXES_SQL[16]="""CREATE INDEX idx_pres_encid ON {0}.prescribing (encounterid);"""
ADD_INDEXES_SQL[17]="""CREATE INDEX idx_pres_patid ON {0}.prescribing (patid);"""
ADD_INDEXES_SQL[18]="""CREATE INDEX idx_pres_rxnorm ON {0}.prescribing (rxnorm_cui);"""
ADD_INDEXES_SQL[19]="""CREATE INDEX idx_vital_patid ON {0}.vital (patid);"""
ADD_INDEXES_SQL[20]="""CREATE INDEX idx_vital_encid ON {0}.vital (encounterid);"""
ADD_INDEXES_SQL[21]="""CREATE INDEX idx_lab_patid ON {0}.lab_result_cm (patid);"""
ADD_INDEXES_SQL[22]="""CREATE INDEX idx_lab_encid ON {0}.lab_result_cm (encounterid);"""
ADD_INDEXES_SQL[23]="""CREATE INDEX idx_loinc_encid ON {0}.lab_result_cm (lab_loinc);"""
ADD_INDEXES_SQL[24]="""CREATE INDEX idx_med_patid ON {0}.med_admin (patid);"""
ADD_INDEXES_SQL[25]="""CREATE INDEX idx_med_encid ON {0}.med_admin (encounterid);"""
ADD_INDEXES_SQL[26]="""CREATE INDEX idx_obsclin_patid ON {0}.obs_clin(patid);"""
ADD_INDEXES_SQL[27]="""CREATE INDEX idx_obsclin_encid ON {0}.obs_clin(encounterid);"""
ADD_INDEXES_SQL[28]="""CREATE INDEX idx_obsgen_patid ON {0}.obs_gen (patid);"""
ADD_INDEXES_SQL[29]="""CREATE INDEX idx_obsgen_encid ON {0}.obs_gen (encounterid);"""
ADD_INDEXES_SQL[30]="""CREATE INDEX idx_geocode_addr ON {0}.private_address_geocode (addressid);"""
ADD_INDEXES_SQL[31]="""CREATE INDEX idx_procm_patid ON {0}.pro_cm (patid);"""
ADD_INDEXES_SQL[32]="""CREATE INDEX idx_imm_patid ON {0}.immunization (patid);"""
ADD_INDEXES_SQL[33]="""CREATE INDEX idx_address_patid ON {0}.lds_address_history (patid);"""
ADD_FOREIGN_KEYS_SQL = [None] * 42
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
ADD_FOREIGN_KEYS_SQL[35] = """ALTER TABLE {0}.immunization ADD CONSTRAINT fk_immun_encounterid FOREIGN KEY(encounterid) REFERENCES {0}.encounter (encounterid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[36] = """ALTER TABLE {0}.immunization ADD CONSTRAINT fk_immun_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[37] = """ALTER TABLE {0}.immunization ADD CONSTRAINT fk_immun_procedureid FOREIGN KEY(proceduresid) REFERENCES {0}.procedures (proceduresid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[38] = """ALTER TABLE {0}.immunization ADD CONSTRAINT fk_immun_providerid FOREIGN KEY(vx_providerid) REFERENCES {0}.provider (providerid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[39] = """ALTER TABLE {0}.lds_address_history ADD CONSTRAINT fk_lds_addhist_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[40] = """ALTER TABLE {0}.private_address_geocode ADD CONSTRAINT fk_gecode_addressid FOREIGN KEY(addressid) REFERENCES {0}.lds_address_history (addressid) DEFERRABLE INITIALLY DEFERRED;"""
ADD_FOREIGN_KEYS_SQL[41] = """ALTER TABLE {0}.private_address_history ADD CONSTRAINT fk_add_history_patid FOREIGN KEY(patid) REFERENCES {0}.demographic (patid) DEFERRABLE INITIALLY DEFERRED;"""

def run_subset_pcornet_by_cohort(conn_str, model_version, source_schema, target_schema, cohort_table,
                         inc_hash=False, index_create=False, fk_create=False, notable=False, nopk=False,
                         limit=False, owner='loading_user', force=False):
    """Create SQL for `select` statement transformations.

    The `search_path` only needs to contain the source schema; the target
    schema is embedded in the SQL statements.

    Returns a set of tuples of (sql_string, msg), where msg is a description
    for the operation to be carried out by the sql_string.

    :param model_version:   PEDSnet model version, e.g. 2.3.0
    :param str source_schema:   schema in which the tables are located
    :param str target_schema:   schema in which to create the subset
    :param str cohort_table:  name of table that contains the cohort
    :param bool inc_hash: if True, include hash_token table
    :param bool index_create: if True, create indexes
    :param bool fk_create: if True, create fks
    :param bool notable: if True, don't create tables
    :param bool nopk: if True, don't create primary keys
    :param bool limit: if True, limit permissions to owner
    :param str owner:  owner of the to grant permissions to
    :param bool force: if True, ignore benign errors
    :returns:   True if the function succeeds
    :rtype: bool
    """

    logger = logging.getLogger(__name__)
    log_dict = combine_dicts({'model_version': model_version, },
                             get_conn_info_dict(conn_str))
    logger.info(combine_dicts({'msg': 'starting PCORnet subset by cohort'},
                              log_dict))
    start_time = time.time()
    stmts = StatementSet()

    table_list = []

    select_patid = {
        'demographic',
        'enrollment',
        'encounter',
        'diagnosis',
        'procedures',
        'vital',
        'dispensing',
        'lab_result_cm',
        'condition',
        'pro_cm',
        'prescribing',
        'pcornet_trial',
        'provider',
        'harvest'
        'death',
        'death_cause',
        'med_admin',
        'obs_clin',
        'obs_gen',
        'lds_address_history',
        'immunization',
        'private_demographic',
        'private_address_history'
    }

    select_all = (
        'provider',
        'harvest'
    )
    special_handling = {
        'lab_history',
        'private_address_geocode',
        'hash_token'
    }

    create_dict = {}
    grant_vacuum_tables = []

    # Initial pass for tables that all rows are selected or are based on patid in cohort table
    if not notable:
        for table in select_patid:
            table_list.append(table)
            create = 'create table ' + target_schema + '.' + table + ' as select t.*'
            create = create + ' from ' + source_schema + '.' + table + ' t'
            if table not in select_all:
                create = create + ' join ' +  target_schema + '.' + cohort_table + ' c on c.patid = t.patid'
            create = create + ';'
            create_dict[table] = create
            grant_vacuum_tables.append(table)

        for table in sorted(table_list):
            create_stmt = Statement(create_dict[table])
            stmts.add(create_stmt)

        # Execute the statements in parallel.
        stmts.parallel_execute(conn_str)

        # Check for any errors and raise exception if they are found.
        for stmt in stmts:
            try:
                check_stmt_err(stmt, 'create initial tables')
            except:
                logger.error(combine_dicts({'msg': 'Fatal error',
                                            'sql': stmt.sql,
                                            'err': str(stmt.err)}, log_dict))
                logger.info(combine_dicts({'msg': 'create initial tables failed',
                                           'elapsed': secs_since(start_time)},
                                          log_dict))
                raise
        logger.info({'msg': 'initial tables created'})

        # Create special handling tables
        del table_list[:]
        create_dict.clear()
        stmts.clear()

        for table in special_handling:
            table_list.append(table)
            create = 'create table ' + target_schema + '.' + table + ' as select t.*'
            create = create + ' from ' + source_schema + '.' + table + ' t'
            if table == 'lab_history':
                create = create + ' join ' + target_schema + '.lab_result_cm l on l.lab_loinc = t.lab_loinc'
            if table == 'private_address_geocode':
                create = create + ' join ' + target_schema + '.lds_address_history l on l.addressid = t.addressid'
            if table == 'hash_token':
                if inc_hash:
                    create = create + ' join ' + target_schema + '.' + cohort_table + ' c on c.patid = t.patid'
                else:
                    create = create + ' where FALSE'

            create = create + ';'
            create_dict[table] = create
            grant_vacuum_tables.append(table)

        for table in sorted(table_list):
            create_stmt = Statement(create_dict[table])
            stmts.add(create_stmt)

        # Execute the statements in parallel.
        stmts.parallel_execute(conn_str)

        # Check for any errors and raise exception if they are found.
        for stmt in stmts:
            try:
                check_stmt_err(stmt, 'create special handling tables')
            except:
                logger.error(combine_dicts({'msg': 'Fatal error',
                                            'sql': stmt.sql,
                                            'err': str(stmt.err)}, log_dict))
                logger.info(combine_dicts({'msg': 'create special handling tables failed',
                                           'elapsed': secs_since(start_time)},
                                          log_dict))
                raise
        logger.info({'msg': 'special handling tables created'})
        stmts.clear()

    if not nopk:
        # Add primary keys to the subset tables
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

    if index_create:
        # Add indexes to the subset tables
        logger.info(combine_dicts({'msg': 'Start adding indexes',
                                   'elapsed': secs_since(start_time)}, log_dict))
        stmts.clear()

        for pk in ADD_INDEXES_SQL:
            add_idx_stmt = Statement(pk.format(target_schema))
            stmts.add(add_idx_stmt)

        # Execute the statements in parallel.
        stmts.parallel_execute(conn_str, 5)

        # Check for any errors and raise exception if they are found.
        for stmt in stmts:
            try:
                check_stmt_err(stmt, 'add indexes')
            except:
                logger.error(combine_dicts({'msg': 'Fatal error',
                                            'sql': stmt.sql,
                                            'err': str(stmt.err)}, log_dict))
                logger.info(combine_dicts({'msg': 'add indexes failed',
                                           'elapsed': secs_since(start_time)},
                                          log_dict))
                raise

        logger.info(combine_dicts({'msg': 'Finished adding indexes',
                                   'elapsed': secs_since(start_time)}, log_dict))

    if fk_create:
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

    # Grant permissions

    # Set up new connection string for manipulating the target schema
    new_conn_str = conn_str_with_search_path(conn_str, target_schema)

    stmts.clear()
    logger.info({'msg': 'setting permissions'})
    if limit:
        users = (owner,)
    else:
        users = ('pcor_et_user',)
    for target_table in grant_vacuum_tables:
        for usr in users:
            grant_stmt = Statement(GRANT_TABLE_SQL.format(target_schema, target_table, usr))
            stmts.add(grant_stmt)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            stmt.execute(conn_str)
            check_stmt_err(stmt, 'grant permissions')
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
    vacuum(new_conn_str, model_version, analyze=True, tables=grant_vacuum_tables)

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished subset PCORnet by cohort',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True