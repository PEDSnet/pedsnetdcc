import logging
import time
from pedsnetdcc.db import StatementSet, Statement
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.transform_runner import add_indexes, drop_unneeded_indexes
from pedsnetdcc.transform_runner import add_foreign_keys
from pedsnetdcc.transform_runner import add_primary_keys
from pedsnetdcc.utils import (get_conn_info_dict, combine_dicts, check_stmt_err, vacuum,
                              stock_metadata, conn_str_with_search_path)
from pedsnetdcc.not_nulls import set_not_nulls
from pedsnetdcc.concept_group_tables import create_index_replacement_tables
from pedsnetdcc import VOCAB_TABLES
ALTER_OWNER_SQL = 'alter table {0}.{1} owner to dcc_owner;'
GRANT_TABLE_SQL = 'grant select on table {0}.{1} to {2}'


def run_subset_by_cohort(conn_str, model_version, source_schema, target_schema, cohort_table,
                         concept_create=False, drug_dose=False, measurement=False, covid_obs=False, inc_hash=False,
                         index_create=False, fk_create=False, notable=False, nopk=False, nonull=False,
                         limit=False, owner='loading_user', force=False):
    """Create SQL for `select` statement transformations.

    The `search_path` only needs to contain the source schema; the target
    schema is embedded in the SQL statements.

    Returns a set of tuples of (sql_string, msg), where msg is a description
    for the operation to be carried out by the sql_string.

    :param model_version:   PEDSnet model version, e.g. 2.3.0
    :param str source_schema:   schema in which the tables are locaated
    :param str target_schema:   schema in which to create the subset
    :param str cohort_table:  name of table that contains the cohort
    :param bool concept_create: if True, create the concept group tables
    :param bool drug_dose: if True, copy drug dose tables
    :param bool measurement: if True, copy measurement tables
    :param bool covid_obs: if True, copy covid observation table
    :param bool inc_hash: if True, include hash_token table
    :param bool index_create: if True, create indexes
    :param bool fk_create: if True, create fks
    :param bool notable: if True, don't create tables
    :param bool nopk: if True, don't create primary keys
    :param bool nonull: if True, don't set column not null
    :param bool limit: if True, limit permissions to owner
    :param str owner:  owner of the to grant permissions to
    :param bool force: if True, ignore benign errors
    :returns:   True if the function succeeds
    :rtype: bool
    """

    logger = logging.getLogger(__name__)
    log_dict = combine_dicts({'model_version': model_version, },
                             get_conn_info_dict(conn_str))
    logger.info(combine_dicts({'msg': 'starting subset by cohort'},
                              log_dict))
    start_time = time.time()

    metadata = stock_metadata(model_version)
    stmts = StatementSet()

    table_list = []
    select_all = (
        'location',
        'location_fips',
        'care_site',
        'provider',
        'specialty',
        'lab_site_mapping'
    )
    special_handling = {
        'visit_payer',
        'fact_relationship',
        'location_history',
        'hash_token'
    }
    measurement_tables = {
        'measurement_bmi',
        'measurement_bmiz',
        'measurement_ht_z',
        'measurement_wt_z'
    }
    create_dict = {}
    grant_vacuum_tables = []

    # Initial pass for tables that all rows are selected or are based on person_id in cohort table
    if not notable:
        for table_name,table in metadata.tables.items():
            if table_name in VOCAB_TABLES:
                continue
            if table_name in special_handling:
                continue

            table_list.append(table_name)
            create = 'create table ' + target_schema + '.' + table_name + ' as select t.*'
            #for column_name,column in table.c.items():
            #    create +=  't.' + column_name + ', '
            #create = create[:-2]
            create = create + ' from ' + source_schema + '.' + table_name + ' t'
            if table_name not in select_all:
                create = create + ' join ' +  target_schema + '.' + cohort_table + ' c on c.person_id = t.person_id'
            create = create + ';'
            create_dict[table_name] = create
            grant_vacuum_tables.append(table_name)

        for table_name in sorted(table_list):
                create_stmt = Statement(create_dict[table_name])
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

        for table_name,table in metadata.tables.items():
            if table_name in special_handling:
                table_list.append(table_name)
                create = 'create table ' + target_schema + '.' + table_name + ' as select t.*'
                #for column_name, column in table.c.items():
                #    create += 't.' + column_name + ', '
                #create = create[:-2]
                create = create + ' from ' + source_schema + '.' + table_name + ' t'
                if table_name == 'fact_relationship':
                    create = create + ' where exists(select 1 from ' + target_schema + '.visit_occurrence v'
                    create = create + ' where t.domain_concept_id_1 = 8 and t.fact_id_1 = v.visit_occurrence_id)'
                    create = create + ' or exists(select 1 from ' + target_schema + '.drug_exposure d'
                    create = create + ' where t.domain_concept_id_1 = 13 and t.fact_id_1 = d.drug_exposure_id)'
                    create = create + ' or exists(select 1 from ' + target_schema + '.measurement m'
                    create = create + ' where t.domain_concept_id_1 = 21 and t.fact_id_1 = m.measurement_id)'
                    create = create + ' or exists(select 1 from ' + target_schema + '.observation o'
                    create = create + ' where t.domain_concept_id_1 = 27 and t.fact_id_1 = o.observation_id)'
                if table_name == 'location_history':
                    create = create + ' join ' + target_schema + '.' + cohort_table + ' c on c.person_id = t.entity_id'
                if table_name == 'visit_payer':
                    create = create + ' join ' + target_schema + '.visit_occurrence v on v.visit_occurrence_id = t.visit_occurrence_id'
                if table_name == 'hash_token':
                    if inc_hash:
                        create = create + ' join ' + target_schema + '.' + cohort_table + ' c on c.person_id = t.person_id'
                    else:
                        create = create + ' where FALSE'
                create = create + ';'
                create_dict[table_name] = create
                grant_vacuum_tables.append(table_name)

        for table_name in sorted(table_list):
            create_stmt = Statement(create_dict[table_name])
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

        # Add drug dose tables
        if drug_dose:
            del table_list[:]
            create_dict.clear()
            stmts.clear()
            drug_dose_tables = ['drug_exposures_mgkg_derivations','drug_exposures_mgkg_metadata']
            for table_name in drug_dose_tables:
                table_list.append(table_name)
                create = 'create table ' + target_schema + '.' + table_name + ' as select t.*'
                create = create + ' from ' + source_schema + '.' + table_name + ' t'
                if table_name ==  'drug_exposures_mgkg_derivations':
                    create = create + ' join ' + target_schema + '.' + cohort_table + ' c on c.person_id = t.person_id'
                if table_name == 'drug_exposures_mgkg_metadata':
                    create = create +  ' join ' + target_schema + '.drug_exposure d on d.drug_exposure_id = t.drug_exposure_id'
                create = create + ';'
                create_dict[table_name] = create
                grant_vacuum_tables.append(table_name)

            for table_name in sorted(table_list):
                create_stmt = Statement(create_dict[table_name])
                stmts.add(create_stmt)

            # Execute the statements in parallel.
            stmts.parallel_execute(conn_str)

            # Check for any errors and raise exception if they are found.
            for stmt in stmts:
                try:
                    check_stmt_err(stmt, 'create drug dose tables')
                except:
                    logger.error(combine_dicts({'msg': 'Fatal error',
                                                'sql': stmt.sql,
                                                'err': str(stmt.err)}, log_dict))
                    logger.info(combine_dicts({'msg': 'create drug dose tables failed',
                                               'elapsed': secs_since(start_time)},
                                              log_dict))
                    raise
            logger.info({'msg': 'drug dose tables created'})
            stmts.clear()

        # Add measurement tables
        if measurement:
            del table_list[:]
            create_dict.clear()
            stmts.clear()
            for table_name in measurement_tables:
                table_list.append(table_name)
                create = 'create table ' + target_schema + '.' + table_name + ' as select t.*'
                create = create + ' from ' + source_schema + '.' + table_name + ' t'
                create = create + ' join ' + target_schema + '.' + cohort_table + ' c on c.person_id = t.person_id'
                create = create + ';'
                create_dict[table_name] = create
                grant_vacuum_tables.append(table_name)

            for table_name in sorted(table_list):
                create_stmt = Statement(create_dict[table_name])
                stmts.add(create_stmt)

            # Execute the statements in parallel.
            stmts.parallel_execute(conn_str)

            # Check for any errors and raise exception if they are found.
            for stmt in stmts:
                try:
                    check_stmt_err(stmt, 'create measurement tables')
                except:
                    logger.error(combine_dicts({'msg': 'Fatal error',
                                                'sql': stmt.sql,
                                                'err': str(stmt.err)}, log_dict))
                    logger.info(combine_dicts({'msg': 'create measurement tables failed',
                                               'elapsed': secs_since(start_time)},
                                              log_dict))
                    raise
            logger.info({'msg': 'measurement tables created'})
            stmts.clear()

        # Add COVID observation table
        if covid_obs:
            del table_list[:]
            create_dict.clear()
            stmts.clear()
            table_name = 'observation_derivation_covid'
            table_list.append(table_name)
            create = 'create table ' + target_schema + '.' + table_name + ' as select t.*'
            create = create + ' from ' + source_schema + '.' + table_name + ' t'
            create = create + ' join ' + target_schema + '.' + cohort_table + ' c on c.person_id = t.person_id'
            create = create + ';'
            create_dict[table_name] = create
            grant_vacuum_tables.append(table_name)

            for table_name in sorted(table_list):
                create_stmt = Statement(create_dict[table_name])
                stmts.add(create_stmt)

            # Execute the statements in parallel.
            stmts.parallel_execute(conn_str)

            # Check for any errors and raise exception if they are found.
            for stmt in stmts:
                try:
                    check_stmt_err(stmt, 'create covid observation table')
                except:
                    logger.error(combine_dicts({'msg': 'Fatal error',
                                                'sql': stmt.sql,
                                                'err': str(stmt.err)}, log_dict))
                    logger.info(combine_dicts({'msg': 'create drug dose tables failed',
                                               'elapsed': secs_since(start_time)},
                                              log_dict))
                    raise
            logger.info({'msg': 'covid observation created'})
            stmts.clear()

    # Set up new connection string for manipulating the target schema
    new_search_path = ','.join((target_schema, 'vocabulary'))
    new_conn_str = conn_str_with_search_path(conn_str, new_search_path)

    if not nopk:
        # Add primary keys to the subset tables
        add_primary_keys(new_conn_str, model_version, force)

    if not nonull:
        # Add NOT NULL constraints to the subset tables (no force option)
        set_not_nulls(new_conn_str, model_version)

    if index_create:
        # Add indexes to the subset tables
        add_indexes(new_conn_str, model_version, force)

        # Drop unneeded indexes from the transformed tables
        drop_unneeded_indexes(new_conn_str, model_version, force)

    if fk_create:
        # Add constraints to the subset tables
        add_foreign_keys(new_conn_str, model_version, force)

    # Create concept index replacement tables normally done during merge.
    if concept_create:
        create_index_replacement_tables(new_conn_str, model_version)
        condition_tables = ['condition_occurrence_source_value', 'condition_occurrence_concept_name']
        drug_tables = ['drug_exposure_source_value','drug_exposure_concept_name']
        measurement_tables = ['measurement_source_value', 'measurement_concept_name']
        procedure_tables =  ['procedure_occurrence_concept_name', 'procedure_occurrence_source_value']

        grant_vacuum_tables = grant_vacuum_tables + condition_tables + drug_tables + measurement_tables + procedure_tables

    # Grant permissions
    stmts.clear()
    logger.info({'msg': 'setting permissions'})
    if limit:
        users = (owner,)
    else:
        users = ('achilles_user', 'dqa_user', 'pcor_et_user', 'peds_staff', 'dcc_analytics')
    for target_table in grant_vacuum_tables:
        # alter_stmt = Statement(ALTER_OWNER_SQL.format(target_schema, target_table))
        # stmts.add(alter_stmt)
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
    logger.info(combine_dicts({'msg': 'finished subset by cohort',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True


def run_index_replace(conn_str, model_version):
    """Create index replacement tables

    :param model_version:   PEDSnet model version, e.g. 2.3.0
    :returns:   True if the function succeeds
    :rtype: bool
    """

    logger = logging.getLogger(__name__)
    log_dict = combine_dicts({'model_version': model_version, },
                             get_conn_info_dict(conn_str))
    logger.info(combine_dicts({'msg': 'starting subset by cohort'},
                              log_dict))
    start_time = time.time()

    metadata = stock_metadata(model_version)
    stmts = StatementSet()

    # Create concept index replacement tables normally done during merge.

    create_index_replacement_tables(conn_str, model_version)

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished subset by cohort',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True
