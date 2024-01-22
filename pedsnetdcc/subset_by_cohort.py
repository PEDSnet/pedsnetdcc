import logging
import time
import hashlib
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
NAME_LIMIT = 30
ALTER_OWNER_SQL = 'alter table {0}.{1} owner to dcc_owner;'
GRANT_TABLE_SQL = 'grant select on table {0}.{1} to {2}'
IDX_MEASURE_LIKE_TABLE_SQL = 'create index {0} on {1}.measurement_{2} ({3})'
FK_MEASURE_LIKE_TABLE_SQL = 'alter table {0}.measurement_{1} add constraint {2} foreign key ({3}) references {4}({5})'
PK_MEASURE_LIKE_TABLE_SQL = '''ALTER TABLE IF EXISTS {0}.measurement_{1}
    ADD CONSTRAINT measurement_{1}_pkey PRIMARY KEY (measurement_id);'''
SET_COLUMN_NOT_NULL = 'alter table {0}.measurement_{1} alter column {2} set not null;'

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

def run_subset_by_cohort(conn_str, model_version, source_schema, target_schema, cohort_table,
                         concept_create=False, drug_dose=False, measurement=False, covid_obs=False, inc_hash=False,
                         index_create=False, fk_create=False, notable=False, nopk=False, nonull=False,
                         limit=False, owner='loading_user', pre_split=False, force=False):
    """Create SQL for `select` statement transformations.

    The `search_path` only needs to contain the source schema; the target
    schema is embedded in the SQL statements.

    Returns a set of tuples of (sql_string, msg), where msg is a description
    for the operation to be carried out by the sql_string.

    :param model_version:   PEDSnet model version, e.g. 2.3.0
    :param str source_schema:   schema in which the tables are located
    :param str target_schema:   schema in which to create the subset
    :param str cohort_table:  name of table that contains the cohort
    :param bool concept_create: if True, create the concept group tables
    :param bool drug_dose: if True, copy drug dose tables
    :param bool measurement: if True, copy measurement derivation tables
    :param bool covid_obs: if True, copy covid observation table
    :param bool inc_hash: if True, include hash_token table
    :param bool index_create: if True, create indexes
    :param bool fk_create: if True, create fks
    :param bool notable: if True, don't create tables
    :param bool nopk: if True, don't create primary keys
    :param bool nonull: if True, don't set column not null
    :param bool limit: if True, limit permissions to owner
    :param str owner:  owner of the to grant permissions to
    :param bool pre_split: if True, measurement table is already split
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
        'hash_token',
        'cohort',
        'cohort_definition'
    }

    if measurement and pre_split:
        measurement_tables = {
            'measurement_anthro',
            'measurement_labs',
            'measurement_vitals',
            'measurement_bmi',
            'measurement_bmiz',
            'measurement_ht_z',
            'measurement_wt_z'
        }
    elif measurement and not pre_split:
        measurement_tables = {
            'measurement_bmi',
            'measurement_bmiz',
            'measurement_ht_z',
            'measurement_wt_z'
        }
    else:
        measurement_tables = {
            'measurement_anthro',
            'measurement_labs',
            'measurement_vitals'
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
            if table_name == 'measurement' and pre_split:
                create = 'create table ' + target_schema + '.measurement (like ' + source_schema + '.measurement);'
            else:
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
                if table_name == 'cohort' or table_name == 'cohort_definition':
                    if table_name == 'cohort':
                        create = create + ' join ' + target_schema + '.' + cohort_table + ' c on c.person_id = t.subject_id'
                    else:
                        # just skip for now
                        table_name == 'cohort_definition'

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
        if measurement or pre_split:
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
        # Add measurement like PK
        if measurement or pre_split:
            for table_name in measurement_tables:
                m_type = table_name[12:]
                measure_pk(new_conn_str, model_version, target_schema, m_type)

    if not nonull:
        # Add NOT NULL constraints to the subset tables (no force option)
        set_not_nulls(new_conn_str, model_version)
        if pre_split:
            add_measurement_not_nulls(new_conn_str, model_version, target_schema)

    if index_create:
        # Add indexes to the subset tables
        add_indexes(new_conn_str, model_version, force)

        # Drop unneeded indexes from the transformed tables
        drop_unneeded_indexes(new_conn_str, model_version, force)

        # Add measurement like indexes
        if measurement or pre_split:
            for table_name in measurement_tables:
                m_type = table_name[12:]
                measure_index(new_conn_str, model_version, target_schema, m_type)

    if fk_create:
        # Add constraints to the subset tables
        add_foreign_keys(new_conn_str, model_version, force)
        if pre_split:
            add_measurement_like_fks(new_conn_str, model_version, target_schema)

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

def measure_pk(conn_str, model_version, schema, m_type):
    logger = logging.getLogger(__name__)
    log_dict = combine_dicts({'model_version': model_version, },
                             get_conn_info_dict(conn_str))
    logger.info(combine_dicts({'msg': 'starting measurement like primary key'},
                              log_dict))
    start_time = time.time()
    stmts = StatementSet()

    logger.info({'msg': 'begin add measurement like primary key'})
    stmts.clear()

    idx_stmt = Statement(PK_MEASURE_LIKE_TABLE_SQL.format(schema, m_type))
    stmts.add(idx_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str, 1)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Run measurement like PK')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding PK failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'add PK complete'})

    return True;

def measure_index(conn_str, model_version, schema, m_type):
    logger = logging.getLogger(__name__)
    log_dict = combine_dicts({'model_version': model_version, },
                             get_conn_info_dict(conn_str))
    logger.info(combine_dicts({'msg': 'starting measurement like indexes'},
                              log_dict))
    start_time = time.time()
    stmts = StatementSet()

    logger.info({'msg': 'begin add measurement like indexes'})
    stmts.clear()
    col_index = ('measurement_age_in_months', 'measurement_concept_id', 'measurement_date',
                 'measurement_type_concept_id', 'person_id', 'site', 'visit_occurrence_id',
                 'measurement_source_value', 'value_as_concept_id', 'value_as_number',)

    for col in col_index:
        idx_name = _make_index_name(m_type, col)
        idx_stmt = Statement(IDX_MEASURE_LIKE_TABLE_SQL.format(idx_name, schema, m_type, col))
        stmts.add(idx_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str, 5)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Run measurement like indexes')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding indexes failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'add indexes complete'})

    return True;

def add_measurement_like_fks(conn_str, model_version, schema):
    """
    :param str conn_str: database connection string
    :param model_version:   PEDSnet model version, e.g. 2.3.0
    :param str schema: target schema
    """

    logger = logging.getLogger(__name__)
    log_dict = combine_dicts({'model_version': model_version, },
                             get_conn_info_dict(conn_str))
    logger.info(combine_dicts({'msg': 'starting measurement like foreign keys'},
                              log_dict))
    start_time = time.time()
    stmts = StatementSet()

    # Add foreign keys (same as measurement)
    stmts.clear()
    logger.info({'msg': 'adding foreign keys'})
    col_fk = ('operator_concept_id', 'person_id', 'priority_concept_id', 'provider_id',
              'range_high_operator_concept_id', 'range_low_operator_concept_id',
              'measurement_type_concept_id', 'unit_concept_id', 'value_as_concept_id',
              'visit_occurrence_id',)

    measure_like_tables = {
        'anthro',
        'labs',
        'vital'
    }

    for measure_like_table in measure_like_tables:
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
            fk_name = "fk_meas_" + base_name + "_" + measure_like_table
            fk_stmt = Statement(FK_MEASURE_LIKE_TABLE_SQL.format(schema,
                                                                 measure_like_table,
                                                                 fk_name, fk, ref_table,
                                                                 ref_col))
            stmts.add(fk_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Execute statements and check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Measurement like table FKs')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding foreign keys failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'foreign keys added'})


def add_measurement_not_nulls(conn_str, model_version, schema):
    """
    :param str conn_str: database connection string
    :param model_version:   PEDSnet model version, e.g. 2.3.0
    :param str schema: target schema
    """

    logger = logging.getLogger(__name__)
    log_dict = combine_dicts({'model_version': model_version, },
                             get_conn_info_dict(conn_str))
    logger.info(combine_dicts({'msg': 'starting set measurement columns not null'},
                              log_dict))
    start_time = time.time()
    stmts = StatementSet()

    # Set not null (same as measurement)
    stmts.clear()
    logger.info({'msg': 'setting columns not null'})
    col_not_null = ('measurement_concept_id', 'measurement_date', 'measurement_datetime',
                    'measurement_source_value', 'measurement_type_concept_id',
                    'person_id', 'value_source_value',)

    measure_like_tables = {
        'anthro',
        'labs',
        'vitals'
    }

    for measure_like_table in measure_like_tables:
        for col in col_not_null:
            set_not_null_stmt = Statement(SET_COLUMN_NOT_NULL.format(schema, measure_like_table, col))
            stmts.add(set_not_null_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Execute statements and check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Measurement like tables set not null')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'set not null failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'columns set not null'})