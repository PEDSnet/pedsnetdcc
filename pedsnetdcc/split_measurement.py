import logging
import time
import hashlib

from pedsnetdcc.db import StatementSet, Statement
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, combine_dicts, get_conn_info_dict, vacuum)

logger = logging.getLogger(__name__)
NAME_LIMIT = 30
CREATE_MEASURE_LIKE_TABLE_SQL = """create table {0}.measurement_{1} as 
select * from {0}.measurement where measurement_concept_id {2} ({3})"""
PK_MEASURE_LIKE_TABLE_SQL = 'alter table {0}.measurement_{1} add primary key(measurement_id)'
IDX_MEASURE_LIKE_TABLE_SQL = 'create index {0} on {1}.measurement_{2} ({3})'
FK_MEASURE_LIKE_TABLE_SQL = 'alter table {0}.measurement_{1} add constraint {2} foreign key ({3}) references {4}({5})'
GRANT_MEASURE_LIKE_TABLE_SQL = 'grant select on table {0}.measurement_{1} to {2}'
DROP_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE = 'alter table {0}.measurement_organism drop constraint fpk_meas_org_meas'
ADD_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE_LABS = """alter table {0}.measurement_organism 
    add constraint fpk_meas_org_meas_lab
    foreign key (measurement_id) 
    references {0}.measurement_labs (measurement_id);"""
TRUNCATE_MEASUREMENT_SQL = 'truncate table {0}.measurement;'
SET_COLUMN_NOT_NULL = 'alter table {0}.measurement_{1) alter column {2} set not null;'

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


def split_measurement_table(conn_str, truncate, view, model_version, search_path):
    """Split measurement into anthro, lab, and vital.

    * Create the measurement_anthro, measurement_labs, and measurement_vitals from measurement
    * Set primary keys
    * Add indexes
    * Add foreign keys
    * Set permissions
    * Truncate measurement table if flag set
    * Create measurements view if flag set
    * Vacuum

    :param str conn_str:      database connection string
    :param bool truncate: if True, truncate the measurement table
    :param bool view: if True, create the measurements view
    :param model_version: PEDSnet model version, e.g. 2.3.0
    :param str search_path: PostgreSQL schema search path
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """
    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'model_version': model_version, },
                             get_conn_info_dict(conn_str))
    logger.info(combine_dicts({'msg': 'starting measurement table split'},
                              log_dict))
    start_time = time.time()
    schema = primary_schema(search_path)

    # List of tables to create
    measure_like_tables = {
        'anthro': 'in',
        'labs': 'not in',
        'vitals': 'in'
    }

    # measurement concept ids to include/exclude
    concept_id = {
        'anthro': (3013762, 3023540, 3038553, 2000000041, 2000000042, 2000000043,
                   2000000044, 2000000045, 3001537, 3025315, 3036277,),
        'labs': (21490852, 21492241, 3027018, 40762499, 3024171, 3034703, 3019962, 3013940,
                 3012888, 3018586, 3035856, 3009395, 3004249, 3020891, 3013762, 3023540,
                 3038553, 2000000041, 2000000042, 2000000043, 2000000044, 2000000045,
                 3001537, 3025315, 3036277,),
        'vitals': (21490852, 21492241, 3027018, 40762499, 3024171, 3034703, 3019962, 3013940,
                   3012888, 3018586, 3035856, 3009395, 3004249, 3020891,),
    }

    # Add a creation statement for each table.
    stmts = StatementSet()
    logger.info({'msg': 'creating split tables'})
    for measure_like_table in measure_like_tables:
        concepts = ','.join(map(str, concept_id[measure_like_table]))
        create_stmt = Statement(CREATE_MEASURE_LIKE_TABLE_SQL.
                                format(schema, measure_like_table, measure_like_tables[measure_like_table], concepts))
        stmts.add(create_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Measurement table split')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'create new tables failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'split tables created'})

    # Set primary keys
    stmts.clear()
    logger.info({'msg': 'setting primary keys'})
    for measure_like_table in measure_like_tables:
        pk_stmt = Statement(PK_MEASURE_LIKE_TABLE_SQL.format(schema, measure_like_table))
        stmts.add(pk_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Measurement table split')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding primary keys failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'primary keys set'})

    # Add indexes (same as measurement)
    stmts.clear()
    logger.info({'msg': 'adding indexes'})
    col_index = ('measurement_age_in_months', 'measurement_concept_id', 'measurement_date',
                 'measurement_type_concept_id', 'person_id', 'site', 'visit_occurrence_id',
                 'measurement_source_value', 'value_as_concept_id', 'value_as_number',)

    for measure_like_table in measure_like_tables:
        for col in col_index:
            idx_name = _make_index_name(measure_like_table, col)
            idx_stmt = Statement(IDX_MEASURE_LIKE_TABLE_SQL.format(idx_name, schema, measure_like_table, col))
            stmts.add(idx_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Measurement table split')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding indexes failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'indexes added'})

    # Add foreign keys (same as measurement)
    stmts.clear()
    logger.info({'msg': 'adding foreign keys'})
    col_fk = ('operator_concept_id', 'person_id', 'priority_concept_id', 'provider_id',
              'range_high_operator_concept_id', 'range_low_operator_concept_id',
              'measurement_type_concept_id', 'unit_concept_id', 'value_as_concept_id',
              'visit_occurrence_id',)

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
            check_stmt_err(stmt, 'Measurement table split')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding foreign keys failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'foreign keys added'})

    # Set not null (same as measurement)
    stmts.clear()
    logger.info({'msg': 'setting columns not null'})
    col_not_null = ('measurement_concept_id', 'measurement_date', 'measurement_datetime',
                    'measurement_source_value', 'measurement_type_concept_id',
                    'person_id', 'value_source_value',)

    for measure_like_table in measure_like_tables:
        for col in col_not_null:
            set_not_null_stmt = Statement(SET_COLUMN_NOT_NULL.format(schema, measure_like_table, col))
            stmts.add(set_not_null_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Execute statements and check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Measurement table split')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'set not null failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'columns set not null'})

    # drop measurement organism fk to measurement
    stmts.clear()
    logger.info({'msg': 'dropping measurement organism fk to measurement'})
    drop_fk_measurement_org = Statement(DROP_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE.format(schema),
                                        "dropping fk to measurement")
    drop_fk_measurement_org.execute(conn_str)
    check_stmt_err(drop_fk_measurement_org, 'drop fk to measurement')
    logger.info({'msg': 'measurement organism fk to measurement dropped'})

    # add measurement organism fk to measurement_labs
    stmts.clear()
    logger.info({'msg': 'adding measurement organism fk to measurement_labs'})
    add_fk_measurement_org = Statement(ADD_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE_LABS.format(schema),
                                       "adding measurement organism fk to measurement_labs")
    add_fk_measurement_org.execute(conn_str)
    check_stmt_err(add_fk_measurement_org, 'add measurement organism fk to measurement_labs')
    logger.info({'msg': 'measurement organism fk to measurement_labs added'})

    # Set permissions
    stmts.clear()
    logger.info({'msg': 'setting permissions'})
    users = ('harvest_user', 'achilles_user', 'dqa_user', 'pcor_et_user', 'peds_staff')
    for measure_like_table in measure_like_tables:
        for usr in users:
            grant_stmt = Statement(GRANT_MEASURE_LIKE_TABLE_SQL.format(schema, measure_like_table, usr))
            stmts.add(grant_stmt)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            stmt.execute(conn_str)
            check_stmt_err(stmt, 'Measurement table split')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'granting permissions failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'permissions set'})

    # Truncate measurement if flag set
    if truncate:
        # truncate the measurement table
        stmts.clear()
        logger.info({'msg': 'truncating measurement table'})
        drop_measurement_stmt = Statement(TRUNCATE_MEASUREMENT_SQL.format(schema), "truncating measurement table")
        drop_measurement_stmt.execute(conn_str)
        check_stmt_err(drop_measurement_stmt, 'truncate measurement table')
        logger.info({'msg': 'measurement table truncated'})


    # Create measurements view if flag set
    if view:
        stmts.clear()
        logger.info({'msg': 'creating measurements view'})
        view_stmt = Statement("""create view measurements as
        select * from measurement_anthro
        union all
        select * from measurement_labs
        union all
        select * from measurement_vitals
        """)

        stmts.add(view_stmt)

        # Execute statements and check for any errors and raise exception if they are found.
        for stmt in stmts:
            try:
                stmt.execute(conn_str)
                check_stmt_err(stmt, 'Measurement table split')
            except:
                logger.error(combine_dicts({'msg': 'Fatal error',
                                            'sql': stmt.sql,
                                            'err': str(stmt.err)}, log_dict))
                logger.info(combine_dicts({'msg': 'create view failed',
                                           'elapsed': secs_since(start_time)},
                                          log_dict))
                raise
        logger.info({'msg': 'measurements view created'})

    # Vacuum analyze tables for piney freshness.
    vacuum(conn_str, model_version, analyze=True,
           tables=['measurement_anthro', 'measurement_labs', 'measurement_vitals'])

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished measurement table split',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True
