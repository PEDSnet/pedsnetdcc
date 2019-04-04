import logging
import time
import hashlib
import os
import re

from pedsnetdcc.db import StatementSet, Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, check_stmt_data, combine_dicts,
                              get_conn_info_dict, vacuum, stock_metadata)
from sh import derive_z

logger = logging.getLogger(__name__)
NAME_LIMIT = 30
CREATE_MEASURE_LIKE_TABLE_SQL = 'create table {0}.measurement_{1} (like {0}.measurement)'
DROP_NULL_Z_TABLE_SQL = 'alter table {0}.measurement_{1} alter column measurement_id drop not null;'
IDX_MEASURE_LIKE_TABLE_SQL = 'create index {0} on {1}.measurement_{2} ({3})'


def _create_bmiz_config_file(config_path, config_file, schema, password, conn_info_dict):
    with open(os.path.join(config_path, config_file), 'wb') as out_config:
        out_config.write('<concept_id_map>' + os.linesep)
        out_config.write('measurement_concept_id = 3038553' + os.linesep)
        out_config.write('<z_score_info>' + os.linesep)
        out_config.write('z_class_system = NHANES_2000' + os.linesep)
        out_config.write('z_class_measure = BMI for Age' + os.linesep)
        out_config.write('z_measurement_concept_id = 2000000043' + os.linesep)
        out_config.write('</z_score_info >' + os.linesep)
        out_config.write('</concept_id_map >' + os.linesep)
        out_config.write('z_measurement_concept_id = 2000000043' + os.linesep)
        out_config.write('z_measurement_type_concept_id = 45754907' + os.linesep)
        out_config.write('z_unit_source_value = SD' + os.linesep)
        out_config.write('clone_z_measurements = 1' + os.linesep)
        out_config.write('input_person_table = person' + os.linesep)
        out_config.write('output_chunk_size = 1000' + os.linesep)
        out_config.write('person_chunk_size = 1000' + os.linesep)
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
        out_config.write('post_connect_sql = set search_path to ' + schema + ', vocabulary;' + os.linesep)
        out_config.write('</src_rdb>' + os.linesep)
        out_config.write('input_measurement_table = measurement_bmi' + os.linesep)
        out_config.write('output_measurement_table = measurement_bmiz' + os.linesep)


def _create_height_z_config_file(config_path, config_file, schema, table, password, conn_info_dict):
    with open(os.path.join(config_path, config_file), 'wb') as out_config:
        out_config.write('<concept_id_map>' + os.linesep)
        out_config.write('measurement_concept_id = 3023540' + os.linesep)
        out_config.write('<z_score_info>' + os.linesep)
        out_config.write('z_class_system = NHANES_2000' + os.linesep)
        out_config.write('z_class_measure = Height for Age' + os.linesep)
        out_config.write('z_measurement_concept_id = 2000000042' + os.linesep)
        out_config.write('</z_score_info >' + os.linesep)
        out_config.write('</concept_id_map >' + os.linesep)
        out_config.write('z_measurement_concept_id = 2000000042' + os.linesep)
        out_config.write('z_measurement_type_concept_id = 45754907' + os.linesep)
        out_config.write('z_unit_source_value = SD' + os.linesep)
        out_config.write('clone_z_measurements = 1' + os.linesep)
        out_config.write('input_person_table = person' + os.linesep)
        out_config.write('output_chunk_size = 1000' + os.linesep)
        out_config.write('person_chunk_size = 1000' + os.linesep)
        out_config.write('verbose = 1' + os.linesep)
        out_config.write('<src_rdb>' + os.linesep)
        out_config.write('driver = Pg' + os.linesep)
        out_config.write('host = ' +  conn_info_dict.get('host') + os.linesep)
        out_config.write('database = ' + conn_info_dict.get('dbname') + os.linesep)
        out_config.write('schema = ' + schema + os.linesep)
        out_config.write('username = ' + conn_info_dict.get('user') + os.linesep)
        out_config.write('password = ' + password + os.linesep)
        out_config.write('domain = stage' + os.linesep)
        out_config.write('type = dcc' + os.linesep)
        out_config.write('post_connect_sql = set search_path to ' + schema + ', vocabulary;' + os.linesep)
        out_config.write('</src_rdb>' + os.linesep)
        out_config.write('input_measurement_table = ' + table + os.linesep)
        out_config.write('output_measurement_table = measurement_ht_z' + os.linesep)


def _create_weight_z_config_file(config_path, config_file, schema, table, password, conn_info_dict):
    with open(os.path.join(config_path, config_file), 'wb') as out_config:
        out_config.write('<concept_id_map>' + os.linesep)
        out_config.write('measurement_concept_id = 3013762' + os.linesep)
        out_config.write('<z_score_info>' + os.linesep)
        out_config.write('z_class_system = NHANES_2000' + os.linesep)
        out_config.write('z_class_measure = Weight for Age' + os.linesep)
        out_config.write('z_measurement_concept_id = 2000000041' + os.linesep)
        out_config.write('</z_score_info >' + os.linesep)
        out_config.write('</concept_id_map >' + os.linesep)
        out_config.write('z_measurement_concept_id = 2000000041' + os.linesep)
        out_config.write('z_measurement_type_concept_id = 45754907' + os.linesep)
        out_config.write('z_unit_source_value = SD' + os.linesep)
        out_config.write('clone_z_measurements = 1' + os.linesep)
        out_config.write('input_person_table = person' + os.linesep)
        out_config.write('output_chunk_size = 1000' + os.linesep)
        out_config.write('person_chunk_size = 1000' + os.linesep)
        out_config.write('verbose = 1' + os.linesep)
        out_config.write('<src_rdb>' + os.linesep)
        out_config.write('driver = Pg' + os.linesep)
        out_config.write('host = ' +  conn_info_dict.get('host') + os.linesep)
        out_config.write('database = ' + conn_info_dict.get('dbname') + os.linesep)
        out_config.write('schema = ' + schema + os.linesep)
        out_config.write('username = ' + conn_info_dict.get('user') + os.linesep)
        out_config.write('password = ' + password + os.linesep)
        out_config.write('domain = stage' + os.linesep)
        out_config.write('type = dcc' + os.linesep)
        out_config.write('post_connect_sql = set search_path to ' + schema + ', vocabulary;' + os.linesep)
        out_config.write('</src_rdb>' + os.linesep)
        out_config.write('input_measurement_table = ' + table + os.linesep)
        out_config.write('output_measurement_table = measurement_wt_z' + os.linesep)


def _make_index_name(z_type, column_name):
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

    table_abbrev = "mea_" + z_type.replace("_","")[:3]
    column_abbrev = ''.join([x[0] for x in column_name.split('_')])
    md5 = hashlib.md5(
        '{}.{}'.format(z_type, column_name).encode('utf-8')). \
        hexdigest()
    hashlen = NAME_LIMIT - (len(table_abbrev) + len(column_abbrev) +
                            3 * len('_') + len('ix'))
    return '_'.join([table_abbrev, column_abbrev, md5[:hashlen], 'ix'])


def _fill_concept_names(conn_str, schema, z_type):
    fill_concept_names_sql = """UPDATE {0}.measurement_{1} zs
        SET measurement_concept_name=v.measurement_concept_name,
        measurement_source_concept_name=v.measurement_source_concept_name, 
        measurement_type_concept_name=v.measurement_type_concept_name, 
        operator_concept_name=v.operator_concept_name, 
        priority_concept_name=v.priority_concept_name, 
        range_high_operator_concept_name=v.range_high_operator_concept_name, 
        range_low_operator_concept_name=v.range_low_operator_concept_name, 
        unit_concept_name=v.unit_concept_name, 
        value_as_concept_name=v.value_as_concept_name
        FROM ( SELECT
        z.measurement_id AS measurement_id,
        v1.concept_name AS measurement_concept_name, 
        v2.concept_name AS measurement_source_concept_name, 
        v3.concept_name AS measurement_type_concept_name, 
        v4.concept_name AS operator_concept_name,  
        v5.concept_name AS priority_concept_name,
        v6.concept_name AS range_high_operator_concept_name, 
        v7.concept_name AS range_low_operator_concept_name, 
        v8.concept_name AS unit_concept_name, 
        v9.concept_name AS value_as_concept_name 
        FROM {0}.measurement_{1} AS z
        LEFT JOIN vocabulary.concept AS v1 ON z.measurement_concept_id = v1.concept_id
        LEFT JOIN vocabulary.concept AS v2 ON z.measurement_source_concept_id = v2.concept_id 
        LEFT JOIN vocabulary.concept AS v3 ON z.measurement_type_concept_id = v3.concept_id
        LEFT JOIN vocabulary.concept AS v4 ON z.operator_concept_id  = v4.concept_id
        LEFT JOIN vocabulary.concept AS v5 ON z.priority_concept_id  = v5.concept_id
        LEFT JOIN vocabulary.concept AS v6 ON z.range_high_operator_concept_id = v6.concept_id
        LEFT JOIN vocabulary.concept AS v7 ON z.range_low_operator_concept_id = v7.concept_id 
        LEFT JOIN vocabulary.concept AS v8 ON z.unit_concept_id = v8.concept_id
        LEFT JOIN vocabulary.concept AS v9 ON z.value_as_concept_id  = v9.concept_id
        ) v
        WHERE zs.measurement_id = v.measurement_id"""

    fill_concept_names_msg = "adding concept names"

    # Add concept names
    add_measurement_ids_stmt = Statement(fill_concept_names_sql.format(schema, z_type), fill_concept_names_msg)

    # Execute the add concept names statement and ensure it didn't error
    add_measurement_ids_stmt.execute(conn_str)
    check_stmt_err(add_measurement_ids_stmt, 'add concept names')


    # If reached without error, then success!
    return True


def _copy_to_dcc_table(conn_str, schema, table, z_type):
    copy_to_sql = """INSERT INTO dcc_pedsnet.{0}(
        measurement_concept_id, measurement_date, measurement_datetime, measurement_id, 
        measurement_order_date, measurement_order_datetime, measurement_result_date, 
        measurement_result_datetime, measurement_source_concept_id, measurement_source_value, 
        measurement_type_concept_id, operator_concept_id, person_id, priority_concept_id, 
        priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
        range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, 
        specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id, 
        value_as_number, value_source_value, visit_occurrence_id, measurement_age_in_months, 
        measurement_result_age_in_months, measurement_concept_name, measurement_source_concept_name, 
        measurement_type_concept_name, operator_concept_name, priority_concept_name, 
        range_high_operator_concept_name, range_low_operator_concept_name, unit_concept_name, 
        value_as_concept_name, site, site_id)
        (select measurement_concept_id, measurement_date, measurement_datetime, measurement_id, 
        measurement_order_date, measurement_order_datetime, measurement_result_date, 
        measurement_result_datetime, measurement_source_concept_id, measurement_source_value, 
        measurement_type_concept_id, operator_concept_id, person_id, priority_concept_id, 
        priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
        range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, 
        specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id, 
        value_as_number, value_source_value, visit_occurrence_id, measurement_age_in_months, 
        measurement_result_age_in_months, measurement_concept_name, measurement_source_concept_name, 
        measurement_type_concept_name, operator_concept_name, priority_concept_name, 
        range_high_operator_concept_name, range_low_operator_concept_name, unit_concept_name, 
        value_as_concept_name, site, site_id
        from {1}.measurement_{2}) ON CONFLICT DO NOTHING"""

    copy_to_msg = "copying {0} to dcc_pedsnet"

    # Insert measurements into measurement table
    copy_to_stmt = Statement(copy_to_sql.format(table, schema, z_type), copy_to_msg.format(table))

    # Execute the insert measurements statement and ensure it didn't error
    copy_to_stmt.execute(conn_str)
    check_stmt_err(copy_to_stmt, 'insert measurements')

    # If reached without error, then success!
    return True


def run_z_calc(z_type, config_file, conn_str, site, copy, table, password, search_path, model_version):
    """Run the Z Score tool.

    * Create config file
    * Create output table
    * Run BMI-Z, Height-Z, or Weight-Z
    * Index output table
    * Add measurement Ids
    * Add the concept names
    * Copy to the measurement table (if selected)
    * Vacuum output table

    :param str z_type:   type of Z score calculation (bmiz, htz, or wtz)
    :param str config_file:   config file name
    :param str conn_str:      database connection string
    :param str site:    site to run BMI for
    :param bool copy: if True, copy results to dcc_pedsnet
    :param str table:    name of input/copy table (measurement/measurement_anthro)
    :param str password:    user's password
    :param str search_path: PostgreSQL schema search path
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    if z_type == 'ht_z':
        z_type_name ="Height-Z"
    elif z_type == 'wt_z':
        z_type_name = "Weight-Z"
    else:
        z_type_name = "BMI-Z"

    conn_info_dict = get_conn_info_dict(conn_str)
    logger_msg = '{0} {1} calculation'

    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'site': site, },
                             conn_info_dict)
    logger.info(combine_dicts({'msg': logger_msg.format('Starting', z_type_name)},
                              log_dict))
    start_time = time.time()
    schema = primary_schema(search_path)

    if password == None:
        pass_match = re.search(r"password=(\S*)", conn_str)
        password = pass_match.group(1)

    # create the congig file
    config_path = "/app"

    if z_type == 'ht_z':
        _create_height_z_config_file(config_path, config_file, schema, table, password, conn_info_dict)
    elif z_type == 'wt_z':
        _create_weight_z_config_file(config_path, config_file, schema, table, password, conn_info_dict)
    else:
        _create_bmiz_config_file(config_path, config_file, schema, password, conn_info_dict)

    # create measurement z_score table
    # Add a creation statement.
    stmts = StatementSet()
    create_stmt = Statement(CREATE_MEASURE_LIKE_TABLE_SQL.format(schema, z_type))
    stmts.add(create_stmt)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            stmt.execute(conn_str)
            check_stmt_err(stmt, logger_msg.format('Run', z_type_name))
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'create table failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise

    # Add drop null statement
    stmts.clear()
    drop_stmt = Statement(DROP_NULL_Z_TABLE_SQL.format(schema, z_type))
    stmts.add(drop_stmt)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            stmt.execute(conn_str)
            check_stmt_err(stmt, logger_msg.format('Run', z_type_name))
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'drop measurement id null failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise

    # Run Z-Score tool
    derive_z(config_file[:-5], '--verbose=1', _cwd='/app', _fg=True)

    # Add indexes to measurement result table (same as measurement)
    logger.info({'msg': 'begin add indexes'})
    stmts.clear()
    col_index = ('measurement_age_in_months', 'measurement_concept_id', 'measurement_date',
                 'measurement_type_concept_id', 'person_id', 'site', 'visit_occurrence_id',
                 'measurement_source_value', 'value_as_concept_id', 'value_as_number',)

    for col in col_index:
        idx_name = _make_index_name(z_type, col)
        idx_stmt = Statement(IDX_MEASURE_LIKE_TABLE_SQL.format(idx_name, schema, z_type, col))
        stmts.add(idx_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, logger_msg.format('Run', z_type_name))
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding indexes failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'add indexes complete'})

    # add measurement_ids
    okay = _add_measurement_ids(z_type, conn_str, site, search_path, model_version)
    if not okay:
        return False

    # Add the concept_names
    logger.info({'msg': 'add concept names'})
    okay = _fill_concept_names(conn_str, schema, z_type)
    if not okay:
        return False
    logger.info({'msg': 'concept names added'})

    # Copy to the measurement table
    if copy:
        logger.info({'msg': 'copy measurements to dcc_pedsnet'})
        okay = _copy_to_dcc_table(conn_str, schema, table, z_type)
        if not okay:
            return False
        logger.info({'msg': 'measurements copied to dcc_pedsnet'})

    # Vacuum analyze tables for piney freshness.
    logger.info({'msg': 'begin vacuum'})
    vacuum(conn_str, model_version, analyze=True, tables=['measurement_' + z_type])
    logger.info({'msg': 'vacuum finished'})

    # Log end of function.
    logger.info(combine_dicts({'msg': logger_msg.format('Finished', z_type_name),
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True


def _add_measurement_ids(z_type, conn_str, site, search_path, model_version):
    """Add measurement ids for the bmi table

    * Find how many ids needed
    * Update dcc_measurement_id with new value
    * Create sequence
    * Set sequence starting number
    * Assign measurement ids
    * Make measurement Id the primary key

    :param str z_type:    type of z score calculation (bmiz,htz, wtz)
    :param str conn_str:      database connection string
    :param str site:    site to run z score for
    :param str search_path: PostgreSQL schema search path
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    new_id_count_sql = """SELECT COUNT(*)
        FROM {0}.measurement_{1} WHERE measurement_id IS NULL"""
    new_id_count_msg = "counting new IDs needed for measurement_{0}"
    lock_last_id_sql = """LOCK {last_id_table_name}"""
    lock_last_id_msg = "locking {table_name} last ID tracking table for update"

    update_last_id_sql = """UPDATE {last_id_table_name} AS new
        SET last_id = new.last_id + '{new_id_count}'::integer
        FROM {last_id_table_name} AS old RETURNING old.last_id, new.last_id"""
    update_last_id_msg = "updating {table_name} last ID tracking table to reserve new IDs"  # noqa
    create_seq_measurement_sql = "create sequence if not exists {0}.{1}_measurement_id_seq"
    create_seq_measurement_msg = "creating measurement id sequence"
    set_seq_number_sql = "alter sequence {0}.{1}_measurement_id_seq restart with {2};"
    set_seq_number_msg = "setting sequence number"
    add_measurement_ids_sql = """update {0}.measurement_{1} set measurement_id = nextval('{0}.{2}_measurement_id_seq')
        where measurement_id is null"""
    add_measurement_ids_msg = "adding the measurement ids to the measurement_{0} table"
    pk_measurement_id_sql = "alter table {0}.measurement_{1} add primary key (measurement_id)"
    pk_measurement_id_msg = "making measurement_id the primary key"

    conn_info_dict = get_conn_info_dict(conn_str)

    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'site': site, },
                             conn_info_dict)

    logger = logging.getLogger(__name__)

    logger.info(combine_dicts({'msg': 'starting measurement_id assignment'},
                              log_dict))
    start_time = time.time()
    schema = primary_schema(search_path)
    table_name = 'measurement'

    # Mapping and last ID table naming conventions.
    last_id_table_name_tmpl = "dcc_{table_name}_id"
    metadata = stock_metadata(model_version)

    # Get table object and start to build tpl_vars map, which will be
    # used throughout for formatting SQL statements.
    table = metadata.tables[table_name]
    tpl_vars = {'table_name': table_name}
    tpl_vars['last_id_table_name'] = last_id_table_name_tmpl.format(**tpl_vars)

    # Build the statement to count how many new ID mappings are needed.
    new_id_count_stmt = Statement(new_id_count_sql.format(schema, z_type), new_id_count_msg.format(z_type))

    # Execute the new ID mapping count statement and ensure it didn't
    # error and did return a result.
    new_id_count_stmt.execute(conn_str)
    check_stmt_err(new_id_count_stmt, 'assign measurement ids')
    check_stmt_data(new_id_count_stmt, 'assign measurement ids')

    # Get the actual count of new ID maps needed and log it.
    tpl_vars['new_id_count'] = new_id_count_stmt.data[0][0]
    logger.info({'msg': 'counted new IDs needed', 'table': table_name,
                 'count': tpl_vars['new_id_count']})

    # Build list of two last id table update statements that need to
    # occur in a single transaction to prevent race conditions.
    update_last_id_stmts = StatementList()
    update_last_id_stmts.append(Statement(
        lock_last_id_sql.format(**tpl_vars),
        lock_last_id_msg.format(**tpl_vars)))
    update_last_id_stmts.append(Statement(
        update_last_id_sql.format(**tpl_vars),
        update_last_id_msg.format(**tpl_vars)))

    # Execute last id table update statements and ensure they didn't
    # error and the second one returned results.
    update_last_id_stmts.serial_execute(conn_str, transaction=True)

    for stmt in update_last_id_stmts:
        check_stmt_err(stmt, 'assign measurement ids')
    check_stmt_data(update_last_id_stmts[1],
                    'assign measurement ids')

    # Get the old and new last IDs from the second update statement.
    tpl_vars['old_last_id'] = update_last_id_stmts[1].data[0][0]
    tpl_vars['new_last_id'] = update_last_id_stmts[1].data[0][1]
    logger.info({'msg': 'last ID tracking table updated',
                 'table': table_name,
                 'old_last_id': tpl_vars['old_last_id'],
                 'new_last_id': tpl_vars['new_last_id']})

    logger.info({'msg': 'begin measurement id sequence creation'})
    # Create the measurement id sequence (if it doesn't exist)
    measurement_seq_stmt = Statement( create_seq_measurement_sql.format(schema, site),
                                      create_seq_measurement_msg)

    # Execute the create the measurement id sequence statement and ensure it didn't error
    measurement_seq_stmt.execute(conn_str)
    check_stmt_err(measurement_seq_stmt, 'create measurement id sequence')
    logger.info({'msg': 'measurement id sequence creation complete'})

    # Set the sequence number
    logger.info({'msg': 'begin set sequence number'})
    seq_number_set_stmt = Statement(set_seq_number_sql.format(schema, site, tpl_vars['old_last_id']),
                                    set_seq_number_msg)

    # Execute the set the sequence number statement and ensure it didn't error
    seq_number_set_stmt.execute(conn_str)
    check_stmt_err(seq_number_set_stmt, 'set the sequence number')
    logger.info({'msg': 'set sequence number complete'})

    # Add the measurement ids
    logger.info({'msg': 'begin add measurement ids'})
    add_measurement_ids_stmt = Statement(add_measurement_ids_sql.format(schema, z_type, site),
                                         add_measurement_ids_msg.format(z_type))

    # Execute the add the measurement ids statement and ensure it didn't error
    add_measurement_ids_stmt.execute(conn_str)
    check_stmt_err(add_measurement_ids_stmt, 'add the measurement ids')
    logger.info({'msg': 'add measurement ids complete'})

    # Make measurement Id the primary key
    logger.info({'msg': 'begin add primary key'})
    pk_measurement_id_stmt = Statement(pk_measurement_id_sql.format(schema, z_type),
                                         pk_measurement_id_msg)

    # Execute the Make measurement Id the primary key statement and ensure it didn't error
    pk_measurement_id_stmt.execute(conn_str)
    check_stmt_err(pk_measurement_id_stmt, 'make measurement Id the primary key')
    logger.info({'msg': 'primary key created'})

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished adding measurement ids',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True
