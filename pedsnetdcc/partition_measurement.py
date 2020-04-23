import psycopg2
import logging
import time

from pedsnetdcc.db import StatementSet, Statement
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, combine_dicts, get_conn_info_dict)

logger = logging.getLogger(__name__)
FUNCTION_INSERT_DCC_MEASUREMENT_SQL = """CREATE OR REPLACE FUNCTION trg_insert_dcc_measurement()
    RETURNS TRIGGER AS $$
    DECLARE
        old_measurement_id {0}.measurement.measurement_id%TYPE := NULL;
        partition_table varchar(7);
    BEGIN
        SELECT measurement_id INTO old_measurement_id
        FROM {0}.measurement
        WHERE measurement_id = NEW.measurement_id;
        IF found THEN
            RAISE unique_violation
            USING MESSAGE = 'Duplicate measurement_id: ' || old_measurement_id;
        END IF;
        
        -- Here we use measurement_concept_id to insert into appropriate partition
        CASE 
           WHEN NEW.measurement_concept_id IN (3013762, 3023540, 3038553, 2000000041, 2000000042, 2000000043, 
                                   2000000044, 2000000045, 3001537, 3025315, 3036277)
                                   THEN partition_table := 'anthro';
           WHEN NEW.measurement_concept_id IN (21490852, 21492241, 3027018, 40762499, 3024171, 3034703, 
                                   3019962, 3013940, 3012888, 3018586, 3035856, 3009395, 
                                   3004249, 3020891)
                                   THEN partition_table := 'vitals';
           WHEN NEW.measurement_concept_id NOT IN (3013762, 3023540, 3038553, 2000000041, 2000000042, 
                                       2000000043,2000000044, 2000000045, 3001537, 3025315, 
                                       3036277, 21490852, 21492241, 3027018, 40762499, 3024171, 
                                       3034703, 3019962, 3013940, 3012888, 3018586, 3035856, 
                                       3009395, 3004249, 3020891)
                                       THEN partition_table := 'labs';
           ELSE
              -- else required
              partition_table := 'unknown';
        END CASE;
        EXECUTE 'insert into {0}.measurement_' || partition_table ||
            ' values ( $1.* )' USING NEW;
        
        -- Prevent insertion into master table
        RETURN NULL;
    EXCEPTION
    WHEN undefined_table THEN
        -- Prevent insertion into master table
        RETURN NULL;
    END;
    $$
    LANGUAGE plpgsql;"""

FUNCTION_INSERT_SITE3_MEASUREMENT_SQL = """CREATE OR REPLACE FUNCTION trg_insert_site_measurement()
    RETURNS TRIGGER AS $$
    DECLARE
        old_measurement_id {0}.measurement.measurement_id%TYPE := NULL;
        partition_table varchar(7);
    BEGIN
        SELECT measurement_id INTO old_measurement_id
        FROM {0}.measurement
        WHERE measurement_id = NEW.measurement_id;
        IF found THEN
            RAISE unique_violation
            USING MESSAGE = 'Duplicate measurement_id: ' || old_measurement_id;
        END IF;

        -- Here we use measurement_concept_id to insert into appropriate partition
        CASE 
           WHEN NEW.measurement_concept_id IN (3013762, 3023540, 3038553, 2000000041, 2000000042, 2000000043, 
                                   2000000044, 2000000045, 3001537, 3025315, 3036277)
                                   THEN partition_table := 'anthro';
           WHEN NEW.measurement_concept_id IN (21490852, 21492241, 3027018, 40762499, 3024171, 3034703, 
                                   3019962, 3013940, 3012888, 3018586, 3035856, 3009395, 
                                   3004249, 3020891)
                                   THEN partition_table := 'vitals';
           WHEN NEW.measurement_concept_id NOT IN (3013762, 3023540, 3038553, 2000000041, 2000000042, 
                                       2000000043,2000000044, 2000000045, 3001537, 3025315, 
                                       3036277, 21490852, 21492241, 3027018, 40762499, 3024171, 
                                       3034703, 3019962, 3013940, 3012888, 3018586, 3035856, 
                                       3009395, 3004249, 3020891)
                                       THEN partition_table := 'labs';
           ELSE
              -- else required
              partition_table := 'unknown';
        END CASE;
        EXECUTE 'insert into {0}.measurement_' || partition_table ||
            ' values ( $1.* )' USING NEW;

        -- Prevent insertion into master table
        RETURN NULL;
    EXCEPTION
    WHEN undefined_table THEN
        -- Prevent insertion into master table
        RETURN NULL;
    END;
    $$
    LANGUAGE plpgsql;"""

FUNCTION_INSERT_SITE_MEASUREMENT_SQL = """CREATE OR REPLACE FUNCTION trg_insert_site_measurement()
    RETURNS TRIGGER AS $$
    DECLARE
        old_measurement_id {0}.measurement.measurement_id%TYPE := NULL;
        partition_table varchar(7);
    BEGIN
        SELECT measurement_id INTO old_measurement_id
        FROM {0}.measurement
        WHERE measurement_id = NEW.measurement_id;
        IF found THEN
            RAISE unique_violation
            USING MESSAGE = 'Duplicate measurement_id: ' || old_measurement_id;
        END IF;

        -- Here we use measurement_concept_id to insert into appropriate partition
        CASE 
           WHEN NEW.measurement_concept_id IN (3013762, 3023540, 2000000044, 2000000045, 3001537, 3025315, 3036277)
                                   THEN partition_table := 'anthro';
           WHEN NEW.measurement_concept_id IN (21490852, 21492241, 3027018, 40762499, 3024171, 3034703, 
                                   3019962, 3013940, 3012888, 3018586, 3035856, 3009395, 
                                   3004249, 3020891)
                                   THEN partition_table := 'vitals';
           WHEN NEW.measurement_concept_id NOT IN (3013762, 3023540, 3038553, 2000000041, 2000000042, 
                                       2000000043,2000000044, 2000000045, 3001537, 3025315, 
                                       3036277, 21490852, 21492241, 3027018, 40762499, 3024171, 
                                       3034703, 3019962, 3013940, 3012888, 3018586, 3035856, 
                                       3009395, 3004249, 3020891)
                                       THEN partition_table := 'labs';
            WHEN NEW.measurement_concept_id IN (3038553)
                                        THEN partition_table := 'bmi';
            WHEN NEW.measurement_concept_id IN (2000000043)
                                        THEN partition_table := 'bmiz';
            WHEN NEW.measurement_concept_id IN (2000000042)
                                        THEN partition_table := 'ht_z';
            WHEN NEW.measurement_concept_id IN (2000000041)
                                        THEN partition_table := 'wt_z';                           
           ELSE
              -- else required
              partition_table := 'unknown';
        END CASE;
        EXECUTE 'insert into {0}.measurement_' || partition_table ||
            ' values ( $1.* )' USING NEW;

        -- Prevent insertion into master table
        RETURN NULL;
    EXCEPTION
    WHEN undefined_table THEN
        -- Prevent insertion into master table
        RETURN NULL;
    END;
    $$
    LANGUAGE plpgsql;"""


TRIGGER_DCC_MEASUREMENT_INSERT_SQL = """CREATE TRIGGER measurement_dcc_insert BEFORE INSERT
    ON {0}.measurement FOR EACH ROW
    EXECUTE PROCEDURE trg_insert_dcc_measurement();"""
TRIGGER_SITE_MEASUREMENT_INSERT_SQL = """CREATE TRIGGER measurement_site_insert BEFORE INSERT
    ON {0}.measurement FOR EACH ROW
    EXECUTE PROCEDURE trg_insert_site_measurement();"""
TRUNCATE_MEASUREMENT_SQL = 'TRUNCATE {0}.measurement'
ADD_CHECK_CONSTRAINT_SQL = """ALTER TABLE {0}.measurement_{1} 
    ADD CONSTRAINT concept_in_{1} CHECK (measurement_concept_id {2} ({3}));"""
ADD_INHERIT_SQL = 'ALTER TABLE {0}.measurement_{1} INHERIT {0}.measurement;'


def _copy_to_measure_table(conn_str, table, concepts):
    copy_to_sql = """INSERT INTO measurement_{0}(
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
        FROM measurement_anthro WHERE measurement_concept_id IN ({1})) ON CONFLICT DO NOTHING"""

    copy_to_msg = "copying {0} to measurement_{0}"

    # Insert BMI measurements into measurement_anthro table
    copy_to_stmt = Statement(copy_to_sql.format(table, concepts), copy_to_msg.format(table))

    # Execute the insert BMI measurements statement and ensure it didn't error
    copy_to_stmt.execute(conn_str)
    check_stmt_err(copy_to_stmt, 'insert ' + table + ' measurements')

    # If reached without error, then success!
    return True


def _delete_measure_from_anthro(conn_str, table, concepts):
    delete_bmi_sql = """DELETE FROM measurement_anthro WHERE
        measurement_concept_id IN ({0})"""

    delete_bmi_msg = "remove {0} from measurement_anthro"

    # Insert BMI measurements into measurement_anthro table
    delete_bmi_stmt = Statement(delete_bmi_sql.format(concepts), delete_bmi_msg.format(table))

    # Execute the insert BMI measurements statement and ensure it didn't error
    delete_bmi_stmt.execute(conn_str)
    check_stmt_err(delete_bmi_stmt, 'remove ' + table + ' measurements')

    # If reached without error, then success!
    return True


def partition_measurement_table(conn_str, model_version, search_path, dcc, site3):
    """Partition measurement using tables based on site (dcc or one of the 6 sites):
    dcc: measurement_anthro, measurement_labs, and measurement_vitals
    site: measurement_anthro, measurement_labs, and measurement_vitals,
    measurement_bmi, measurement_bmiz, measurement_ht_z, and measurement_wt_z

    * Truncate Measurement Table
    * Alter split tables to add check constraints by measurement concept id
    * Alter split tables to inherit from the measurement table
    * Create trg_insert_measurement function to route measurements to correct split table
    * Add before insert trigger measurement_insert to measurement table

    :param str conn_str:      database connection string
    :param model_version: PEDSnet model version, e.g. 2.3.0
    :param str search_path: PostgreSQL schema search path
    :param bool dcc:      is dcc versus site table
    :param bool site3:      is site but partition as dcc
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """
    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'model_version': model_version, },
                             get_conn_info_dict(conn_str))
    logger.info(combine_dicts({'msg': 'starting partitioning the measurement table'},
                              log_dict))
    start_time = time.time()
    schema = primary_schema(search_path)

    # move site bmi measurements if site (not dcc or site3)
    if not dcc or not site3:
        move_measures = {
            'bmi': (3038553,),
            'bmiz': (2000000043,),
            'ht_z': (2000000042,),
            'wt_z': (2000000041,),
        }
        for measure in move_measures:
            logger.info({'msg': 'moving ' + measure + ' measurements'})
            concepts = ','.join(map(str, move_measures[measure]))
            copied = _copy_to_measure_table(conn_str, measure, concepts)
            if copied:
                _delete_measure_from_anthro(conn_str, measure, concepts)
                logger.info({'msg': measure + ' measurements moved'})
            else:
                logger.info({'msg': 'error moving ' + measure + ' measurements'})


    # truncate the measurement table
    logger.info({'msg': 'truncating measurement table'})
    drop_measurement_stmt = Statement(TRUNCATE_MEASUREMENT_SQL.format(schema), "truncating measurement table")
    drop_measurement_stmt.execute(conn_str)
    check_stmt_err(drop_measurement_stmt, 'truncate measurement table')
    logger.info({'msg': 'measurement table truncated'})

    # List of tables to use as partitions
    if dcc or site3:
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
    else:
        measure_like_tables = {
            'anthro': 'in',
            'labs': 'not in',
            'vitals': 'in',
            'bmi': 'in',
            'bmiz': 'in',
            'ht_z': 'in',
            'wt_z': 'in'
        }
        # measurement concept ids to include/exclude
        concept_id = {
            'anthro': (3013762, 3023540, 2000000044, 2000000045, 3001537, 3025315, 3036277,),
            'labs': (21490852, 21492241, 3027018, 40762499, 3024171, 3034703, 3019962, 3013940,
                     3012888, 3018586, 3035856, 3009395, 3004249, 3020891, 3013762, 3023540,
                     3038553, 2000000041, 2000000042, 2000000043, 2000000044, 2000000045,
                     3001537, 3025315, 3036277,),
            'vitals': (21490852, 21492241, 3027018, 40762499, 3024171, 3034703, 3019962, 3013940,
                       3012888, 3018586, 3035856, 3009395, 3004249, 3020891,),
            'bmi': (3038553,),
            'bmiz': (2000000043,),
            'ht_z': (2000000042,),
            'wt_z': (2000000041,),
        }

    # Add check constraint for measurement concept ids in each table.
    stmts = StatementSet()
    logger.info({'msg': 'adding check constraints'})
    for measure_like_table in measure_like_tables:
        concepts = ','.join(map(str, concept_id[measure_like_table]))
        create_constraint_stmt = Statement(ADD_CHECK_CONSTRAINT_SQL.format(schema,
                                                                           measure_like_table,
                                                                           measure_like_tables[measure_like_table],
                                                                           concepts))
        stmts.add(create_constraint_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'partition measurement table')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding check constraints',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'check constraints added'})

    # Add inherit from measurement for each table.
    stmts = StatementSet()
    logger.info({'msg': 'adding inherit from measurement'})
    for measure_like_table in measure_like_tables:
        inherit_stmt = Statement(ADD_INHERIT_SQL.format(schema, measure_like_table))
        stmts.add(inherit_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'partition measurement table')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding inherit from measurement',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'inherit from measurement added'})

    # Define trg_insert_measurement function needed to determine which partition measurement
    # should be directed to (anthro, labs, or vitals)
    # Use trg_insert_measurement in before insert trigger measurement_insert
    logger.info({'msg': 'creating measurement before insert trigger'})
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            if dcc:
                cursor.execute(FUNCTION_INSERT_DCC_MEASUREMENT_SQL.format(schema))
                cursor.execute(TRIGGER_DCC_MEASUREMENT_INSERT_SQL.format(schema))
            elif site3:
                cursor.execute(FUNCTION_INSERT_SITE3_MEASUREMENT_SQL.format(schema))
                cursor.execute(TRIGGER_SITE_MEASUREMENT_INSERT_SQL.format(schema))
            else:
                cursor.execute(FUNCTION_INSERT_SITE_MEASUREMENT_SQL.format(schema))
                cursor.execute(TRIGGER_SITE_MEASUREMENT_INSERT_SQL.format(schema))
    conn.close()
    logger.info({'msg': 'measurement before insert trigger created'})

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished partitioning the measurement table',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True
