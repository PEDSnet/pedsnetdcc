import psycopg2
import logging
import time

from pedsnetdcc.db import StatementSet, Statement
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, combine_dicts, get_conn_info_dict, vacuum)

logger = logging.getLogger(__name__)
FUNCTION_INSERT_MEASUREMENT_SQL = """CREATE OR REPLACE FUNCTION trg_insert_measurement()
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

TRIGGER_MEASUREMENT_INSERT_SQL = """CREATE TRIGGER measurement_insert BEFORE INSERT
    ON {0}.measurement FOR EACH ROW
    EXECUTE PROCEDURE trg_insert_measurement();"""
TRUNCATE_MEASUREMENT_SQL = 'TRUNCATE {0}.measurement'
ADD_CHECK_CONSTRAINT_SQL = """ALTER TABLE {0}.measurement_{1} 
    ADD CONSTRAINT concept_in_{1} CHECK (measurement_concept_id {2} ({3}));"""
ADD_INHERIT_SQL = 'ALTER TABLE {0}.measurement_{1} INHERIT {0}.measurement;'


def partition_measurement_table(conn_str, model_version, search_path):
    """Partition measurement using measurement_anthro, measurement_labs, and measurement_vitals split tables.

    * Truncate Measurement Table
    * Alter split tables to add check constraints by measurement concept id
    * Alter split tables to inherit from the easurement table
    * Create trg_insert_measurement function to route measurements to correct split table
    * Add before insert trigger measurement_insert to measurement table

    :param str conn_str:      database connection string
    :param model_version: PEDSnet model version, e.g. 2.3.0
    :param str search_path: PostgreSQL schema search path
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

    # truncate the measurement table
    logger.info({'msg': 'truncating measurement table'})
    drop_measurement_stmt = Statement(TRUNCATE_MEASUREMENT_SQL.format(schema), "truncating measurement table")
    drop_measurement_stmt.execute(conn_str)
    check_stmt_err(drop_measurement_stmt, 'truncate measurement table')
    logger.info({'msg': 'measurement table truncated'})


    # List of tables to use as partitions
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
            cursor.execute(FUNCTION_INSERT_MEASUREMENT_SQL.format(schema))
            cursor.execute(TRIGGER_MEASUREMENT_INSERT_SQL.format(schema))
    conn.close()
    logger.info({'msg': 'measurement before insert trigger created'})

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished partitioning the measurement table',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True

