import logging
import time

from pedsnetdcc.db import StatementSet, Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, combine_dicts, get_conn_info_dict)

logger = logging.getLogger(__name__)
DROP_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE = 'alter table {0}.measurement_organism drop constraint fpk_meas_org_meas'
ADD_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE = """alter table {0}.measurement_organism 
    add constraint fpk_meas_org_meas
    foreign key (measurement_id) 
    references {0}.measurement (measurement_id);"""


def _add_primary_key(conn_str, schema):
    # Add primary keys
    pk_sql = "alter table {0}.updated_measurement add primary key (measurement_id)"
    pk_msg = "making measurement_id the primary key"

    # Make measurement Id the primary key
    logger.info({'msg': 'begin add primary key'})
    pk_stmt = Statement(pk_sql.format(schema),
                               pk_msg)

    # Execute the make measurement Id the primary key statement and ensure it didn't error
    pk_stmt.execute(conn_str)
    check_stmt_err(pk_stmt, 'make measurement_id the primary key')
    logger.info({'msg': 'primary key created'})

    # If reached without error, then success!
    return True


def _rename_table(conn_str, schema, current_name, new_name):
    # Rename table
    rename_sql = "alter table {0}.{1} rename to {2}"
    rename_msg = "renaming table {0} to {1}"

    # Rename the table
    logger.info({'msg': 'begin rename table'})
    rename_stmt = Statement(rename_sql.format(schema, current_name, new_name),
                               rename_msg.format(current_name, new_name))

    # Execute the rename table and ensure it didn't error
    rename_stmt.execute(conn_str)
    check_stmt_err(rename_stmt, 'rename table {0} to {1}'.format(current_name, new_name))
    logger.info({'msg': 'table renamed'})

    # If reached without error, then success!
    return True


def run_post_lab_lonic(conn_str, site, search_path):
    """Run the post Lab Loinc steps.

    * Add primary key
    * Rename measurement
    * Rename updated_measurement
    * Drop/Add measurement_organism/measurement constraint

    :param str conn_str:      database connection string
    :param str site:    site to run derivation for
    :param str search_path: PostgreSQL schema search path
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    conn_info_dict = get_conn_info_dict(conn_str)

    # Log start of the function and set the starting time.
    logger_msg = '{0} post lab_lonic'
    log_dict = combine_dicts({'site': site, },
                             conn_info_dict)
    logger.info(combine_dicts({'msg': logger_msg.format("Starting")},
                              log_dict))
    start_time = time.time()
    schema = primary_schema(search_path)

    stmts = StatementSet()

    # Add primary keys
    _add_primary_key(conn_str, schema)

    # Rename measurement to measurement_orig
    _rename_table(conn_str, schema, 'measurement', 'measurement_orig')

    # Rename updated_measurement to measurement
    _rename_table(conn_str, schema, 'updated_measurement', 'measurement')

    # Drop/Add measurement_organism measurement constraint
    # drop measurement organism fk to measurement
    stmts.clear()
    logger.info({'msg': 'dropping measurement organism fk to measurement_orig'})
    drop_fk_measurement_org = Statement(DROP_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE.format(schema),
                                        "dropping fk to measurement_orig")
    drop_fk_measurement_org.execute(conn_str)
    check_stmt_err(drop_fk_measurement_org, 'drop fk to measurement_orig')
    logger.info({'msg': 'measurement organism fk to measurement_orig dropped'})

    # add measurement organism fk to measurement
    stmts.clear()
    logger.info({'msg': 'adding measurement organism fk to measurement'})
    add_fk_measurement_org = Statement(ADD_FOREIGN_KEY_MEASURE_ORG_TO_MEASURE.format(schema),
                                       "adding measurement organism fk to measurement_labs")
    add_fk_measurement_org.execute(conn_str)
    check_stmt_err(add_fk_measurement_org, 'add measurement organism fk to measurement_labs')
    logger.info({'msg': 'measurement organism fk to measurement_labs added'})

    # Log end of function.
    logger.info(combine_dicts({'msg': logger_msg.format("Finished"),
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True