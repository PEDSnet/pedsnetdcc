import logging
import time
import hashlib

from pedsnetdcc.db import StatementSet, Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, combine_dicts, get_conn_info_dict)

logger = logging.getLogger(__name__)
NAME_LIMIT = 30
IDX_MEASURE_LIKE_TABLE_SQL = 'create index {0} on {1}.measurement ({2})'
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


def _make_index_name(table_name, column_name):
    """
    Create an index name for a given table/column combination with
    a NAME_LIMIT-character (Oracle) limit.  The table/column combination
    `provider.gender_source_concept_name` results in the index name
    `pro_gscn_ae1fd5b22b92397ca9_ix`.  We opt for a not particularly
    human-readable name in order to avoid collisions, which are all too
    possible with columns like provider.gender_source_concept_name and
    person.gender_source_concept_name
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


def run_post_lab_loinc(conn_str, site, search_path):
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
    logger_msg = '{0} post lab_loinc'
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

    # Add indexes (same as measurement)
    stmts.clear()
    logger.info({'msg': 'adding indexes'})
    col_index = ('measurement_age_in_months', 'measurement_concept_id', 'measurement_date',
                 'measurement_type_concept_id', 'person_id', 'site', 'visit_occurrence_id',
                 'value_as_concept_id', 'value_as_number',)

    for col in col_index:
        idx_name = _make_index_name('upd', col)
        idx_stmt = Statement(IDX_MEASURE_LIKE_TABLE_SQL.format(idx_name, schema, col))
        stmts.add(idx_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Measurement table indexes')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding indexes failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'indexes added'})

    # Log end of function.
    logger.info(combine_dicts({'msg': logger_msg.format("Finished"),
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True
