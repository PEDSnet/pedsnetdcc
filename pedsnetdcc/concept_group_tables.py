import logging
import time

from pedsnetdcc.db import StatementSet, Statement
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import (check_stmt_err, combine_dicts,
                              get_conn_info_dict)


def create_index_replacement_tables(conn_str, model_version):
    logger = logging.getLogger(__name__)
    create_by_table = {
        'condition_occurrence': ('condition_source_value', 'condition_concept_name',),
        'drug_exposure': ('drug_source_value','drug_concept_name',),
        'measurement': ('measurement_source_value', 'measurement_concept_name',),
        'procedure_occurrence': ('procedure_concept_name', 'procedure_source_value',),
    }

    create_concept_group_sql = 'create table {0} as select {1}, {2}, count({2}) from {3} group by {1},{2} order by {1}'
    pk_concept_group_sql = 'alter table {0} add primary key ({1},{2})'

    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'model_version': model_version, },
                             get_conn_info_dict(conn_str))
    logger.info(combine_dicts({'msg': 'starting index replacement tables'},
                              log_dict))
    start_time = time.time()

    table_stmts = StatementSet()
    pk_stmts = StatementSet()

    for tbl, cols in create_by_table.items():
        for col in cols:
            col_short_name = '_'.join(col.split('_')[1:])
            new_tbl_name = tbl + "_" + col_short_name
            if col_short_name.endswith('name'):
                col_id = '_'.join(col.split('_')[0:2]) + '_id'
            else:
                col_id = '_'.join(col.split('_')[0:1]) + '_concept_id'
            create_stmt = Statement(create_concept_group_sql.format(new_tbl_name, col, col_id, tbl))
            table_stmts.add(create_stmt)
            pk_stmt = Statement(pk_concept_group_sql.format(new_tbl_name, col, col_id))
            pk_stmts.add(pk_stmt)

    logger.info({'msg': 'begin creating index replacement tables'})

    # Execute the statements in parallel.
    table_stmts.parallel_execute(conn_str)

    # Check for any errors and raise exception if they are found.
    for table_stmt in table_stmts:
        try:
            check_stmt_err(table_stmt, 'create index replacement tables')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': table_stmt.sql,
                                        'err': str(table_stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'create index replacement tables failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'finished creating index replacement tables'})

    # Create the primary keys
    logger.info({'msg': 'begin adding primary keys to index replacement tables'})

    # Execute the statements in parallel.
    pk_stmts.parallel_execute(conn_str)

    # Check for any errors and raise exception if they are found.
    for pk_stmt in pk_stmts:
        try:
            check_stmt_err(pk_stmt, 'create pk for index replacement tables')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': pk_stmt.sql,
                                        'err': str(pk_stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'create pk for index replacement tables failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'finished adding primary keys to index replacement tables'})

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished index replacement tables',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True

