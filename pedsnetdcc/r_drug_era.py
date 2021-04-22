import logging
import time
import re
import shutil
import os


from pedsnetdcc.db import StatementSet, Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, check_stmt_data, combine_dicts,
                              get_conn_info_dict, vacuum, stock_metadata)
from sh import Rscript

logger = logging.getLogger(__name__)
DROP_PK_CONSTRAINT_ERA_SQL = """alter table {0}_era drop constraint if exists xpk_{0}_era;
    alter table {0}_era drop constraint if exists {0}_era_pkey;"""
DROP_NULL_ERA_SQL = 'alter table {0}_era alter column {0}_era_id drop not null;'
TRUNCATE_ERA_SQL = 'TRUNCATE {0}.drug_era;'
IDX_ERA_SQL = 'create index {0} on {1}_era ({2})'

def _create_argos_file(config_path, config_file, schema, password, conn_info_dict):
    with open(os.path.join(config_path, config_file), 'wb') as out_config:
        out_config.write('{' + os.linesep)
        out_config.write('"src_name": "Postgres",' + os.linesep)
        out_config.write('"src_args": {' + os.linesep)
        out_config.write('"host": "' + conn_info_dict.get('host') + '",' + os.linesep)
        out_config.write('"port": 5432,' + os.linesep)
        out_config.write('"dbname": "' + conn_info_dict.get('dbname') + '",' + os.linesep)
        out_config.write('"user": "' + conn_info_dict.get('user') + '",' + os.linesep)
        out_config.write('"password": "' + password + '",' + os.linesep)
        out_config.write('"bigint": "numeric",' + os.linesep)
        out_config.write('"options": "-c search_path=' + schema + ',vocabulary"' + os.linesep)
        out_config.write('}' + os.linesep)
        out_config.write('}' + os.linesep)


def _fix_site_info(file_path, site, schema):
    try:
        with open(os.path.join(file_path,'site','site_info.R'), 'r') as site_file:
            file_data = site_file.read()
        file_data = file_data.replace('<SITE>', site)
        file_data = file_data.replace('<SCHEMA>', schema)
        with open(os.path.join(file_path,'site','site_info.R'), 'w') as site_file:
            site_file.write(file_data)
    except:
        # this query package may not have this file
        return False

    return True

def _fix_size(file_path, size):
    try:
        with open(os.path.join(file_path,'code','driver.R'), 'r') as driver_file:
            file_data = driver_file.read()
        file_data = file_data.replace('<SIZE>', str(size))
        file_data = file_data.replace('<SIZEMINUS1>', str(size-1))
        with open(os.path.join(file_path,'code','driver.R'), 'w') as driver_file:
            driver_file.write(file_data)
    except:
        # this query package may not have this file
        return False

    return True

def _fix_run(file_path, site):
    try:
        with open(os.path.join(file_path,'site','run.R'), 'r') as site_file:
            file_data = site_file.read()
        file_data = file_data.replace('<SITE>', site)
        with open(os.path.join(file_path,'site','run.R'), 'w') as site_file:
            site_file.write(file_data)
    except:
        # this query package may not have this file
        return False

    return True


def _copy_to_dcc_table(conn_str, era_type, schema):
    copy_to_drug_sql = """INSERT INTO dcc_pedsnet.drug_era(
        drug_concept_id, drug_era_end_date, drug_era_start_date, drug_exposure_count, 
        gap_days, drug_concept_name, site, drug_era_id, site_id, person_id)
        (select drug_concept_id, drug_era_end_date, drug_era_start_date, drug_exposure_count, 
        gap_days, drug_concept_name, site, drug_era_id, site_id, person_id
        from {0}.drug_era) ON CONFLICT DO NOTHING"""
    copy_to_msg = "copying {0}_era to dcc_pedsnet"

    # Insert era data into dcc_pedsnet era table
    copy_to_stmt = Statement(copy_to_drug_sql.format(schema), copy_to_msg.format(era_type))

    # Execute the insert era statement and ensure it didn't error
    copy_to_stmt.execute(conn_str)
    check_stmt_err(copy_to_stmt, 'insert {0}_era data'.format(era_type))

    # If reached without error, then success!
    return True


def run_r_drug_era(conn_str, site, copy, neg_ids, search_path, password, model_version, id_name, notable=False,
                   noids=False, nopk=False, novac=False, size='5000'):

    """Run the Condition or Drug Era derivation.

    * Execute SQL
    * Add Ids
    * Add the concept names
    * Copy to dcc_pedsnet (if selected)
    * Vacuum output table

    :param str era_type:    type of derivation (condition or drug)
    :param str conn_str:      database connection string
    :param str site:    site to run derivation for
    :param bool copy: if True, copy results to dcc_pedsnet
    :param bool neg_ids: if True, use negative ids
    :param str search_path: PostgreSQL schema search path
    :param str password:    user's password
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str id_name: name of the id (ex. dcc or onco)
    :param bool notable:      skip creating tables if it already exists
    :param bool noids:        skip ids if already exist
    :param bool nopk:         skip primary keys if already exist
    :param bool noidx:        skip ndexes if already exist
    :param bool novac:        skip vaccuum if already done
    :param str size:          size for # of persons in each group
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors

    """
    era_type = 'drug'
    package = 'drug_era'
    config_file = site + "_" + package + "_argos_temp.json";

    if password == None:
        pass_match = re.search(r"password=(\S*)", conn_str)
        password = pass_match.group(1)

    conn_info_dict = get_conn_info_dict(conn_str)

    # Log start of the function and set the starting time.
    logger_msg = '{0} {1} era calculation'
    log_dict = combine_dicts({'site': site, },
                             conn_info_dict)
    logger.info(combine_dicts({'msg': logger_msg.format("starting",era_type)},
                              log_dict))
    start_time = time.time()
    schema = primary_schema(search_path)

    stmts = StatementSet()

    if not notable:
        # Drop primary key.
        drop_pk_stmt = Statement(DROP_PK_CONSTRAINT_ERA_SQL.format(era_type))
        stmts.add(drop_pk_stmt)

        # Check for any errors and raise exception if they are found.
        for stmt in stmts:
            try:
                stmt.execute(conn_str)
                check_stmt_err(stmt, logger_msg.format('Run', era_type))
            except:
                logger.error(combine_dicts({'msg': 'Fatal error',
                                            'sql': stmt.sql,
                                            'err': str(stmt.err)}, log_dict))
                logger.info(combine_dicts({'msg': 'drop pk failed',
                                           'elapsed': secs_since(start_time)},
                                          log_dict))
                raise

        # Add drop null statement.
        stmts.clear()
        drop_stmt = Statement(DROP_NULL_ERA_SQL.format(era_type))
        stmts.add(drop_stmt)

        # Check for any errors and raise exception if they are found.
        for stmt in stmts:
            try:
                stmt.execute(conn_str)
                check_stmt_err(stmt, logger_msg.format('Run', era_type))
            except:
                logger.error(combine_dicts({'msg': 'Fatal error',
                                            'sql': stmt.sql,
                                            'err': str(stmt.err)}, log_dict))
                logger.info(combine_dicts({'msg': 'drop null failed',
                                           'elapsed': secs_since(start_time)},
                                          log_dict))
                raise

        # Truncate table
        trunc_stmt = Statement(TRUNCATE_ERA_SQL.format(schema))
        stmts.add(trunc_stmt)

        # Check for any errors and raise exception if they are found.
        for stmt in stmts:
            try:
                stmt.execute(conn_str)
                check_stmt_err(stmt, logger_msg.format('Run', era_type))
            except:
                logger.error(combine_dicts({'msg': 'Fatal error',
                                            'sql': stmt.sql,
                                            'err': str(stmt.err)}, log_dict))
                logger.info(combine_dicts({'msg': 'truncate failed',
                                           'elapsed': secs_since(start_time)},
                                          log_dict))
                raise

        # Run the derivation query
        logger.info({'msg': 'run {0} era derivation R Script'.format(era_type)})
        run_query_msg = "running {0} era derivation R Script"

        source_path = os.path.join(os.sep, 'app', package)
        dest_path = os.path.join(source_path, site)
        # delete any old versions
        if os.path.isdir(dest_path):
            shutil.rmtree(dest_path)
        # copy base files to site specific
        shutil.copytree(source_path, dest_path)
        # create the Argos congig file
        _create_argos_file(dest_path, config_file, schema, password, conn_info_dict)
        # modify site_info and run.R to add actual site
        _fix_site_info(dest_path, site, schema)
        _fix_run(dest_path, site)
        size = int(size)
        _fix_size(dest_path, size)

        query_path = os.path.join(os.sep, 'app', package, site, 'site', 'run.R')
        # Run R script
        Rscript(query_path, '--verbose=1', _cwd='/app', _fg=True)
        logger.info(combine_dicts({'msg': 'finished R Script',
                                  'elapsed': secs_since(start_time)}, log_dict))

    # add ids
    if not noids:
        okay = _add_era_ids(era_type, conn_str, site, neg_ids, search_path, model_version, id_name)
        if not okay:
            return False

    # Copy to the dcc_pedsnet table
    if copy:
        logger.info({'msg': 'copy {0}_era to dcc_pedsnet'.format(era_type)})
        okay = _copy_to_dcc_table(conn_str, era_type, schema)
        if not okay:
            return False
        logger.info({'msg': '{0}_era copied to dcc_pedsnet'.format(era_type)})

    # Add primary keys
    if not nopk:
        _add_primary_key(era_type, conn_str, schema)

    # Vacuum analyze tables for piney freshness.
    if not novac:
        logger.info({'msg': 'begin vacuum'})
        vacuum(conn_str, model_version, analyze=True, tables=['drug_era'])
        logger.info({'msg': 'vacuum finished'})

        # Log end of function.
        logger.info(combine_dicts({'msg': logger_msg.format("finished",era_type),
                                   'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True


def _add_primary_key(era_type, conn_str, schema):
    # Add primary keys
    pk_era_id_sql = "alter table {0}.{1}_era add primary key ({2}_era_id)"
    pk_era_id_msg = "making {0}_era_id the priomary key"
    temp_era_type = era_type

    # Make era Id the primary key
    logger.info({'msg': 'begin add primary key'})
    pk_era_id_stmt = Statement(pk_era_id_sql.format(schema, era_type, temp_era_type),
                               pk_era_id_msg.format(temp_era_type))

    # Execute the make era Id the primary key statement and ensure it didn't error
    pk_era_id_stmt.execute(conn_str)
    check_stmt_err(pk_era_id_stmt, 'make {0}_era_id the primary key'.format(era_type))
    logger.info({'msg': 'primary key created'})

    # If reached without error, then success!
    return True


def _add_era_ids(era_type, conn_str, site, neg_ids, search_path, model_version, id_name):
    """Add ids for the era table

    * Find how many ids needed
    * Update dcc_id with new value
    * Create sequence
    * Set sequence starting number
    * Assign era ids
    * Make era Id the primary key

    :param str era_type:      type of era derivation (condition or drug)
    :param str conn_str:      database connection string
    :param str site:    site to run derivation for
    :param bool neg_ids:    use negative ids
    :param str search_path: PostgreSQL schema search path
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str id_name: name of the id (ex. dcc or onco)
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    new_id_count_sql = """SELECT COUNT(*)
        FROM {0}_era WHERE {1}_era_id IS NULL"""
    new_id_count_msg = "counting new IDs needed for {0}_era"
    lock_last_id_sql = """LOCK {last_id_table_name}"""
    lock_last_id_msg = "locking {table_name} last ID tracking table for update"

    update_last_id_sql = """UPDATE {last_id_table_name} AS new
        SET last_id = new.last_id + '{new_id_count}'::integer
        FROM {last_id_table_name} AS old RETURNING old.last_id, new.last_id"""
    update_last_id_msg = "updating {table_name} last ID tracking table to reserve new IDs"  # noqa
    create_seq_sql = "create sequence if not exists {0}.{1}_{2}_era_id_seq"
    create_neg_seq_sql = """create sequence if not exists {0}.{1}_{2}_era_id_seq
        INCREMENT 1 START -2147483647 MINVALUE -2147483647 MAXVALUE 0"""
    create_seq_msg = "creating {0} era id sequence"
    set_seq_number_sql = "alter sequence {0}.{1}_{2}_era_id_seq restart with {3};"
    set_seq_number_msg = "setting sequence number"
    add_era_ids_sql = """update {0}.{1}_era set {3}_era_id = nextval('{0}.{2}_{1}_era_id_seq')
        where {3}_era_id is null"""
    add_era_ids_msg = "adding the era ids to the {0}_era table"

    conn_info_dict = get_conn_info_dict(conn_str)

    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'site': site, },
                             conn_info_dict)

    logger = logging.getLogger(__name__)

    logger.info(combine_dicts({'msg': 'starting id assignment'},
                              log_dict))
    start_time = time.time()
    schema = primary_schema(search_path)
    table_name = era_type + "_era"
    temp_table_name = table_name
    temp_era_type = era_type

    # Mapping and last ID table naming conventions.
    last_id_table_name_tmpl = id_name + "_{table_name}_id"
    metadata = stock_metadata(model_version)

    # Get table object and start to build tpl_vars map, which will be
    # used throughout for formatting SQL statements.
    table = metadata.tables[temp_table_name]
    tpl_vars = {'table_name': temp_table_name}
    tpl_vars['last_id_table_name'] = last_id_table_name_tmpl.format(**tpl_vars)

    # Build the statement to count how many new ID mappings are needed.
    new_id_count_stmt = Statement(new_id_count_sql.format(era_type, temp_era_type), new_id_count_msg.format(era_type))

    # Execute the new ID mapping count statement and ensure it didn't
    # error and did return a result.
    new_id_count_stmt.execute(conn_str)
    check_stmt_err(new_id_count_stmt, 'assign ids')
    check_stmt_data(new_id_count_stmt, 'assign ids')

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
        check_stmt_err(stmt, 'assign ids')
    check_stmt_data(update_last_id_stmts[1],
                    'assign ids')

    # Get the old and new last IDs from the second update statement.
    tpl_vars['old_last_id'] = update_last_id_stmts[1].data[0][0]
    tpl_vars['new_last_id'] = update_last_id_stmts[1].data[0][1]
    logger.info({'msg': 'last ID tracking table updated',
                 'table': temp_table_name,
                 'old_last_id': tpl_vars['old_last_id'],
                 'new_last_id': tpl_vars['new_last_id']})

    logger.info({'msg': 'begin id sequence creation'})

    # Create the id sequence (if it doesn't exist)
    if neg_ids:
        era_seq_stmt = Statement(create_neg_seq_sql.format(schema, site, era_type),
                                 create_seq_msg.format(era_type))
    else:
        era_seq_stmt = Statement(create_seq_sql.format(schema, site, era_type),
                                 create_seq_msg.format(era_type))

    # Execute the create the era id sequence statement and ensure it didn't error
    era_seq_stmt.execute(conn_str)
    check_stmt_err(era_seq_stmt, 'create {0} era id sequence'.format(era_type))
    logger.info({'msg': 'sequence creation complete'})

    # Set the sequence number
    logger.info({'msg': 'begin set sequence number'})
    seq_number_set_stmt = Statement(set_seq_number_sql.format(schema, site, era_type, tpl_vars['old_last_id']),
                                    set_seq_number_msg)

    # Execute the set the sequence number statement and ensure it didn't error
    seq_number_set_stmt.execute(conn_str)
    check_stmt_err(seq_number_set_stmt, 'set the sequence number')
    logger.info({'msg': 'set sequence number complete'})

    # Add the era ids
    logger.info({'msg': 'begin adding ids'})
    add_era_ids_stmt = Statement(add_era_ids_sql.format(schema, era_type, site, temp_era_type),
                                         add_era_ids_msg.format(era_type))

    # Execute the add the era ids statement and ensure it didn't error
    add_era_ids_stmt.execute(conn_str)
    check_stmt_err(add_era_ids_stmt, 'add the {0} era ids'.format(era_type))
    logger.info({'msg': 'add {0} era ids complete'.format(era_type)})

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished adding {0} era ids for the {0}_era table'.format(era_type),
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True


def copy_era_dcc(era_type, conn_str, site, search_path):
    """Run the Condition or Drug Era copy.

    * Copy to dcc_pedsnet

    :param str era_type:    type of derivation (condition or drug)
    :param str conn_str:      database connection string
    :param str site:    site to run derivation for
    :param str search_path: PostgreSQL schema search path
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    conn_info_dict = get_conn_info_dict(conn_str)

    # Log start of the function and set the starting time.
    logger_msg = '{0} {1} era entries'
    log_dict = combine_dicts({'site': site, },
                             conn_info_dict)
    logger.info(combine_dicts({'msg': logger_msg.format("start copying",era_type)},
                              log_dict))
    start_time = time.time()
    schema = primary_schema(search_path)

    # Copy to the dcc_pedsnet table
    logger.info({'msg': 'copy {0}_era to dcc_pedsnet'.format(era_type)})
    okay = _copy_to_dcc_table(conn_str, era_type, schema)
    if not okay:
        return False
    logger.info({'msg': '{0}_era copied to dcc_pedsnet'.format(era_type)})

    # Log end of function.
    logger.info(combine_dicts({'msg': logger_msg.format("finished copying",era_type),
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True
