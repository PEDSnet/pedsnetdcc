import logging
import time
import shutil
import os
import re
import hashlib

from pedsnetdcc.db import StatementSet, Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, check_stmt_data, combine_dicts,
                              get_conn_info_dict, vacuum, stock_metadata)
from sh import Rscript

logger = logging.getLogger(__name__)
NAME_LIMIT = 30
IDX_OBS_LIKE_TABLE_SQL = 'create index {0} on {1}.observation_derivation_covid ({2})'
FK_OBS_LIKE_TABLE_SQL = 'alter table {0}.observation_derivation_covid add constraint {1} foreign key ({2}) references {3}({4})'
GRANT_OBS_LIKE_TABLE_SQL = 'grant select on table {0}.observation_derivation_covid to {1}'
DROP_PK_CONSTRAINT_SQL = """alter table {0}.observation_derivation_covid drop constraint if exists xpk_observation_derivation_covid;
    alter table {0}.observation_derivation_covid drop constraint if exists observation_derivation_covid_pkey;"""
DROP_NULL_SQL = 'alter table {0}.observation_derivation_covid alter column observation_id drop not null;'


def _fill_concept_names(conn_str, schema):
    fill_concept_names_sql = """UPDATE {0}.observation_derivation_covid zs
        SET observation_concept_name=v.observation_concept_name,
        observation_source_concept_name=v.observation_source_concept_name, 
        observation_type_concept_name=v.observation_type_concept_name, 
        qualifier_concept_name=v.qualifier_concept_name, 
        unit_concept_name=v.unit_concept_name, 
        value_as_concept_name=v.value_as_concept_name
        FROM ( SELECT
        z.observation_id AS observation_id,
        v1.concept_name AS observation_concept_name, 
        v2.concept_name AS observation_source_concept_name, 
        v3.concept_name AS observation_type_concept_name, 
        v4.concept_name AS qualifier_concept_name,  
        v5.concept_name AS unit_concept_name,
        v6.concept_name AS value_as_concept_name
        FROM {0}.observation_derivation_covid AS z
        LEFT JOIN vocabulary.concept AS v1 ON z.observation_concept_id = v1.concept_id
        LEFT JOIN vocabulary.concept AS v2 ON z.observation_source_concept_id = v2.concept_id 
        LEFT JOIN vocabulary.concept AS v3 ON z.observation_type_concept_id = v3.concept_id
        LEFT JOIN vocabulary.concept AS v4 ON z.qualifier_concept_id  = v4.concept_id
        LEFT JOIN vocabulary.concept AS v5 ON z.unit_concept_id  = v5.concept_id
        LEFT JOIN vocabulary.concept AS v6 ON z.value_as_concept_id  = v6.concept_id
        ) v
        WHERE zs.observation_id = v.observation_id"""

    fill_concept_names_msg = "adding concept names"

    # Add concept names
    add_observation_concept_stmt = Statement(fill_concept_names_sql.format(schema), fill_concept_names_msg)

    # Execute the add concept names statement and ensure it didn't error
    add_observation_concept_stmt.execute(conn_str)
    check_stmt_err(add_observation_concept_stmt, 'add concept names')

    # If reached without error, then success!
    return True


def _fill_age_in_months(conn_str, schema):
    fill_add_age_in_months_sql = """
    create or replace function {0}.last_month_of_interval(timestamp, timestamp)
         returns timestamp strict immutable language sql as $$
           select $1 + interval '1 year' * extract(years from age($2, $1)) + interval '1 month' * extract(months from age($2, $1))
    $$;
    comment on function {0}.last_month_of_interval(timestamp, timestamp) is
    'Return the timestamp of the last month of the interval between two timestamps';
    
    create or replace function {0}.month_after_last_month_of_interval(timestamp, timestamp)
         returns timestamp strict immutable language sql as $$
           select $1 + interval '1 year' * extract(years from age($2, $1)) + interval '1 month' * (extract(months from age($2, $1)) + 1)
    $$;
    comment on function {0}.month_after_last_month_of_interval(timestamp, timestamp) is
    'Return the timestamp of the month AFTER the last month of the interval between two timestamps';
    
    create or replace function {0}.days_in_last_month_of_interval(timestamp, timestamp)
         returns double precision strict immutable language sql as $$
           select extract(days from {0}.month_after_last_month_of_interval($1, $2) - {0}.last_month_of_interval($1, $2))
    $$;
    comment on function {0}.days_in_last_month_of_interval(timestamp, timestamp) is
    'Return the number of days in the last month of the interval between two timestamps';
    
    create or replace function {0}.months_in_interval(timestamp, timestamp)
     returns double precision strict immutable language sql as $$
      select extract(years from age($2, $1)) * 12 + extract(months from age($2, $1)) + extract(days from age($2, $1))/{0}.days_in_last_month_of_interval($1, $2)
    $$;
    comment on function {0}.months_in_interval(timestamp, timestamp) is
       'Return the number of months (double precision) between two timestamps.
        The number of years/months/days is computed by PostgreSQL''s
        extract/date_part function.  The fractional months value is
        computed by dividing the extracted number of days by the total
        number of days in the last month overlapping the interval; note
        that this is not a calendar month but, say, the number of days
        between Feb 2, 2001 and Mar 2, 2001.  You should be able to obtain
        the original timestamp from the resulting value, albeit with great
        difficulty.';
        
    update {0}.observation_derivation_covid od
    set observation_age_in_months=subquery.observation_age_in_months
    from (select observation_id, 
        {0}.months_in_interval(p.birth_datetime, o.observation_datetime::timestamp without time zone) as observation_age_in_months
        from {0}.observation_derivation_covid o
        join {0}.person p on p.person_id = o.person_id) AS subquery
    where od.observation_id=subquery.observation_id;"""

    fill_add_age_in_months_msg = "adding age in months"

    # Add age in months
    add_age_in_months_stmt = Statement(fill_add_age_in_months_sql.format(schema), fill_add_age_in_months_msg)

    # Execute the add concept names statement and ensure it didn't error
    add_age_in_months_stmt.execute(conn_str)
    check_stmt_err(add_age_in_months_stmt, 'add age in months')

    # If reached without error, then success!
    return True


def _copy_to_obs_table(conn_str, schema):
    copy_to_sql = """INSERT INTO {0}.observation(
            observation_concept_id, observation_date, observation_datetime, 
            observation_source_concept_id, observation_source_value, 
            observation_type_concept_id, qualifier_concept_id, qualifier_source_value, 
            unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, 
            value_as_string, observation_age_in_months, observation_concept_name, 
            observation_source_concept_name, observation_type_concept_name, 
            qualifier_concept_name, unit_concept_name, value_as_concept_name, site, 
            observation_id, site_id, provider_id, visit_occurrence_id, person_id
        )
        (select observation_concept_id, observation_date, observation_datetime, 
            observation_source_concept_id, observation_source_value, 
            observation_type_concept_id, qualifier_concept_id, qualifier_source_value, 
            unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, 
            value_as_string, observation_age_in_months, observation_concept_name, 
            observation_source_concept_name, observation_type_concept_name, 
            qualifier_concept_name, unit_concept_name, value_as_concept_name, site, 
            observation_id, site_id, provider_id, visit_occurrence_id, person_id
        from {0}.observation_derivation_covid) ON CONFLICT DO NOTHING"""

    copy_to_msg = "copying {0} to observation"

    # Insert observations into observation table
    copy_to_stmt = Statement(copy_to_sql.format(schema), copy_to_msg)

    # Execute the insert observations statement and ensure it didn't error
    copy_to_stmt.execute(conn_str)
    check_stmt_err(copy_to_stmt, 'insert observations')

    # If reached without error, then success!
    return True


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


def _make_index_name(column_name):
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
    table_name = 'covid_derivation'
    table_abbrev = "obs_" + table_name[:3]
    column_abbrev = ''.join([x[0] for x in column_name.split('_')])
    md5 = hashlib.md5(
        '{}.{}'.format(table_name, column_name).encode('utf-8')). \
        hexdigest()
    hashlen = NAME_LIMIT - (len(table_abbrev) + len(column_abbrev) +
                            3 * len('_') + len('ix'))
    return '_'.join([table_abbrev, column_abbrev, md5[:hashlen], 'ix'])


def run_r_obs_covid(conn_str, site, password, search_path, model_version, id_name, copy,
                    limit=False, owner='loading_user'):
    """Run an R script.

    * Create argos file
    * Run R Script

    :param str conn_str:      database connection string
    :param str site:    site to run script for
    :param str password:    user's password
    :param str search_path: PostgreSQL schema search path
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str id_name: name of the id (ex. dcc or onco)
    :param bool copy: if True, copy results to output directory
    :param bool limit: if True, limit permissions to owner
    :param str owner:  owner of the to grant permissions to
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    package = 'covid19_observation_derivations'
    config_file = site + "_" + package + "_argos_temp.json";
    conn_info_dict = get_conn_info_dict(conn_str)

    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'site': site, },
                             conn_info_dict)
    logger.info(combine_dicts({'msg': 'starting R Script'},
                              log_dict))
    start_time = time.time()
    logger_msg = 'covid19_observation_derivations'
    schema = primary_schema(search_path)

    if password == None:
        pass_match = re.search(r"password=(\S*)", conn_str)
        password = pass_match.group(1)

    source_path = os.path.join(os.sep,'app', package)
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

    query_path = os.path.join(os.sep,'app', package, site, 'site', 'run.R')
    # Run R script
    Rscript(query_path, '--verbose=1', _cwd='/app', _fg=True)

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished R Script',
                               'elapsed': secs_since(start_time)}, log_dict))

    # Drop primary key.
    stmts = StatementSet()
    drop_pk_stmt = Statement(DROP_PK_CONSTRAINT_SQL.format(schema))
    stmts.add(drop_pk_stmt)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            stmt.execute(conn_str)
            check_stmt_err(stmt, logger_msg.format('Run'))
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
    drop_stmt = Statement(DROP_NULL_SQL.format(schema))
    stmts.add(drop_stmt)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            stmt.execute(conn_str)
            check_stmt_err(stmt, logger_msg.format('Run'))
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'drop null failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise

    # clear "fake" ids added by R query
    okay = _clear_fake_ids(conn_str, schema)
    if not okay:
        return False

    # add observation_ids
    okay = _add_observation_ids(conn_str, site, search_path, model_version, id_name)
    if not okay:
        return False

    # Add the concept_names
    logger.info({'msg': 'add concept names'})
    okay = _fill_concept_names(conn_str, schema)
    if not okay:
        return False
    logger.info({'msg': 'concept names added'})

    # Add age in months
    logger.info({'msg': 'add age in months'})
    okay = _fill_age_in_months(conn_str, schema)
    if not okay:
        return False
    logger.info({'msg': 'age in months added'})

    # Add indexes (same as observation)
    stmts.clear()
    logger.info({'msg': 'adding indexes'})
    col_index = ('observation_concept_id', 'observation_date', 'person_id',
                 'visit_occurrence_id', 'observation_age_in_months', 'site',)

    for col in col_index:
        idx_name = _make_index_name(col)
        idx_stmt = Statement(IDX_OBS_LIKE_TABLE_SQL.format(idx_name, schema, col))
        stmts.add(idx_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str, 5)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Covid Observation Derivation')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding indexes failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'indexes added'})

    # Add foreign keys (same as observation)
    stmts.clear()
    logger.info({'msg': 'adding foreign keys'})
    col_fk = ('observation_concept_id', 'person_id', 'provider_id',
              'observation_source_concept_id', 'qualifier_concept_id',
              'observation_type_concept_id', 'unit_concept_id', 'value_as_concept_id',
              'visit_occurrence_id',)

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
        fk_name = "fk_obs_" + base_name + "_covid"
        fk_stmt = Statement(FK_OBS_LIKE_TABLE_SQL.format(schema,
                                                             fk_name, fk, ref_table,
                                                             ref_col))
        stmts.add(fk_stmt)

    # Execute the statements in parallel.
    stmts.parallel_execute(conn_str, 5)

    # Execute statements and check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            check_stmt_err(stmt, 'Covid Observation Derivation')
        except:
            logger.error(combine_dicts({'msg': 'Fatal error',
                                        'sql': stmt.sql,
                                        'err': str(stmt.err)}, log_dict))
            logger.info(combine_dicts({'msg': 'adding foreign keys failed',
                                       'elapsed': secs_since(start_time)},
                                      log_dict))
            raise
    logger.info({'msg': 'foreign keys added'})

    # Copy to the observation table
    if copy:
        logger.info({'msg': 'copy to observation'})
        okay = _copy_to_obs_table(conn_str, schema)
        if not okay:
            return False
        logger.info({'msg': 'copied to observation'})

    # Set permissions
    stmts.clear()
    logger.info({'msg': 'setting permissions'})
    if limit:
        users = (owner,)
    else:
        users = ('peds_staff', 'dcc_analytics')

    for usr in users:
        grant_stmt = Statement(GRANT_OBS_LIKE_TABLE_SQL.format(schema, usr))
        stmts.add(grant_stmt)

    # Check for any errors and raise exception if they are found.
    for stmt in stmts:
        try:
            stmt.execute(conn_str)
            check_stmt_err(stmt, 'Covid Observation Derivation')
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
    logger.info({'msg': 'begin vacuum'})
    vacuum(conn_str, model_version, analyze=True, tables=['observation_derivation_covid'])
    logger.info({'msg': 'vacuum finished'})

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished Covid Observation Derivation',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True


def _clear_fake_ids(conn_str, schema):
    # Clear "fake" ids
    fake_id_sql = "update {0}.observation_derivation_covid set observation_id = NULL"
    fake_id_msg = "clearing 'fake' ids"

    # Clear Ids
    logger.info({'msg': 'begin clearing "fake" ids'})
    fake_id_stmt = Statement(fake_id_sql.format(schema),
                               fake_id_msg)

    # Execute the clear "fake" ids statement and ensure it didn't error
    fake_id_stmt.execute(conn_str)
    check_stmt_err(fake_id_stmt, 'clear "fake" ids')
    logger.info({'msg': '"fake ids cleared'})

    # If reached without error, then success!
    return True


def _add_observation_ids(conn_str, site, search_path, model_version, id_name):
    """Add observation ids for the derivation table

    * Find how many ids needed
    * Update observation_id with new value
    * Create sequence
    * Set sequence starting number
    * Assign observation ids
    * Make  observation_id the primary key

    :param str conn_str:      database connection string
    :param str site:    site for derivation
    :param str search_path: PostgreSQL schema search path
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :param str id_name: name of the id (ex. dcc or onco)
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    new_id_count_sql = """SELECT COUNT(*)
        FROM {0}.observation_derivation_covid WHERE observation_id IS NULL"""
    new_id_count_msg = "counting new IDs needed for observation_derivation_covid"
    lock_last_id_sql = """LOCK {last_id_table_name}"""
    lock_last_id_msg = "locking {table_name} last ID tracking table for update"

    update_last_id_sql = """UPDATE {last_id_table_name} AS new
        SET last_id = new.last_id + '{new_id_count}'::bigint
        FROM {last_id_table_name} AS old RETURNING old.last_id, new.last_id"""
    update_last_id_msg = "updating {table_name} last ID tracking table to reserve new IDs"  # noqa
    create_seq_observation_sql = "create sequence if not exists {0}.{1}_observation_derivation_covid_seq"
    create_seq_observation_msg = "creating observation_derivation_covid sequence"
    set_seq_number_sql = "alter sequence {0}.{1}_observation_derivation_covid_seq restart with {2};"
    set_seq_number_msg = "setting sequence number"
    add_observation_ids_sql = """update {0}.observation_derivation_covid set observation_id = nextval('{0}.{1}_observation_derivation_covid_seq')
        where observation_id is null"""
    add_observation_ids_msg = "adding the observation ids to the observation_derivation_covid table"
    pk_observation_id_sql = "alter table {0}.observation_derivation_covid add primary key (observation_id)"
    pk_observation_id_msg = "making observation_id the primary key"

    conn_info_dict = get_conn_info_dict(conn_str)

    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'site': site, },
                             conn_info_dict)

    logger = logging.getLogger(__name__)

    logger.info(combine_dicts({'msg': 'starting observation_id assignment'},
                              log_dict))
    start_time = time.time()
    schema = primary_schema(search_path)
    table_name = 'observation'

    # Mapping and last ID table naming conventions.
    last_id_table_name_tmpl = id_name + "_{table_name}_id"
    metadata = stock_metadata(model_version)

    # Get table object and start to build tpl_vars map, which will be
    # used throughout for formatting SQL statements.
    table = metadata.tables[table_name]
    tpl_vars = {'table_name': table_name}
    tpl_vars['last_id_table_name'] = last_id_table_name_tmpl.format(**tpl_vars)

    # Build the statement to count how many new ID mappings are needed.
    new_id_count_stmt = Statement(new_id_count_sql.format(schema), new_id_count_msg)

    # Execute the new ID mapping count statement and ensure it didn't
    # error and did return a result.
    new_id_count_stmt.execute(conn_str)
    check_stmt_err(new_id_count_stmt, 'assign observation ids')
    check_stmt_data(new_id_count_stmt, 'assign observation ids')

    # Get the actual count of new ID maps needed and log it.
    tpl_vars['new_id_count'] = new_id_count_stmt.data[0][0] + 1
    logger.info({'msg': 'counted new IDs needed', 'table': 'observation_derivation_covid',
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
        check_stmt_err(stmt, 'assign observation ids')
    check_stmt_data(update_last_id_stmts[1],
                    'assign observation ids')

    # Get the old and new last IDs from the second update statement.
    tpl_vars['old_last_id'] = update_last_id_stmts[1].data[0][0]
    tpl_vars['new_last_id'] = update_last_id_stmts[1].data[0][1]
    logger.info({'msg': 'last ID tracking table updated',
                 'table': table_name,
                 'old_last_id': tpl_vars['old_last_id'],
                 'new_last_id': tpl_vars['new_last_id']})

    logger.info({'msg': 'begin observation id sequence creation'})
    # Create the observation id sequence (if it doesn't exist)
    observation_seq_stmt = Statement( create_seq_observation_sql.format(schema, site),
                                      create_seq_observation_msg)

    # Execute the create the observation id sequence statement and ensure it didn't error
    observation_seq_stmt.execute(conn_str)
    check_stmt_err(observation_seq_stmt, 'create observation id sequence')
    logger.info({'msg': 'observation id sequence creation complete'})

    # Set the sequence number
    logger.info({'msg': 'begin set sequence number'})
    seq_number_set_stmt = Statement(set_seq_number_sql.format(schema, site, (tpl_vars['old_last_id'] + 1)),
                                    set_seq_number_msg)

    # Execute the set the sequence number statement and ensure it didn't error
    seq_number_set_stmt.execute(conn_str)
    check_stmt_err(seq_number_set_stmt, 'set the sequence number')
    logger.info({'msg': 'set sequence number complete'})

    # Add the observation ids
    logger.info({'msg': 'begin add observation ids'})
    add_observation_ids_stmt = Statement(add_observation_ids_sql.format(schema, site),
                                         add_observation_ids_msg)

    # Execute the add the observation ids statement and ensure it didn't error
    add_observation_ids_stmt.execute(conn_str)
    check_stmt_err(add_observation_ids_stmt, 'add the observation ids')
    logger.info({'msg': 'add observation ids complete'})

    # Make observation Id the primary key
    logger.info({'msg': 'begin add primary key'})
    pk_observation_id_stmt = Statement(pk_observation_id_sql.format(schema),
                                         pk_observation_id_msg)

    # Execute the Make observation Id the primary key statement and ensure it didn't error
    pk_observation_id_stmt.execute(conn_str)
    check_stmt_err(pk_observation_id_stmt, 'make observation Id the primary key')
    logger.info({'msg': 'primary key created'})

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished adding observation ids',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True
