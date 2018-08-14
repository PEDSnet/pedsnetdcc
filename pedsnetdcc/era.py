import logging
import time
import re

from pedsnetdcc.db import StatementSet, Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, check_stmt_data, combine_dicts,
                              get_conn_info_dict, vacuum, stock_metadata)

logger = logging.getLogger(__name__)
DROP_PK_CONSTRAINT_ERA_SQL = """alter table {0}_era drop constraint if exists xpk_{0}_era;
    alter table {0}_era drop constraint if exists {0}_era_pkey;"""
DROP_NULL_ERA_SQL = 'alter table {0}_era alter column {0}_era_id drop not null;'
IDX_ERA_SQL = 'create index {0} on {1}_era ({2})'
CONDITION_ERA_SQL= """TRUNCATE {0}.condition_era;
    DROP TABLE IF EXISTS {1}_cteConditionTarget;
    -- create base eras from the concepts found in condition_occurrence
    CREATE TEMP TABLE {1}_cteConditionTarget
    AS
    SELECT
        co.person_id
        ,co.condition_concept_id
        ,co.condition_start_date
        ,COALESCE(co.condition_end_date, (condition_start_date + 1*INTERVAL'1 day')) AS condition_end_date
    FROM
    {0}.coddition_occurrence co;
    --------------------------------------
    DROP TABLE IF EXISTS {1}_cteCondEndDates;
    CREATE TEMP TABLE {1}_cteCondEndDates
    AS
    SELECT
        person_id
        ,condition_concept_id
        ,(event_date+ - 30*INTERVAL'1 day') AS end_date -- unpad the end date
    FROM
    (
        SELECT e1.person_id
            ,e1.condition_concept_id
            ,e1.event_date
            ,COALESCE(e1.start_ordinal, MAX(e2.start_ordinal)) start_ordinal
            ,e1.overall_ord
        FROM (
            SELECT person_id
                ,condition_concept_id
                ,event_date
                ,event_type
                ,start_ordinal
                ,ROW_NUMBER() OVER (
                    PARTITION BY person_id
                    ,condition_concept_id ORDER BY event_date
                        ,event_type
                    ) AS overall_ord -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
            FROM (
                -- select the start dates, assigning a row number to each
                SELECT person_id
                    ,condition_concept_id
                    ,condition_start_date AS event_date
                    ,- 1 AS event_type
                    ,ROW_NUMBER() OVER (
                        PARTITION BY person_id
                        ,condition_concept_id ORDER BY condition_start_date
                        ) AS start_ordinal
                FROM {1}_cteConditionTarget
                UNION ALL
                -- pad the end dates by 30 to allow a grace period for overlapping ranges.
                SELECT person_id
                    ,condition_concept_id
                    ,(condition_end_date + 30*INTERVAL'1 day')
                    ,1 AS event_type
                    ,NULL
                FROM {1}_cteConditionTarget
                ) RAWDATA
            ) e1
        INNER JOIN (
            SELECT person_id
                ,condition_concept_id
                ,condition_start_date AS event_date
                ,ROW_NUMBER() OVER (
                    PARTITION BY person_id
                    ,condition_concept_id ORDER BY condition_start_date
                    ) AS start_ordinal
            FROM {1}_cteConditionTarget
            ) e2 ON e1.person_id = e2.person_id
            AND e1.condition_concept_id = e2.condition_concept_id
            AND e2.event_date<= e1.event_date
        GROUP BY e1.person_id
            ,e1.condition_concept_id
            ,e1.event_date
            ,e1.start_ordinal
            ,e1.overall_ord
        ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0;
    ------------------------------------------------
    DROP TABLE IF EXISTS {1}_cteConditionEnds;
    CREATE TEMP TABLE {1}_cteConditionEnds
    AS
    SELECT
        c.person_id
        ,c.condition_concept_id
        ,c.condition_start_date
        ,MIN(e.end_date) AS era_end_date
    FROM
    {1}_cteConditionTarget c
    INNER JOIN {1}_cteCondEndDates e ON c.person_id = e.person_id
        AND c.condition_concept_id = e.condition_concept_id
        AND e.end_date >= c.condition_start_date
    GROUP BY c.person_id
        ,c.condition_concept_id
        ,c.condition_start_date;
    -------------------------------
    INSERT INTO {0}.condition_era (
        condition_era_id
        ,person_id
        ,condition_concept_id
        ,condition_era_start_date
        ,condition_era_end_date
        ,condition_occurrence_count
        )
    SELECT row_number() OVER (
            ORDER BY person_id
            ) AS condition_era_id
        ,person_id
        ,condition_concept_id
        ,min(condition_start_date) AS condition_era_start_date
        ,era_end_date AS CONDITION_era_end_date
        ,COUNT(*) AS coddition_occurrence_COUNT
    FROM {1}_cteConditionEnds
    GROUP BY person_id
        ,condition_concept_id
        ,era_end_date;
"""
DRUG_ERA_SQL = """TRUNCATE {0}.drug_era;
    DROP TABLE IF EXISTS {2}_cteDrugTarget;
    -- Normalize drug_exposure_end_date to either the existing drug exposure end date, or add days supply, or add 1 day to the start date
    CREATE TEMP TABLE {2}_cteDrugTarget
    AS
    SELECT
        d.drug_exposure_id
        ,d.person_od
        ,c.concept_id
        ,d.drug_type_concept_id
        ,drug_exposure_start_date
        ,COALESCE(drug_exposure_end_date, (drug_exposure_start_date + days_supply*INTERVAL'1 day'), (drug_exposure_start_date + 1*INTERVAL'1 day')) AS drug_exposure_end_date
        ,c.concept_id AS ingredient_concept_id
    FROM
    {0}.drug_exposure d
    INNER JOIN {1}.concept_ancestor ca ON ca.descendant_concept_id = d.drug_concept_id
    INNER JOIN {1}.concept c ON ca.ancestor_concept_id = c.concept_id
    WHERE c.vocabulary_id = 'RxNorm'
        AND c.concept_class_id = 'Ingredient';
    ------------------------------------																								 
    DROP TABLE IF EXISTS {2}_cteEndDates;
    CREATE TEMP TABLE {2}_cteEndDates
    AS
    SELECT
        person_id
        ,ingredient_concept_id
        ,(event_date + - 30*INTERVAL'1 day') AS end_date -- unpad the end date
    FROM
    (
        SELECT e1.person_id
            ,e1.ingredient_concept_id
            ,e1.event_date
            ,COALESCE(e1.start_ordinal, MAX(e2.start_ordinal)) start_ordinal
            ,e1.overall_ord
        FROM (
            SELECT person_id
                ,ingredient_concept_id
                ,event_date
                ,event_type
                ,start_ordinal
                ,ROW_NUMBER() OVER (
                    PARTITION BY person_id
                    ,ingredient_concept_id ORDER BY event_date
                        ,event_type
                    ) AS overall_ord -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
            FROM (
                -- select the start dates, assigning a row number to each
                SELECT person_id
                    ,ingredient_concept_id
                    ,drug_exposure_start_date AS event_date
                    ,0 AS event_type
                    ,ROW_NUMBER() OVER (
                        PARTITION BY person_id
                        ,ingredient_concept_id ORDER BY drug_exposure_start_date
                        ) AS start_ordinal
                FROM {2}_cteDrugTarget
                UNION ALL
                -- add the end dates with NULL as the row number, padding the end dates by 30 to allow a grace period for overlapping ranges.
                SELECT person_id
                    ,ingredient_concept_id
                    ,(drug_exposure_end_date + 30*INTERVAL'1 day')
                    ,1 AS event_type
                    ,NULL
                FROM {2}_cteDrugTarget
                ) RAWDATA
            ) e1
        INNER JOIN (
            SELECT person_id
                ,ingredient_concept_id
                ,drug_exposure_start_date AS event_date
                ,ROW_NUMBER() OVER (
                    PARTITION BY person_id
                    ,ingredient_concept_id ORDER BY drug_exposure_start_date
                    ) AS start_ordinal
            FROM {2}_cteDrugTarget
            ) e2 ON e1.person_id = e2.person_id
            AND e1.ingredient_concept_id = e2.ingredient_concept_id
            AND e2.event_date <= e1.event_date
        GROUP BY e1.person_id
            ,e1.ingredient_concept_id
            ,e1.event_date
            ,e1.start_ordinal
            ,e1.overall_ord
        ) e
    WHERE 2 * e.start_ordinal - e.overall_ord = 0;
    ----------------------------------------------
    DROP TABLE IF EXISTS {2}_cteDrugExpEnds;
    CREATE TEMP TABLE {2}_cteDrugExpEnds
    AS
    SELECT
        d.person_id
        ,d.ingredient_concept_id
        ,d.drug_type_concept_id
        ,d.drug_exposure_start_date
        ,MIN(e.end_date) AS era_end_date
    FROM
    {2}_cteDrugTarget d
    INNER JOIN {2}_cteEndDates e ON d.person_id = e.person_id
        AND d.ingredient_concept_id = e.ingredient_concept_id
        AND e.end_date >= d.drug_exposure_start_date
    GROUP BY d.person_id
        ,d.ingredient_concept_id
        ,d.drug_type_concept_id
        ,d.drug_exposure_start_date;
    ------------------------------------------				  
    INSERT INTO {0}.drug_era
    SELECT ROW_NUMBER() OVER (
            ORDER BY person_id
            ) AS drug_era_id
        ,person_id
        ,ingredient_concept_id
        ,min(drug_exposure_start_date) AS drug_era_start_date
        ,era_end_date
        ,COUNT(*) AS drug_exposure_count
        ,30 AS gap_days
    FROM {2}_cteDrugExpEnds
    GROUP BY person_id
        ,ingredient_concept_id
        ,drug_type_concept_id
        ,era_end_date;
    """


def _fill_concept_names(conn_str, era_type, site):
    fill_concept_names_sql = """UPDATE {0}_era era
        SET {0}_concept_name=v.{0}_concept_name,site='{1}'
        FROM ( SELECT
        e.{0}_era_id AS {0}_id,
        v1.concept_name AS {0}_concept_name
        FROM {0}_era AS e
        LEFT JOIN vocabulary.concept AS v1 ON e.{0}_concept_id = v1.concept_id
        ) v
        WHERE era.{0}_era_id = v.{0}_id"""

    fill_concept_names_msg = "adding concept names"

    # Add concept names
    add_era_ids_stmt = Statement(fill_concept_names_sql.format(era_type, site), fill_concept_names_msg)

    # Execute the add concept names statement and ensure it didn't error
    add_era_ids_stmt.execute(conn_str)
    check_stmt_err(add_era_ids_stmt, 'add concept names')


    # If reached without error, then success!
    return True


def _copy_to_dcc_table(conn_str, era_type, schema):
    copy_to_condition_sql = """INSERT INTO dcc_pedsnet.condition_era(
        condition_concept_id, condition_era_end_date, condition_era_start_date, 
        condition_occurrence_count, condition_concept_name, site, condition_era_id, 
        site_id, person_id)
        (select condition_concept_id, condition_era_end_date, condition_era_start_date, 
        condition_occurrence_count, condition_concept_name, site, condition_era_id, 
        site_id, person_id
        from {0}.condition_era) ON CONFLICT DO NOTHING"""
    copy_to_drug_sql = """INSERT INTO dcc_pedsnet.drug_era(
        drug_concept_id, drug_era_end_date, drug_era_start_date, drug_exposure_count, 
        gap_days, drug_concept_name, site, drug_era_id, site_id, person_id)
        (select drug_concept_id, drug_era_end_date, drug_era_start_date, drug_exposure_count, 
        gap_days, drug_concept_name, site, drug_era_id, site_id, person_id
        from {0}.drug_era) ON CONFLICT DO NOTHING"""

    copy_to_msg = "copying {0}_era to dcc_pedsnet"

    # Insert era data into dcc_pedsnet era table
    if era_type == "condition":
        copy_to_stmt = Statement(copy_to_condition_sql.format(schema), copy_to_msg.format(era_type))
    else:
        copy_to_stmt = Statement(copy_to_drug_sql.format(schema), copy_to_msg.format(era_type))

    # Execute the insert BMI measurements statement and ensure it didn't error
    copy_to_stmt.execute(conn_str)
    check_stmt_err(copy_to_stmt, 'insert {0}_era data'.format(era_type))

    # If reached without error, then success!
    return True


def run_era(era_type, conn_str, site, copy, search_path, model_version):
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
    :param str search_path: PostgreSQL schema search path
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    conn_info_dict = get_conn_info_dict(conn_str)

    # Log start of the function and set the starting time.
    logger_msg = '{0} {1} era calculation'
    log_dict = combine_dicts({'site': site, },
                             conn_info_dict)
    logger.info(combine_dicts({'msg': logger_msg.format("starting",era_type)},
                              log_dict))
    start_time = time.time()
    schema = primary_schema(search_path)

    # Drop primary key.
    stmts = StatementSet()
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

   # Run the derivation query
    logger.info({'msg': 'run {0} era derivation query'.format(era_type)})
    run_query_msg = "running {0} era derivation query"

    # run query
    stmts.clear()
    if era_type == "condition":
        era_query_stmt = Statement(CONDITION_ERA_SQL.format(schema, site), run_query_msg.format(era_type))
    else:
        era_query_stmt = Statement(DRUG_ERA_SQL.format(schema, "vocabulary", site),
                                       run_query_msg.format(era_type))

    # Execute the query and ensure it didn't error
    era_query_stmt.execute(conn_str)
    check_stmt_err(era_query_stmt, 'run {0} era derivation query'.format(era_type))
    logger.info({'msg': '{0} era derivation query complete'.format(era_type)})

    # add ids
    okay = _add_era_ids(era_type, conn_str, site, search_path, model_version)
    if not okay:
        return False

    # Add the concept_names
    logger.info({'msg': 'add concept names'})
    okay = _fill_concept_names(conn_str, era_type, site)
    if not okay:
        return False
    logger.info({'msg': 'concept names added'})

    # Copy to the dcc_pedsnet table
    if copy:
        logger.info({'msg': 'copy {0}_era to dcc_pedsnet'.format(era_type)})
        okay = _copy_to_dcc_table(conn_str, era_type, schema)
        if not okay:
            return False
        logger.info({'msg': '{0}_era copied to dcc_pedsnet'.format(era_type)})

    # Vacuum analyze tables for piney freshness.
    logger.info({'msg': 'begin vacuum'})
    vacuum(conn_str, model_version, analyze=True, tables=[era_type + "_era"])
    logger.info({'msg': 'vacuum finished'})

    # Log end of function.
    logger.info(combine_dicts({'msg': logger_msg.format("finished",era_type),
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True


def _add_era_ids(era_type, conn_str, site, search_path, model_version):
    """Add ids for the era table

    * Find how many ids needed
    * Update dcc_id with new value
    * Create sequence
    * Set sequence starting number
    * Assign measurement ids
    * Make measurement Id the primary key

    :param str era_type:      type of era derivation (condition or drug)
    :param str conn_str:      database connection string
    :param str site:    site to run derivation for
    :param str search_path: PostgreSQL schema search path
    :param str model_version: pedsnet model version, e.g. 2.3.0
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    new_id_count_sql = """SELECT COUNT(*)
        FROM {0}_era WHERE {0}_era_id IS NULL"""
    new_id_count_msg = "counting new IDs needed for {0}_era"
    lock_last_id_sql = """LOCK {last_id_table_name}"""
    lock_last_id_msg = "locking {table_name} last ID tracking table for update"

    update_last_id_sql = """UPDATE {last_id_table_name} AS new
        SET last_id = new.last_id + '{new_id_count}'::integer
        FROM {last_id_table_name} AS old RETURNING old.last_id, new.last_id"""
    update_last_id_msg = "updating {table_name} last ID tracking table to reserve new IDs"  # noqa
    create_seq_sql = "create sequence if not exists {0}.{1}_era_id_seq"
    create_seq_msg = "creating {0} era id sequence"
    set_seq_number_sql = "alter sequence {0}.{1}_era_id_seq restart with {2};"
    set_seq_number_msg = "setting sequence number"
    add_era_ids_sql = """update {0}.{1}_era set {1}_era_id = nextval('{0}.{1}_era_id_seq')
        where {1}_era_id is null"""
    add_era_ids_msg = "adding the measurement ids to the {0}_era table"
    pk_era_id_sql = "alter table {0}.{1}_era add primary key ({1}_era_id)"
    pk_era_id_msg = "making {0}_era_id the priomary key"

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

    # Mapping and last ID table naming conventions.
    last_id_table_name_tmpl = "dcc_{table_name}_id"
    metadata = stock_metadata(model_version)

    # Get table object and start to build tpl_vars map, which will be
    # used throughout for formatting SQL statements.
    table = metadata.tables[table_name]
    tpl_vars = {'table_name': table_name}
    tpl_vars['last_id_table_name'] = last_id_table_name_tmpl.format(**tpl_vars)

    # Build the statement to count how many new ID mappings are needed.
    new_id_count_stmt = Statement(new_id_count_sql.format(era_type), new_id_count_msg.format(era_type))

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
                 'table': table_name,
                 'old_last_id': tpl_vars['old_last_id'],
                 'new_last_id': tpl_vars['new_last_id']})

    logger.info({'msg': 'begin id sequence creation'})

    # Create the id sequence (if it doesn't exist)
    era_seq_stmt = Statement(create_seq_sql.format(schema, era_type),
                                      create_seq_msg.format(era_type))

    # Execute the create the era id sequence statement and ensure it didn't error
    era_seq_stmt.execute(conn_str)
    check_stmt_err(era_seq_stmt, 'create {0} era id sequence'.format(era_type))
    logger.info({'msg': 'sequence creation complete'})

    # Set the sequence number
    logger.info({'msg': 'begin set sequence number'})
    seq_number_set_stmt = Statement(set_seq_number_sql.format(schema, era_type, tpl_vars['old_last_id']),
                                    set_seq_number_msg)

    # Execute the set the sequence number statement and ensure it didn't error
    seq_number_set_stmt.execute(conn_str)
    check_stmt_err(seq_number_set_stmt, 'set the sequence number')
    logger.info({'msg': 'set sequence number complete'})

    # Add the measurement ids
    logger.info({'msg': 'begin adding ids'})
    add_era_ids_stmt = Statement(add_era_ids_sql.format(schema, era_type),
                                         add_era_ids_msg.format(era_type))

    # Execute the add the era ids statement and ensure it didn't error
    add_era_ids_stmt.execute(conn_str)
    check_stmt_err(add_era_ids_stmt, 'add the {0} era ids'.format(era_type))
    logger.info({'msg': 'add {0} era ids complete'.format(era_type)})

    # Make era Id the primary key
    logger.info({'msg': 'begin add primary key'})
    pk_era_id_stmt = Statement(pk_era_id_sql.format(schema, era_type),
                                         pk_era_id_msg.format(era_type))

    # Execute the make era Id the primary key statement and ensure it didn't error
    pk_era_id_stmt.execute(conn_str)
    check_stmt_err(pk_era_id_stmt, 'make {0}_era_id the primary key'.format(era_type))
    logger.info({'msg': 'primary key created'})

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished adding {0} era ids for the {0}_era table'.format(era_type),
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True
