import logging
import time
import re

from pedsnetdcc.db import StatementSet, Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, check_stmt_data, combine_dicts,
                              get_conn_info_dict, vacuum, stock_metadata)

logger = logging.getLogger(__name__)
DROP_NULL_ERA_SQL = 'alter table {0} alter column {0}_era_id drop not null;'
IDX_ERA_SQL = 'create index {0} on {1}_era ({2})'
CONDITION_ERA_SQL= """TRUNCATE {0}.condition_era;
    WITH cteConditionTarget (condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date) AS
    (
        SELECT
            co.condition_occurrence_id
            , co.person_id
            , co.condition_concept_id
            , co.condition_start_date
            , COALESCE(NULLIF(co.condition_end_date,NULL), condition_start_date + INTERVAL '1 day') AS condition_end_date
        FROM {0}.condition_occurrence co
        /* Depending on the needs of your data, you can put more filters on to your code. We assign 0 to our unmapped condition_concept_id's,
         * and since we don't want different conditions put in the same era, we put in the filter below.
         */
        ---WHERE condition_concept_id != 0
    ),
    --------------------------------------------------------------------------------------------------------------
    cteEndDates (person_id, condition_concept_id, end_date) AS -- the magic
    (
        SELECT
            person_id
            , condition_concept_id
            , event_date - INTERVAL '30 days' AS end_date -- unpad the end date
        FROM
        (
            SELECT
                person_id
                , condition_concept_id
                , event_date
                , event_type
                , MAX(start_ordinal) OVER (PARTITION BY person_id, condition_concept_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal -- this pulls the current START down from the prior rows so that the NULLs from the END DATES will contain a value we can compare with 
                , ROW_NUMBER() OVER (PARTITION BY person_id, condition_concept_id ORDER BY event_date, event_type) AS overall_ord -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
            FROM
            (
                -- select the start dates, assigning a row number to each
                SELECT
                    person_id
                    , condition_concept_id
                    , condition_start_date AS event_date
                    , -1 AS event_type
                    , ROW_NUMBER() OVER (PARTITION BY person_id
                    , condition_concept_id ORDER BY condition_start_date) AS start_ordinal
                FROM cteConditionTarget
            
                UNION ALL
            
                -- pad the end dates by 30 to allow a grace period for overlapping ranges.
                SELECT
                    person_id
                        , condition_concept_id
                    , condition_end_date + INTERVAL '30 days'
                    , 1 AS event_type
                    , NULL
                FROM cteConditionTarget
            ) RAWDATA
        ) e
        WHERE (2 * e.start_ordinal) - e.overall_ord = 0
    ),
    --------------------------------------------------------------------------------------------------------------
    cteConditionEnds (person_id, condition_concept_id, condition_start_date, era_end_date) AS
    (
    SELECT
            c.person_id
        , c.condition_concept_id
        , c.condition_start_date
        , MIN(e.end_date) AS era_end_date
    FROM cteConditionTarget c
    JOIN cteEndDates e ON c.person_id = e.person_id AND c.condition_concept_id = e.condition_concept_id AND e.end_date >= c.condition_start_date
    GROUP BY
            c.condition_occurrence_id
        , c.person_id
        , c.condition_concept_id
        , c.condition_start_date
    )
    --------------------------------------------------------------------------------------------------------------
    INSERT INTO {0}.condition_era(person_id, condition_concept_id, condition_era_start_date, condition_era_end_date, condition_occurrence_count)
    SELECT
        person_id
        , condition_concept_id
        , MIN(condition_start_date) AS condition_era_start_date
        , era_end_date AS condition_era_end_date
        , COUNT(*) AS condition_occurrence_count
    FROM cteConditionEnds
    GROUP BY person_id, condition_concept_id, era_end_date
    ORDER BY person_id, condition_concept_id
    ;
"""
DRUG_ERA_SQL = """TRUNCATE {0}.drug_era;
    WITH
    ctePreDrugTarget(drug_exposure_id, person_id, ingredient_concept_id, drug_exposure_start_date, days_supply, drug_exposure_end_date) AS
    (-- Normalize DRUG_EXPOSURE_END_DATE to either the existing drug exposure end date, or add days supply, or add 1 day to the start date
        SELECT
            d.drug_exposure_id
            , d.person_id
            , c.concept_id AS ingredient_concept_id
            , d.drug_exposure_start_date AS drug_exposure_start_date
            , d.days_supply AS days_supply
            , COALESCE(
                NULLIF(drug_exposure_end_date, NULL) ---If drug_exposure_end_date != NULL, return drug_exposure_end_date, otherwise go to next case
                , NULLIF(drug_exposure_start_date + (INTERVAL '1 day' * days_supply), drug_exposure_start_date) ---If days_supply != NULL or 0, return drug_exposure_start_date + days_supply, otherwise go to next case
                , drug_exposure_start_date + INTERVAL '1 day' ---Add 1 day to the drug_exposure_start_date since there is no end_date or INTERVAL for the days_supply
            ) AS drug_exposure_end_date
        FROM {0}.drug_exposure d
        JOIN {1}.concept_ancestor ca ON ca.descendant_concept_id = d.drug_concept_id
        JOIN {0}.concept c ON ca.ancestor_concept_id = c.concept_id
        WHERE c.vocabulary_id = 'RxNorm' --- was = 8 selects RxNorm from the vocabulary_id
        AND c.concept_class_id = 'Ingredient' --- was concept_class
        /* Depending on the needs of your data, you can put more filters on to your code. We assign 0 to unmapped drug_concept_id's, and we found data where days_supply was negative.
         * We don't want different drugs put in the same era, so the code below shows how we filtered them out.
         * We also don't want negative days_supply, because that will pull our end_date before the start_date due to our second parameter in the COALESCE function.
         * For now, we are filtering those out as well, but this is a data quality issue that we are trying to solve.
         */
        ---AND d.drug_concept_id != 0
        ---AND d.days_supply >= 0
    )
    
    , cteSubExposureEndDates (person_id, ingredient_concept_id, end_date) AS --- A preliminary sorting that groups all of the overlapping exposures into one exposure so that we don't double-count non-gap-days
    (
        SELECT
            person_id
            , ingredient_concept_id
            , event_date AS end_date
        FROM
        (
            SELECT
                person_id
                , ingredient_concept_id
                , event_date
                , event_type
                , MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type ROWS unbounded preceding) AS start_ordinal -- this pulls the current START down from the prior rows so that the NULLs from the END DATES will contain a value we can compare with
                , ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
            FROM (
                -- select the start dates, assigning a row number to each
                SELECT
                    person_id
                    , ingredient_concept_id
                    , drug_exposure_start_date AS event_date
                    , -1 AS event_type
                    , ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_exposure_start_date) AS start_ordinal
                FROM ctePreDrugTarget
            
                UNION ALL
    
                SELECT
                    person_id
                    , ingredient_concept_id
                    , drug_exposure_end_date
                    , 1 AS event_type
                    , NULL
                FROM ctePreDrugTarget
            ) RAWDATA
        ) e
        WHERE (2 * e.start_ordinal) - e.overall_ord = 0 
    )
        
    , cteDrugExposureEnds (person_id, drug_concept_id, drug_exposure_start_date, drug_sub_exposure_end_date) AS
    (
        SELECT 
               dt.person_id
               , dt.ingredient_concept_id
               , dt.drug_exposure_start_date
               , MIN(e.end_date) AS drug_sub_exposure_end_date
        FROM ctePreDrugTarget dt
        JOIN cteSubExposureEndDates e ON dt.person_id = e.person_id AND dt.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= dt.drug_exposure_start_date
        GROUP BY 
                  dt.drug_exposure_id
                  , dt.person_id
              , dt.ingredient_concept_id
              , dt.drug_exposure_start_date
    )
    --------------------------------------------------------------------------------------------------------------
    , cteSubExposures(row_number, person_id, drug_concept_id, drug_sub_exposure_start_date, drug_sub_exposure_end_date, drug_exposure_count) AS
    (
        SELECT
            ROW_NUMBER() OVER (PARTITION BY person_id, drug_concept_id, drug_sub_exposure_end_date)
            , person_id
            , drug_concept_id
            , MIN(drug_exposure_start_date) AS drug_sub_exposure_start_date
            , drug_sub_exposure_end_date
            , COUNT(*) AS drug_exposure_count
        FROM cteDrugExposureEnds
        GROUP BY person_id, drug_concept_id, drug_sub_exposure_end_date
        ORDER BY person_id, drug_concept_id
    )
    --------------------------------------------------------------------------------------------------------------
    /*Everything above grouped exposures into sub_exposures if there was overlap between exposures.
     *This means no persistence window was implemented. Now we CAN add the persistence window to calculate eras.
     */
    --------------------------------------------------------------------------------------------------------------
    , cteFinalTarget(row_number, person_id, ingredient_concept_id, drug_sub_exposure_start_date, drug_sub_exposure_end_date, drug_exposure_count, days_exposed) AS
    (
        SELECT
            row_number
            , person_id
            , drug_concept_id
            , drug_sub_exposure_start_date
            , drug_sub_exposure_end_date
            , drug_exposure_count
            , drug_sub_exposure_end_date - drug_sub_exposure_start_date AS days_exposed
        FROM cteSubExposures
    )
    --------------------------------------------------------------------------------------------------------------
    , cteEndDates (person_id, ingredient_concept_id, end_date) AS -- the magic
    (
        SELECT
            person_id
            , ingredient_concept_id
            , event_date - INTERVAL '30 days' AS end_date -- unpad the end date
        FROM
        (
            SELECT
                person_id
                , ingredient_concept_id
                , event_date
                , event_type
                , MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal -- this pulls the current START down from the prior rows so that the NULLs from the END DATES will contain a value we can compare with
                , ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
            FROM (
                -- select the start dates, assigning a row number to each
                SELECT
                    person_id
                    , ingredient_concept_id
                    , drug_sub_exposure_start_date AS event_date
                    , -1 AS event_type
                    , ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_sub_exposure_start_date) AS start_ordinal
                FROM cteFinalTarget
            
                UNION ALL
            
                -- pad the end dates by 30 to allow a grace period for overlapping ranges.
                SELECT
                    person_id
                    , ingredient_concept_id
                    , drug_sub_exposure_end_date + INTERVAL '30 days'
                    , 1 AS event_type
                    , NULL
                FROM cteFinalTarget
            ) RAWDATA
        ) e
        WHERE (2 * e.start_ordinal) - e.overall_ord = 0 
    
    )
    , cteDrugEraEnds (person_id, drug_concept_id, drug_sub_exposure_start_date, drug_era_end_date, drug_exposure_count, days_exposed) AS
    (
    SELECT 
        ft.person_id
        , ft.ingredient_concept_id
        , ft.drug_sub_exposure_start_date
        , MIN(e.end_date) AS era_end_date
        , ft.drug_exposure_count
        , ft.days_exposed
    FROM cteFinalTarget ft
    JOIN cteEndDates e ON ft.person_id = e.person_id AND ft.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= ft.drug_sub_exposure_start_date
    GROUP BY 
            ft.person_id
        , ft.ingredient_concept_id
        , ft.drug_sub_exposure_start_date
        , ft.drug_exposure_count
        , ft.days_exposed
    )
    INSERT INTO {0}.drug_era(person_id, drug_concept_id, drug_era_start_date, drug_era_end_date, drug_exposure_count, gap_days)
    SELECT
        person_id
        , drug_concept_id
        , MIN(drug_sub_exposure_start_date) AS drug_era_start_date
        , drug_era_end_date
        , SUM(drug_exposure_count) AS drug_exposure_count
        , EXTRACT(EPOCH FROM drug_era_end_date - MIN(drug_sub_exposure_start_date) - SUM(days_exposed)) / 86400 AS gap_days
    FROM cteDrugEraEnds
    GROUP BY person_id, drug_concept_id, drug_era_end_date
    ORDER BY person_id, drug_concept_id
    
    /*
    SELECT
        (
            SELECT SUM(drug_exposure_count) FROM cteFinalTarget
        ) AS sum
        , (
            SELECT COUNT(*) FROM {0}.drug_exposure d
                JOIN {1}.concept_ancestor ca ON ca.descendant_concept_id = d.drug_concept_id
                JOIN {1}.concept c ON ca.ancestor_concept_id = c.concept_id
                WHERE c.vocabulary_id = 'RxNorm' --- was = 8
                AND c.concept_class_id = 'Ingredient' --- was concept_class
                AND d.drug_concept_id != 0
                AND d.days_supply >= 0
        ) AS count
    */
    """
DRUG_ERA_STOCKPILE_SQL = """TRUNCATE {0}.drug_era;
    WITH cteDrugPreTarget(drug_exposure_id, person_id, ingredient_concept_id, drug_exposure_start_date, days_supply, drug_exposure_end_date) AS
        (
        -- Normalize DRUG_EXPOSURE_END_DATE to either the existing drug exposure end date, or add days supply, or add 1 day to the start date
        SELECT
            d.drug_exposure_id
            , d.person_id
            , c.concept_id AS ingredient_concept_id
            , d.drug_exposure_start_date AS drug_exposure_start_date
            , d.days_supply AS days_supply
            , COALESCE(
                ---NULLIF returns NULL if both values are the same, otherwise it returns the first parameter
                NULLIF(drug_exposure_end_date, NULL),
                ---If drug_exposure_end_date != NULL, return drug_exposure_end_date, otherwise go to next case
                NULLIF(drug_exposure_start_date + (INTERVAL '1 day' * days_supply), drug_exposure_start_date),
                ---If days_supply != NULL or 0, return drug_exposure_start_date + days_supply, otherwise go to next case
                drug_exposure_start_date + INTERVAL '1 day'
                ---Add 1 day to the drug_exposure_start_date since there is no end_date or INTERVAL for the days_supply
            ) AS drug_exposure_end_date
        FROM {0}.drug_exposure d
            JOIN {1}.concept_ancestor ca ON ca.descendant_concept_id = d.drug_concept_id
            JOIN {1}.concept c ON ca.ancestor_concept_id = c.concept_id
            WHERE c.vocabulary_id = 'RxNorm' --- was = 8 / 8 selects RxNorm from the vocabulary_id
            AND c.concept_class_id = 'Ingredient' --- was concept_class = 'Ingredient'
            /* Depending on the needs of your data, you can put more filters on to your code. We assign 0 to unmapped drug_concept_id's, and we found data where days_supply was negative.
             * We don't want different drugs put in the same era, so the code below shows how we filtered them out.
             * We also don't want negative days_supply, because that will pull our end_date before the start_date due to our second parameter in the COALESCE function.
             * For now, we are filtering those out as well, but this is a data quality issue that we are trying to solve.
             */
            ---AND d.drug_concept_id != 0
            ---AND d.days_supply >= 0
    )
    --------------------------------------------------------------------------------------------------------------
    , cteDrugTarget(drug_exposure_id, person_id, ingredient_concept_id, drug_exposure_start_date, days_supply, drug_exposure_end_date, days_of_exposure) AS
    (
        SELECT
            drug_exposure_id
            , person_id
            , ingredient_concept_id
            , drug_exposure_start_date
            , days_supply
            , drug_exposure_end_date
            , drug_exposure_end_date - drug_exposure_start_date AS days_of_exposure ---Calculates the days of exposure to the drug so at the end we can subtract the SUM of these days from the total days in the era.
        FROM cteDrugPreTarget
    )
    --------------------------------------------------------------------------------------------------------------
    , cteEndDates (person_id, ingredient_concept_id, end_date) AS -- the magic
    (
        SELECT
            person_id
            , ingredient_concept_id
            , event_date - INTERVAL '30 days' AS end_date -- unpad the end date
        FROM
        (
            SELECT
                person_id
                , ingredient_concept_id
                , event_date
                , event_type
                , MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type ROWS unbounded preceding) AS start_ordinal -- this pulls the current START down from the prior rows so that the NULLs from the END DATES will contain a value we can compare with
                , ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
            FROM (
                -- select the start dates, assigning a row number to each
                SELECT
                    person_id
                    , ingredient_concept_id
                    , drug_exposure_start_date AS event_date
                    , -1 AS event_type
                    , ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_exposure_start_date) AS start_ordinal
                FROM cteDrugTarget
            
                UNION ALL
            
                -- pad the end dates by 30 to allow a grace period for overlapping ranges.
                SELECT
                    person_id
                    , ingredient_concept_id
                    , drug_exposure_end_date + INTERVAL '30 days'
                    , 1 AS event_type
                    , NULL
                FROM cteDrugTarget
            ) RAWDATA
        ) e
        WHERE (2 * e.start_ordinal) - e.overall_ord = 0 
    
    )
    --------------------------------------------------------------------------------------------------------------
    , cteDrugExposureEnds (person_id, drug_concept_id, drug_exposure_start_date, drug_era_end_date, days_of_exposure) AS
    (
        SELECT 
               dt.person_id
               , dt.ingredient_concept_id
               , dt.drug_exposure_start_date
               , MIN(e.end_date) AS era_end_date
               , dt.days_of_exposure AS days_of_exposure
        FROM cteDrugTarget dt
        JOIN cteEndDates e ON dt.person_id = e.person_id AND dt.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= dt.drug_exposure_start_date
        GROUP BY 
                  dt.drug_exposure_id
                  , dt.person_id
              , dt.ingredient_concept_id
              , dt.drug_exposure_start_date
              , dt.days_of_exposure
    )
    --------------------------------------------------------------------------------------------------------------
    INSERT INTO {0}.drug_era(person_id, drug_concept_id, drug_era_start_date, drug_era_end_date, drug_exposure_count, gap_days)
    SELECT
        person_id
        , drug_concept_id
        , MIN(drug_exposure_start_date) AS drug_era_start_date
        , drug_era_end_date
        , COUNT(*) AS drug_exposure_count
        , EXTRACT(EPOCH FROM (drug_era_end_date - MIN(drug_exposure_start_date)) - SUM(days_of_exposure)) / 86400 AS gap_days
                  ---dividing by 86400 puts the integer in the "units" of days.
                  ---There are no actual units on this, it is just an integer, but we want it to represent days and dividing by 86400 does that.
    FROM cteDrugExposureEnds
    GROUP BY person_id, drug_concept_id, drug_era_end_date
    ORDER BY person_id, drug_concept_id
    ;
    
    /*
    ---This is a common test to make sure you have the same number of exposures going in as contribute to the count at the end.
    ---Make sure the JOIN and AND statements are the same as above so that your counts actually represent what you should be getting.
    SELECT
        (SELECT COUNT(*) FROM {0}.drug_exposure d JOIN {1}.concept_ancestor ca ON ca.descendant_concept_id = d.drug_concept_id
            JOIN {1}.concept c ON ca.ancestor_concept_id = c.concept_id
            WHERE c.vocabulary_id = 'RxNorm' ---8 selects RxNorm from the vocabulary_id
            AND c.concept_class_id = 'Ingredient'
            AND d.drug_concept_id != 0 ---Our unmapped drug_concept_id's are set to 0, so we don't want different drugs wrapped up in the same era
            AND d.days_supply >= 0) AS count
        , (SELECT SUM(drug_exposure_count) FROM {0}.drug_era) AS sum
    */
"""
def _fill_concept_names(conn_str, era_type):
    fill_concept_names_sql = """UPDATE {0}_era era
        SET {0}_concept_name=v.{0}_concept_name
        FROM ( SELECT
        e.{0}_era_id AS {0}_id,
        v1.concept_name AS {0}_concept_name
        FROM {0}_era AS e
        LEFT JOIN vocabulary.concept AS v1 ON e.{0}_concept_id = v1.concept_id
        ) v
        WHERE era.{0}_era_id = v.era.{0}_era_id"""

    fill_concept_names_msg = "adding concept names"

    # Add concept names
    add_era_ids_stmt = Statement(fill_concept_names_sql.format(era_type), fill_concept_names_msg)

    # Execute the add concept names statement and ensure it didn't error
    add_era_ids_stmt.execute(conn_str)
    check_stmt_err(add_era_ids_stmt, 'add concept names')


    # If reached without error, then success!
    return True


def _copy_to_dcc_table(conn_str, table, era_type):
    copy_to_condition_sql = """INSERT INTO dcc_pedsnet.condition_era(
        condition_concept_id, condition_era_end_date, condition_era_start_date, 
        condition_occurrence_count, condition_concept_name, site, condition_era_id, 
        site_id, person_id)
        (select condition_concept_id, condition_era_end_date, condition_era_start_date, 
        condition_occurrence_count, condition_concept_name, site, condition_era_id, 
        site_id, person_id
        from condition_era) ON CONFLICT DO NOTHING"""
    copy_to_drug_sql = """INSERT INTO dcc_pedsnet.drug_era(
        drug_concept_id, drug_era_end_date, drug_era_start_date, drug_exposure_count, 
        gap_days, drug_concept_name, site, drug_era_id, site_id, person_id)
        (select drug_concept_id, drug_era_end_date, drug_era_start_date, drug_exposure_count, 
        gap_days, drug_concept_name, site, drug_era_id, site_id, person_id
        from drug_era) ON CONFLICT DO NOTHING"""

    copy_to_msg = "copying {0}_era to dcc_pedsnet"

    # Insert era data into dcc_pedsnet era table
    if era_type == "condition":
        copy_to_stmt = Statement(copy_to_condition_sql.format(table), copy_to_msg.format({0}))
    else:
        copy_to_stmt = Statement(copy_to_drug_sql.format(table), copy_to_msg.format({0}))

    # Execute the insert BMI measurements statement and ensure it didn't error
    copy_to_stmt.execute(conn_str)
    check_stmt_err(copy_to_stmt, 'insert {0}_era data'.format(era_type))

    # If reached without error, then success!
    return True


def run_era(era_type, stockpile, conn_str, site, copy, search_path, model_version):
    """Run the Condition or Drug Era derivation.

    * Execute SQL
    * Add Ids
    * Add the concept names
    * Copy to dcc_pedsnet (if selected)
    * Vacuum output table

    :param str era_type:    type of derivation (condition or drug)
    :param bool stockpile:    if type drug to use stockpiling
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

    # Add drop null statement.
    stmts = StatementSet()
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
        era_query_stmt = Statement(CONDITION_ERA_SQL.format(schema), run_query_msg.format(era_type))
    else:
        if stockpile:
            era_query_stmt = Statement(DRUG_ERA_STOCKPILE_SQL.format(schema, "vocabulary"), run_query_msg.format(era_type))
        else:
            era_query_stmt = Statement(DRUG_ERA_SQL.format(schema, "vocabulary"),
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
    okay = _fill_concept_names(conn_str, era_type)
    if not okay:
        return False
    logger.info({'msg': 'concept names added'})

    # Copy to the dcc_pedsnet table
    if copy:
        logger.info({'msg': 'copy {0}_era to dcc_pedsnet'.format(era_type)})
        okay = _copy_to_dcc_table(conn_str, era_type)
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
    add_era_ids_sql = """update {0}.{1)_era set {1}_era_id = nextval('{0}.{1)_era_id_seq')
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
