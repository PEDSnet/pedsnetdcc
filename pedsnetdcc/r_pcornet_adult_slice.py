import logging
import time
import shutil
import os
import re

from pedsnetdcc.db import StatementSet, Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.schema import (primary_schema)
from pedsnetdcc.utils import (check_stmt_err, check_stmt_data, combine_dicts,
                              get_conn_info_dict, vacuum, stock_metadata)
from sh import Rscript

logger = logging.getLogger(__name__)
NAME_LIMIT = 30

def _create_argos_file(config_path, config_file, source_schema, target_schema, password, conn_info_dict):
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
        out_config.write('"options": "-c search_path=' + source_schema + ',' + target_schema + '"' + os.linesep)
        out_config.write('}' + os.linesep)
        out_config.write('}' + os.linesep)


def _fix_site_info(file_path, site, source_schema, target_schema):
    try:
        with open(os.path.join(file_path,'site','site_info.R'), 'r') as site_file:
            file_data = site_file.read()
        file_data = file_data.replace('<SITE>', site)
        file_data = file_data.replace('<SOURCE_SCHEMA>', source_schema)
        file_data = file_data.replace('<TARGET_SCHEMA>', target_schema)
        with open(os.path.join(file_path,'site','site_info.R'), 'w') as site_file:
            site_file.write(file_data)
    except:
        # this query package may not have this file
        return False

    return True


def _fix_run(file_path, site, source_schema, target_schema):
    try:
        with open(os.path.join(file_path,'site','run.R'), 'r') as site_file:
            file_data = site_file.read()
        file_data = file_data.replace('<SITE>', site)
        file_data = file_data.replace('<SOURCE_SCHEMA>', source_schema)
        file_data = file_data.replace('<TARGET_SCHEMA>', target_schema)
        with open(os.path.join(file_path,'site','run.R'), 'w') as site_file:
            site_file.write(file_data)
    except:
        # this query package may not have this file
        return False

    return True


def run_r_pcornet_adult_slice(config_file, conn_str, site, password, source_schema, target_schema):
    """Run an R script.

    * Create argos file
    * Run R Script

    :param str config_file:   config file name
    :param str conn_str:      database connection string
    :param str site:    site to run script for
    :param str password:    user's password
    :param str source_schema: schema to use as source (adult + pediatric schema)
    :param str target_schema: schema to put results (pediatric schema)
    :returns:                 True if the function succeeds
    :rtype:                   bool
    :raises DatabaseError:    if any of the statement executions cause errors
    """

    conn_info_dict = get_conn_info_dict(conn_str)

    # Log start of the function and set the starting time.
    log_dict = combine_dicts({'site': site, },
                             conn_info_dict)
    logger.info(combine_dicts({'msg': 'starting R Script'},
                              log_dict))
    start_time = time.time()
    source_schema = source_schema
    target_schema = target_schema

    if password == None:
        pass_match = re.search(r"password=(\S*)", conn_str)
        password = pass_match.group(1)

    package = 'pcornet_adult_slice'
    source_path = os.path.join(os.sep,'app', package)
    dest_path = os.path.join(source_path, site)
    # delete any old versions
    if os.path.isdir(dest_path):
        shutil.rmtree(dest_path)
    # copy base files to site specific
    shutil.copytree(source_path, dest_path)
    # create the Argos congig file
    _create_argos_file(dest_path, config_file, source_schema, target_schema, password, conn_info_dict)
    # modify site_info and run.R to add actual site
    _fix_site_info(dest_path, site, source_schema, target_schema)
    _fix_run(dest_path, site, source_schema, target_schema)

    query_path = os.path.join(os.sep,'app', package, site, 'site', 'run.R')
    # Run R script
    Rscript(query_path, '--verbose=1', _cwd='/app', _fg=True)

    # Log end of function.
    logger.info(combine_dicts({'msg': 'finished R Script',
                               'elapsed': secs_since(start_time)}, log_dict))

    # If reached without error, then success!
    return True