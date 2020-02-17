import logging
import time
import hashlib
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


def _fix_site_info(config_path, site):
    try:
        with open(os.path.join(config_path,'site','site_info.R'), 'r') as site_file:
            site_data = site_file.read()
        site_data = site_data.replace('<SITE>', site)
        with open(os.path.join(config_path,'site','site_info.R'), 'w') as site_file:
            site_file.write(site_data)
    except:
        # this query package may not have this file
        return False

    return True


def run_r_query(config_file, conn_str, site, package, password, search_path, model_version):
    """Run an R script.

    * Create argos file
    * Run R Script

    :param str config_file:   config file name
    :param str conn_str:      database connection string
    :param str site:    site to run script for
    :param str package:    package to run
    :param str password:    user's password
    :param str search_path: PostgreSQL schema search path
    :param str model_version: pedsnet model version, e.g. 2.3.0
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
    schema = primary_schema(search_path)

    if password == None:
        pass_match = re.search(r"password=(\S*)", conn_str)
        password = pass_match.group(1)

    # create the congig file
    config_path = "/app/" + package
    _create_argos_file(config_path, config_file, schema, password, conn_info_dict)
    _fix_site_info(config_path, site)

    # Add a creation statement.
    stmts = StatementSet()

    # Run R script
    Rscript("/app/" + package + "/site/run.R", '--verbose=1', _cwd='/app', _fg=True)

    # If reached without error, then success!
    return True