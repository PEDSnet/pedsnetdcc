import click
import logging
import sys

from pedsnetdcc import __version__
from pedsnetdcc.utils import make_conn_str
from pedsnetdcc.dict_logging import DictLogFilter
from pedsnetdcc.cleanup import cleanup_site_directories

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])



@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--logfmt', type=click.Choice(['tty', 'text', 'json']),
              help='Logging output format.')
@click.option('--loglvl', default='INFO', help='Logging output level.',
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR',
                                 'CRITICAL']))
@click.version_option(version=__version__)
def pedsnetdcc(logfmt, loglvl):
    """A CLI tool for executing PEDSnet Data Coordinating Center ETL tasks.
    """

    logger = logging.getLogger('pedsnetdcc')
    sh = logging.StreamHandler()

    # Without explicit logfmt at tty use tty format.
    if not logfmt and sys.stderr.isatty():
            logfmt = 'tty'

    if logfmt == 'tty':
        sh.addFilter(DictLogFilter('tty'))
    elif logfmt == 'text':
        sh.addFilter(DictLogFilter('text'))
    else:
        # Default format is json.
        sh.addFilter(DictLogFilter('json'))

    logger.addHandler(sh)
    logger.setLevel(logging.getLevelName(loglvl))


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.argument('dburi')
def sync_observation_period(searchpath, pwprompt, dburi):
    """Sync the observation period table to the fact data.

    Delete any existing records in the observation period table and calculate a
    completely new set of records from the fact data in the database. Log the
    number of new records and timing at INFO level.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    from pedsnetdcc.sync_observation_period import sync_observation_period

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)
    success = sync_observation_period(conn_str)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--output', '-o', default='both', help='Output format.',
              type=click.Choice(['both', 'percent', 'samples']))
@click.option('--poolsize', type=int,
              help='Number of parallel processes to use.')
@click.argument('dburi')
def check_fact_relationship(searchpath, pwprompt, output, poolsize, dburi):
    """Check the referential integrity of the fact relationship table.

    Execute SQL statements, in parallel, to inspect the fact relationship table
    for validity. The statements needed to get the requested output format are
    executed and the results are logged. Problems are reported at the WARNING
    level and positive results and timing are reported at the INFO level.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    from pedsnetdcc.check_fact_relationship import check_fact_relationship

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)
    success = check_fact_relationship(conn_str, output, poolsize)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.argument('dburi')
def create_id_maps(dburi, pwprompt):
    """Create id map tables to map the relationship between site ids and the dcc ids

    Mapping between external site ids and dcc ids are neccessary to ensure data stays consistent
    data cycles. This creates the tables neccessary for preserving that data.  
    Does not fill in any data.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    from pedsnetdcc.id_maps import create_id_map_tables, create_dcc_ids_tables

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, password=password)
    create_dcc_ids_tables(conn_str)
    create_id_map_tables(conn_str)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.argument('dburi')
@click.argument('old_db')
@click.argument('new_db')
def copy_id_maps(dburi, old_db, new_db, pwprompt):
    """Copy id map tables from the last data cycles database into the new data cycles database
    The databases should be specified using DBURIs:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    from pedsnetdcc.id_maps import copy_id_maps

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    old_conn_str = make_conn_str(dburi + old_db, password=password)
    new_conn_str = make_conn_str(dburi + new_db, password=password)

    copy_id_maps(old_conn_str, new_conn_str)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name to add to tables.')
@click.option('--force', is_flag=True, default=False,
              help='Ignore any "already exists" errors from the database.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--undo', is_flag=True, default=False,
              help='Replace transformed tables with backup tables.')
@click.argument('dburi')
def transform(pwprompt, searchpath, site, force, model_version, undo, dburi):
    """Transform PEDSnet data into the DCC format.

    Using the hard-coded set of transformations in this tool, transform data
    from the given PEDSnet model version format into the DCC format. Existing
    tables are backed up to a new '<searchpath>_backup' schema.

    The currently defined transformations are:

      - Add '_age_in_months' columns alongside specified time columns.
      - Add '_concept_name' columns next to all '_concept_id' columns.
      - Add 'site' columns with the specified site name to all tables.
      - Generate new DCC IDs and replace site IDs with DCC IDs in all tables.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    if not undo:
        from pedsnetdcc.transform_runner import run_transformation
        success = run_transformation(conn_str, model_version, site, searchpath,
                                     force)
    else:
        from pedsnetdcc.transform_runner import undo_transformation
        success = undo_transformation(conn_str, model_version, searchpath)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--force', is_flag=True, default=False,
              help='Ignore any "already exists" errors from the database.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--undo', is_flag=True, default=False,
              help='Remove merged DCC data tables.')
@click.argument('dburi')
def merge(pwprompt, force, model_version, undo, dburi):
    """Merge site data into a single, aggregated DCC dataset

    Site data from the site data schemas (named like '<site>_pedsnet') into the
    DCC data schema (named 'dcc_pedsnet'). The `transform` command must have
    already been run on each of the site data sets.

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, '', password)

    if not undo:
        from pedsnetdcc.merge_site_data import merge_site_data
        success = merge_site_data(model_version, conn_str, force)
    else:
        from pedsnetdcc.merge_site_data import clear_dcc_data
        success = clear_dcc_data(model_version, conn_str, force)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--dcc-only', type=bool, is_flag=True, default=False,
              help='Only create schemas for the dcc.')
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.argument('dburi')
def prepdb(model_version, dcc_only, pwprompt, dburi):
    """Create a database and schemas.

    The database should be specified using a model version and a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    from pedsnetdcc.prepdb import prepare_database

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, password=password)
    success = prepare_database(model_version, conn_str, update=False,
                               dcc_only=dcc_only)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--site-root', '-s',
              help='Override default site data root directory')
@click.argument('backup_dir', required=False)
def cleanup(backup_dir, site_root):
    """Backup and delete older site data directories"""

    success = cleanup_site_directories(backup_dir, site_root)

    if not success:
        sys.exit(1)

    sys.exit(0)

@pedsnetdcc.command()
@click.argument('dburi', required=True)
@click.argument('in_file', required=True)
@click.argument('table_name', required=True)
@click.option('--search-path', '-s',
              help="Target site for load")
@click.option('--out-file', '-o',
              help='Output path for a csv file of results')
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
def map_external_ids(dburi, in_file, search_path, out_file, table_name, pwprompt):
    """Takes a CSV from an external site with IDS and creates and maps those IDS to a DCC_ID
    Optionally outputs a csv file with mapping of ids

    The database should be specified using a model version and a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    from pedsnetdcc.external_id_mapper import map_external_ids

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    if not out_file:
        substring = in_file.split('.csv')[0]

        out_file = substring + "_RESULTS.csv"

    conn_str = make_conn_str(dburi,
                             password=password,
                             search_path=search_path)

    map_external_ids(conn_str, str(in_file), str(out_file), str(table_name))

@pedsnetdcc.command()
@click.argument('dburi', required=True)
@click.option('--pwprompt', '-p', is_flag=True, default=False)
@click.option('--full', '-f', is_flag=True, default=False)
def grant_permissions(dburi, pwprompt, full):
    """Grants the appropriate permissions for all schemas and tables, as well as vocabulary schemas and tables
    
    This is normally set during prepdb and transform, but this command can be used to manually set permissions if there is an issue

    The database should be specified using a model version and a dburi

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """
    
    from pedsnetdcc.permissions import grant_schema_permissions, grant_vocabulary_permissions, grant_loading_user_permissions

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, password=password)

    if full:
	grant_loading_user_permissions(conn_str)

    grant_schema_permissions(conn_str)
    grant_vocabulary_permissions(conn_str)

@pedsnetdcc.command()		  
@click.argument('dburi', required=True)		 
@click.option('--pwprompt', '-p', is_flag=True, default=False)
@click.option('--searchpath', '-s', required=True)
@click.option('--site', required=True,
              help='PEDSnet site name to add to tables.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
def generate_transform_statements(dburi, pwprompt, searchpath, model_version, site):		
     		
    from pedsnetdcc.transform_runner import _transform_select_sql		
    from pedsnetdcc.schema import create_schema, primary_schema		
    if pwprompt:		
        password = click.prompt('Database password', hide_input=True)		
        
    conn_str = make_conn_str(dburi, password=password)		

    # TODO: do we need to validate the primary schema at all?		
    schema = primary_schema(searchpath)		
	
    tmp_schema = schema + '_' + 'transformed'			
    for sql, msg in _transform_select_sql(model_version, site, tmp_schema):		
        print("msg: " + sql)


if __name__ == '__main__':
    pedsnetdcc()
