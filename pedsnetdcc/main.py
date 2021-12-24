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
@click.option('--site', required=True,
              help='PEDSnet site name to add to tables.')
@click.option('--name', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.option('--force', is_flag=True, default=False,
              help='Ignore any "already exists" errors from the database.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.argument('dburi')
def post_load(searchpath, pwprompt, dburi, site, name, force, model_version):
    """Run all post load operations

    Run check_fact_relationship
    Run sync_observation_period
    Run transform

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.check_fact_relationship import check_fact_relationship
    success = check_fact_relationship(conn_str)

    if not success:
        sys.exit(1)

    from pedsnetdcc.sync_observation_period import sync_observation_period
    success = sync_observation_period(conn_str)

    if not success:
        sys.exit(1)

    from pedsnetdcc.transform_runner import run_transformation
    success = run_transformation(conn_str, model_version, site, searchpath, name,
                                 force)

    if not success:
        sys.exit(1)

    sys.exit(0)


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
@click.option('--skipsites', required=False, default='',
              help='sites to skip delimited by ,')
@click.option('--addsites', required=False, default='',
              help='sites to add delimited by ,')
@click.option('--name', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.option('--type', required=False, default='INTEGER',
              help='type of the id (ex: BIGINT')
@click.argument('dburi')
def create_id_maps(dburi, pwprompt, skipsites, addsites, name, type):
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
    create_dcc_ids_tables(conn_str, name, type)
    create_id_map_tables(conn_str, skipsites, addsites, name, type)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--name', required=True,
              help='name of the id (ex: onco')
@click.argument('dburi')
def populate_last_id(dburi, pwprompt, searchpath, name):
    """Populates the last_id of of the study id map id tables created with create_id_maps

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    from pedsnetdcc.id_maps import populate_last_id
    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, password=password)
    success = populate_last_id(conn_str, searchpath, name)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--name', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.option('--skipsites', required=False, default='',
              help='sites to skip delimited by ,')
@click.option('--addsites', required=False, default='',
              help='sites to add delimited by ,')
@click.argument('dburi')
@click.argument('old_db')
@click.argument('new_db')
def copy_id_maps(dburi, old_db, new_db, pwprompt, name, skipsites, addsites):
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

    copy_id_maps(old_conn_str, new_conn_str, name, skipsites, addsites)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name to add to tables.')
@click.option('--name', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.option('--force', is_flag=True, default=False,
              help='Ignore any "already exists" errors from the database.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--undo', is_flag=True, default=False,
              help='Replace transformed tables with backup tables.')
@click.argument('dburi')
def transform(pwprompt, searchpath, site, name, force, model_version, undo, dburi):
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
        success = run_transformation(conn_str, model_version, site, searchpath, name,
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
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name to add to tables.')
@click.option('--force', is_flag=True, default=False,
              help='Ignore any "already exists" errors from the database.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--table', required=True,
              help='table(s) to transform delimited by ,')
@click.option('--undo', is_flag=True, default=False,
              help='Replace transformed tables with backup tables.')
@click.argument('dburi')
def age_transform(pwprompt, searchpath, site, force, model_version, table, undo, dburi):
    """Transform PEDSnet data into the DCC format.

    Using the hard-coded set of transformations in this tool, transform data
    from the given PEDSnet model version format into the DCC format. Existing
    tables are backed up to a new '<searchpath>_backup' schema.

    The currently defined transformations are:

      - Add '_age_in_months' columns alongside specified time columns.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    if not undo:
        from pedsnetdcc.transform_runner import run_age_transformation
        success = run_age_transformation(conn_str, model_version, site, searchpath, table, force)

    if not success:
        sys.exit(1)

    sys.exit(0)


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
@click.option('--table', required=True,
              help='table(s) to transform delimited by ,')
@click.option('--undo', is_flag=True, default=False,
              help='Replace transformed tables with backup tables.')
@click.argument('dburi')
def concept_transform(pwprompt, searchpath, site, force, model_version, table, undo, dburi):
    """Transform PEDSnet data into the DCC format.

    Using the hard-coded set of transformations in this tool, transform data
    from the given PEDSnet model version format into the DCC format. Existing
    tables are backed up to a new '<searchpath>_backup' schema.

    The currently defined transformations are:

      - Add '_concept name' columns alongside concept id columns.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    if not undo:
        from pedsnetdcc.transform_runner import run_concept_transformation
        success = run_concept_transformation(conn_str, model_version, site, searchpath, table, force)

    if not success:
        sys.exit(1)

    sys.exit(0)


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
@click.option('--table', required=True,
              help='table(s) to transform delimited by ,')
@click.option('--undo', is_flag=True, default=False,
              help='Replace transformed tables with backup tables.')
@click.argument('dburi')
def site_transform(pwprompt, searchpath, site, force, model_version, table, undo, dburi):
    """Transform PEDSnet data into the DCC format.

    Using the hard-coded set of transformations in this tool, transform data
    from the given PEDSnet model version format into the DCC format. Existing
    tables are backed up to a new '<searchpath>_backup' schema.

    The currently defined transformations are:

      - Add 'site' columns column.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    if not undo:
        from pedsnetdcc.transform_runner import run_site_transformation
        success = run_site_transformation(conn_str, model_version, site, searchpath, table, force)

    if not success:
        sys.exit(1)

    sys.exit(0)


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
@click.option('--table', required=True,
              help='table(s) to transform delimited by ,')
@click.option('--undo', is_flag=True, default=False,
              help='Replace transformed tables with backup tables.')
@click.argument('dburi')
def index_transform(pwprompt, searchpath, site, force, model_version, table, undo, dburi):
    """Transform PEDSnet data into the DCC format.

    Using the hard-coded set of transformations in this tool, transform data
    from the given PEDSnet model version format into the DCC format. Existing
    tables are backed up to a new '<searchpath>_backup' schema.

    The currently defined transformations are:

      - Add indexes for primary keys.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    if not undo:
        from pedsnetdcc.transform_runner import run_index_transformation
        success = run_index_transformation(conn_str, model_version, site, searchpath, table, force)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name to add to tables.')
@click.option('--name', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.option('--force', is_flag=True, default=False,
              help='Ignore any "already exists" errors from the database.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--table', required=True,
              help='table(s) to transform delimited by ,')
@click.option('--undo', is_flag=True, default=False,
              help='Replace transformed tables with backup tables.')
@click.argument('dburi')
def id_transform(pwprompt, searchpath, site, name, force, model_version, table, undo, dburi):
    """Transform PEDSnet data into the DCC format.

    Using the hard-coded set of transformations in this tool, transform data
    from the given PEDSnet model version format into the DCC format. Existing
    tables are backed up to a new '<searchpath>_backup' schema.

    The currently defined transformations are:

      - Move site id to site_id column for primary key and assign new id to replace original site submitted id.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    if not undo:
        from pedsnetdcc.transform_runner import run_id_transformation
        success = run_id_transformation(conn_str, model_version, site, searchpath, table, name, force)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name to add to tables.')
@click.option('--name', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.option('--force', is_flag=True, default=False,
              help='Ignore any "already exists" errors from the database.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--table', required=True,
              help='table(s) to transform delimited by ,')
@click.option('--undo', is_flag=True, default=False,
              help='Replace transformed tables with backup tables.')
@click.argument('dburi')
def run_target_transform(pwprompt, searchpath, site, name, force, model_version, table, undo, dburi):
    """Transform PEDSnet data into the DCC format.

    Using the hard-coded set of transformations in this tool, transform data
    from the given PEDSnet model version format into the DCC format. Existing
    tables are backed up to a new '<searchpath>_backup' schema.

    The currently defined transformations are:

      - Move site id to site_id column for primary key and assign new id to replace original site submitted id.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    if not undo:
        from pedsnetdcc.transform_runner import run_target_transformation
        success = run_target_transformation(conn_str, model_version, site, searchpath, table, name, force)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--addsites', required=False, default='',
              help='sites to add delimited by ,')
@click.option('--force', is_flag=True, default=False,
              help='Ignore any "already exists" errors from the database.')
@click.option('--notable', is_flag=True, default=False,
              help='Skip the union when tables already exist.')
@click.option('--nolog', is_flag=True, default=False,
              help='Skip set logged if already done.')
@click.option('--nopk', is_flag=True, default=False,
              help='Skip primary keys if already exist.')
@click.option('--nonull', is_flag=True, default=False,
              help='Skip set not null if already done.')
@click.option('--noidx', is_flag=True, default=False,
              help='Skip indexes if already exist.')
@click.option('--nodrop', is_flag=True, default=False,
              help='Skip drop unused indexes if already done.')
@click.option('--norep', is_flag=True, default=False,
              help='Skip index replacement tables if already exist.')
@click.option('--nofk', is_flag=True, default=False,
              help='Skip foreign keys if already exist.')
@click.option('--novac', is_flag=True, default=False,
              help='Skip vaccuum if already done.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--undo', is_flag=True, default=False,
              help='Remove merged DCC data tables.')
@click.argument('dburi')
def merge(pwprompt, addsites, force, notable, nolog, nopk, nonull, noidx, nodrop, norep, nofk, novac, model_version, undo, dburi):
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
        success = merge_site_data(model_version, conn_str, addsites, force, notable, nolog, nopk,
                                  nonull, noidx, nodrop, norep, nofk, novac)
    else:
        from pedsnetdcc.merge_site_data import clear_dcc_data
        success = clear_dcc_data(model_version, conn_str, force)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--schema', required=True,
              help='schema to merge into.')
@click.option('--altname', required=False, default='',
              help='alterate name of site schemas i.e. <site>_atltame.')
@click.option('--skipsites', required=False, default='',
              help='sites to skip delimited by ,')
@click.option('--addsites', required=False, default='',
              help='sites to add delimited by ,')
@click.option('--force', is_flag=True, default=False,
              help='Ignore any "already exists" errors from the database.')
@click.option('--notable', is_flag=True, default=False,
              help='Skip the union when tables already exist.')
@click.option('--nolog', is_flag=True, default=False,
              help='Skip set logged if already done.')
@click.option('--nopk', is_flag=True, default=False,
              help='Skip primary keys if already exist.')
@click.option('--nonull', is_flag=True, default=False,
              help='Skip set not null if already done.')
@click.option('--noidx', is_flag=True, default=False,
              help='Skip indexes if already exist.')
@click.option('--nodrop', is_flag=True, default=False,
              help='Skip drop unused indexes if already done.')
@click.option('--norep', is_flag=True, default=False,
              help='Skip index replacement tables if already exist.')
@click.option('--nofk', is_flag=True, default=False,
              help='Skip foreign keys if already exist.')
@click.option('--novac', is_flag=True, default=False,
              help='Skip vaccuum if already done.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--undo', is_flag=True, default=False,
              help='Remove merged DCC data tables.')
@click.argument('dburi')
def merge_schema(pwprompt, schema, altname, skipsites, addsites, force, notable, nolog, nopk, nonull, noidx, nodrop, norep, nofk,
                 novac, model_version, undo, dburi):
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
        from pedsnetdcc.merge_site_data import merge_data_to_schema
        success = merge_data_to_schema(model_version, conn_str, schema, altname, skipsites, addsites ,force, notable, nolog, nopk,
                                       nonull, noidx, nodrop, norep, nofk, novac)
    else:
        from pedsnetdcc.merge_site_data import clear_schema_data
        success = clear_schema_data(model_version, conn_str, schema, force)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--truncate', is_flag=True, default=False,
              help='truncate measurement table after split.')
@click.option('--view', is_flag=True, default=False,
              help='Create measurements view.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.argument('dburi')
def split_measurement(pwprompt, searchpath, truncate, view, model_version, dburi):
    """Split measurement table into anthro, labs, and vitals.

    The steps are:

    - Create the measurement_anthro, measurement_labs, and measurement_vitals from measurement
    - Set primary keys
    - Add indexes
    - Add foreign keys
    - Set permissions
    - Truncate measurement table if flag set
    - Create measurements view if flag set
    - Vacuum

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.split_measurement import split_measurement_table
    success = split_measurement_table(conn_str, truncate, view, model_version, searchpath)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for the config file.')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to dcc_pedsnet.')
@click.option('--noids', is_flag=True, default=False,
              help='DO NOT add measurement ids for z scores.')
@click.option('--noindexes', is_flag=True, default=False,
              help='DO NOT add indexes.')
@click.option('--noconcept', is_flag=True, default=False,
              help='DO NOT add concept names for z scores.')
@click.option('--neg_ids', is_flag=True, default=False,
              help='Use negative ids.')
@click.option('--skip_calc', is_flag=True, default=False,
              help='Skip actual calculation.')
@click.option('--no_ids', is_flag=True, default=False,
              help='Do not assign ids for drug/condition eras.')
@click.option('--no_concept', is_flag=True, default=False,
              help='Do not add concept names for drug/condtion eras.')
@click.option('--table', required=True,
              help='Table to use for input as well as copy (measurement, measurement_anthro.')
@click.option('--person', required=False, default='person',
              help='name of the person table')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.argument('dburi')
def run_derivations(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, neg_ids, skip_calc, no_ids,
                    no_concept, table, person, model_version, idname, dburi):
    """Run all derivations.

    The steps are:

      - Run BMI.
      - Run BMIZ.
      - Run HeightZ.
      - Run WeightZ
      - Run Drug Era
      - Run Condition Era

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    ids = True
    if noids:
        ids = False

    indexes = True
    if noindexes:
        indexes = False

    concept = True
    if noconcept:
        concept = False


    config_file = site + "_bmi_temp.conf"
    from pedsnetdcc.bmi import run_bmi_calc
    success = run_bmi_calc(config_file, conn_str, site, copy, ids, indexes, concept, neg_ids, skip_calc,
                           table, password, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    config_file = site + "_bmiz_temp.conf"
    from pedsnetdcc.z_score import run_z_calc
    success = run_z_calc('bmiz', config_file, conn_str, site, copy, ids, indexes, concept, neg_ids,
                         skip_calc, table, person, password, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    config_file = site + "_htz_temp.conf"
    success = run_z_calc('ht_z', config_file, conn_str, site, copy, ids, indexes, concept, neg_ids,
                         skip_calc, table, person, password, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    config_file = site + "_wtz_temp.conf"
    success = run_z_calc('wt_z', config_file, conn_str, site, copy, ids, indexes, concept, neg_ids,
                         skip_calc, table, person, password, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    from pedsnetdcc.era import run_era
    success = run_era("drug", conn_str, site, copy, neg_ids, no_ids, no_concept, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    success = run_era("drug_scdf", conn_str, site, copy, neg_ids, no_ids, no_concept, searchpath, model_version, idname)
    if not success:
        sys.exit(1)

    success = run_era("condition", conn_str, site, copy, neg_ids, no_ids, no_concept, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for the config file.')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to dcc_pedsnet.')
@click.option('--noids', is_flag=True, default=False,
              help='DO NOT add measurement ids.')
@click.option('--noindexes', is_flag=True, default=False,
              help='DO NOT add indexes.')
@click.option('--noconcept', is_flag=True, default=False,
              help='DO NOT add concept names.')
@click.option('--neg_ids', is_flag=True, default=False,
              help='Use negative ids.')
@click.option('--skip_calc', is_flag=True, default=False,
              help='Skip actual calculation.')
@click.option('--table', required=True,
              help='Table to use for input as well as copy (measurement, measurement_anthro.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.argument('dburi')
def run_bmi(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, neg_ids, skip_calc,
            table, model_version, idname, dburi):
    """Run BMI derivation.

    The steps are:

      - Create the config file.
      - Create the output table.
      - Run the derivation.
      - Add indexes to output table
      - Add measurement ids
      - Add concept names
      - Copy BMI measurements to dcc_pedsnet.measurement_anthro
      - Vacuum the output table

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)
    config_file = site + "_bmi_temp.conf"

    ids = True
    if noids:
        ids = False

    indexes = True
    if noindexes:
        indexes = False

    concept = True
    if noconcept:
        concept = False

    from pedsnetdcc.bmi import run_bmi_calc
    success = run_bmi_calc(config_file, conn_str, site, copy, ids, indexes, concept, neg_ids,
                           skip_calc, table, password, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name')
@click.option('--table', required=True,
              help='Table to use for copy (measurement, measurement_anthro.')
@click.argument('dburi')
def copy_bmi(pwprompt, searchpath, site, table, dburi):
    """Copy BMI table to dcc_pedsnet.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.bmi import copy_bmi_dcc
    success = copy_bmi_dcc(conn_str, site, table)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for the config file.')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to dcc_pedsnet.')
@click.option('--noids', is_flag=True, default=False,
              help='DO NOT add measurement ids.')
@click.option('--noindexes', is_flag=True, default=False,
              help='DO NOT add indexes.')
@click.option('--noconcept', is_flag=True, default=False,
              help='DO NOT add concept names.')
@click.option('--neg_ids', is_flag=True, default=False,
              help='Use negative ids.')
@click.option('--skip_calc', is_flag=True, default=False,
              help='Skip actual calculation.')
@click.option('--table', required=True,
              help='Table to use for input as well as copy (measurement, measurement_anthro.')
@click.option('--person', required=False, default='person',
              help='name of the person table')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.argument('dburi')
def run_bmiz(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, neg_ids, skip_calc, table,
             person, model_version, idname, dburi):
    """Run BMI-Z derivation.

    The steps are:

      - Create the config file.
      - Create the output table.
      - Run the derivation.
      - Add indexes to output table
      - Add measurement ids
      - Add concept names
      - Copy BMI-Z measurements to dcc_pedsnet
      - Vacuum the output table

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)
    config_file = site + "_bmiz_temp.conf"

    ids = True
    if noids:
        ids = False

    indexes = True
    if noindexes:
        indexes = False

    concept = True
    if noconcept:
        concept = False

    from pedsnetdcc.z_score import run_z_calc
    success = run_z_calc('bmiz', config_file, conn_str, site, copy, ids, indexes, concept, neg_ids,
                         skip_calc, table, person, password, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name')
@click.option('--table', required=True,
              help='Table to use for copy (measurement, measurement_anthro.')
@click.argument('dburi')
def copy_bmiz(pwprompt, searchpath, site, table, dburi):
    """Copy BMIZ table to dcc_pedsnet.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.z_score import copy_z_dcc
    success = copy_z_dcc('bmiz', conn_str, site, table, searchpath)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for the config file.')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to dcc_pedsnet.')
@click.option('--noids', is_flag=True, default=False,
              help='DO NOT add measurement ids.')
@click.option('--noindexes', is_flag=True, default=False,
              help='DO NOT add indexes.')
@click.option('--noconcept', is_flag=True, default=False,
              help='DO NOT add concept names.')
@click.option('--neg_ids', is_flag=True, default=False,
              help='Use negative ids.')
@click.option('--skip_calc', is_flag=True, default=False,
              help='Skip actual calculation.')
@click.option('--table', required=True,
              help='Table to use for input as well as copy (measurement, measurement_anthro.')
@click.option('--person', required=False, default='person',
              help='name of the person table')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.argument('dburi')
def run_bmi_bmiz(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, neg_ids, skip_calc,
                 table, person, model_version, idname, dburi):
    """Run BMI and BMI-Z derivations.

    The steps are:

      - Run BMI.
      - Run BMIZ.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    ids = True
    if noids:
        ids = False

    indexes = True
    if noindexes:
        indexes = False

    concept = True
    if noconcept:
        concept = False

    config_file = site + "_bmi_temp.conf"
    from pedsnetdcc.bmi import run_bmi_calc
    success = run_bmi_calc(config_file, conn_str, site, copy, ids, indexes, concept, neg_ids,
                           skip_calc, table, password, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    config_file = site + "_bmiz_temp.conf"
    from pedsnetdcc.z_score import run_z_calc
    success = run_z_calc('bmiz', config_file, conn_str, site, copy, ids, indexes, concept, neg_ids,
                         skip_calc, table, person, password, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for the config file.')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to dcc_pedsnet.')
@click.option('--noids', is_flag=True, default=False,
              help='DO NOT add measurement ids.')
@click.option('--noindexes', is_flag=True, default=False,
              help='DO NOT add indexes.')
@click.option('--noconcept', is_flag=True, default=False,
              help='DO NOT add concept names.')
@click.option('--neg_ids', is_flag=True, default=False,
              help='Use negative ids.')
@click.option('--skip_calc', is_flag=True, default=False,
              help='Skip actual calculation.')
@click.option('--table', required=True,
              help='Table to use for input as well as copy (measurement, measurement_anthro.')
@click.option('--person', required=False, default='person',
              help='name of the person table')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.argument('dburi')
def run_height_z(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, neg_ids, skip_calc, table,
                 person, model_version, idname, dburi):
    """Run HEIGHT-Z derivation.

    The steps are:

      - Create the config file.
      - Create the output table.
      - Run the derivation.
      - Add indexes to output table
      - Add measurement ids
      - Add concept names
      - Copy Height-Z measurements to dcc_pedsnet
      - Vacuum the output table

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)
    config_file = site + "_htz_temp.conf"

    ids = True
    if noids:
        ids = False

    indexes = True
    if noindexes:
        indexes = False

    concept = True
    if noconcept:
        concept = False

    from pedsnetdcc.z_score import run_z_calc
    success = run_z_calc('ht_z', config_file, conn_str, site, copy, ids, indexes, concept, neg_ids,
                         skip_calc, table, person, password, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name')
@click.option('--table', required=True,
              help='Table to use for copy (measurement, measurement_anthro.')
@click.argument('dburi')
def copy_height_z(pwprompt, searchpath, site, table, dburi):
    """Copy Height_Z table to dcc_pedsnet.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.z_score import copy_z_dcc
    success = copy_z_dcc('ht_z', conn_str, site, table, searchpath)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for the config file.')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to dcc_pedsnet.')
@click.option('--noids', is_flag=True, default=False,
              help='DO NOT add measurement ids.')
@click.option('--noindexes', is_flag=True, default=False,
              help='DO NOT add indexes.')
@click.option('--noconcept', is_flag=True, default=False,
              help='DO NOT add concept names.')
@click.option('--neg_ids', is_flag=True, default=False,
              help='Use negative ids.')
@click.option('--skip_calc', is_flag=True, default=False,
              help='Skip actual calculation.')
@click.option('--table', required=True,
              help='Table to use for input as well as copy (measurement, measurement_anthro.')
@click.option('--person', required=False, default='person',
              help='name of the person table')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.argument('dburi')
def run_weight_z(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, neg_ids, skip_calc, table,
                 person, model_version, idname, dburi):
    """Run Weight-Z derivation.

    The steps are:

      - Create the config file.
      - Create the output table.
      - Run the derivation.
      - Add indexes to output table
      - Add measurement ids
      - Add concept names
      - Copy Weight-Z measurements to dcc_pedsnet
      - Vacuum the output table

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)
    config_file = site + "_wtz_temp.conf"

    ids = True
    if noids:
        ids = False

    indexes = True
    if noindexes:
        indexes = False

    concept = True
    if noconcept:
        concept = False

    from pedsnetdcc.z_score import run_z_calc
    success = run_z_calc('wt_z', config_file, conn_str, site, copy, ids, indexes, concept, neg_ids,
                         skip_calc, table, person, password, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name')
@click.option('--table', required=True,
              help='Table to use for copy (measurement, measurement_anthro.')
@click.argument('dburi')
def copy_weight_z(pwprompt, searchpath, site, table, dburi):
    """Copy Weight_Z table to dcc_pedsnet.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.z_score import copy_z_dcc
    success = copy_z_dcc('wt_z', conn_str, site, table, searchpath)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for the config file.')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to dcc_pedsnet.')
@click.option('--noids', is_flag=True, default=False,
              help='DO NOT add measurement ids.')
@click.option('--noindexes', is_flag=True, default=False,
              help='DO NOT add indexes.')
@click.option('--noconcept', is_flag=True, default=False,
              help='DO NOT add concept names.')
@click.option('--neg_ids', is_flag=True, default=False,
              help='Use negative ids.')
@click.option('--skip_calc', is_flag=True, default=False,
              help='Skip actual calculation.')
@click.option('--table', required=True,
              help='Table to use for input as well as copy (measurement, measurement_anthro.')
@click.option('--person', required=False, default='person',
              help='name of the person table')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.argument('dburi')
def run_ht_wt_z(pwprompt, searchpath, site, copy, noids, noindexes, noconcept, neg_ids, skip_calc, table,
                person, model_version, idname, dburi):
    """Run height-z and weight-z.

    The steps are:

      - Run HeightZ.
      - Run WeightZ

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    ids = True
    if noids:
        ids = False

    indexes = True
    if noindexes:
        indexes = False

    concept = True
    if noconcept:
        concept = False

    from pedsnetdcc.z_score import run_z_calc

    config_file = site + "_htz_temp.conf"
    success = run_z_calc('ht_z', config_file, conn_str, site, copy, ids, indexes, concept, neg_ids,
                         skip_calc, table, person, password, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    config_file = site + "_wtz_temp.conf"
    success = run_z_calc('wt_z', config_file, conn_str, site, copy, ids, indexes, concept, neg_ids,
                         skip_calc, table, person, password, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for derivation.')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to dcc_pedsnet')
@click.option('--neg_ids', is_flag=True, default=False,
              help='Use negative ids.')
@click.option('--no_ids', is_flag=True, default=False,
              help='Do not assign ids.')
@click.option('--no_concept', is_flag=True, default=False,
              help='Do not add concept names.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.argument('dburi')
def run_drug_era(pwprompt, searchpath, site, copy, neg_ids, no_ids, no_concept, model_version, idname, dburi):
    """Run Drug Era derivation.

    The steps are:

      - Run the derivation.
      - Add ids
      - Add concept names
      - Copy to dcc_pedsnet
      - Vacuum the output table

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.era import run_era
    success = run_era("drug", conn_str, site, copy, neg_ids, no_ids, no_concept, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for derivation.')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to dcc_pedsnet')
@click.option('--neg_ids', is_flag=True, default=False,
              help='Use negative ids.')
@click.option('--no_ids', is_flag=True, default=False,
              help='Do not assign ids.')
@click.option('--no_concept', is_flag=True, default=False,
              help='Do not add concept names.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.argument('dburi')
def run_drug_scdf_era(pwprompt, searchpath, site, copy, neg_ids, no_ids, no_concept, model_version, idname, dburi):
    """Run Drug Era derivation.

    The steps are:

      - Run the derivation.
      - Add ids
      - Add concept names
      - Copy to dcc_pedsnet
      - Vacuum the output table

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.era import run_era
    success = run_era("drug_scdf", conn_str, site, copy, neg_ids, no_ids, no_concept, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name')
@click.argument('dburi')
def copy_drug_era(pwprompt, searchpath, site, dburi):
    """Copy Drug Era table to dcc_pedsnet.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None
    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.era import copy_era_dcc
    success = copy_era_dcc("drug", conn_str, site, searchpath)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for derivation.')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to dcc_pedsnet')
@click.option('--neg_ids', is_flag=True, default=False,
              help='Use negative ids.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.option('--notable', is_flag=True, default=False,
              help='Skip fill table when exists.')
@click.option('--noids', is_flag=True, default=False,
              help='Skip ids if already exist.')
@click.option('--nopk', is_flag=True, default=False,
              help='Skip primary keys if already exist.')
@click.option('--novac', is_flag=True, default=False,
              help='Skip vaccuum if already done.')
@click.option('--size', required=False, default='5000',
              help='size of the group of persons processed at a time')
@click.argument('dburi')
def run_r_drug_era(pwprompt, searchpath, site, copy, neg_ids, model_version, idname, notable, noids, nopk, novac, size, dburi):
    """Run Drug Era derivation.

    The steps are:

      - Run the derivation.
      - Add ids
      - Add concept names
      - Copy to dcc_pedsnet
      - Vacuum the output table

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.r_drug_era import run_r_drug_era
    success = run_r_drug_era(conn_str, site, copy, neg_ids, searchpath, password, model_version, idname,
                             notable, noids, nopk, novac, size)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for derivation.')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to dcc_pedsnet')
@click.option('--neg_ids', is_flag=True, default=False,
              help='Use negative ids.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.option('--notable', is_flag=True, default=False,
              help='Skip fill table when exists.')
@click.option('--noids', is_flag=True, default=False,
              help='Skip ids if already exist.')
@click.option('--nopk', is_flag=True, default=False,
              help='Skip primary keys if already exist.')
@click.option('--novac', is_flag=True, default=False,
              help='Skip vaccuum if already done.')
@click.option('--size', required=False, default='5000',
              help='size of the group of persons processed at a time')
@click.argument('dburi')
def run_r_drug_era_test(pwprompt, searchpath, site, copy, neg_ids, model_version, idname, notable, noids, nopk, novac,
                        size, dburi):
    """Run Drug Era derivation.

    The steps are:

      - Run the derivation.
      - Add ids
      - Add concept names
      - Copy to dcc_pedsnet
      - Vacuum the output table

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.r_drug_era import run_r_drug_era
    success = run_r_drug_era(conn_str, site, copy, neg_ids, searchpath, password, model_version, idname,
                             notable, noids, nopk, novac, size, True)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for derivation.')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to dcc_pedsnet')
@click.option('--neg_ids', is_flag=True, default=False,
              help='Use negative ids.')
@click.option('--no_ids', is_flag=True, default=False,
              help='Do not assign ids.')
@click.option('--no_concept', is_flag=True, default=False,
              help='Do not add concept names.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.option('--notable', is_flag=True, default=False,
              help='Skip fill table when exists.')
@click.option('--nopk', is_flag=True, default=False,
              help='Skip primary keys if already exist.')
@click.option('--novac', is_flag=True, default=False,
              help='Skip vaccuum if already done.')
@click.argument('dburi')
def run_condition_era(pwprompt, searchpath, site, copy, neg_ids, no_ids, no_concept, model_version, idname,
                      notable, nopk, novac, dburi):
    """Run Condition Era derivation.

    The steps are:

      - Run the derivation.
      - Add ids
      - Add concept names
      - Copy to dcc_pedsnet
      - Vacuum the output table

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.era import run_era
    success = run_era("condition", conn_str, site, copy, neg_ids, no_ids, no_concept, searchpath, model_version,
                      idname, notable, nopk, novac)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name')
@click.argument('dburi')
def copy_condition_era(pwprompt, searchpath, site, dburi):
    """Copy Condition Era table to dcc_pedsnet.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.era import copy_era_dcc
    success = copy_era_dcc("condition", conn_str, site, searchpath)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for the config file.')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to dcc_pedsnet.')
@click.option('--neg_ids', is_flag=True, default=False,
              help='Use negative ids.')
@click.option('--no_ids', is_flag=True, default=False,
              help='Do not assign ids.')
@click.option('--no_concept', is_flag=True, default=False,
              help='Do not add concept names.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.argument('dburi')
def run_drug_condition_era(pwprompt, searchpath, site, copy, neg_ids, no_ids, no_concept, model_version, idname, dburi):
    """Run Drug Condition.

    The steps are:

      - Run Drug Era
      - Run Condition Era

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.era import run_era
    success = run_era("drug", conn_str, site, copy, neg_ids, no_ids, no_concept, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    from pedsnetdcc.era import run_era
    success = run_era("condition", conn_str, site, copy, neg_ids, no_ids, no_concept, searchpath, model_version, idname)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name')
@click.option('--table', required=True,
              help='Table to use for copy (measurement, measurement_anthro.')
@click.argument('dburi')
def copy_to_dcc(pwprompt, searchpath, site, table, dburi):
    """Copy bmi, bmiz, ht_z, wt_z, drug_era and condition_era tables to dcc_pedsnet.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.z_score import copy_z_dcc

    from pedsnetdcc.bmi import copy_bmi_dcc
    success = copy_bmi_dcc(conn_str, site, table)

    if not success:
        sys.exit(1)

    success = copy_z_dcc('bmiz', conn_str, site, table, searchpath)

    if not success:
        sys.exit(1)

    success = copy_z_dcc('ht_z', conn_str, site, table, searchpath)

    if not success:
        sys.exit(1)

    success = copy_z_dcc('wt_z', conn_str, site, table, searchpath)

    if not success:
        sys.exit(1)

    from pedsnetdcc.era import copy_era_dcc
    success = copy_era_dcc("drug", conn_str, site, searchpath)

    if not success:
        sys.exit(1)

    success = copy_era_dcc("condition", conn_str, site, searchpath)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--dcc', is_flag=True, default=False,
              help='partition dcc vs site measurement table')
@click.option('--site3', is_flag=True, default=False,
              help='partition site measurement table in 3')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.argument('dburi')
def partition_measurement(pwprompt, searchpath, dcc, site3, model_version, dburi):
    """Partition measurement using measurement_anthro, measurement_labs, and measurement_vitals split tables

    The steps are:

    - Truncate Measurement Table
    - Alter split tables to add check constraints by measurement concept id
    - Alter split tables to inherit from the measurement table
    - Create trg_insert_measurement function to route measurements to correct split table
    - Add before insert trigger measurement_insert to measurement table

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.partition_measurement import partition_measurement_table
    success = partition_measurement_table(conn_str, model_version, searchpath, dcc, site3)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--dcc', is_flag=True, default=False,
              help='unpartition dcc vs site measurement table')
@click.option('--site3', is_flag=True, default=False,
              help='unpartition site measurement table in 3')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.argument('dburi')
def unpartition_measurement(pwprompt, searchpath, dcc, site3, model_version, dburi):
    """Undo Partition measurement using measurement_anthro, measurement_labs, and measurement_vitals split tables

    The steps are:

    - Drop before insert trigger measurement_insert to measurement table
    - Drop check constraints by measurement concept id
    - Drop inherit from the measurement table

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.partition_measurement import unpartition_measurement_table
    success = unpartition_measurement_table(conn_str, model_version, searchpath, dcc, site3)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--source_schema', required=True,
              help='Schema where the tables are located')
@click.option('--target_schema', required=True,
              help='Schema where the views should be located')
@click.option('--file_name', required=True,
              help='File name for SQL output')
def create_oracle_views_sql(model_version, source_schema, target_schema, file_name):
    """Create lower case views for Oracle.

    The steps are:

      - Loop thru all the tables in the model.
      - Lopp thru all the columns in thhe table
      - Create view and grant SQL statements
      - Output SQL statement to output file

    """

    from pedsnetdcc.views import create_oracle_views
    success = create_oracle_views(model_version, source_schema, target_schema, file_name)

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
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--name', required=True,
              help='Alternate name for database name')
@click.option('--addsites', required=False, default='',
              help='sites to add delimited by ,')
@click.option('--new', type=bool, is_flag=True, default=False,
              help='for db version > 10')
@click.option('--limit', type=bool, is_flag=True, default=False,
              help='limit access to super users')
@click.option('--dcc-only', type=bool, is_flag=True, default=False,
              help='Only create schemas for the dcc.')
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.argument('dburi')
def prepdb_altname(model_version, name, addsites, new, limit, dcc_only, pwprompt, dburi):
    """Create a database and schemas.

    The database should be specified using a model version and a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    from pedsnetdcc.prepdb import prepare_database_altname

    if new:
        new = True
    else:
        new = False

    if limit:
        limit = True
    else:
        limit = False

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, password=password)
    success = prepare_database_altname(model_version, conn_str, name, addsites, new, limit, update=False,
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

    success = map_external_ids(conn_str, str(in_file), str(out_file), str(table_name), search_path)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.argument('dburi', required=True)
@click.option('--pwprompt', '-p', is_flag=True, default=False)
@click.option('--full', '-f', is_flag=True, default=False)
def grant_permissions(dburi, pwprompt, full):
    """Grants the appropriate permissions for all schemas and tables, as well as vocabulary schemas and tables

    This is normally set during prepdb and transform, but this command can be used to manually set permissions
    if there is an issue

    The database should be specified using a model version and a dburi

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    from pedsnetdcc.permissions import grant_schema_permissions, grant_vocabulary_permissions, \
        grant_loading_user_permissions

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, password=password)

    if full:
        grant_loading_user_permissions(conn_str)

    grant_schema_permissions(conn_str)
    grant_vocabulary_permissions(conn_str)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--force', is_flag=True, default=False,
              help='Ignore any "already exists" errors from the database.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.argument('dburi')
def vocab_indexes(pwprompt, searchpath, force, model_version, dburi):
    """
    Adjust the vocabulary indexes to the new specifications
    """
    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.transform_runner import run_vocab_indexes
    success = run_vocab_indexes(conn_str, model_version, searchpath, force)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for the config file.')
@click.option('--package', required=True,
              help='R package to run.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to output.')
@click.argument('dburi')
def run_r_query(pwprompt, searchpath, site, package, model_version, copy, dburi):
    """Run R Script.

    The steps are:

      - Create the Argos file.
      - Run the script.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)
    config_file = site + "_" + package + "_argos_temp.json"

    from pedsnetdcc.r_query import run_r_query
    success = run_r_query(config_file, conn_str, site, package, password, searchpath, model_version, copy)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for the config file.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to output.')
@click.argument('dburi')
def run_r_lab_loinc(pwprompt, searchpath, site, model_version, copy, dburi):
    """Run Lab Loinc R Script and if successful do post tasks.

    The steps are:

      - Create the Argos file.
      - Run the script.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """
    package = 'lab_loinc'
    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)
    config_file = site + "_" + package + "_argos_temp.json"

    from pedsnetdcc.r_query import run_r_query
    success = run_r_query(config_file, conn_str, site, package, password, searchpath, model_version, copy)

    if not success:
        sys.exit(1)

    from pedsnetdcc.lab_loinc import run_post_lab_loinc
    success = run_post_lab_loinc(conn_str, site, searchpath)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for the config file.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--idname', required=False, default='dcc',
              help='name of the id (ex: onco')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to output.')
@click.argument('dburi')
def run_r_obs_covid(pwprompt, searchpath, site, model_version, idname, copy, dburi):
    """Run R Script.

    The steps are:

      - Create the Argos file.
      - Run the script.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.r_obs_covid import run_r_obs_covid
    success = run_r_obs_covid(conn_str, site, password, searchpath, model_version, idname, copy)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--site', required=True,
              help='PEDSnet site name for the config file.')
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--copy', is_flag=True, default=False,
              help='Copy results to drug_exposure.')
@click.argument('dburi')
def run_r_dose(pwprompt, searchpath, site, model_version, copy, dburi):
    """Run R Script.

    The steps are:

      - Create the Argos file.
      - Run the script.

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.r_dose import run_r_dose
    success = run_r_dose(conn_str, site, password, searchpath, model_version, copy)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--source_schema', required=True,
              help='Schema where the source tables are located')
@click.option('--target_schema', required=True,
              help='Schema where the subset tables should be located')
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.option('--concept_create', is_flag=True, default=False,
              help='Create concept index replacement tables.')
@click.option('--drug_dose', is_flag=True, default=False,
              help='Copy drug dose tables.')
@click.option('--covid_obs', is_flag=True, default=False,
              help='Copy COVID observation derivation table.')
@click.option('--inc_hash', is_flag=True, default=False,
              help='Include hash_token table.')
@click.option('--split_measure', is_flag=True, default=False,
              help='Split/partition the measurement table.')
@click.option('--index_create', is_flag=True, default=False,
              help='Create indexes on tables.')
@click.option('--fk_create', is_flag=True, default=False,
              help='Create FKs on tables.')
@click.option('--notable', is_flag=True, default=False,
              help='Skip fill table when exists.')
@click.option('--nopk', is_flag=True, default=False,
              help='Skip primary keys.')
@click.option('--nonull', is_flag=True, default=False,
              help='Skip set columns not null.')
@click.option('--force', is_flag=True, default=False,
              help='Ignore any "already exists" errors from the database.')
@click.option('--cohort_table', required=True,
              help='Name of the cohort table where the person_ids are located')
@click.argument('dburi')
def subset_by_cohort(searchpath, pwprompt, dburi, model_version, force, source_schema, target_schema, cohort_table,
                     concept_create, drug_dose, covid_obs, inc_hash, split_measure, index_create,
                     fk_create, notable, nopk, nonull):
    """Create tables for subset based on a cohort/person_id table

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.subset_by_cohort import run_subset_by_cohort
    success = run_subset_by_cohort(conn_str, model_version, source_schema, target_schema, cohort_table,
                         concept_create, drug_dose, covid_obs, inc_hash, index_create, fk_create, notable,
                         nopk, nonull, force)

    if not success:
        sys.exit(1)

    if split_measure:
        from pedsnetdcc.split_measurement import split_measurement_table
        success = split_measurement_table(conn_str, False, False, model_version, searchpath)
        if success:
            from pedsnetdcc.partition_measurement import partition_measurement_table
            success = partition_measurement_table(conn_str, model_version, searchpath, False, True)

    if not success:
        sys.exit(1)

    sys.exit(0)


@pedsnetdcc.command()
@click.option('--model-version', '-v', required=True,
              help='PEDSnet model version (e.g. 2.3.0).')
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password.')
@click.option('--searchpath', '-s', help='Schema search path in database.')
@click.argument('dburi')
def create_index_replace(searchpath, pwprompt, dburi, model_version):
    """Create index replacement tables

    The database should be specified using a DBURI:

    \b
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    password = None

    if pwprompt:
        password = click.prompt('Database password', hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)

    from pedsnetdcc.subset_by_cohort import run_index_replace
    success = run_index_replace(conn_str, model_version)

    if not success:
        sys.exit(1)

    sys.exit(0)


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

    # TODO: do we need to validate the primary schema at all?
    schema = primary_schema(searchpath)

    tmp_schema = schema + '_' + 'transformed'
    for sql, msg in _transform_select_sql(model_version, site, tmp_schema):
        print("msg: " + sql)


if __name__ == '__main__':
    pedsnetdcc()