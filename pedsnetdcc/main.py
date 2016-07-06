import click
import logging
import sys

from pedsnetdcc import __version__
from pedsnetdcc.utils import make_conn_str
from pedsnetdcc.dict_logging import DictLogFilter

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

    \b')
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&..]
    """

    from pedsnetdcc.sync_observation_period import sync_observation_period

    password = None

    if pwprompt:
        password = click.prompt(hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)
    sync_observation_period(conn_str)


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
        password = click.prompt(hide_input=True)

    conn_str = make_conn_str(dburi, searchpath, password)
    check_fact_relationship(conn_str, output, poolsize)


if __name__ == '__main__':
    pedsnetdcc()
