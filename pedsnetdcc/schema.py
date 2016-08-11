from pedsnetdcc.db import Statement
from pedsnetdcc.utils import DatabaseError, pg_error


def create_schema_statement(schema):
    """Return Statement to create a schema.

    :param schema: database schema
    :return: statement object
    :rtype Statement
    """
    sql = 'CREATE SCHEMA {0}'.format(schema)
    return Statement(sql)


def create_schema(conn_str, schema, force=False):
    """Create a schema in the database defined by conn_str.

    In `force` mode, error 42P06 ("duplicate_schema") is ignored.

    :param conn_str: pq/libpq connection string
    :param schema: database schema
    :param force: if true, ignore if the schema already exists
    :return: True
    :rtype bool:
    :raise: DatabaseError
    """
    stmt = create_schema_statement(schema).execute(conn_str)
    if stmt.err:
        if force and pg_error(stmt) == 'DUPLICATE_SCHEMA':
            return True
        tpl = 'failed to create schema `{0}`: {1}'
        raise DatabaseError(tpl.format(schema, stmt.err))
    return True


def drop_schema_statement(schema, if_exists, cascade):
    """Return a Statement to drop a schema.

    If `if_exists` is true, an `IF EXISTS` clause is used.

    If `cascade` is true, a cascading drop is performed.

    :param str schema: database schema
    :param bool if_exists: optionally conditional using `IF EXISTS`
    :param bool cascade: optionally perform a cascading drop
    :return: statement object
    :rtype Statement:
    """
    if_exists_clause = 'IF EXISTS' if if_exists else ''
    cascade_clause = 'CASCADE' if cascade else ''
    sql = 'DROP SCHEMA {if_exists} {schema} {cascade}'.format(
        if_exists=if_exists_clause, schema=schema, cascade=cascade_clause)
    return Statement(sql)


def drop_schema(conn_str, schema, if_exists, cascade):
    """Drop a schema in the database defined by conn_str.

    If `if_exists` is true, an `IF EXISTS` clause is used, and an error
    will not be raised if the schema does not exist.

    If `cascade` is true, a cascading drop is performed, so all tables will
    be dropped from the schema prior to the schema being dropped.

    :param str conn_str: pq/libpq connection string
    :param str schema: database schema
    :param bool if_exists: optionally conditional using `IF EXISTS`
    :param bool cascade: optionally perform a cascading drop
    :return: True
    :rtype bool:
    :raise: DatabaseError
    """
    stmt = drop_schema_statement(schema, if_exists, cascade).execute(conn_str)
    if stmt.err:
        tpl = 'failed to create schema `{0}`: {1}'
        raise DatabaseError(tpl.format(schema, stmt.err))


def primary_schema(search_path):
    """Return the first schema in the search_path.

    :param str search_path: PostgreSQL search path of comma-separated schemas
    :return: first schema in path
    :raise: ValueError
    """
    search_path = search_path.strip()
    if not search_path:
        schemas = None
    else:
        schemas = search_path.split(',')
    if not schemas:
        raise ValueError('search_path must be non-empty')
    return schemas[0].strip()


def schema_exists(conn_str, schema):
    """Return True if schema exists in database; False otherwise.

    :param str conn_str: pq connection string
    :param str schema: name of schema to check
    :return: whether schema exists or not
    :rtype: bool
    """
    sql = "select 1 from information_schema.schemata " \
        "where schema_name = '{}'".format(schema)
    stmt = Statement(sql)
    stmt.execute(conn_str)
    if stmt.err:
        tpl = 'error detecting schema {sch} ({sql}): {err}'
        raise DatabaseError(tpl.format(sch=schema, sql=stmt.sql, err=stmt.err))
    return len(stmt.data) > 0


def tables_in_schema(conn_str, schema):
    """Return set of tables in `schema`

    :param str conn_str: pq connection string
    :param str schema: name of schema to list tables for
    :return: list of table names
    :rtype: list(str)
    """
    sql = "select table_name from information_schema.tables " \
        "where table_schema = '{}'".format(schema)
    stmt = Statement(sql)
    stmt.execute(conn_str)
    if stmt.err:
        tpl = 'error listing tables for schema {sch} ({sql}): {err}'
        raise DatabaseError(tpl.format(sch=schema, sql=stmt.sql, err=stmt.err))
    tables = set()
    for row in stmt.data:
        tables.add(row[0])
    return tables
