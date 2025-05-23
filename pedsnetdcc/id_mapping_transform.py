import logging
import sqlalchemy
from sqlalchemy.sql.expression import cast
import time

from pedsnetdcc import VOCAB_TABLES, FACT_RELATIONSHIP_DOMAINS
from pedsnetdcc.abstract_transform import Transform
from pedsnetdcc.db import Statement, StatementList
from pedsnetdcc.dict_logging import secs_since
from pedsnetdcc.utils import check_stmt_err, check_stmt_data

# Pre-transform ID map creation statements.

new_id_count_sql = """SELECT COUNT(*)
FROM {table_name} LEFT JOIN {map_table_name} ON {pkey_name} = site_id
WHERE site_id IS NULL"""

new_id_count_msg = "counting new IDs needed for {table_name}"

lock_last_id_sql = """LOCK {last_id_table_name}"""
lock_last_id_msg = "locking {table_name} last ID tracking table for update"

update_last_id_sql = """UPDATE {last_id_table_name} AS new
SET last_id = new.last_id + '{new_id_count}'::bigint
FROM {last_id_table_name} AS old RETURNING old.last_id, new.last_id"""
update_last_id_msg = "updating {table_name} last ID tracking table to reserve new IDs"  # noqa

insert_new_maps_sql = """INSERT INTO {map_table_name} (site_id, {id_name}_id)
SELECT {pkey_name}, row_number() over (range unbounded preceding) + '{old_last_id}'
FROM {table_name} LEFT JOIN {map_table_name} on {pkey_name} = site_id
WHERE site_id IS NULL"""  # noqaxf

insert_new_maps_msg = "inserting new {table_name} ID mappings into map table"

# Mapping and last ID table naming conventions.

map_table_name_tmpl = "{table_name}_ids"
last_id_table_name_tmpl = "{id_name}_{table_name}_id"

CREATE_ID_MAP_INDEX_SQL = """CREATE INDEX IF NOT EXISTS {0}_maps_idx_site ON {0}_ids (site_id)"""

logger = logging.getLogger(__name__)


class IDMappingTransform(Transform):
    @classmethod
    def pre_transform(cls, conn_str, metadata, id_name='dcc', id_type='BigInteger'):
        """Generate DCC IDs in the database.

        See also Transform.pre_transform.

        :raises ValueError: if any of the non-vocabulary tables in metadata
                            have composite primary keys
        """

        logger.info({'msg': 'starting ID mapping pre-transform'})
        starttime = time.time()

        for table_name in set(metadata.tables.keys()) - set(VOCAB_TABLES):

            # Fact relationship and lab_site_mapping tables have no primary key to map.
            if table_name == 'fact_relationship' or table_name == 'lab_site_mapping':
                continue

            # Drug Strength table has two primary keys (really belongs in vocab).
            if table_name == 'drug_strength':
                continue

            # Cohort table has two primary keys, fix in future
            if table_name == 'cohort':
                continue

            # Get table object and start to build tpl_vars map, which will be
            # used throughout for formatting SQL statements.

            table = metadata.tables[table_name]
            tpl_vars = {'table_name': table_name}

            # Set name of the id
            tpl_vars['id_name'] = id_name

            # Set name of the id type
            tpl_vars['id_type'] = id_type

            # In some versions the death table has a primary key constraint
            # on the person_id column.
            if (table_name == 'death' and
                    'person_id' in table.primary_key.columns):
                continue

            # Error if the table has more than one primary key column.
            if len(table.primary_key.columns) > 1:
                err = ValueError('cannot generate IDs for multi-column primary'
                                 ' key on table {0}'.format(table_name))
                logger.error({'msg': 'exiting ID mapping pre-transform',
                              'table': table_name, 'err': err})
                raise err

            # Error if the table has no primary key column (except death).
            if len(table.primary_key.columns) == 0:
                # In some versions the death table has no primary key.
                if table_name == 'death':
                    continue

                err = ValueError('cannot generate IDs for table {0} with no'
                                 ' primary key'.format(table_name))
                logger.error({'msg': 'exiting ID mapping pre-transform',
                              'table': table_name, 'err': err})
                raise err

            # Get primary key, mapping table, and last id tracking table names.
            # The mapping table and last id tracking table names are defined
            # by convention.

            tpl_vars['pkey_name'] = list(table.primary_key.columns.keys())[0]
            tpl_vars['map_table_name'] = map_table_name_tmpl.format(**tpl_vars)
            tpl_vars['last_id_table_name'] = last_id_table_name_tmpl.format(**tpl_vars)

            # Build the statement to count how many new ID mappings are needed.
            new_id_count_stmt = Statement(new_id_count_sql.format(**tpl_vars),
                                          new_id_count_msg.format(**tpl_vars))

            # Execute the new ID mapping count statement and ensure it didn't
            # error and did return a result.
            new_id_count_stmt.execute(conn_str)
            check_stmt_err(new_id_count_stmt, 'ID mapping pre-transform')
            check_stmt_data(new_id_count_stmt, 'ID mapping pre-transform')

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
                check_stmt_err(stmt, 'ID mapping pre-transform')
            check_stmt_data(update_last_id_stmts[1],
                            'ID mapping pre-transform')

            # Get the old and new last IDs from the second update statement.
            tpl_vars['old_last_id'] = update_last_id_stmts[1].data[0][0]
            tpl_vars['new_last_id'] = update_last_id_stmts[1].data[0][1]
            logger.info({'msg': 'last ID tracking table updated',
                         'table': table_name,
                         'old_last_id': tpl_vars['old_last_id'],
                         'new_last_id': tpl_vars['new_last_id']})

            # Build statement to generate new ID maps.
            insert_new_maps_stmt = Statement(
                insert_new_maps_sql.format(**tpl_vars),
                insert_new_maps_msg.format(**tpl_vars))

            # Execute new map generation statement and ensure it didn't error.
            insert_new_maps_stmt.execute(conn_str)
            check_stmt_err(insert_new_maps_stmt, 'id mapping pre-transform')
            logger.info({'msg': 'generated new ID mappings',
                         'table': table_name,
                         'count': insert_new_maps_stmt.rowcount,
                         'elapsed': secs_since(starttime)})

            index_stmt = Statement(CREATE_ID_MAP_INDEX_SQL.format(table))
            index_stmt.execute(conn_str)
            check_stmt_err(index_stmt, 'id mapping indexes')
            logger.info({'msg': 'created ID mapping indexes',
                         'table': table_name,
                         'elapsed': secs_since(starttime)})

    @classmethod
    def modify_select(cls, metadata, table_name, select, join, id_name='dcc', id_type='BigInteger'):
        """Alter foreign key columns to get mapped DCC IDs.

        The primary key and each foreign key which points at a data table
        (i.e., not a vocabulary table) is altered to instead get the dcc_id's
        from the mapping table, which requires a join to be added.
        """

        # Get table object.
        table = metadata.tables[table_name]

        # Data from PCORnet may not be numeric
        site_id_type = sqlalchemy.BigInteger
        if id_type == 'String':
            site_id_type = sqlalchemy.String(256)

        # Raise error if attempted on a multi-column primary key table.
        # Make exception for Cohort table
        if len(table.primary_key.columns) > 1 and table_name != 'cohort':
            raise ValueError('cannot map IDs for multi-column primary key'
                             ' on table {0}'.format(table_name))

        # Skip primary key mapping if there is none (fact_relationship
        # also skip Cohort table as it currently has multi-column PK and pre
        # 2.3 death tables). Also, in some versions, the death table has a
        # primary key constraint on the person_id column.
        if not (len(table.primary_key.columns) == 0 or table_name == 'cohort' or (table_name == 'death'
                and 'person_id' in table.primary_key.columns)):

            # Get primary key name and mapping table name, defined by
            # convention.
            pkey_name = list(table.primary_key.columns.keys())[0]
            map_table_name = map_table_name_tmpl.format(table_name=table_name)

            # Construct table object for mapping table, if necessary.
            if map_table_name not in metadata.tables:
                map_table = sqlalchemy.Table(
                    map_table_name, metadata,
                    sqlalchemy.Column(id_name + '_id', sqlalchemy.BigInteger),
                    sqlalchemy.Column('site_id', site_id_type))
            else:
                map_table = metadata.tables[map_table_name]

            map_table_alias = map_table.alias()
            join = join.join(map_table_alias, table.c[pkey_name] ==
                             map_table_alias.c['site_id'])

            # Create a new select object, because we need to replace the
            # primary key column with the joined mapping table dcc_id column.
            new_select = sqlalchemy.select()

            # Get rid of the primary key column.
            for c in select.inner_columns:
                if c.name != pkey_name:
                    new_select.append_column(c)

            # Add the mapping table dcc_id column as the primary key column.
            new_select.append_column(map_table_alias.c[id_name + '_id'].label(pkey_name))

            # Add the original site primary key as the site_id column.
            new_select.append_column(table.c[pkey_name].label('site_id'))

            # Put the new select object in the original var for further use.
            select = new_select

        # Add a mapping join and column substitution for each foreign key.
        for fkey in table.foreign_key_constraints:

            ref_table_name = fkey.referred_table.name

            # Do not operate on foreign keys to vocabulary tables.
            if ref_table_name in VOCAB_TABLES:
                continue

            # Get foreign key name and mapping table name, defined by
            # convention.
            fkey_name = fkey.column_keys[0]
            map_table_name = map_table_name_tmpl.\
                format(table_name=ref_table_name)

            # Construct table object for mapping table, if necessary.
            if map_table_name not in metadata.tables:
                map_table = sqlalchemy.Table(
                    map_table_name, metadata,
                    sqlalchemy.Column(id_name + '_id', sqlalchemy.BigInteger),
                    sqlalchemy.Column('site_id', site_id_type))
            else:
                map_table = metadata.tables[map_table_name]

            # Make the join an outer join if the foreign key is nullable,
            # otherwise rows without this foreign key will not be included in
            # the final product.
            isouter = table.c[fkey_name].nullable

            map_table_alias = map_table.alias()
            # Add a join to the mapping table.
            join = join.join(map_table_alias, cast(table.c[fkey_name], site_id_type) ==
                             map_table_alias.c['site_id'], isouter=isouter)

            # Create a new select object, because we need to replace the
            # foreign key column with the joined mapping table dcc_id column.
            new_select = sqlalchemy.select()

            # Get rid of the foreign key column.
            for c in select.inner_columns:
                if c.name != fkey_name:
                    new_select.append_column(c)

            # Add the mapping table dcc_id column as the foreign key column.
            new_select.append_column(map_table_alias.c[id_name + '_id'].label(fkey_name))

            # Put the new select object back in the original var for next use.
            select = new_select

        # Special handling for fact_relationship.
        if table_name == 'fact_relationship':

            case_1 = {}
            case_2 = {}

            for ref_table_name, domain_concept_id in \
                    FACT_RELATIONSHIP_DOMAINS.items():

                # Get mapping table name, by convention.
                map_table_name = map_table_name_tmpl.\
                    format(table_name=ref_table_name)

                # Construct table object for mapping table, if necessary.
                if map_table_name not in metadata.tables:
                    map_table = sqlalchemy.Table(
                        map_table_name, metadata,
                        sqlalchemy.Column(id_name + '_id', sqlalchemy.BigInteger),
                        sqlalchemy.Column('site_id', site_id_type))
                else:
                    map_table = metadata.tables[map_table_name]

                # Make two aliases of the mapping table for joining to each of
                # the fact_id columns.
                map_table_1 = map_table.alias()
                map_table_2 = map_table.alias()

                # Add two joins to the mapping table
                join = join.join(map_table_1, sqlalchemy.and_(
                                     table.c['fact_id_1'] ==
                                     map_table_1.c['site_id'],
                                     table.c['domain_concept_id_1'] ==
                                     domain_concept_id),
                                 isouter=True)
                join = join.join(map_table_2, sqlalchemy.and_(
                                     table.c['fact_id_2'] ==
                                     map_table_2.c['site_id'],
                                     table.c['domain_concept_id_2'] ==
                                     domain_concept_id),
                                 isouter=True)

                # Add conditions to the case dictionary constructs.
                case_1[domain_concept_id] = map_table_1.c[id_name + '_id']
                case_2[domain_concept_id] = map_table_2.c[id_name + '_id']

            # Create a new select object, because we need to replace the
            # fact_id columns with the case constructs.
            new_select = sqlalchemy.select()

            # Get rid of the fact_id columns.
            for c in select.inner_columns:
                if c.name not in ['fact_id_1', 'fact_id_2']:
                    new_select.append_column(c)

            # Add the case constructs as the new fact_id columns.
            new_select.append_column(sqlalchemy.case(
                case_1, value=table.c['domain_concept_id_1']).
                label('fact_id_1'))
            new_select.append_column(sqlalchemy.case(
                case_2, value=table.c['domain_concept_id_2']).
                label('fact_id_2'))

            # Add the original fact_id columns as the site_id columns.
            new_select.append_column(table.c['fact_id_1'].label('site_id_1'))
            new_select.append_column(table.c['fact_id_2'].label('site_id_2'))

            # Put the new select object in the original var for further use.
            select = new_select

        return select, join

    @classmethod
    def modify_table(cls, metadata, table, id_type='BigInteger'):
        """Helper function to apply the transformation to a table in place.
        See Transform.modify_table for signature.
        """

        site_id_type = sqlalchemy.BigInteger
        if id_type == 'String':
            site_id_type = sqlalchemy.String(256)

        if len(table.primary_key.columns) == 1 and not \
                (table.name == 'death' and 'person_id' in
                 table.primary_key.columns):
            new_col = sqlalchemy.Column('site_id', site_id_type)
            table.append_column(new_col)

        if table.name == 'fact_relationship':
            new_col_1 = sqlalchemy.Column('site_id_1', site_id_type)
            new_col_2 = sqlalchemy.Column('site_id_2', site_id_type)
            table.append_column(new_col_1)
            table.append_column(new_col_2)


