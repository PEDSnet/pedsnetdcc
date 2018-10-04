from pedsnetdcc.utils import (stock_metadata)
import os


def create_oracle_views(model_version, source_schema, target_schema, view_file_name):
    """Create SQL for `select` statement transformations.

    The `search_path` only needs to contain the source schema; the target
    schema is embedded in the SQL statements.

    The returned statements are not sufficient for the transformation;
    the `pre_transform` needs to be run beforehand.

    Returns a set of tuples of (sql_string, msg), where msg is a description
    for the operation to be carried out by the sql_string.

    :param model_version:   PEDSnet model version, e.g. 2.3.0
    :param str source_schema:   schema in which the tables are located
    :param str target_schema:   schema in which to create the views
    :param str view_file_name:  name of file to put the output in
    :returns:   True if the function succeeds
    :rtype: bool
    """
    metadata = stock_metadata(model_version)
    target_schema = target_schema.upper()
    source_schema = source_schema.upper()
    table_list = []
    create_dict = {}
    grant_dict = {}

    for table_name,table in metadata.tables.items():
        table_list.append(table_name)
        create = 'create or replace view "' + target_schema + '"."' + table_name + '" as select '
        for column_name,column in table.c.items():
            create += '"' + column_name.upper() + '" as "' + column_name + '", '
        create = create[:-2]
        create = create + ' from "' + source_schema + '"."' + table_name.upper() + '";'
        create_dict[table_name] = create
        grant = 'grant select on "' + target_schema + '"."' + table_name + '" to "' + target_schema + '";'
        grant_dict[table_name] = grant

    with open(view_file_name, 'w') as view_file:
        for table_name in sorted(table_list):
            view_file.write(create_dict[table_name] + os.linesep)

        for table_name in sorted(table_list):
            view_file.write(grant_dict[table_name] + os.linesep)

    # If reached without error, then success!
    return True
