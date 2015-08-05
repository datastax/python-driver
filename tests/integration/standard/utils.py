"""
Helper module to populate a dummy Cassandra tables with data.
"""

from tests.integration.datatype_utils import PRIMITIVE_DATATYPES, get_sample

def create_table_with_all_types(table_name, session):
    """
    Method that given a table_name and session construct a table that contains
    all possible primitive types.

    :param table_name: Name of table to create
    :param session: session to use for table creation
    :return: a string containing the names of all the columns.
             This can be used to query the table.
    """
    # create table
    alpha_type_list = ["primkey int PRIMARY KEY"]
    col_names = ["primkey"]
    start_index = ord('a')
    for i, datatype in enumerate(PRIMITIVE_DATATYPES):
        alpha_type_list.append("{0} {1}".format(chr(start_index + i), datatype))
        col_names.append(chr(start_index + i))

    session.execute("CREATE TABLE {0} ({1})".format(
                        table_name, ', '.join(alpha_type_list)), timeout=120)

    # create the input
    params = get_all_primitive_params()

    # insert into table as a simple statement
    columns_string = ', '.join(col_names)
    placeholders = ', '.join(["%s"] * len(col_names))
    session.execute("INSERT INTO {0} ({1}) VALUES ({2})".format(
                        table_name, columns_string, placeholders), params, timeout=120)
    return columns_string


def get_all_primitive_params():
    """
    Simple utility method used to give back a list of all possible primitive data sample types.
    """
    params = [0]
    for datatype in PRIMITIVE_DATATYPES:
        params.append(get_sample(datatype))
    return params
