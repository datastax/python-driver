"""
Helper module to populate a dummy Cassandra tables with data.
"""

from tests.integration.datatype_utils import PRIMITIVE_DATATYPES, get_sample


def create_table_with_all_types(table_name, session, N):
    """
    Method that given a table_name and session construct a table that contains
    all possible primitive types.

    :param table_name: Name of table to create
    :param session: session to use for table creation
    :param N: the number of items to insert into the table

    :return: a list of column names
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

    for key in range(N):
        params = get_all_primitive_params(key)

        # insert into table as a simple statement
        columns_string = ', '.join(col_names)
        placeholders = ', '.join(["%s"] * len(col_names))
        session.execute("INSERT INTO {0} ({1}) VALUES ({2})".format(
                            table_name, columns_string, placeholders), params, timeout=120)
    return col_names


def get_all_primitive_params(key):
    """
    Simple utility method used to give back a list of all possible primitive data sample types.
    """
    params = [key]
    for datatype in PRIMITIVE_DATATYPES:
        # Also test for empty strings
        if key == 1 and datatype == 'ascii':
            params.append('')
        else:
            params.append(get_sample(datatype))
    return params


def get_primitive_datatypes():
    return ['int'] + list(PRIMITIVE_DATATYPES)
