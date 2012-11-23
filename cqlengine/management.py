from cqlengine.connection import get_connection


def create_keyspace(name):
    con = get_connection(None)
    cur = con.cursor()
    cur.execute("""create keyspace {}
       with strategy_class = 'SimpleStrategy'
       and strategy_options:replication_factor=1;""".format(name))

def delete_keyspace(name):
    pass

def create_column_family(model):
    #TODO: check for existing column family
    #construct query string
    qs = ['CREATE TABLE {}'.format(model.column_family_name())]

    #add column types
    pkeys = []
    qtypes = []
    def add_column(col):
        s = '{} {}'.format(col.db_field_name, col.db_type)
        if col.primary_key: pkeys.append(col.db_field_name)
        qtypes.append(s)
    for name, col in model._columns.items():
        add_column(col)

    qtypes.append('PRIMARY KEY ({})'.format(', '.join(pkeys)))

    qs += ['({})'.format(', '.join(qtypes))]
    qs = ' '.join(qs)

    #add primary key
    conn = get_connection(model.keyspace)
    cur = conn.cursor()
    try:
        cur.execute(qs)
    except BaseException, e:
        if 'Cannot add already existing column family' not in e.message:
            raise


def delete_column_family(model):
    #TODO: check that model exists
    conn = get_connection(model.keyspace)
    cur = conn.cursor()
    cur.execute('drop table {};'.format(model.column_family_name()))

    pass
