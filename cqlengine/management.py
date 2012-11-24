from cqlengine.connection import get_connection

def create_keyspace(name):
    con = get_connection(None)
    cur = con.cursor()
    cur.execute("""CREATE KEYSPACE {}
       WITH strategy_class = 'SimpleStrategy'
       AND strategy_options:replication_factor=1;""".format(name))

def delete_keyspace(name):
    con = get_connection(None)
    cur = con.cursor()
    cur.execute("DROP KEYSPACE {}".format(name))

def create_column_family(model):
    #construct query string
    cf_name = model.column_family_name()

    conn = get_connection(model.keyspace)
    cur = conn.cursor()

    #check for an existing column family
    ks_info = conn.client.describe_keyspace(model.keyspace)
    if not any([cf_name == cf.name for cf in ks_info.cf_defs]):
        qs = ['CREATE TABLE {}'.format(cf_name)]

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

        cur.execute(qs)

    indexes = [c for n,c in model._columns.items() if c.index]
    if indexes:
        import ipdb; ipdb.set_trace()
        for column in indexes:
            #TODO: check for existing index...
            #can that be determined from the connection client?
            qs = ['CREATE INDEX {}'.format(column.db_index_name)]
            qs += ['ON {}'.format(cf_name)]
            qs += ['({})'.format(column.db_field_name)]
            qs = ' '.join(qs)
            cur.execute(qs)


def delete_column_family(model):
    #check that model exists
    cf_name = model.column_family_name()
    conn = get_connection(model.keyspace)
    ks_info = conn.client.describe_keyspace(model.keyspace)
    if any([cf_name == cf.name for cf in ks_info.cf_defs]):
        cur = conn.cursor()
        cur.execute('drop table {};'.format(cf_name))

