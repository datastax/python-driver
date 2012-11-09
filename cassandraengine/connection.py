#http://pypi.python.org/pypi/cql/1.0.4
#http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2/
#http://cassandra.apache.org/doc/cql/CQL.html

import cql

_keyspace = 'cse_test'
_col_fam = 'colfam'

conn = cql.connect('127.0.0.1', 9160)

#cli examples here:
#http://wiki.apache.org/cassandra/CassandraCli
try:
    conn.set_initial_keyspace(_keyspace)
except cql.ProgrammingError:
    #http://www.datastax.com/docs/1.0/references/cql/CREATE_KEYSPACE
    cur = conn.cursor()
    cur.execute("create keyspace {} with strategy_class = 'SimpleStrategy' and strategy_options:replication_factor=1;".format(_keyspace))
    conn.set_initial_keyspace(_keyspace)
cur = conn.cursor()

#http://www.datastax.com/docs/1.0/dml/using_cql
#http://stackoverflow.com/questions/10871595/how-do-you-create-a-counter-columnfamily-with-cql3-in-cassandra
try:
    cur.execute("""create table colfam (id int PRIMARY KEY);""")
except cql.ProgrammingError:
    #cur.execute('drop table colfam;')
    pass

#http://www.datastax.com/docs/1.0/references/cql/INSERT
#updates/inserts do the same thing
cur.execute('insert into colfam (id, content) values (:id, :content)', dict(id=1, content='yo!'))

cur.execute('select * from colfam WHERE id=1;')
cur.fetchone()

cur.execute("update colfam set content='hey' where id=1;")
cur.execute('select * from colfam WHERE id=1;')
cur.fetchone()

cur.execute('delete from colfam WHERE id=1;')
cur.execute('delete from colfam WHERE id in (1);')

#TODO: Add alter altering existing schema functionality
#http://www.datastax.com/docs/1.0/references/cql/ALTER_COLUMNFAMILY

def create_column_family(name):
    """
    """

def select_row(column_family, **kwargs):
    """
    """

def create_row(column_family, **kwargs):
    """
    """

def update_row(column_family, **kwargs):
    """
    """

def delete_row(column_family, **kwargs):
    """
    """

