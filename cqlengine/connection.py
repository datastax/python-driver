#http://pypi.python.org/pypi/cql/1.0.4
#http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2 /
#http://cassandra.apache.org/doc/cql/CQL.html

import cql
from cqlengine.exceptions import CQLEngineException

class CQLConnectionError(CQLEngineException): pass

_keyspace = 'cassengine_test'

#TODO: look into the cql connection pool class
_conn = {}
def get_connection(keyspace, create_missing_keyspace=True):
    con = _conn.get(keyspace)
    if con is None:
        con = cql.connect('127.0.0.1', 9160)
        con.set_cql_version('3.0.0')

        if keyspace:
            try:
                con.set_initial_keyspace(keyspace)
            except cql.ProgrammingError, e:
                if create_missing_keyspace:
                    from cqlengine.management import create_keyspace
                    create_keyspace(keyspace)
                    #http://www.datastax.com/docs/1.0/references/cql/CREATE_KEYSPACE
                    cur = con.cursor()
                    cur.execute("""create keyspace {}
                                   with strategy_class = 'SimpleStrategy'
                                   and strategy_options:replication_factor=1;""".format(keyspace))
                    con.set_initial_keyspace(keyspace)
                else:
                    raise CQLConnectionError('"{}" is not an existing keyspace'.format(keyspace))

        _conn[keyspace] = con

    return con

#cli examples here:
#http://wiki.apache.org/cassandra/CassandraCli
#http://www.datastax.com/docs/1.0/dml/using_cql
#http://stackoverflow.com/questions/10871595/how-do-you-create-a-counter-columnfamily-with-cql3-in-cassandra
"""
try:
    cur.execute("create table colfam (id int PRIMARY KEY);")
except cql.ProgrammingError:
    #cur.execute('drop table colfam;')
    pass
"""

#http://www.datastax.com/docs/1.0/references/cql/INSERT
#updates/inserts do the same thing

"""
cur.execute('insert into colfam (id, content) values (:id, :content)', dict(id=1, content='yo!'))

cur.execute('select * from colfam WHERE id=1;')
cur.fetchone()

cur.execute("update colfam set content='hey' where id=1;")
cur.execute('select * from colfam WHERE id=1;')
cur.fetchone()

cur.execute('delete from colfam WHERE id=1;')
cur.execute('delete from colfam WHERE id in (1);')
"""

#TODO: Add alter altering existing schema functionality
#http://www.datastax.com/docs/1.0/references/cql/ALTER_COLUMNFAMILY

