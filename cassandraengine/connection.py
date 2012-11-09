#http://pypi.python.org/pypi/cql/1.0.4
#http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2/

import cql

_keyspace = 'cse_test'
_col_fam = 'colfam'

conn = cql.connect('127.0.0.1', 9160)
cur = conn.cursor()

#cli examples here:
#http://wiki.apache.org/cassandra/CassandraCli
cur.execute('create keyspace {};'.format(_keyspace))

#http://stackoverflow.com/questions/10871595/how-do-you-create-a-counter-columnfamily-with-cql3-in-cassandra
cur.execute('create column family {};'.format(_col_fam))

cur.execute('insert into colfam (obj_id, content) values (:obj_id, :content)', dict(obj_id=str(uuid.uuid4()), content='yo!'))
