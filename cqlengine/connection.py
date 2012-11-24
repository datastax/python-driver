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
                else:
                    raise CQLConnectionError('"{}" is not an existing keyspace'.format(keyspace))

        _conn[keyspace] = con

    return con

