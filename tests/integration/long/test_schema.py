import logging

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

log = logging.getLogger(__name__)


class SchemaTests(unittest.TestCase):

    def test_recreates(self):
        cluster = Cluster()
        session = cluster.connect()
        replication_factor = 3

        for i in range(2):
            for keyspace in range(5):
                keyspace = 'ks_%s' % keyspace
                results = session.execute('SELECT keyspace_name FROM system.schema_keyspaces')
                existing_keyspaces = [row[0] for row in results]
                if keyspace in existing_keyspaces:
                    ddl = 'DROP KEYSPACE %s' % keyspace
                    log.debug(ddl)
                    session.execute(ddl)

                ddl = """
                    CREATE KEYSPACE %s
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '%s'}
                    """ % (keyspace, str(replication_factor))
                log.debug(ddl)
                session.execute(ddl)

                ddl = 'CREATE TABLE %s.cf (k int PRIMARY KEY, i int)' % keyspace
                log.debug(ddl)
                session.execute(ddl)

                statement = 'USE %s' % keyspace
                log.debug(ddl)
                session.execute(statement)

                statement = 'INSERT INTO %s(k, i) VALUES (0, 0)' % 'cf'
                log.debug(statement)
                ss = SimpleStatement(statement,
                                     consistency_level=ConsistencyLevel.QUORUM)
                session.execute(ss)
