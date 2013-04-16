import unittest

from cassandra.cluster import Cluster


class MetadataTest(unittest.TestCase):

    ksname = "MetadataTest"

    @property
    def cfname(self):
        return self._testMethodName.lower()

    @classmethod
    def setup_class(cls):
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
        try:
            results = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
            existing_keyspaces = [r.values()[0] for r in results.results]
            if cls.ksname in existing_keyspaces:
                session.execute("DROP KEYSPACE %s" % cls.ksname)

            session.execute(
                """
                CREATE KEYSPACE %s
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
                """ % cls.ksname)
        finally:
            cluster.shutdown()

    @classmethod
    def teardown_class(cls):
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
        try:
            session.execute("DROP KEYSPACE %s" % cls.ksname)
        finally:
            cluster.shutdown()

    def setUp(self):
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect()

    def tearDown(self):
        try:
            self.session.execute(
                """
                DROP TABLE {ksname}.{cfname}
                """.format(ksname=self.ksname, cfname=self.cfname))
        finally:
            self.cluster.shutdown()

    def test(self):
        self.session.execute(
            """
            CREATE TABLE {ksname}.{cfname} (
                a text PRIMARY KEY,
                b text,
                c text
            )
            """.format(ksname=self.ksname, cfname=self.cfname))

        self.cluster._control_connection.refresh_schema()
