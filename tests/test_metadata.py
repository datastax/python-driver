import unittest

from cassandra.cluster import Cluster


class MetadataTest(unittest.TestCase):

    ksname = "MetadataTest"

    @classmethod
    def setup_class(cls):
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
        session.execute(
            """
            CREATE KEYSPACE %s
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
            """ % cls.ksname)
        cluster.shutdown()

    @classmethod
    def teardown_class(cls):
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
        session.execute("DROP KEYSPACE %s" % cls.ksname)
        cluster.shutdown()

    def setUp(self):
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect()

    def tearDown(self):
        cfname = str(self.id).split()[4]
        try:
            self.session.execute(
                """
                DROP TABLE {ksname}.{cfname}
                """.format(ksname=self.ksname, cfname=cfname))
        finally:
            self.cluster.shutdown()

    def test(self):
        cfname = str(self.id).split()[4]
        self.session.execute(
            """
            CREATE TABLE {ksname}.{cfname} (
                a text PRIMARY KEY,
                b text,
                c text
            )
            """.format(ksname=self.ksname, cfname=cfname))

        self.cluster._control_connection.refresh_schema()
