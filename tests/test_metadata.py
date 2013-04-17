import unittest

from cassandra.cluster import Cluster
from cassandra.metadata import TableMetadata


class MetadataTest(unittest.TestCase):

    ksname = "metadatatest"

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

    def test_basic_table_meta_properties(self):
        self.session.execute(
            """
            CREATE TABLE {ksname}.{cfname} (
                a text PRIMARY KEY,
                b text,
                c text
            )
            """.format(ksname=self.ksname, cfname=self.cfname))

        self.cluster._control_connection.refresh_schema()

        meta = self.cluster.metadata
        self.assertNotEqual(meta.cluster, None)
        # self.assertNotEqual(meta.cluster_name, None)  # TODO needs to be fixed
        self.assertTrue(self.ksname in meta.keyspaces)
        ksmeta = meta.keyspaces[self.ksname]

        self.assertEqual(ksmeta.name, self.ksname)
        self.assertTrue(ksmeta.durable_writes)
        self.assertTrue(ksmeta.replication['class'].endswith('SimpleStrategy'))
        self.assertEqual(ksmeta.replication['replication_factor'], '1')

        self.assertTrue(self.cfname in ksmeta.tables)
        tablemeta = ksmeta.tables[self.cfname]
        self.assertEqual(tablemeta.keyspace, ksmeta)
        self.assertEqual(tablemeta.name, self.cfname)

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([], tablemeta.clustering_key)
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        for option in TableMetadata.recognized_options:
            self.assertTrue(option in tablemeta.options)

    def test_compound_primary_keys(self):
        self.session.execute(
            """
            CREATE TABLE {ksname}.{cfname} (
                a text,
                b text,
                c text,
                PRIMARY KEY (a, b)
            )
            """.format(ksname=self.ksname, cfname=self.cfname))

        self.cluster._control_connection.refresh_schema()
        tablemeta = self.cluster.metadata.keyspaces[self.ksname].tables[self.cfname]

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        for option in TableMetadata.recognized_options:
            self.assertTrue(option in tablemeta.options)
