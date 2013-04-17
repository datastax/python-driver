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

    def make_create_statement(self, partition_cols, clustering_cols=None, other_cols=None, compact=False):
        clustering_cols = clustering_cols or []
        other_cols = other_cols or []

        statement = "CREATE TABLE %s.%s (" % (self.ksname, self.cfname)
        if len(partition_cols) == 1 and not clustering_cols:
            statement += "%s text PRIMARY KEY, " % partition_cols[0]
        else:
            statement += ", ".join("%s text" % col for col in partition_cols)
            statement += ", "

        statement += ", ".join("%s text" % col for col in clustering_cols + other_cols)

        if len(partition_cols) != 1 or clustering_cols:
            statement += ", PRIMARY KEY ("

            if len(partition_cols) > 1:
                statement += "(" + ", ".join(partition_cols) + ")"
            else:
                statement += partition_cols[0]

            if clustering_cols:
                statement += ", "
                statement += ", ".join(clustering_cols)

            statement += ")"

        statement += ")"
        if compact:
            statement += " WITH COMPACT STORAGE"

        return statement

    def check_create_statement(self, tablemeta, original):
        recreate = tablemeta.as_cql_query(formatted=False)
        self.assertEquals(original, recreate[:len(original)])
        self.session.execute("DROP TABLE %s.%s" % (self.ksname, self.cfname))
        self.session.execute(recreate)

    def test_basic_table_meta_properties(self):
        create_statement = self.make_create_statement(["a"], [], ["b", "c"])
        self.session.execute(create_statement)

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

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys(self):
        create_statement = self.make_create_statement(["a"], ["b"], ["c"])
        self.session.execute(create_statement)
        self.cluster._control_connection.refresh_schema()
        tablemeta = self.cluster.metadata.keyspaces[self.ksname].tables[self.cfname]

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_more_columns(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d", "e", "f"])
        self.session.execute(create_statement)
        self.cluster._control_connection.refresh_schema()
        tablemeta = self.cluster.metadata.keyspaces[self.ksname].tables[self.cfname]

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b', u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual(
            [u'a', u'b', u'c', u'd', u'e', u'f'],
            sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_primary_key(self):
        create_statement = self.make_create_statement(["a", "b"], [], ["c"])
        self.session.execute(create_statement)
        self.cluster._control_connection.refresh_schema()
        tablemeta = self.cluster.metadata.keyspaces[self.ksname].tables[self.cfname]

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([], tablemeta.clustering_key)
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_in_compound_primary_key(self):
        create_statement = self.make_create_statement(["a", "b"], ["c"], ["d", "e"])
        self.session.execute(create_statement)
        self.cluster._control_connection.refresh_schema()
        tablemeta = self.cluster.metadata.keyspaces[self.ksname].tables[self.cfname]

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c', u'd', u'e'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_compact(self):
        create_statement = self.make_create_statement(["a"], ["b"], ["c"], compact=True)
        self.session.execute(create_statement)
        self.cluster._control_connection.refresh_schema()
        tablemeta = self.cluster.metadata.keyspaces[self.ksname].tables[self.cfname]

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_more_columns_compact(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d"], compact=True)
        self.session.execute(create_statement)
        self.cluster._control_connection.refresh_schema()
        tablemeta = self.cluster.metadata.keyspaces[self.ksname].tables[self.cfname]

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b', u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c', u'd'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_primary_key_compact(self):
        create_statement = self.make_create_statement(["a", "b"], [], ["c"], compact=True)
        self.session.execute(create_statement)
        self.cluster._control_connection.refresh_schema()
        tablemeta = self.cluster.metadata.keyspaces[self.ksname].tables[self.cfname]

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([], tablemeta.clustering_key)
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_in_compound_primary_key_compact(self):
        create_statement = self.make_create_statement(["a", "b"], ["c"], ["d"], compact=True)
        self.session.execute(create_statement)
        self.cluster._control_connection.refresh_schema()
        tablemeta = self.cluster.metadata.keyspaces[self.ksname].tables[self.cfname]

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c', u'd'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)
