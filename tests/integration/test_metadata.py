try:
    import unittest2 as unittest
except ImportError:
    import unittest

from cassandra.cluster import Cluster
from cassandra.metadata import TableMetadata, Token, MD5Token, TokenMap
from cassandra.policies import SimpleConvictionPolicy
from cassandra.pool import Host

from tests.integration import get_cluster

class SchemaMetadataTest(unittest.TestCase):

    ksname = "schemametadatatest"

    @property
    def cfname(self):
        return self._testMethodName.lower()

    @classmethod
    def setup_class(cls):
        cluster = Cluster()
        session = cluster.connect()
        try:
            results = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
            existing_keyspaces = [row[0] for row in results]
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

        # create the table again, but with formatting enabled
        self.session.execute("DROP TABLE %s.%s" % (self.ksname, self.cfname))
        recreate = tablemeta.as_cql_query(formatted=True)
        self.session.execute(recreate)

    def get_table_metadata(self):
        self.cluster.control_connection.refresh_schema()
        return self.cluster.metadata.keyspaces[self.ksname].tables[self.cfname]

    def test_basic_table_meta_properties(self):
        create_statement = self.make_create_statement(["a"], [], ["b", "c"])
        self.session.execute(create_statement)

        self.cluster.control_connection.refresh_schema()

        meta = self.cluster.metadata
        self.assertNotEqual(meta.cluster_ref, None)
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
        create_statement += " WITH CLUSTERING ORDER BY (b ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_more_columns(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d", "e", "f"])
        create_statement += " WITH CLUSTERING ORDER BY (b ASC, c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b', u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual(
            [u'a', u'b', u'c', u'd', u'e', u'f'],
            sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_primary_key(self):
        create_statement = self.make_create_statement(["a", "b"], [], ["c"])
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([], tablemeta.clustering_key)
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_in_compound_primary_key(self):
        create_statement = self.make_create_statement(["a", "b"], ["c"], ["d", "e"])
        create_statement += " WITH CLUSTERING ORDER BY (c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c', u'd', u'e'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_compact(self):
        create_statement = self.make_create_statement(["a"], ["b"], ["c"], compact=True)
        create_statement += " AND CLUSTERING ORDER BY (b ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_more_columns_compact(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d"], compact=True)
        create_statement += " AND CLUSTERING ORDER BY (b ASC, c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b', u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c', u'd'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_primary_key_compact(self):
        create_statement = self.make_create_statement(["a", "b"], [], ["c"], compact=True)
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([], tablemeta.clustering_key)
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_in_compound_primary_key_compact(self):
        create_statement = self.make_create_statement(["a", "b"], ["c"], ["d"], compact=True)
        create_statement += " AND CLUSTERING ORDER BY (c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c', u'd'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_ordering(self):
        create_statement = self.make_create_statement(["a"], ["b"], ["c"])
        create_statement += " WITH CLUSTERING ORDER BY (b DESC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_more_columns_ordering(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d", "e", "f"])
        create_statement += " WITH CLUSTERING ORDER BY (b DESC, c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    def test_composite_in_compound_primary_key_ordering(self):
        create_statement = self.make_create_statement(["a", "b"], ["c"], ["d", "e"])
        create_statement += " WITH CLUSTERING ORDER BY (c DESC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    def test_indexes(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d", "e", "f"])
        create_statement += " WITH CLUSTERING ORDER BY (b ASC, c ASC)"
        self.session.execute(create_statement)

        d_index = "CREATE INDEX d_index ON %s.%s (d)" % (self.ksname, self.cfname)
        e_index = "CREATE INDEX e_index ON %s.%s (e)" % (self.ksname, self.cfname)
        self.session.execute(d_index)
        self.session.execute(e_index)

        tablemeta = self.get_table_metadata()
        statements = tablemeta.export_as_string().strip()
        statements = [s.strip() for s in statements.split(';')]
        statements = filter(bool, statements)
        self.assertEqual(3, len(statements))
        self.assertEqual(d_index, statements[1])
        self.assertEqual(e_index, statements[2])


class TokenMetadataTest(unittest.TestCase):
    """
    Test of TokenMap creation and other behavior.
    """

    def test_token(self):
        expected_node_count = len(get_cluster().nodes)

        cluster = Cluster()
        cluster.connect()
        tmap = cluster.metadata.token_map
        self.assertTrue(issubclass(tmap.token_class, Token))
        self.assertEqual(expected_node_count, len(tmap.ring))
        self.assertEqual(expected_node_count, len(tmap.tokens_to_hosts))
        cluster.shutdown()

    def test_getting_replicas(self):
        tokens = [MD5Token(str(i)) for i in range(0, (2 ** 127 - 1), 2 ** 125)]
        hosts = [Host("ip%d" % i, SimpleConvictionPolicy) for i in range(len(tokens))]
        tokens_to_hosts = dict((t, set([h])) for t, h in zip(tokens, hosts))
        token_map = TokenMap(MD5Token, tokens_to_hosts, tokens)

        # tokens match node tokens exactly
        for token, expected_host in zip(tokens, hosts):
            replicas = token_map.get_replicas(token)
            self.assertEqual(replicas, set([expected_host]))

        # shift the tokens back by one
        for token, expected_host in zip(tokens[1:], hosts[1:]):
            replicas = token_map.get_replicas(MD5Token(str(token.value - 1)))
            self.assertEqual(replicas, set([expected_host]))

        # shift the tokens forward by one
        for i, token in enumerate(tokens):
            replicas = token_map.get_replicas(MD5Token(str(token.value + 1)))
            expected_host = hosts[(i + 1) % len(hosts)]
            self.assertEqual(replicas, set([expected_host]))
