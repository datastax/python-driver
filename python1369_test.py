import logging
import unittest

from cassandra.cluster import Cluster, Session

class Python1369Test(unittest.TestCase):

    def setUp(self):
        #log = logging.getLogger()
        #log.setLevel('DEBUG')

        #handler = logging.StreamHandler()
        #handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        #log.addHandler(handler)

        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect()
        self.session.execute("drop keyspace if exists test")
        ks_stmt = """CREATE KEYSPACE test
  WITH REPLICATION = { 
   'class' : 'SimpleStrategy', 
   'replication_factor' : 1
  }"""
        self.session.execute(ks_stmt)
    
    def _create_and_populate_table(self, subtype="float"):
        table_stmt = """CREATE TABLE test.foo (
    i int PRIMARY KEY,
    j vector<%s, 3>
)""" % (subtype,)
        self.session.execute(table_stmt)
        self.session.execute("CREATE CUSTOM INDEX ann_index ON test.foo (j) USING 'StorageAttachedIndex'")
        self.session.execute("insert into test.foo (i,j) values (1,[8, 2.3, 58])")
        self.session.execute("insert into test.foo (i,j) values (2,[1.2, 3.4, 5.6])")
        self.session.execute("insert into test.foo (i,j) values (5,[23, 18, 3.9])")

    def test_float_vector(self):
        self._create_and_populate_table(subtype="float")

        rs = self.session.execute("select j from test.foo order by j ann of [3.4, 7.8, 9.1] limit 1")
        rows = rs.all()
        self.assertEqual(len(rows), 1)
        observed = rows[0].j
        expected = [1.2, 3.4, 5.6]
        for idx in range(0, 3):
            self.assertAlmostEqual(observed[idx], expected[idx], places=5)

        self.session.execute("drop table test.foo")

    def test_float_varint(self):
        self._create_and_populate_table(subtype="varint")

        rs = self.session.execute("select j from test.foo order by j ann of [3.4, 7.8, 9.1] limit 1")
        rows = rs.all()
        self.assertEqual(len(rows), 1)
        observed = rows[0].j
        expected = [1.2, 3.4, 5.6]
        for idx in range(0, 3):
            self.assertAlmostEqual(observed[idx], expected[idx], places=5)

        self.session.execute("drop table test.foo")
