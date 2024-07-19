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

    def _create_table(self, subtype):
        table_stmt = """CREATE TABLE test.foo (
    i int PRIMARY KEY,
    j vector<%s, 3>
)""" % (subtype,)
        self.session.execute(table_stmt)

    def _populate_table(self, data):
        for k,v in data.items():
            self.session.execute("insert into test.foo (i,j) values (%d,%s)" % (k,v))

    def _create_and_populate_table(self, subtype="float", data={}):
        self._create_table(subtype)
        self._populate_table(data)

    def _execute_test(self, expected, test_fn):
        rs = self.session.execute("select j from test.foo where i = 2")
        rows = rs.all()
        self.assertEqual(len(rows), 1)
        observed = rows[0].j
        for idx in range(0, 3):
            test_fn(observed[idx], expected[idx])

    def test_float_vector(self):
        def test_fn(observed, expected):
            self.assertAlmostEqual(observed, expected, places=5)
        expected = [1.2, 3.4, 5.6]
        data = {1:[8, 2.3, 58], 2:expected, 5:[23, 18, 3.9]}
        self._create_and_populate_table(subtype="float", data=data)
        self._execute_test(expected, test_fn)
        self.session.execute("drop table test.foo")

    def test_varint_vector(self):
        def test_fn(observed, expected):
            self.assertEqual(observed, expected)
        expected=[1, 3, 5]
        data = {1:[8, 2, 58], 2:expected, 5:[23, 18, 3]}
        self._create_and_populate_table(subtype="varint", data=data)
        self._execute_test(expected, test_fn)
        self.session.execute("drop table test.foo")

    def test_string_vector(self):
        def test_fn(observed, expected):
            self.assertEqual(observed, expected)
        expected=["foo", "bar", "baz"]
        data = {1:["a","b","c"], 2:expected, 5:["x","y","z"]}
        self._create_and_populate_table(subtype="text", data=data)
        self._execute_test(expected, test_fn)
        self.session.execute("drop table test.foo")

    def test_map_vector(self):
        def test_fn(observed, expected):
            self.assertEqual(observed, expected)
        expected=[{"foo":1}, {"bar":2}, {"baz":3}]
        data = {1:[{"a":1},{"b":2},{"c":3}], 2:expected, 5:[{"x":1},{"y":2},{"z":3}]}
        self._create_table("map<text,int>")
        for k,v in data.items():
            self.session.execute("insert into test.foo (i,j) values (%s,%s)", (k,v))
        self._execute_test(expected, test_fn)
        self.session.execute("drop table test.foo")

    #@unittest.skip
    def test_vector_of_vector(self):
        def test_fn(observed, expected):
            self.assertEqual(observed, expected)
        expected=[[1,2], [4,5], [7,8]]
        data = {1:[[10,20], [40,50], [70,80]], 2:expected, 5:[[100,200], [400,500], [700,800]]}
        self._create_table("vector<int,2>")
        for k,v in data.items():
            self.session.execute("insert into test.foo (i,j) values (%s,%s)", (k,v))
        self._execute_test(expected, test_fn)
        #self.session.execute("drop table test.foo")
