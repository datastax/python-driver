# Copyright 2013-2015 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from collections import namedtuple
from functools import partial

from cassandra import InvalidRequest
from cassandra.cluster import Cluster, UserTypeDoesNotExist
from cassandra.query import dict_factory
from cassandra.util import OrderedMap

from tests.integration import get_server_versions, use_singledc, PROTOCOL_VERSION, execute_until_pass
from tests.integration.datatype_utils import update_datatypes, PRIMITIVE_DATATYPES, COLLECTION_TYPES, \
    get_sample, get_collection_sample

nested_collection_udt = namedtuple('nested_collection_udt', ['m', 't', 'l', 's'])
nested_collection_udt_nested = namedtuple('nested_collection_udt_nested', ['m', 't', 'l', 's', 'u'])


def setup_module():
    use_singledc()
    update_datatypes()


class UDTTests(unittest.TestCase):

    def setUp(self):
        self._cass_version, self._cql_version = get_server_versions()

        if self._cass_version < (2, 1, 0):
            raise unittest.SkipTest("User Defined Types were introduced in Cassandra 2.1")

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()
        execute_until_pass(self.session,
                           "CREATE KEYSPACE udttests WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}")
        self.cluster.shutdown()

    def tearDown(self):
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()
        execute_until_pass(self.session, "DROP KEYSPACE udttests")
        self.cluster.shutdown()

    def test_can_insert_unprepared_registered_udts(self):
        """
        Test the insertion of unprepared, registered UDTs
        """

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect("udttests")

        s.execute("CREATE TYPE user (age int, name text)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        User = namedtuple('user', ('age', 'name'))
        c.register_user_type("udttests", "user", User)

        s.execute("INSERT INTO mytable (a, b) VALUES (%s, %s)", (0, User(42, 'bob')))
        result = s.execute("SELECT b FROM mytable WHERE a=0")
        self.assertEqual(1, len(result))
        row = result[0]
        self.assertEqual(42, row.b.age)
        self.assertEqual('bob', row.b.name)
        self.assertTrue(type(row.b) is User)

        # use the same UDT name in a different keyspace
        s.execute("""
            CREATE KEYSPACE udt_test_unprepared_registered2
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_unprepared_registered2")
        s.execute("CREATE TYPE user (state text, is_cool boolean)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        User = namedtuple('user', ('state', 'is_cool'))
        c.register_user_type("udt_test_unprepared_registered2", "user", User)

        s.execute("INSERT INTO mytable (a, b) VALUES (%s, %s)", (0, User('Texas', True)))
        result = s.execute("SELECT b FROM mytable WHERE a=0")
        self.assertEqual(1, len(result))
        row = result[0]
        self.assertEqual('Texas', row.b.state)
        self.assertEqual(True, row.b.is_cool)
        self.assertTrue(type(row.b) is User)

        c.shutdown()

    def test_can_register_udt_before_connecting(self):
        """
        Test the registration of UDTs before session creation
        """

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        s.execute("""
            CREATE KEYSPACE udt_test_register_before_connecting
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_register_before_connecting")
        s.execute("CREATE TYPE user (age int, name text)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        s.execute("""
            CREATE KEYSPACE udt_test_register_before_connecting2
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_register_before_connecting2")
        s.execute("CREATE TYPE user (state text, is_cool boolean)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        # now that types are defined, shutdown and re-create Cluster
        c.shutdown()
        c = Cluster(protocol_version=PROTOCOL_VERSION)

        User1 = namedtuple('user', ('age', 'name'))
        User2 = namedtuple('user', ('state', 'is_cool'))

        c.register_user_type("udt_test_register_before_connecting", "user", User1)
        c.register_user_type("udt_test_register_before_connecting2", "user", User2)

        s = c.connect()

        s.set_keyspace("udt_test_register_before_connecting")
        s.execute("INSERT INTO mytable (a, b) VALUES (%s, %s)", (0, User1(42, 'bob')))
        result = s.execute("SELECT b FROM mytable WHERE a=0")
        self.assertEqual(1, len(result))
        row = result[0]
        self.assertEqual(42, row.b.age)
        self.assertEqual('bob', row.b.name)
        self.assertTrue(type(row.b) is User1)

        # use the same UDT name in a different keyspace
        s.set_keyspace("udt_test_register_before_connecting2")
        s.execute("INSERT INTO mytable (a, b) VALUES (%s, %s)", (0, User2('Texas', True)))
        result = s.execute("SELECT b FROM mytable WHERE a=0")
        self.assertEqual(1, len(result))
        row = result[0]
        self.assertEqual('Texas', row.b.state)
        self.assertEqual(True, row.b.is_cool)
        self.assertTrue(type(row.b) is User2)

        c.shutdown()

    def test_can_insert_prepared_unregistered_udts(self):
        """
        Test the insertion of prepared, unregistered UDTs
        """

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect("udttests")

        s.execute("CREATE TYPE user (age int, name text)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        User = namedtuple('user', ('age', 'name'))
        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(insert, (0, User(42, 'bob')))

        select = s.prepare("SELECT b FROM mytable WHERE a=?")
        result = s.execute(select, (0,))
        self.assertEqual(1, len(result))
        row = result[0]
        self.assertEqual(42, row.b.age)
        self.assertEqual('bob', row.b.name)

        # use the same UDT name in a different keyspace
        s.execute("""
            CREATE KEYSPACE udt_test_prepared_unregistered2
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_prepared_unregistered2")
        s.execute("CREATE TYPE user (state text, is_cool boolean)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        User = namedtuple('user', ('state', 'is_cool'))
        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(insert, (0, User('Texas', True)))

        select = s.prepare("SELECT b FROM mytable WHERE a=?")
        result = s.execute(select, (0,))
        self.assertEqual(1, len(result))
        row = result[0]
        self.assertEqual('Texas', row.b.state)
        self.assertEqual(True, row.b.is_cool)

        c.shutdown()

    def test_can_insert_prepared_registered_udts(self):
        """
        Test the insertion of prepared, registered UDTs
        """

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect("udttests")

        s.execute("CREATE TYPE user (age int, name text)")
        User = namedtuple('user', ('age', 'name'))
        c.register_user_type("udttests", "user", User)

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(insert, (0, User(42, 'bob')))

        select = s.prepare("SELECT b FROM mytable WHERE a=?")
        result = s.execute(select, (0,))
        self.assertEqual(1, len(result))
        row = result[0]
        self.assertEqual(42, row.b.age)
        self.assertEqual('bob', row.b.name)
        self.assertTrue(type(row.b) is User)

        # use the same UDT name in a different keyspace
        s.execute("""
            CREATE KEYSPACE udt_test_prepared_registered2
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_prepared_registered2")
        s.execute("CREATE TYPE user (state text, is_cool boolean)")
        User = namedtuple('user', ('state', 'is_cool'))
        c.register_user_type("udt_test_prepared_registered2", "user", User)

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(insert, (0, User('Texas', True)))

        select = s.prepare("SELECT b FROM mytable WHERE a=?")
        result = s.execute(select, (0,))
        self.assertEqual(1, len(result))
        row = result[0]
        self.assertEqual('Texas', row.b.state)
        self.assertEqual(True, row.b.is_cool)
        self.assertTrue(type(row.b) is User)

        c.shutdown()

    def test_can_insert_udts_with_nulls(self):
        """
        Test the insertion of UDTs with null and empty string fields
        """

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect("udttests")

        s.execute("CREATE TYPE user (a text, b int, c uuid, d blob)")
        User = namedtuple('user', ('a', 'b', 'c', 'd'))
        c.register_user_type("udttests", "user", User)

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (0, ?)")
        s.execute(insert, [User(None, None, None, None)])

        results = s.execute("SELECT b FROM mytable WHERE a=0")
        self.assertEqual((None, None, None, None), results[0].b)

        select = s.prepare("SELECT b FROM mytable WHERE a=0")
        self.assertEqual((None, None, None, None), s.execute(select)[0].b)

        # also test empty strings
        s.execute(insert, [User('', None, None, '')])
        results = s.execute("SELECT b FROM mytable WHERE a=0")
        self.assertEqual(('', None, None, ''), results[0].b)
        self.assertEqual(('', None, None, ''), s.execute(select)[0].b)

        c.shutdown()

    def test_can_insert_udts_with_varying_lengths(self):
        """
        Test for ensuring extra-lengthy udts are properly inserted
        """

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect("udttests")

        MAX_TEST_LENGTH = 1024

        # create the seed udt, increase timeout to avoid the query failure on slow systems
        s.execute("CREATE TYPE lengthy_udt ({})"
                  .format(', '.join(['v_{} int'.format(i)
                                    for i in range(MAX_TEST_LENGTH)])))

        # create a table with multiple sizes of nested udts
        # no need for all nested types, only a spot checked few and the largest one
        s.execute("CREATE TABLE mytable ("
                  "k int PRIMARY KEY, "
                  "v frozen<lengthy_udt>)")

        # create and register the seed udt type
        udt = namedtuple('lengthy_udt', tuple(['v_{}'.format(i) for i in range(MAX_TEST_LENGTH)]))
        c.register_user_type("udttests", "lengthy_udt", udt)

        # verify inserts and reads
        for i in (0, 1, 2, 3, MAX_TEST_LENGTH):
            # create udt
            params = [j for j in range(i)] + [None for j in range(MAX_TEST_LENGTH - i)]
            created_udt = udt(*params)

            # write udt
            s.execute("INSERT INTO mytable (k, v) VALUES (0, %s)", (created_udt,))

            # verify udt was written and read correctly, increase timeout to avoid the query failure on slow systems
            result = s.execute("SELECT v FROM mytable WHERE k=0")[0]
            self.assertEqual(created_udt, result.v)

        c.shutdown()

    def nested_udt_schema_helper(self, session, MAX_NESTING_DEPTH):
        # create the seed udt
        execute_until_pass(session, "CREATE TYPE depth_0 (age int, name text)")

        # create the nested udts
        for i in range(MAX_NESTING_DEPTH):
            execute_until_pass(session, "CREATE TYPE depth_{} (value frozen<depth_{}>)".format(i + 1, i))

        # create a table with multiple sizes of nested udts
        # no need for all nested types, only a spot checked few and the largest one
        execute_until_pass(session, "CREATE TABLE mytable ("
                                    "k int PRIMARY KEY, "
                                    "v_0 frozen<depth_0>, "
                                    "v_1 frozen<depth_1>, "
                                    "v_2 frozen<depth_2>, "
                                    "v_3 frozen<depth_3>, "
                                    "v_{0} frozen<depth_{0}>)".format(MAX_NESTING_DEPTH))

    def nested_udt_creation_helper(self, udts, i):
        if i == 0:
            return udts[0](42, 'Bob')
        else:
            return udts[i](self.nested_udt_creation_helper(udts, i - 1))

    def nested_udt_verification_helper(self, session, MAX_NESTING_DEPTH, udts):
        for i in (0, 1, 2, 3, MAX_NESTING_DEPTH):
            # create udt
            udt = self.nested_udt_creation_helper(udts, i)

            # write udt via simple statement
            session.execute("INSERT INTO mytable (k, v_%s) VALUES (0, %s)", [i, udt])

            # verify udt was written and read correctly
            result = session.execute("SELECT v_{0} FROM mytable WHERE k=0".format(i))[0]
            self.assertEqual(udt, result["v_{0}".format(i)])

            # write udt via prepared statement
            insert = session.prepare("INSERT INTO mytable (k, v_{0}) VALUES (1, ?)".format(i))
            session.execute(insert, [udt])

            # verify udt was written and read correctly
            result = session.execute("SELECT v_{0} FROM mytable WHERE k=1".format(i))[0]
            self.assertEqual(udt, result["v_{0}".format(i)])

    def test_can_insert_nested_registered_udts(self):
        """
        Test for ensuring nested registered udts are properly inserted
        """

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect("udttests")
        s.row_factory = dict_factory

        MAX_NESTING_DEPTH = 16

        # create the schema
        self.nested_udt_schema_helper(s, MAX_NESTING_DEPTH)

        # create and register the seed udt type
        udts = []
        udt = namedtuple('depth_0', ('age', 'name'))
        udts.append(udt)
        c.register_user_type("udttests", "depth_0", udts[0])

        # create and register the nested udt types
        for i in range(MAX_NESTING_DEPTH):
            udt = namedtuple('depth_{}'.format(i + 1), ('value'))
            udts.append(udt)
            c.register_user_type("udttests", "depth_{}".format(i + 1), udts[i + 1])

        # insert udts and verify inserts with reads
        self.nested_udt_verification_helper(s, MAX_NESTING_DEPTH, udts)

        c.shutdown()

    def test_can_insert_nested_unregistered_udts(self):
        """
        Test for ensuring nested unregistered udts are properly inserted
        """

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect("udttests")
        s.row_factory = dict_factory

        MAX_NESTING_DEPTH = 16

        # create the schema
        self.nested_udt_schema_helper(s, MAX_NESTING_DEPTH)

        # create the seed udt type
        udts = []
        udt = namedtuple('depth_0', ('age', 'name'))
        udts.append(udt)

        # create the nested udt types
        for i in range(MAX_NESTING_DEPTH):
            udt = namedtuple('depth_{}'.format(i + 1), ('value'))
            udts.append(udt)

        # insert udts via prepared statements and verify inserts with reads
        for i in (0, 1, 2, 3, MAX_NESTING_DEPTH):
            # create udt
            udt = self.nested_udt_creation_helper(udts, i)

            # write udt
            insert = s.prepare("INSERT INTO mytable (k, v_{0}) VALUES (0, ?)".format(i))
            s.execute(insert, [udt])

            # verify udt was written and read correctly
            result = s.execute("SELECT v_{0} FROM mytable WHERE k=0".format(i))[0]
            self.assertEqual(udt, result["v_{0}".format(i)])

        c.shutdown()

    def test_can_insert_nested_registered_udts_with_different_namedtuples(self):
        """
        Test for ensuring nested udts are inserted correctly when the
        created namedtuples are use names that are different the cql type.
        """

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect("udttests")
        s.row_factory = dict_factory

        MAX_NESTING_DEPTH = 16

        # create the schema
        self.nested_udt_schema_helper(s, MAX_NESTING_DEPTH)

        # create and register the seed udt type
        udts = []
        udt = namedtuple('level_0', ('age', 'name'))
        udts.append(udt)
        c.register_user_type("udttests", "depth_0", udts[0])

        # create and register the nested udt types
        for i in range(MAX_NESTING_DEPTH):
            udt = namedtuple('level_{}'.format(i + 1), ('value'))
            udts.append(udt)
            c.register_user_type("udttests", "depth_{}".format(i + 1), udts[i + 1])

        # insert udts and verify inserts with reads
        self.nested_udt_verification_helper(s, MAX_NESTING_DEPTH, udts)

        c.shutdown()

    def test_raise_error_on_nonexisting_udts(self):
        """
        Test for ensuring that an error is raised for operating on a nonexisting udt or an invalid keyspace
        """

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect("udttests")
        User = namedtuple('user', ('age', 'name'))

        with self.assertRaises(UserTypeDoesNotExist):
            c.register_user_type("some_bad_keyspace", "user", User)

        with self.assertRaises(UserTypeDoesNotExist):
            c.register_user_type("system", "user", User)

        with self.assertRaises(InvalidRequest):
            s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        c.shutdown()

    def test_can_insert_udt_all_datatypes(self):
        """
        Test for inserting various types of PRIMITIVE_DATATYPES into UDT's
        """

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect("udttests")

        # create UDT
        alpha_type_list = []
        start_index = ord('a')
        for i, datatype in enumerate(PRIMITIVE_DATATYPES):
            alpha_type_list.append("{0} {1}".format(chr(start_index + i), datatype))

        s.execute("""
            CREATE TYPE alldatatypes ({0})
        """.format(', '.join(alpha_type_list))
        )

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<alldatatypes>)")

        # register UDT
        alphabet_list = []
        for i in range(ord('a'), ord('a') + len(PRIMITIVE_DATATYPES)):
            alphabet_list.append('{}'.format(chr(i)))
        Alldatatypes = namedtuple("alldatatypes", alphabet_list)
        c.register_user_type("udttests", "alldatatypes", Alldatatypes)

        # insert UDT data
        params = []
        for datatype in PRIMITIVE_DATATYPES:
            params.append((get_sample(datatype)))

        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(insert, (0, Alldatatypes(*params)))

        # retrieve and verify data
        results = s.execute("SELECT * FROM mytable")
        self.assertEqual(1, len(results))

        row = results[0].b
        for expected, actual in zip(params, row):
            self.assertEqual(expected, actual)

        c.shutdown()

    def test_can_insert_udt_all_collection_datatypes(self):
        """
        Test for inserting various types of COLLECTION_TYPES into UDT's
        """

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect("udttests")

        # create UDT
        alpha_type_list = []
        start_index = ord('a')
        for i, collection_type in enumerate(COLLECTION_TYPES):
            for j, datatype in enumerate(PRIMITIVE_DATATYPES):
                if collection_type == "map":
                    type_string = "{0}_{1} {2}<{3}, {3}>".format(chr(start_index + i), chr(start_index + j),
                                                                 collection_type, datatype)
                elif collection_type == "tuple":
                    type_string = "{0}_{1} frozen<{2}<{3}>>".format(chr(start_index + i), chr(start_index + j),
                                                            collection_type, datatype)
                else:
                    type_string = "{0}_{1} {2}<{3}>".format(chr(start_index + i), chr(start_index + j),
                                                            collection_type, datatype)
                alpha_type_list.append(type_string)

        s.execute("""
            CREATE TYPE alldatatypes ({0})
        """.format(', '.join(alpha_type_list))
        )

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<alldatatypes>)")

        # register UDT
        alphabet_list = []
        for i in range(ord('a'), ord('a') + len(COLLECTION_TYPES)):
            for j in range(ord('a'), ord('a') + len(PRIMITIVE_DATATYPES)):
                alphabet_list.append('{0}_{1}'.format(chr(i), chr(j)))

        Alldatatypes = namedtuple("alldatatypes", alphabet_list)
        c.register_user_type("udttests", "alldatatypes", Alldatatypes)

        # insert UDT data
        params = []
        for collection_type in COLLECTION_TYPES:
            for datatype in PRIMITIVE_DATATYPES:
                params.append((get_collection_sample(collection_type, datatype)))

        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(insert, (0, Alldatatypes(*params)))

        # retrieve and verify data
        results = s.execute("SELECT * FROM mytable")
        self.assertEqual(1, len(results))

        row = results[0].b
        for expected, actual in zip(params, row):
            self.assertEqual(expected, actual)

        c.shutdown()

    def insert_select_column(self, session, table_name, column_name, value):
        insert = session.prepare("INSERT INTO %s (k, %s) VALUES (?, ?)" % (table_name, column_name))
        session.execute(insert, (0, value))
        result = session.execute("SELECT %s FROM %s WHERE k=%%s" % (column_name, table_name), (0,))[0][0]
        self.assertEqual(result, value)

    def test_can_insert_nested_collections(self):
        """
        Test for inserting various types of nested COLLECTION_TYPES into tables and UDTs
        """

        if self._cass_version < (2, 1, 3):
            raise unittest.SkipTest("Support for nested collections was introduced in Cassandra 2.1.3")

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect("udttests")
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        name = self._testMethodName

        s.execute("""
            CREATE TYPE %s (
                m frozen<map<int,text>>,
                t tuple<int,text>,
                l frozen<list<int>>,
                s frozen<set<int>>
            )""" % name)
        s.execute("""
            CREATE TYPE %s_nested (
                m frozen<map<int,text>>,
                t tuple<int,text>,
                l frozen<list<int>>,
                s frozen<set<int>>,
                u frozen<%s>
            )""" % (name, name))
        s.execute("""
            CREATE TABLE %s (
                k int PRIMARY KEY,
                map_map map<frozen<map<int,int>>, frozen<map<int,int>>>,
                map_set map<frozen<set<int>>, frozen<set<int>>>,
                map_list map<frozen<list<int>>, frozen<list<int>>>,
                map_tuple map<frozen<tuple<int, int>>, frozen<tuple<int>>>,
                map_udt map<frozen<%s_nested>, frozen<%s>>,
            )""" % (name, name, name))

        validate = partial(self.insert_select_column, s, name)
        validate('map_map', OrderedMap([({1: 1, 2: 2}, {3: 3, 4: 4}), ({5: 5, 6: 6}, {7: 7, 8: 8})]))
        validate('map_set', OrderedMap([(set((1, 2)), set((3, 4))), (set((5, 6)), set((7, 8)))]))
        validate('map_list', OrderedMap([([1, 2], [3, 4]), ([5, 6], [7, 8])]))
        validate('map_tuple', OrderedMap([((1, 2), (3,)), ((4, 5), (6,))]))

        value = nested_collection_udt({1: 'v1', 2: 'v2'}, (3, 'v3'), [4, 5, 6, 7], set((8, 9, 10)))
        key = nested_collection_udt_nested(value.m, value.t, value.l, value.s, value)
        key2 = nested_collection_udt_nested({3: 'v3'}, value.t, value.l, value.s, value)
        validate('map_udt', OrderedMap([(key, value), (key2, value)]))

        c.shutdown()