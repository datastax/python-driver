# Copyright 2013-2014 DataStax, Inc.
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
    import unittest # noqa

# try:
#     from blist import sortedset
# except ImportError:
#     sortedset = set  # noqa

import logging
log = logging.getLogger(__name__)

from collections import namedtuple

from cassandra.cluster import Cluster

from tests.integration import get_server_versions, PROTOCOL_VERSION
from tests.integration.datatype_utils import get_sample, get_nonprim_sample,\
    DATA_TYPE_PRIMITIVES, DATA_TYPE_NON_PRIMITIVE_NAMES

class TypeTests(unittest.TestCase):

    def setUp(self):
        if PROTOCOL_VERSION < 3:
            raise unittest.SkipTest("v3 protocol is required for UDT tests")

        self._cass_version, self._cql_version = get_server_versions()

    def test_unprepared_registered_udts(self):
        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        s.execute("""
            CREATE KEYSPACE udt_test_unprepared_registered
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_unprepared_registered")
        s.execute("CREATE TYPE user (age int, name text)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b user)")

        User = namedtuple('user', ('age', 'name'))
        c.register_user_type("udt_test_unprepared_registered", "user", User)

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
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b user)")

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

    def test_register_before_connecting(self):
        User1 = namedtuple('user', ('age', 'name'))
        User2 = namedtuple('user', ('state', 'is_cool'))

        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        s.execute("""
            CREATE KEYSPACE udt_test_register_before_connecting
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_register_before_connecting")
        s.execute("CREATE TYPE user (age int, name text)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b user)")

        s.execute("""
            CREATE KEYSPACE udt_test_register_before_connecting2
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_register_before_connecting2")
        s.execute("CREATE TYPE user (state text, is_cool boolean)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b user)")

        # now that types are defined, shutdown and re-create Cluster
        c.shutdown()
        c = Cluster(protocol_version=PROTOCOL_VERSION)
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

    def test_prepared_unregistered_udts(self):
        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        s.execute("""
            CREATE KEYSPACE udt_test_prepared_unregistered
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_prepared_unregistered")
        s.execute("CREATE TYPE user (age int, name text)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b user)")

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
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b user)")

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

    def test_prepared_registered_udts(self):
        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        s.execute("""
            CREATE KEYSPACE udt_test_prepared_registered
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_prepared_registered")
        s.execute("CREATE TYPE user (age int, name text)")
        User = namedtuple('user', ('age', 'name'))
        c.register_user_type("udt_test_prepared_registered", "user", User)

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b user)")

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

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b user)")

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

    def test_non_existing_types(self):
        c = Cluster(protocol_version=PROTOCOL_VERSION)
        c.connect()
        User = namedtuple('user', ('age', 'name'))
        self.assertRaises(UserTypeDoesNotExist, c.register_user_type, "some_bad_keyspace", "user", User)
        self.assertRaises(UserTypeDoesNotExist, c.register_user_type, "system", "user", User)

    def test_primitive_datatypes(self):
        """
        Test for inserting various types of DATA_TYPE_PRIMITIVES into UDT's
        """
        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        # create keyspace
        s.execute("""
            CREATE KEYSPACE test_primitive_datatypes
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("test_primitive_datatypes")

        # create UDT
        alpha_type_list = []
        start_index = ord('a')
        for i, datatype in enumerate(DATA_TYPE_PRIMITIVES):
            alpha_type_list.append("{0} {1}".format(chr(start_index + i), datatype))

        s.execute("""
            CREATE TYPE alldatatypes ({0})
        """.format(', '.join(alpha_type_list))
        )

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b alldatatypes)")

        # register UDT
        alphabet_list = []
        for i in range(ord('a'), ord('a')+len(DATA_TYPE_PRIMITIVES)):
            alphabet_list.append('{}'.format(chr(i)))
        Alldatatypes = namedtuple("alldatatypes", alphabet_list)
        c.register_user_type("test_primitive_datatypes", "alldatatypes", Alldatatypes)

        # insert UDT data
        params = []
        for datatype in DATA_TYPE_PRIMITIVES:
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

    def test_nonprimitive_datatypes(self):
        """
        Test for inserting various types of DATA_TYPE_NON_PRIMITIVE into UDT's
        """
        c = Cluster(protocol_version=PROTOCOL_VERSION)
        s = c.connect()

        # create keyspace
        s.execute("""
            CREATE KEYSPACE test_nonprimitive_datatypes
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("test_nonprimitive_datatypes")

        # create UDT
        alpha_type_list = []
        start_index = ord('a')
        for i, datatype in enumerate(DATA_TYPE_NON_PRIMITIVE_NAMES):
            if datatype == "map":
                alpha_type_list.append("{0} {1}<text, text>".format(chr(start_index + i), datatype))
            else:
                alpha_type_list.append("{0} {1}<text>".format(chr(start_index + i), datatype))

        s.execute("""
            CREATE TYPE alldatatypes ({0})
        """.format(', '.join(alpha_type_list))
        )

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b alldatatypes)")

        # register UDT
        alphabet_list = []
        for i in range(ord('a'), ord('a')+len(DATA_TYPE_NON_PRIMITIVE_NAMES)):
            alphabet_list.append('{}'.format(chr(i)))
        Alldatatypes = namedtuple("alldatatypes", alphabet_list)
        c.register_user_type("test_nonprimitive_datatypes", "alldatatypes", Alldatatypes)

        # insert UDT data
        params = []
        for datatype in DATA_TYPE_NON_PRIMITIVE_NAMES:
            params.append((get_nonprim_sample(datatype)))

        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(insert, (0, Alldatatypes(*params)))

        # retrieve and verify data
        results = s.execute("SELECT * FROM mytable")
        self.assertEqual(1, len(results))

        row = results[0].b
        for expected, actual in zip(params, row):
            self.assertEqual(expected, actual)

        c.shutdown()