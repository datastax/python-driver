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

import logging

from cassandra import ConsistencyLevel, OperationTimedOut
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from tests.integration import PROTOCOL_VERSION

try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

log = logging.getLogger(__name__)


class SchemaTests(unittest.TestCase):

    def test_recreates(self):
        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
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

    def test_for_schema_disagreements_different_keyspaces(self):
        cluster = Cluster()
        session = cluster.connect()

        for i in xrange(30):
            try:
                session.execute('''
                    CREATE KEYSPACE test_%s
                    WITH replication = {'class': 'SimpleStrategy',
                                        'replication_factor': 1}
                ''' % i)

                session.execute('''
                    CREATE TABLE test_%s.cf (
                        key int,
                        value int,
                        PRIMARY KEY (key))
                ''' % i)

                for j in xrange(100):
                    session.execute('INSERT INTO test_%s.cf (key, value) VALUES (%s, %s)' % (i, j, j))

                session.execute('''
                    DROP KEYSPACE test_%s
                ''' % i)
            except OperationTimedOut: pass

    def test_for_schema_disagreements_same_keyspace(self):
        cluster = Cluster()
        session = cluster.connect()

        for i in xrange(30):
            try:
                session.execute('''
                    CREATE KEYSPACE test
                    WITH replication = {'class': 'SimpleStrategy',
                                        'replication_factor': 1}
                ''')

                session.execute('''
                    CREATE TABLE test.cf (
                        key int,
                        value int,
                        PRIMARY KEY (key))
                ''')

                for j in xrange(100):
                    session.execute('INSERT INTO test.cf (key, value) VALUES (%s, %s)' % (j, j))

                session.execute('''
                    DROP KEYSPACE test
                ''')
            except OperationTimedOut: pass
