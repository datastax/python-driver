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

import logging, sys, traceback

from cassandra import ConsistencyLevel, OperationTimedOut
from cassandra.cluster import Cluster
from cassandra.protocol import ConfigurationException
from cassandra.query import SimpleStatement
from tests.integration import use_singledc, PROTOCOL_VERSION

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

log = logging.getLogger(__name__)


def setup_module():
    use_singledc()


class SchemaTests(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cls.session = cls.cluster.connect()

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    def test_recreates(self):
        """
        Basic test for repeated schema creation and use, using many different keyspaces
        """

        session = self.session

        for i in range(2):
            for keyspace_number in range(5):
                keyspace = "ks_{0}".format(keyspace_number)

                results = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
                existing_keyspaces = [row[0] for row in results]
                if keyspace in existing_keyspaces:
                    drop = "DROP KEYSPACE {0}".format(keyspace)
                    log.debug(drop)
                    session.execute(drop)

                create = "CREATE KEYSPACE {0} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}}".format(keyspace)
                log.debug(create)
                session.execute(create)

                create = "CREATE TABLE {0}.cf (k int PRIMARY KEY, i int)".format(keyspace)
                log.debug(create)
                session.execute(create)

                use = "USE {0}".format(keyspace)
                log.debug(use)
                session.execute(use)

                insert = "INSERT INTO cf (k, i) VALUES (0, 0)"
                log.debug(insert)
                ss = SimpleStatement(insert, consistency_level=ConsistencyLevel.QUORUM)
                session.execute(ss)

    def test_for_schema_disagreements_different_keyspaces(self):
        """
        Tests for any schema disagreements using many different keyspaces
        """

        session = self.session

        for i in xrange(30):
            try:
                session.execute("CREATE KEYSPACE test_{0} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}".format(i))
                session.execute("CREATE TABLE test_{0}.cf (key int PRIMARY KEY, value int)".format(i))

                for j in xrange(100):
                    session.execute("INSERT INTO test_{0}.cf (key, value) VALUES ({1}, {1})".format(i, j))
            except OperationTimedOut:
                ex_type, ex, tb = sys.exc_info()
                log.warn("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                del tb
            finally:
                while True:
                    try:
                        session.execute("DROP KEYSPACE test_{0}".format(i))
                        break
                    except OperationTimedOut:
                        ex_type, ex, tb = sys.exc_info()
                        log.warn("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                        del tb
                    except ConfigurationException:
                        # We're good, the keyspace was never created due to OperationTimedOut
                        break

    def test_for_schema_disagreements_same_keyspace(self):
        """
        Tests for any schema disagreements using the same keyspace multiple times
        """

        cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()

        for i in xrange(30):
            try:
                session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
                session.execute("CREATE TABLE test.cf (key int PRIMARY KEY, value int)")

                for j in xrange(100):
                    session.execute("INSERT INTO test.cf (key, value) VALUES ({0}, {0})".format(j))
            except OperationTimedOut:
                ex_type, ex, tb = sys.exc_info()
                log.warn("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                del tb
            finally:
                while True:
                    try:
                        session.execute("DROP KEYSPACE test")
                        break
                    except OperationTimedOut:
                        ex_type, ex, tb = sys.exc_info()
                        log.warn("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                        del tb
                    except ConfigurationException:
                        # We're good, the keyspace was never created due to OperationTimedOut
                        break
