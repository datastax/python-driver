# Copyright DataStax, Inc.
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

from decimal import Decimal
import os
import random
import unittest

from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import HostFilterPolicy, RoundRobinPolicy,  SimpleConvictionPolicy, \
    WhiteListRoundRobinPolicy, ColDesc, AES256ColumnEncryptionPolicy, AES256_KEY_SIZE_BYTES
from cassandra.pool import Host
from cassandra.connection import DefaultEndPoint

from tests.integration import local, use_singledc, TestCluster

from concurrent.futures import wait as wait_futures


def setup_module():
    use_singledc()


class HostFilterPolicyTests(unittest.TestCase):

    def test_predicate_changes(self):
        """
        Test to validate host filter reacts correctly when the predicate return
        a different subset of the hosts
        HostFilterPolicy
        @since 3.8
        @jira_ticket PYTHON-961
        @expected_result the excluded hosts are ignored

        @test_category policy
        """
        external_event = True
        contact_point = DefaultEndPoint("127.0.0.1")

        single_host = {Host(contact_point, SimpleConvictionPolicy)}
        all_hosts = {Host(DefaultEndPoint("127.0.0.{}".format(i)), SimpleConvictionPolicy) for i in (1, 2, 3)}

        predicate = lambda host: host.endpoint == contact_point if external_event else True
        hfp = ExecutionProfile(
            load_balancing_policy=HostFilterPolicy(RoundRobinPolicy(), predicate=predicate)
        )
        cluster = TestCluster(contact_points=(contact_point,), execution_profiles={EXEC_PROFILE_DEFAULT: hfp},
                              topology_event_refresh_window=0,
                              status_event_refresh_window=0)
        session = cluster.connect(wait_for_all_pools=True)

        queried_hosts = set()
        for _ in range(10):
            response = session.execute("SELECT * from system.local")
            queried_hosts.update(response.response_future.attempted_hosts)

        self.assertEqual(queried_hosts, single_host)

        external_event = False
        futures = session.update_created_pools()
        wait_futures(futures, timeout=cluster.connect_timeout)

        queried_hosts = set()
        for _ in range(10):
            response = session.execute("SELECT * from system.local")
            queried_hosts.update(response.response_future.attempted_hosts)
        self.assertEqual(queried_hosts, all_hosts)


class WhiteListRoundRobinPolicyTests(unittest.TestCase):

    @local
    def test_only_connects_to_subset(self):
        only_connect_hosts = {"127.0.0.1", "127.0.0.2"}
        white_list = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(only_connect_hosts))
        cluster = TestCluster(execution_profiles={"white_list": white_list})
        #cluster = Cluster(load_balancing_policy=WhiteListRoundRobinPolicy(only_connect_hosts))
        session = cluster.connect(wait_for_all_pools=True)
        queried_hosts = set()
        for _ in range(10):
            response = session.execute('SELECT * from system.local', execution_profile="white_list")
            queried_hosts.update(response.response_future.attempted_hosts)
        queried_hosts = set(host.address for host in queried_hosts)
        self.assertEqual(queried_hosts, only_connect_hosts)

class ColumnEncryptionPolicyTest(unittest.TestCase):

    def _recreate_keyspace(self, session):
        session.execute("drop keyspace if exists foo")
        session.execute("CREATE KEYSPACE foo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        session.execute("CREATE TABLE foo.bar(encrypted blob, unencrypted int, primary key(unencrypted))")

    def test_end_to_end_prepared(self):

        # We only currently perform testing on a single type/expected value pair since CLE functionality is essentially
        # independent of the underlying type.  We intercept data after it's been encoded when it's going out and before it's
        # encoded when coming back; the actual types of the data involved don't impact us.
        expected = 12345
        expected_type = "int"

        key = os.urandom(AES256_KEY_SIZE_BYTES)
        cl_policy = AES256ColumnEncryptionPolicy()
        col_desc = ColDesc('foo','bar','encrypted')
        cl_policy.add_column(col_desc, key, expected_type)

        cluster = TestCluster(column_encryption_policy=cl_policy)
        session = cluster.connect()
        self._recreate_keyspace(session)

        prepared = session.prepare("insert into foo.bar (encrypted, unencrypted) values (?,?)")
        session.execute(prepared, (expected,expected))

        # A straight select from the database will now return the decrypted bits.  We select both encrypted and unencrypted
        # values here to confirm that we don't interfere with regular processing of unencrypted vals.
        (encrypted,unencrypted) = session.execute("select encrypted, unencrypted from foo.bar where unencrypted = %s allow filtering", (expected,)).one()
        self.assertEquals(expected, encrypted)
        self.assertEquals(expected, unencrypted)

        # Confirm the same behaviour from a subsequent prepared statement as well
        prepared = session.prepare("select encrypted, unencrypted from foo.bar where unencrypted = ? allow filtering")
        (encrypted,unencrypted) = session.execute(prepared, [expected]).one()
        self.assertEquals(expected, encrypted)
        self.assertEquals(expected, unencrypted)

    def test_end_to_end_simple(self):

        expected = 67890
        expected_type = "int"

        key = os.urandom(AES256_KEY_SIZE_BYTES)
        cl_policy = AES256ColumnEncryptionPolicy()
        col_desc = ColDesc('foo','bar','encrypted')
        cl_policy.add_column(col_desc, key, expected_type)

        cluster = TestCluster(column_encryption_policy=cl_policy)
        session = cluster.connect()
        self._recreate_keyspace(session)

        # Use encode_and_encrypt helper function to populate date
        session.execute("insert into foo.bar (encrypted, unencrypted) values (%s,%s)",(cl_policy.encode_and_encrypt(col_desc, expected), expected))

        # A straight select from the database will now return the decrypted bits.  We select both encrypted and unencrypted
        # values here to confirm that we don't interfere with regular processing of unencrypted vals.
        (encrypted,unencrypted) = session.execute("select encrypted, unencrypted from foo.bar where unencrypted = %s allow filtering", (expected,)).one()
        self.assertEquals(expected, encrypted)
        self.assertEquals(expected, unencrypted)

        # Confirm the same behaviour from a subsequent prepared statement as well
        prepared = session.prepare("select encrypted, unencrypted from foo.bar where unencrypted = ? allow filtering")
        (encrypted,unencrypted) = session.execute(prepared, [expected]).one()
        self.assertEquals(expected, encrypted)
        self.assertEquals(expected, unencrypted)

    def test_end_to_end_python1356(self):
        """Test to validate the fix for PYTHON-1356.  Class variables used to pass CLE policy down shouldn't persist."""

        expected = 13579
        expected_type = "int"

        key = os.urandom(AES256_KEY_SIZE_BYTES)
        cl_policy = AES256ColumnEncryptionPolicy()
        col_desc = ColDesc('foo','bar','encrypted')
        cl_policy.add_column(col_desc, key, expected_type)

        cluster = TestCluster(column_encryption_policy=cl_policy)
        session = cluster.connect()
        self._recreate_keyspace(session)

        # Use encode_and_encrypt helper function to populate date
        session.execute("insert into foo.bar (encrypted, unencrypted) values (%s,%s)",(cl_policy.encode_and_encrypt(col_desc, expected), expected))

        # We now open a new session _without_ the CLE policy specified.  We should _not_ be able to read decrypted bits from this session.
        cluster2 = TestCluster()
        session2 = cluster2.connect()

        # A straight select from the database will now return the decrypted bits.  We select both encrypted and unencrypted
        # values here to confirm that we don't interfere with regular processing of unencrypted vals.
        (encrypted,unencrypted) = session2.execute("select encrypted, unencrypted from foo.bar where unencrypted = %s allow filtering", (expected,)).one()
        self.assertEquals(cl_policy.encode_and_encrypt(col_desc, expected), encrypted)
        self.assertEquals(expected, unencrypted)

        # Confirm the same behaviour from a subsequent prepared statement as well
        prepared = session2.prepare("select encrypted, unencrypted from foo.bar where unencrypted = ? allow filtering")
        (encrypted,unencrypted) = session2.execute(prepared, [expected]).one()
        self.assertEquals(cl_policy.encode_and_encrypt(col_desc, expected), encrypted)
        self.assertEquals(expected, unencrypted)
