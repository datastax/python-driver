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

import os
import unittest

from tests.integration import use_singledc, TestCluster

from cassandra.policies import ColDesc

from cassandra.column_encryption.policies import AES256ColumnEncryptionPolicy, \
    AES256_KEY_SIZE_BYTES

def setup_module():
    use_singledc()

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
