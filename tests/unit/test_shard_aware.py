# Copyright 2020 ScyllaDB, Inc.
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

from cassandra.connection import ShardingInfo
from cassandra.metadata import Murmur3Token

class TestShardAware(unittest.TestCase):
    def test_parsing_and_calculating_shard_id(self):
        '''
        Testing the parsing of the options command
        and the calculation getting a shard id from a Murmur3 token
        '''
        class OptionsHolder():
            options = {
                'SCYLLA_SHARD': ['1'], 
                'SCYLLA_NR_SHARDS': ['12'],
                'SCYLLA_PARTITIONER': ['org.apache.cassandra.dht.Murmur3Partitioner'],
                'SCYLLA_SHARDING_ALGORITHM': ['biased-token-round-robin'],
                'SCYLLA_SHARDING_IGNORE_MSB': ['12']
            }
        shard_id, shard_info = ShardingInfo.parse_sharding_info(OptionsHolder())

        self.assertEqual(shard_id, 1)
        self.assertEqual(shard_info.shard_id(Murmur3Token.from_key(b"a")), 4)
        self.assertEqual(shard_info.shard_id(Murmur3Token.from_key(b"b")), 6)
        self.assertEqual(shard_info.shard_id(Murmur3Token.from_key(b"c")), 6)
        self.assertEqual(shard_info.shard_id(Murmur3Token.from_key(b"e")), 4)
        self.assertEqual(shard_info.shard_id(Murmur3Token.from_key(b"100000")), 2)