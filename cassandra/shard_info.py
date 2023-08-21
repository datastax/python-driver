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

import logging
from cassandra.murmur3 import INT64_MIN

log = logging.getLogger(__name__)


class _ShardingInfo(object):

    def __init__(self, shard_id, shards_count, partitioner, sharding_algorithm, sharding_ignore_msb, shard_aware_port, shard_aware_port_ssl):
        self.shards_count = int(shards_count)
        self.partitioner = partitioner
        self.sharding_algorithm = sharding_algorithm
        self.sharding_ignore_msb = int(sharding_ignore_msb)
        self.shard_aware_port = int(shard_aware_port) if shard_aware_port else None
        self.shard_aware_port_ssl = int(shard_aware_port_ssl) if shard_aware_port_ssl else None

    def shard_id_from_token(self, token):
        """
        Convert a Murmur3 token to shard_id based on the number of shards on the host
        """
        token += INT64_MIN
        token <<= self.sharding_ignore_msb
        tokLo = token & 0xffffffff
        tokHi = (token >> 32) & 0xffffffff
        mul1 = tokLo * self.shards_count
        mul2 = tokHi * self.shards_count
        _sum = (mul1 >> 32) + mul2
        output = _sum >> 32
        return output


try:
    from .c_shard_info import ShardingInfo
except ImportError:
    ShardingInfo = _ShardingInfo