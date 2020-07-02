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

from libc.stdint cimport INT64_MIN, UINT32_MAX, uint64_t, int64_t

cdef extern from *:
    ctypedef unsigned int __uint128_t

cdef class ShardingInfo():
    cdef readonly int shards_count
    cdef readonly str partitioner
    cdef readonly str sharding_algorithm
    cdef readonly int sharding_ignore_msb
    cdef readonly int shard_aware_port
    cdef readonly int shard_aware_port_ssl

    cdef object __weakref__

    def __init__(self, shard_id, shards_count, partitioner, sharding_algorithm, sharding_ignore_msb, shard_aware_port,
                 shard_aware_port_ssl):
        self.shards_count = int(shards_count)
        self.partitioner = partitioner
        self.sharding_algorithm = sharding_algorithm
        self.sharding_ignore_msb = int(sharding_ignore_msb)
        self.shard_aware_port = int(shard_aware_port) if shard_aware_port else 0
        self.shard_aware_port_ssl = int(shard_aware_port_ssl) if shard_aware_port_ssl else 0

    @staticmethod
    def parse_sharding_info(message):
        shard_id = message.options.get('SCYLLA_SHARD', [''])[0] or None
        shards_count = message.options.get('SCYLLA_NR_SHARDS', [''])[0] or None
        partitioner = message.options.get('SCYLLA_PARTITIONER', [''])[0] or None
        sharding_algorithm = message.options.get('SCYLLA_SHARDING_ALGORITHM', [''])[0] or None
        sharding_ignore_msb = message.options.get('SCYLLA_SHARDING_IGNORE_MSB', [''])[0] or None
        shard_aware_port = message.options.get('SCYLLA_SHARD_AWARE_PORT', [''])[0] or None
        shard_aware_port_ssl = message.options.get('SCYLLA_SHARD_AWARE_PORT_SSL', [''])[0] or None

        if not (shard_id or shards_count or partitioner == "org.apache.cassandra.dht.Murmur3Partitioner" or
            sharding_algorithm == "biased-token-round-robin" or sharding_ignore_msb):
            return 0, None

        return int(shard_id), ShardingInfo(shard_id, shards_count, partitioner, sharding_algorithm, sharding_ignore_msb,
                                           shard_aware_port, shard_aware_port_ssl)

    
    def shard_id_from_token(self, int64_t token_input):
        cdef uint64_t biased_token = token_input + (<uint64_t>1 << 63);
        biased_token <<= self.sharding_ignore_msb;
        cdef int shardId = (<__uint128_t>biased_token * self.shards_count) >> 64;
        return shardId