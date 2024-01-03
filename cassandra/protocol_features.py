import logging

from cassandra.shard_info import _ShardingInfo

log = logging.getLogger(__name__)


RATE_LIMIT_ERROR_EXTENSION = "SCYLLA_RATE_LIMIT_ERROR"
TABLETS_ROUTING_V1 = "TABLETS_ROUTING_V1"

class ProtocolFeatures(object):
    rate_limit_error = None
    shard_id = 0
    sharding_info = None
    tablets_routing_v1 = False

    def __init__(self, rate_limit_error=None, shard_id=0, sharding_info=None, tablets_routing_v1=False):
        self.rate_limit_error = rate_limit_error
        self.shard_id = shard_id
        self.sharding_info = sharding_info
        self.tablets_routing_v1 = tablets_routing_v1

    @staticmethod
    def parse_from_supported(supported):
        rate_limit_error = ProtocolFeatures.maybe_parse_rate_limit_error(supported)
        shard_id, sharding_info = ProtocolFeatures.parse_sharding_info(supported)
        tablets_routing_v1 = ProtocolFeatures.parse_tablets_info(supported)
        return ProtocolFeatures(rate_limit_error, shard_id, sharding_info, tablets_routing_v1)

    @staticmethod
    def maybe_parse_rate_limit_error(supported):
        vals = supported.get(RATE_LIMIT_ERROR_EXTENSION)
        if vals is not None:
            code_str = ProtocolFeatures.get_cql_extension_field(vals, "ERROR_CODE")
            return int(code_str)

    #  Looks up a field which starts with `key=` and returns the rest
    @staticmethod
    def get_cql_extension_field(vals, key):
        for v in vals:
            stripped_v = v.strip()
            if stripped_v.startswith(key) and stripped_v[len(key)] == '=':
                result = stripped_v[len(key) + 1:]
                return result
        return None

    def add_startup_options(self, options):
        if self.rate_limit_error is not None:
            options[RATE_LIMIT_ERROR_EXTENSION] = ""
        if self.tablets_routing_v1:
            options[TABLETS_ROUTING_V1] = ""

    @staticmethod
    def parse_sharding_info(options):
        shard_id = options.get('SCYLLA_SHARD', [''])[0] or None
        shards_count = options.get('SCYLLA_NR_SHARDS', [''])[0] or None
        partitioner = options.get('SCYLLA_PARTITIONER', [''])[0] or None
        sharding_algorithm = options.get('SCYLLA_SHARDING_ALGORITHM', [''])[0] or None
        sharding_ignore_msb = options.get('SCYLLA_SHARDING_IGNORE_MSB', [''])[0] or None
        shard_aware_port = options.get('SCYLLA_SHARD_AWARE_PORT', [''])[0] or None
        shard_aware_port_ssl = options.get('SCYLLA_SHARD_AWARE_PORT_SSL', [''])[0] or None
        log.debug("Parsing sharding info from message options %s", options)

        if not (shard_id or shards_count or partitioner == "org.apache.cassandra.dht.Murmur3Partitioner" or
            sharding_algorithm == "biased-token-round-robin" or sharding_ignore_msb):
            return 0, None

        return int(shard_id), _ShardingInfo(shard_id, shards_count, partitioner, sharding_algorithm, sharding_ignore_msb,
                                            shard_aware_port, shard_aware_port_ssl)


    @staticmethod
    def parse_tablets_info(options):
        return TABLETS_ROUTING_V1 in options
