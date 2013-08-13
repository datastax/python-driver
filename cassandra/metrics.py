from itertools import chain
import logging

from greplin import scales

log = logging.getLogger(__name__)

class Metrics(object):

    _cluster_proxy = None

    def __init__(self, cluster_proxy):
        log.debug("Starting metric capture")

        self.stats = scales.collection('/cassandra',
            scales.IntStat('connection_errors'),
            scales.IntStat('write_timeouts'),
            scales.IntStat('read_timeouts'),
            scales.IntStat('unavailables'),
            scales.IntStat('other_errors'),
            scales.IntStat('retries'),
            scales.IntStat('ignores'),

            # gauges
            scales.Stat('known_hosts',
                lambda: len(cluster_proxy.metadata.all_hosts())),
            scales.Stat('connected_to',
                lambda: len(set(chain.from_iterable(s._pools.keys() for s in cluster_proxy.sessions)))),
            scales.Stat('open_connections',
                lambda: sum(sum(p.open_count for p in s._pools.values()) for s in cluster_proxy.sessions)))
