from itertools import islice, cycle, groupby, repeat
from random import randint
from threading import RLock

from decoder import ConsistencyLevel

class HostDistance(object):

    IGNORED = -1
    LOCAL = 0
    REMOTE = 1

class LoadBalancingPolicy(object):

    def distance(self, host):
        raise NotImplemented()

    def make_query_plan(self, query=None):
        raise NotImplemented()


class RoundRobinPolicy(LoadBalancingPolicy):

    def __init__(self):
        self._lock = RLock()

    def populate(self, cluster, hosts):
        self._live_hosts = set(hosts)
        if len(hosts) == 1:
            self._position = 0
        else:
            self._position = randint(0, len(hosts) - 1)

    def distance(self, host):
        return HostDistance.LOCAL

    def make_query_plan(self, query=None):
        with self._lock:
            pos = self._position
            self._position += 1

        length = len(self._live_hosts)
        if length:
            pos %= length
            return islice(cycle(self._live_hosts), pos, pos + length)
        else:
            return []

    def on_up(self, host):
        self._live_hosts.add(host)

    def on_down(self, host):
        self._live_hosts.discard(host)

    def on_add(self, host):
        self._live_hosts.add(host)

    def on_remove(self, host):
        self._live_hosts.remove(host)


class DCAwareRoundRobinPolicy(LoadBalancingPolicy):

    def __init__(self, local_dc, used_hosts_per_remote_dc=0):
        self.local_dc = local_dc
        self.used_hosts_per_remote_dc = used_hosts_per_remote_dc
        self._dc_live_hosts = {}
        self._lock = RLock()

    def populate(self, cluster, hosts):
        for dc, dc_hosts in groupby(hosts, lambda h: h.datacenter):
            self._dc_live_hosts[dc] = set(dc_hosts)

        # position is currently only used for local hosts
        local_live = self._dc_live_hosts.get(self.local_dc)
        if len(local_live) == 1:
            self._position = 0
        else:
            self._position = randint(0, len(local_live) - 1)

    def distance(self, host):
        if host.datacenter == self.local_dc:
            return HostDistance.LOCAL

        if not self.used_hosts_per_remote_dc:
            return HostDistance.IGNORED
        else:
            dc_hosts = self._dc_live_hosts.get(host.datacenter)
            if not dc_hosts:
                return HostDistance.IGNORED

            if host in list(dc_hosts)[:self.used_hosts_per_remote_dc]:
                return HostDistance.REMOTE
            else:
                return HostDistance.IGNORED

    def make_query_plan(self, query=None):
        with self._lock:
            pos = self._position
            self._position += 1

        local_live = list(self._dc_live_hosts.get(self.local_dc))
        pos %= len(local_live)
        for host in islice(cycle(local_live), pos, pos + len(local_live)):
            yield host

        for dc, current_dc_hosts in self._dc_live_hosts.iteritems():
            if dc == self.local_dc:
                continue

            for host in current_dc_hosts:
                yield host

    def on_up(self, host):
        self._dc_live_hosts.setdefault(host.datacenter, set()).add(host)

    def on_down(self, host):
        self._dc_live_hosts.setdefault(host.datacenter, set()).discard(host)

    def on_add(self, host):
        self._dc_live_hosts.setdefault(host.datacenter, set()).add(host)

    def on_remove(self, host):
        self._dc_live_hosts.setdefault(host.datacenter, set()).discard(host)


class ConvictionPolicy(object):

    def __init__(self, host):
        self.host = host

    def add_failure(connection_exc):
        """
        Implementations should return ``True`` if the host should be
        convicted, ``False`` otherwise.
        """
        raise NotImplemented()

    def reset(self):
        """
        Implementations should clear out any convictions or state regarding
        the host.
        """
        raise NotImplemented()


class SimpleConvictionPolicy(ConvictionPolicy):

    def add_failure(connection_exc):
        return True

    def reset(self):
        pass


class ReconnectionPolicy(object):

    def new_schedule(self):
        raise NotImplemented()


class ConstantReconnectionPolicy(ReconnectionPolicy):

    def __init__(self, delay):
        if delay < 0:
            raise ValueError("Delay may not be negative")

        self.delay = delay

    def new_schedule(self):
        return repeat(self.delay)


class ExponentialReconnectionPolicy(ReconnectionPolicy):

    def __init__(self, base_delay, max_delay):
        """
        `base_delay` and `max_delay` should be in floating point units of
        seconds.
        """
        if base_delay < 0 or max_delay < 0:
            raise ValueError("Delays may not be negative")

        if max_delay < base_delay:
            raise ValueError("Max delay must be greater than base delay")

        self.base_delay = base_delay
        self.max_delay = max_delay

    def new_schedule(self):
        return (min(self.base_delay * (2 ** i), self.max_delay) for i in xrange(64))


class WriteType(object):

    SIMPLE = 0
    BATCH = 1
    UNLOGGED_BATCH = 2
    COUNTER = 3
    BATCH_LOG = 4


class RetryPolicy(object):

    RETRY = 0
    RETHROW = 1
    IGNORE = 2

    def on_read_timeout(self, query, consistency, required_responses,
                        received_responses, data_retrieved, retry_num):
        if retry_num != 0:
            return (self.RETHROW, None)
        elif received_responses >= required_responses and not data_retrieved:
            return (self.RETRY, consistency)
        else:
            return (self.RETHROW, None)

    def on_write_timeout(self, query, consistency, write_type,
                         required_responses, received_responses, retry_num):
        if retry_num != 0:
            return (self.RETHROW, None)
        elif write_type == WriteType.BATCH_LOG:
            return (self.RETRY, consistency)
        else:
            return (self.RETHROW, None)

    def on_unavailable(self, query, consistency, required_replicas, alive_replicas, retry_num):
        return (self.RETHROW, None)


class FallthroughRetryPolicy(RetryPolicy):

    def on_read_timeout(self, *args, **kwargs):
        return (self.RETHROW, None)

    def on_write_timeout(self, *args, **kwargs):
        return (self.RETHROW, None)

    def on_unavailable(self, *args, **kwargs):
        return (self.RETHROW, None)


class DowngradingConsistencyRetryPolicy(RetryPolicy):

    def _pick_consistency(self, num_responses):
        if num_responses >= 3:
            return (self.RETRY, ConsistencyLevel.THREE)
        elif num_responses >= 2:
            return (self.RETRY, ConsistencyLevel.TWO)
        elif num_responses >= 1:
            return (self.RETRY, ConsistencyLevel.ONE)
        else:
            return (self.RETHROW, None)

    def on_read_timeout(self, query, consistency, required_responses,
                        received_responses, data_retrieved, retry_num):
        if retry_num != 0:
            return (self.RETHROW, None)
        elif received_responses < required_responses:
            return self._pick_consistency(received_responses)
        elif not data_retrieved:
            return (self.RETRY, consistency)
        else:
            return (self.RETHROW, None)

    def on_write_timeout(self, query, consistency, write_type,
                         required_responses, received_responses, retry_num):
        if retry_num != 0:
            return (self.RETHROW, None)
        elif write_type in (WriteType.SIMPLE, WriteType.BATCH, WriteType.COUNTER):
            return (self.IGNORE, None)
        elif write_type == WriteType.UNLOGGED_BATCH:
            return self._pick_consistency(received_responses)
        elif write_type == WriteType.BATCH_LOG:
            return (self.RETRY, consistency)
        else:
            return (self.RETHROW, None)

    def on_unavailable(self, query, consistency, required_replicas, alive_replicas, retry_num):
        if retry_num != 0:
            return (self.RETHROW, None)
        else:
            return self._pick_consistency(alive_replicas)
