from itertools import islice, cycle, groupby, repeat
import logging
from random import randint

from cassandra import ConsistencyLevel

log = logging.getLogger(__name__)


class HostDistance(object):
    """
    A measure of how "distant" a node is from the client, which
    may influence how the load balancer distributes requests
    and how many connections are opened to the node.
    """

    IGNORED = -1
    """
    A node with this distance should never be queried or have
    connections opened to it.
    """

    LOCAL = 0
    """
    Nodes with ``LOCAL`` distance will be preferred for operations
    under some load balancing policies (such as :class:`.DCAwareRoundRobinPolicy`)
    and will have a greater number of connections opened against
    them by default.

    This distance is typically used for nodes within the same
    datacenter as the client.
    """

    REMOTE = 1
    """
    Nodes with ``REMOTE`` distance will be treated as a last resort
    by some load balancing policies (such as :class:`.DCAwareRoundRobinPolicy`)
    and will have a smaller number of connections opened against
    them by default.

    This distance is typically used for nodes outside of the
    datacenter that the client is running in.
    """


class HostStateListener(object):

    def on_up(self, host):
        """ Called when a node is marked up. """
        raise NotImplementedError()

    def on_down(self, host):
        """ Called when a node is marked down. """
        raise NotImplementedError()

    def on_add(self, host):
        """
        Called when a node is added to the cluster.  The newly added node
        should be considered up.
        """
        raise NotImplementedError()

    def on_remove(self, host):
        """ Called when a node is removed from the cluster. """
        raise NotImplementedError()


class LoadBalancingPolicy(HostStateListener):
    """
    Load balancing policies are used to decide how to distribute
    requests among all possible coordinator nodes in the cluster.

    In particular, they may focus on querying "near" nodes (those
    in a local datacenter) or on querying nodes who happen to
    be replicas for the requested data.

    You may also use subclasses of :class:`.LoadBalancingPolicy` for
    custom behavior.
    """

    def distance(self, host):
        """
        Returns a measure of how remote a :class:`~.pool.Host` is in
        terms of the :class:`.HostDistance` enums.
        """
        raise NotImplementedError()

    def populate(self, cluster, hosts):
        """
        This method is called to initialize the load balancing
        policy with a set of :class:`.Host` instances before its
        first use.  The `cluster` parameter is an instance of
        :class:`.Cluster`.
        """
        raise NotImplementedError()

    def make_query_plan(self, working_keyspace=None, query=None):
        """
        Given a :class:`~.query.Statement` instance, return a iterable
        of :class:`.Host` instances which should be queried in that
        order.  A generator may work well for custom implementations
        of this method.

        Note that the `query` argument may be :const:`None` when preparing
        statements.

        `working_keyspace` should be the string name of the current keyspace,
        as set through :meth:`.Session.set_keyspace()` or with a ``USE``
        statement.
        """
        raise NotImplementedError()

    def check_supported(self):
        """
        This will be called after the cluster Metadata has been initialized.
        If the load balancing policy implementation cannot be supported for
        some reason (such as a missing C extension), this is the point at
        which it should raise an exception.
        """
        pass


class RoundRobinPolicy(LoadBalancingPolicy):
    """
    A subclass of :class:`.LoadBalancingPolicy` which evenly
    distributes queries across all nodes in the cluster,
    regardless of what datacenter the nodes may be in.

    This load balancing policy is used by default.
    """

    def populate(self, cluster, hosts):
        self._live_hosts = set(hosts)
        if len(hosts) <= 1:
            self._position = 0
        else:
            self._position = randint(0, len(hosts) - 1)

    def distance(self, host):
        return HostDistance.LOCAL

    def make_query_plan(self, working_keyspace=None, query=None):
        # not thread-safe, but we don't care much about lost increments
        # for the purposes of load balancing
        pos = self._position
        self._position += 1

        length = len(self._live_hosts)
        if length:
            pos %= length
            return list(islice(cycle(self._live_hosts), pos, pos + length))
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
    """
    Similar to :class:`.RoundRobinPolicy`, but prefers hosts
    in the local datacenter and only uses nodes in remote
    datacenters as a last resort.
    """

    local_dc = None
    used_hosts_per_remote_dc = 0

    def __init__(self, local_dc, used_hosts_per_remote_dc=0):
        """
        The `local_dc` parameter should be the name of the datacenter
        (such as is reported by ``nodetool ring``) that should
        be considered local.

        `used_hosts_per_remote_dc` controls how many nodes in
        each remote datacenter will have connections opened
        against them. In other words, `used_hosts_per_remote_dc` hosts
        will be considered :attr:`~.HostDistance.REMOTE` and the
        rest will be considered :attr:`~.HostDistance.IGNORED`.
        By default, all remote hosts are ignored.
        """
        self.local_dc = local_dc
        self.used_hosts_per_remote_dc = used_hosts_per_remote_dc
        self._dc_live_hosts = {}

    def _dc(self, host):
        return host.datacenter or self.local_dc

    def populate(self, cluster, hosts):
        for dc, dc_hosts in groupby(hosts, lambda h: self._dc(h)):
            self._dc_live_hosts[dc] = set(dc_hosts)

        # position is currently only used for local hosts
        local_live = self._dc_live_hosts.get(self.local_dc)
        if not local_live:
            self._position = 0
        elif len(local_live) == 1:
            self._position = 0
        else:
            self._position = randint(0, len(local_live) - 1)

    def distance(self, host):
        dc = self._dc(host)
        if dc == self.local_dc:
            return HostDistance.LOCAL

        if not self.used_hosts_per_remote_dc:
            return HostDistance.IGNORED
        else:
            dc_hosts = self._dc_live_hosts.get(dc)
            if not dc_hosts:
                return HostDistance.IGNORED

            if host in list(dc_hosts)[:self.used_hosts_per_remote_dc]:
                return HostDistance.REMOTE
            else:
                return HostDistance.IGNORED

    def make_query_plan(self, working_keyspace=None, query=None):
        # not thread-safe, but we don't care much about lost increments
        # for the purposes of load balancing
        pos = self._position
        self._position += 1

        local_live = list(self._dc_live_hosts.get(self.local_dc, ()))
        pos = (pos % len(local_live)) if local_live else 0
        for host in islice(cycle(local_live), pos, pos + len(local_live)):
            yield host

        for dc, current_dc_hosts in self._dc_live_hosts.iteritems():
            if dc == self.local_dc:
                continue

            for host in list(current_dc_hosts)[:self.used_hosts_per_remote_dc]:
                yield host

    def on_up(self, host):
        self._dc_live_hosts.setdefault(self._dc(host), set()).add(host)

    def on_down(self, host):
        self._dc_live_hosts.setdefault(self._dc(host), set()).discard(host)

    def on_add(self, host):
        self._dc_live_hosts.setdefault(self._dc(host), set()).add(host)

    def on_remove(self, host):
        self._dc_live_hosts.setdefault(self._dc(host), set()).discard(host)


class TokenAwarePolicy(LoadBalancingPolicy):
    """
    A :class:`.LoadBalancingPolicy` wrapper that adds token awareness to
    a child policy.

    This alters the child policy's behavior so that it first attempts to
    send queries to :attr:`~.HostDistance.LOCAL` replicas (as determined
    by the child policy) based on the :class:`.Statement`'s
    :attr:`~.Statement.routing_key`.  Once those hosts are exhausted, the
    remaining hosts in the child policy's query plan will be used.

    If no :attr:`~.Statement.routing_key` is set on the query, the child
    policy's query plan will be used as is.
    """

    _child_policy = None
    _cluster_metadata = None

    def __init__(self, child_policy):
        self._child_policy = child_policy

    def populate(self, cluster, hosts):
        self._cluster_metadata = cluster.metadata
        self._child_policy.populate(cluster, hosts)

    def check_supported(self):
        if not self._cluster_metadata.can_support_partitioner():
            raise Exception(
                '%s cannot be used with the cluster partitioner (%s) because '
                'the relevant C extension for this driver was not compiled. '
                'See the installation instructions for details on building '
                'and installing the C extensions.' % (self.__class__.__name__,
                self._cluster_metadata.partitioner))

    def distance(self, *args, **kwargs):
        return self._child_policy.distance(*args, **kwargs)

    def make_query_plan(self, working_keyspace=None, query=None):
        if query and query.keyspace:
            keyspace = query.keyspace
        else:
            keyspace = working_keyspace

        child = self._child_policy
        if query is None:
            for host in child.make_query_plan(keyspace, query):
                yield host
        else:
            routing_key = query.routing_key
            if routing_key is None:
                for host in child.make_query_plan(keyspace, query):
                    yield host
            else:
                replicas = self._cluster_metadata.get_replicas(keyspace, routing_key)
                for replica in replicas:
                    if replica.is_up and \
                            child.distance(replica) == HostDistance.LOCAL:
                        yield replica

                for host in child.make_query_plan(keyspace, query):
                    # skip if we've already listed this host
                    if host not in replicas or \
                            child.distance(host) == HostDistance.REMOTE:
                        yield host

    def on_up(self, *args, **kwargs):
        return self._child_policy.on_up(*args, **kwargs)

    def on_down(self, *args, **kwargs):
        return self._child_policy.on_down(*args, **kwargs)

    def on_add(self, *args, **kwargs):
        return self._child_policy.on_add(*args, **kwargs)

    def on_remove(self, *args, **kwargs):
        return self._child_policy.on_remove(*args, **kwargs)


class WhiteListRoundRobinPolicy(RoundRobinPolicy):
    """
    A subclass of :class:`.RoundRobinPolicy` which evenly
    distributes queries across all nodes in the cluster,
    regardless of what datacenter the nodes may be in, but
    only if that node exists in the list of allowed nodes

    This policy is addresses the issue described in
    https://datastax-oss.atlassian.net/browse/JAVA-145
    Where connection errors occur when connection
    attempts are made to private IP addresses remotely
    """
    def __init__(self, hosts):
        """
        :param hosts: List of hosts
        """
        self._allowed_hosts = hosts

    def populate(self, cluster, hosts):
        self._live_hosts = set()
        for host in hosts:
            if host.address in self._allowed_hosts:
                self._live_hosts.add(host)

        if len(hosts) <= 1:
            self._position = 0
        else:
            self._position = randint(0, len(hosts) - 1)

    def distance(self, host):
        if host.address in self._allowed_hosts:
            return HostDistance.LOCAL
        else:
            return HostDistance.IGNORED

    def on_up(self, host):
        if host.address in self._allowed_hosts:
            self._live_hosts.add(host)

    def on_add(self, host):
        if host.address in self._allowed_hosts:
            self._live_hosts.add(host)

    def on_remove(self, host):
        self._live_hosts.discard(host)


class ConvictionPolicy(object):
    """
    A policy which decides when hosts should be considered down
    based on the types of failures and the number of failures.

    If custom behavior is needed, this class may be subclassed.
    """

    def __init__(self, host):
        """
        `host` is an instance of :class:`.Host`.
        """
        self.host = host

    def add_failure(self, connection_exc):
        """
        Implementations should return :const:`True` if the host should be
        convicted, :const:`False` otherwise.
        """
        raise NotImplementedError()

    def reset(self):
        """
        Implementations should clear out any convictions or state regarding
        the host.
        """
        raise NotImplementedError()


class SimpleConvictionPolicy(ConvictionPolicy):
    """
    The default implementation of :class:`ConvictionPolicy`,
    which simply marks a host as down after the first failure
    of any kind.
    """

    def add_failure(self, connection_exc):
        return True

    def reset(self):
        pass


class ReconnectionPolicy(object):
    """
    This class and its subclasses govern how frequently an attempt is made
    to reconnect to nodes that are marked as dead.

    If custom behavior is needed, this class may be subclassed.
    """

    def new_schedule(self):
        """
        This should return a finite or infinite iterable of delays (each as a
        floating point number of seconds) inbetween each failed reconnection
        attempt.  Note that if the iterable is finite, reconnection attempts
        will cease once the iterable is exhausted.
        """
        raise NotImplementedError()


class ConstantReconnectionPolicy(ReconnectionPolicy):
    """
    A :class:`.ReconnectionPolicy` subclass which sleeps for a fixed delay
    inbetween each reconnection attempt.
    """

    def __init__(self, delay, max_attempts=64):
        """
        `delay` should be a floating point number of seconds to wait inbetween
        each attempt.

        `max_attempts` should be a total number of attempts to be made before
        giving up, or :const:`None` to continue reconnection attempts forever.
        The default is 64.
        """
        if delay < 0:
            raise ValueError("delay must not be negative")
        if max_attempts < 0:
            raise ValueError("max_attempts must not be negative")

        self.delay = delay
        self.max_attempts = max_attempts

    def new_schedule(self):
        return repeat(self.delay, self.max_attempts)


class ExponentialReconnectionPolicy(ReconnectionPolicy):
    """
    A :class:`.ReconnectionPolicy` subclass which exponentially increases
    the length of the delay inbetween each reconnection attempt up to
    a set maximum delay.
    """

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
    """
    For usage with :class:`.RetryPolicy`, this describe a type
    of write operation.
    """

    SIMPLE = 0
    """
    A write to a single partition key. Such writes are guaranteed to be atomic
    and isolated.
    """

    BATCH = 1
    """
    A write to multiple partition keys that used the distributed batch log to
    ensure atomicity.
    """

    UNLOGGED_BATCH = 2
    """
    A write to multiple partition keys that did not use the distributed batch
    log. Atomicity for such writes is not guaranteed.
    """

    COUNTER = 3
    """
    A counter write (for one or multiple partition keys). Such writes should
    not be replayed in order to avoid overcount.
    """

    BATCH_LOG = 4
    """
    The initial write to the distributed batch log that Cassandra performs
    internally before a BATCH write.
    """


class RetryPolicy(object):
    """
    A policy that describes whether to retry, rethrow, or ignore timeout
    and unavailable failures.

    To specify a default retry policy, set the
    :attr:`.Cluster.default_retry_policy` attribute to an instance of this
    class or one of its subclasses.

    To specify a retry policy per query, set the :attr:`.Statement.retry_policy`
    attribute to an instance of this class or one of its subclasses.

    If custom behavior is needed for retrying certain operations,
    this class may be subclassed.
    """

    RETRY = 0
    """
    This should be returned from the below methods if the operation
    should be retried on the same connection.
    """

    RETHROW = 1
    """
    This should be returned from the below methods if the failure
    should be propagated and no more retries attempted.
    """

    IGNORE = 2
    """
    This should be returned from the below methods if the failure
    should be ignored but no more retries should be attempted.
    """

    def on_read_timeout(self, query, consistency, required_responses,
                        received_responses, data_retrieved, retry_num):
        """
        This is called when a read operation times out from the coordinator's
        perspective (i.e. a replica did not respond to the coordinator in time).
        It should return a tuple with two items: one of the class enums (such
        as :attr:`.RETRY`) and a :class:`.ConsistencyLevel` to retry the
        operation at or :const:`None` to keep the same consistency level.

        `query` is the :class:`.Statement` that timed out.

        `consistency` is the :class:`.ConsistencyLevel` that the operation was
        attempted at.

        The `required_responses` and `received_responses` parameters describe
        how many replicas needed to respond to meet the requested consistency
        level and how many actually did respond before the coordinator timed
        out the request. `data_retrieved` is a boolean indicating whether
        any of those responses contained data (as opposed to just a digest).

        `retry_num` counts how many times the operation has been retried, so
        the first time this method is called, `retry_num` will be 0.

        By default, operations will be retried at most once, and only if
        a sufficient number of replicas responded (with data digests).
        """
        if retry_num != 0:
            return (self.RETHROW, None)
        elif received_responses >= required_responses and not data_retrieved:
            return (self.RETRY, consistency)
        else:
            return (self.RETHROW, None)

    def on_write_timeout(self, query, consistency, write_type,
                         required_responses, received_responses, retry_num):
        """
        This is called when a write operation times out from the coordinator's
        perspective (i.e. a replica did not respond to the coordinator in time).

        `query` is the :class:`.Statement` that timed out.

        `consistency` is the :class:`.ConsistencyLevel` that the operation was
        attempted at.

        `write_type` is one of the :class:`.WriteType` enums describing the
        type of write operation.

        The `required_responses` and `received_responses` parameters describe
        how many replicas needed to acknowledge the write to meet the requested
        consistency level and how many replicas actually did acknowledge the
        write before the coordinator timed out the request.

        `retry_num` counts how many times the operation has been retried, so
        the first time this method is called, `retry_num` will be 0.

        By default, failed write operations will retried at most once, and
        they will only be retried if the `write_type` was
        :attr:`~.WriteType.BATCH_LOG`.
        """
        if retry_num != 0:
            return (self.RETHROW, None)
        elif write_type == WriteType.BATCH_LOG:
            return (self.RETRY, consistency)
        else:
            return (self.RETHROW, None)

    def on_unavailable(self, query, consistency, required_replicas, alive_replicas, retry_num):
        """
        This is called when the coordinator node determines that a read or
        write operation cannot be successful because the number of live
        replicas are too low to meet the requested :class:`.ConsistencyLevel`.
        This means that the read or write operation was never forwared to
        any replicas.

        `query` is the :class:`.Statement` that failed.

        `consistency` is the :class:`.ConsistencyLevel` that the operation was
        attempted at.

        `required_replicas` is the number of replicas that would have needed to
        acknowledge the operation to meet the requested consistency level.
        `alive_replicas` is the number of replicas that the coordinator
        considered alive at the time of the request.

        `retry_num` counts how many times the operation has been retried, so
        the first time this method is called, `retry_num` will be 0.

        By default, no retries will be attempted and the error will be re-raised.
        """
        return (self.RETHROW, None)


class FallthroughRetryPolicy(RetryPolicy):
    """
    A retry policy that never retries and always propagates failures to
    the application.
    """

    def on_read_timeout(self, *args, **kwargs):
        return (self.RETHROW, None)

    def on_write_timeout(self, *args, **kwargs):
        return (self.RETHROW, None)

    def on_unavailable(self, *args, **kwargs):
        return (self.RETHROW, None)


class DowngradingConsistencyRetryPolicy(RetryPolicy):
    """
    A retry policy that sometimes retries with a lower consistency level than
    the one initially requested.

    **BEWARE**: This policy may retry queries using a lower consistency
    level than the one initially requested. By doing so, it may break
    consistency guarantees. In other words, if you use this retry policy,
    there are cases (documented below) where a read at :attr:`~.QUORUM`
    *may not* see a preceding write at :attr:`~.QUORUM`. Do not use this
    policy unless you have understood the cases where this can happen and
    are ok with that. It is also recommended to subclass this class so
    that queries that required a consistency level downgrade can be
    recorded (so that repairs can be made later, etc).

    This policy implements the same retries as :class:`.RetryPolicy`,
    but on top of that, it also retries in the following cases:

    * On a read timeout: if the number of replicas that responded is
      greater than one but lower than is required by the requested
      consistency level, the operation is retried at a lower consistency
      level.
    * On a write timeout: if the operation is an :attr:`~.UNLOGGED_BATCH`
      and at least one replica acknowledged the write, the operation is
      retried at a lower consistency level.  Furthermore, for other
      write types, if at least one replica acknowledged the write, the
      timeout is ignored.
    * On an unavailable exception: if at least one replica is alive, the
      operation is retried at a lower consistency level.

    The reasoning behind this retry policy is as follows: if, based
    on the information the Cassandra coordinator node returns, retrying the
    operation with the initially requested consistency has a chance to
    succeed, do it. Otherwise, if based on that information we know the
    initially requested consistency level cannot be achieved currently, then:

    * For writes, ignore the exception (thus silently failing the
      consistency requirement) if we know the write has been persisted on at
      least one replica.
    * For reads, try reading at a lower consistency level (thus silently
      failing the consistency requirement).

    In other words, this policy implements the idea that if the requested
    consistency level cannot be achieved, the next best thing for writes is
    to make sure the data is persisted, and that reading something is better
    than reading nothing, even if there is a risk of reading stale data.
    """
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
