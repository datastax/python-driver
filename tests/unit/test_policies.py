try:
    import unittest2 as unittest
except ImportError:
    import unittest

from itertools import islice, cycle
from mock import Mock
import struct
from threading import Thread

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.metadata import Metadata
from cassandra.policies import (RoundRobinPolicy, DCAwareRoundRobinPolicy,
                                TokenAwarePolicy, SimpleConvictionPolicy,
                                HostDistance, ExponentialReconnectionPolicy,
                                RetryPolicy, WriteType,
                                DowngradingConsistencyRetryPolicy)
from cassandra.pool import Host
from cassandra.query import Query

class TestRoundRobinPolicy(unittest.TestCase):

    def test_basic(self):
        hosts = [0, 1, 2, 3]
        policy = RoundRobinPolicy()
        policy.populate(None, hosts)
        qplan = list(policy.make_query_plan())
        self.assertEqual(sorted(qplan), hosts)

    def test_multiple_query_plans(self):
        hosts = [0, 1, 2, 3]
        policy = RoundRobinPolicy()
        policy.populate(None, hosts)
        for i in xrange(20):
            qplan = list(policy.make_query_plan())
            self.assertEqual(sorted(qplan), hosts)

    def test_single_host(self):
        policy = RoundRobinPolicy()
        policy.populate(None, [0])
        qplan = list(policy.make_query_plan())
        self.assertEqual(qplan, [0])

    def test_status_updates(self):
        hosts = [0, 1, 2, 3]
        policy = RoundRobinPolicy()
        policy.populate(None, hosts)
        policy.on_down(0)
        policy.on_remove(1)
        policy.on_up(4)
        policy.on_add(5)
        qplan = list(policy.make_query_plan())
        self.assertEqual(sorted(qplan), [2, 3, 4, 5])

    def test_thread_safety(self):
        hosts = range(100)
        policy = RoundRobinPolicy()
        policy.populate(None, hosts)

        def check_query_plan():
            for i in range(100):
                qplan = list(policy.make_query_plan())
                self.assertEqual(sorted(qplan), hosts)

        threads = [Thread(target=check_query_plan) for i in range(4)]
        map(lambda t: t.start(), threads)
        map(lambda t: t.join(), threads)


class TestDCAwareRoundRobinPolicy(unittest.TestCase):

    def test_no_remote(self):
        hosts = []
        for i in range(4):
            h = Host(i, SimpleConvictionPolicy)
            h.set_location_info("dc1", "rack1")
            hosts.append(h)

        policy = DCAwareRoundRobinPolicy("dc1")
        policy.populate(None, hosts)
        qplan = list(policy.make_query_plan())
        self.assertEqual(sorted(qplan), sorted(hosts))

    def test_with_remotes(self):
        hosts = [Host(i, SimpleConvictionPolicy) for i in range(4)]
        for h in hosts[:2]:
            h.set_location_info("dc1", "rack1")
        for h in hosts[2:]:
            h.set_location_info("dc2", "rack1")

        local_hosts = set(h for h in hosts if h.datacenter == "dc1")
        remote_hosts = set(h for h in hosts if h.datacenter != "dc1")

        # allow all of the remote hosts to be used
        policy = DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=2)
        policy.populate(None, hosts)
        qplan = list(policy.make_query_plan())
        self.assertEqual(set(qplan[:2]), local_hosts)
        self.assertEqual(set(qplan[2:]), remote_hosts)

        # allow only one of the remote hosts to be used
        policy = DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=1)
        policy.populate(None, hosts)
        qplan = list(policy.make_query_plan())
        self.assertEqual(set(qplan[:2]), local_hosts)

        used_remotes = set(qplan[2:])
        self.assertEqual(1, len(used_remotes))
        self.assertIn(qplan[2], remote_hosts)

        # allow no remote hosts to be used
        policy = DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=0)
        policy.populate(None, hosts)
        qplan = list(policy.make_query_plan())
        self.assertEqual(2, len(qplan))
        self.assertEqual(local_hosts, set(qplan))

    def test_get_distance(self):
        policy = DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=0)
        host = Host("ip1", SimpleConvictionPolicy)
        host.set_location_info("dc1", "rack1")
        policy.populate(None, [host])

        self.assertEqual(policy.distance(host), HostDistance.LOCAL)

        # used_hosts_per_remote_dc is set to 0, so ignore it
        remote_host = Host("ip2", SimpleConvictionPolicy)
        remote_host.set_location_info("dc2", "rack1")
        self.assertEqual(policy.distance(remote_host), HostDistance.IGNORED)

        # dc2 isn't registered in the policy's live_hosts dict
        policy.used_hosts_per_remote_dc = 1
        self.assertEqual(policy.distance(remote_host), HostDistance.IGNORED)

        # make sure the policy has both dcs registered
        policy.populate(None, [host, remote_host])
        self.assertEqual(policy.distance(remote_host), HostDistance.REMOTE)

        # since used_hosts_per_remote_dc is set to 1, only the first
        # remote host in dc2 will be REMOTE, the rest are IGNORED
        second_remote_host = Host("ip3", SimpleConvictionPolicy)
        second_remote_host.set_location_info("dc2", "rack1")
        policy.populate(None, [host, remote_host, second_remote_host])
        distances = set([policy.distance(remote_host), policy.distance(second_remote_host)])
        self.assertEqual(distances, set([HostDistance.REMOTE, HostDistance.IGNORED]))

    def test_status_updates(self):
        hosts = [Host(i, SimpleConvictionPolicy) for i in range(4)]
        for h in hosts[:2]:
            h.set_location_info("dc1", "rack1")
        for h in hosts[2:]:
            h.set_location_info("dc2", "rack1")

        policy = DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=1)
        policy.populate(None, hosts)
        policy.on_down(hosts[0])
        policy.on_remove(hosts[2])

        new_local_host = Host(4, SimpleConvictionPolicy)
        new_local_host.set_location_info("dc1", "rack1")
        policy.on_up(new_local_host)

        new_remote_host = Host(5, SimpleConvictionPolicy)
        new_remote_host.set_location_info("dc9000", "rack1")
        policy.on_add(new_remote_host)

        # we now have two local hosts and two remote hosts in separate dcs
        qplan = list(policy.make_query_plan())
        self.assertEqual(set(qplan[:2]), set([hosts[1], new_local_host]))
        self.assertEqual(set(qplan[2:]), set([hosts[3], new_remote_host]))

        # since we have hosts in dc9000, the distance shouldn't be IGNORED
        self.assertEqual(policy.distance(new_remote_host), HostDistance.REMOTE)


class TokenAwarePolicyTest(unittest.TestCase):

    def test_wrap_round_robin(self):
        cluster = Mock(spec=Cluster)
        cluster.metadata = Mock(spec=Metadata)
        hosts = [Host(str(i), SimpleConvictionPolicy) for i in range(4)]

        def get_replicas(packed_key):
            index = struct.unpack('>i', packed_key)[0]
            return list(islice(cycle(hosts), index, index + 2))

        cluster.metadata.get_replicas.side_effect = get_replicas

        policy = TokenAwarePolicy(RoundRobinPolicy())
        policy.populate(cluster, hosts)

        for i in range(4):
            query = Query(routing_key=struct.pack('>i', i))
            qplan = list(policy.make_query_plan(query))

            replicas = get_replicas(struct.pack('>i', i))
            other = set(h for h in hosts if h not in replicas)
            self.assertEquals(replicas, qplan[:2])
            self.assertEquals(other, set(qplan[2:]))

    def test_wrap_dc_aware(self):
        cluster = Mock(spec=Cluster)
        cluster.metadata = Mock(spec=Metadata)
        hosts = [Host(str(i), SimpleConvictionPolicy) for i in range(4)]
        for h in hosts[:2]:
            h.set_location_info("dc1", "rack1")
        for h in hosts[2:]:
            h.set_location_info("dc2", "rack1")

        def get_replicas(packed_key):
            index = struct.unpack('>i', packed_key)[0]
            # return one node from each DC
            if index % 2 == 0:
                return [hosts[0], hosts[2]]
            else:
                return [hosts[1], hosts[3]]

        cluster.metadata.get_replicas.side_effect = get_replicas

        policy = TokenAwarePolicy(DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=1))
        policy.populate(cluster, hosts)

        for i in range(4):
            query = Query(routing_key=struct.pack('>i', i))
            qplan = list(policy.make_query_plan(query))
            replicas = get_replicas(struct.pack('>i', i))

            # first should be the only local replica
            self.assertIn(qplan[0], replicas)
            self.assertEquals(qplan[0].datacenter, "dc1")

            # then the local non-replica
            self.assertNotIn(qplan[1], replicas)
            self.assertEquals(qplan[1].datacenter, "dc1")

            # then one of the remotes (used_hosts_per_remote_dc is 1, so we
            # shouldn't see two remotes)
            self.assertEquals(qplan[2].datacenter, "dc2")
            self.assertEquals(3, len(qplan))


class ExponentialReconnectionPolicyTest(unittest.TestCase):

    def test_bad_vals(self):
        self.assertRaises(ValueError, ExponentialReconnectionPolicy, -1, 0)
        self.assertRaises(ValueError, ExponentialReconnectionPolicy, 0, -1)
        self.assertRaises(ValueError, ExponentialReconnectionPolicy, 9000, 1)

    def test_schedule(self):
        policy = ExponentialReconnectionPolicy(base_delay=2, max_delay=100)
        schedule = list(policy.new_schedule())
        self.assertEqual(len(schedule), 64)
        for i, delay in enumerate(schedule):
            if i == 0:
                self.assertEqual(delay, 2)
            elif i < 6:
                self.assertEqual(delay, schedule[i - 1] * 2)
            else:
                self.assertEqual(delay, 100)


class RetryPolicyTest(unittest.TestCase):

    def test_read_timeout(self):
        policy = RetryPolicy()

        # if this is the second or greater attempt, rethrow
        retry, consistency = policy.on_read_timeout(
            query=None, consistency="ONE", required_responses=1, received_responses=2,
            data_retrieved=True, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)

        # if we didn't get enough responses, rethrow
        retry, consistency = policy.on_read_timeout(
            query=None, consistency="ONE", required_responses=2, received_responses=1,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)

        # if we got enough responses, but also got a data response, rethrow
        retry, consistency = policy.on_read_timeout(
            query=None, consistency="ONE", required_responses=2, received_responses=2,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)

        # we got enough reponses but no data response, so retry
        retry, consistency = policy.on_read_timeout(
            query=None, consistency="ONE", required_responses=2, received_responses=2,
            data_retrieved=False, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, "ONE")

    def test_write_timeout(self):
        policy = RetryPolicy()

        # if this is the second or greater attempt, rethrow
        retry, consistency = policy.on_write_timeout(
            query=None, consistency="ONE", write_type=WriteType.SIMPLE,
            required_responses=1, received_responses=2, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)

        # if it's not a BATCH_LOG write, don't retry it
        retry, consistency = policy.on_write_timeout(
            query=None, consistency="ONE", write_type=WriteType.SIMPLE,
            required_responses=1, received_responses=2, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)

        # retry BATCH_LOG writes regardless of received responses
        retry, consistency = policy.on_write_timeout(
            query=None, consistency="ONE", write_type=WriteType.BATCH_LOG,
            required_responses=10000, received_responses=1, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, "ONE")


class DowngradingConsistencyRetryPolicyTest(unittest.TestCase):

    def test_read_timeout(self):
        policy = DowngradingConsistencyRetryPolicy()

        # if this is the second or greater attempt, rethrow
        retry, consistency = policy.on_read_timeout(
            query=None, consistency="ONE", required_responses=1, received_responses=2,
            data_retrieved=True, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)

        # if we didn't get enough responses, retry at a lower consistency
        retry, consistency = policy.on_read_timeout(
            query=None, consistency="ONE", required_responses=3, received_responses=2,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, ConsistencyLevel.TWO)

        # retry consistency level goes down based on the # of recv'd responses
        retry, consistency = policy.on_read_timeout(
            query=None, consistency="ONE", required_responses=3, received_responses=1,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, ConsistencyLevel.ONE)

        # if we got no responses, rethrow
        retry, consistency = policy.on_read_timeout(
            query=None, consistency="ONE", required_responses=3, received_responses=0,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)

        # if we got enough response but no data, retry
        retry, consistency = policy.on_read_timeout(
            query=None, consistency="ONE", required_responses=3, received_responses=3,
            data_retrieved=False, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)

        # if we got enough responses, but also got a data response, rethrow
        retry, consistency = policy.on_read_timeout(
            query=None, consistency="ONE", required_responses=2, received_responses=2,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)

    def test_write_timeout(self):
        policy = DowngradingConsistencyRetryPolicy()

        # if this is the second or greater attempt, rethrow
        retry, consistency = policy.on_write_timeout(
            query=None, consistency="ONE", write_type=WriteType.SIMPLE,
            required_responses=1, received_responses=2, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)

        # ignore failures on these types of writes
        for write_type in (WriteType.SIMPLE, WriteType.BATCH, WriteType.COUNTER):
            retry, consistency = policy.on_write_timeout(
                query=None, consistency="ONE", write_type=write_type,
                required_responses=1, received_responses=2, retry_num=0)
            self.assertEqual(retry, RetryPolicy.IGNORE)

        # downgrade consistency level on unlogged batch writes
        retry, consistency = policy.on_write_timeout(
            query=None, consistency="ONE", write_type=WriteType.UNLOGGED_BATCH,
            required_responses=3, received_responses=1, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, ConsistencyLevel.ONE)

        # retry batch log writes at the same consistency level
        retry, consistency = policy.on_write_timeout(
            query=None, consistency="ONE", write_type=WriteType.BATCH_LOG,
            required_responses=3, received_responses=1, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, "ONE")

    def test_unavailable(self):
        policy = DowngradingConsistencyRetryPolicy()

        # if this is the second or greater attempt, rethrow
        retry, consistency = policy.on_unavailable(
            query=None, consistency="ONE", required_replicas=3, alive_replicas=1, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)

        # downgrade consistency on unavailable exceptions
        retry, consistency = policy.on_unavailable(
            query=None, consistency="ONE", required_replicas=3, alive_replicas=1, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, ConsistencyLevel.ONE)
