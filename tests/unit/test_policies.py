# Copyright 2013-2017 DataStax, Inc.
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

from itertools import islice, cycle
from mock import Mock, patch
from random import randint
import six
import sys
import struct
from threading import Thread

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.metadata import Metadata
from cassandra.policies import (RoundRobinPolicy, WhiteListRoundRobinPolicy, DCAwareRoundRobinPolicy,
                                TokenAwarePolicy, SimpleConvictionPolicy,
                                HostDistance, ExponentialReconnectionPolicy,
                                RetryPolicy, WriteType,
                                DowngradingConsistencyRetryPolicy, ConstantReconnectionPolicy,
                                LoadBalancingPolicy, ConvictionPolicy, ReconnectionPolicy, FallthroughRetryPolicy,
                                IdentityTranslator, EC2MultiRegionTranslator)
from cassandra.pool import Host
from cassandra.query import Statement

from six.moves import xrange


class LoadBalancingPolicyTest(unittest.TestCase):
    def test_non_implemented(self):
        """
        Code coverage for interface-style base class
        """

        policy = LoadBalancingPolicy()
        host = Host("ip1", SimpleConvictionPolicy)
        host.set_location_info("dc1", "rack1")

        self.assertRaises(NotImplementedError, policy.distance, host)
        self.assertRaises(NotImplementedError, policy.populate, None, host)
        self.assertRaises(NotImplementedError, policy.make_query_plan)
        self.assertRaises(NotImplementedError, policy.on_up, host)
        self.assertRaises(NotImplementedError, policy.on_down, host)
        self.assertRaises(NotImplementedError, policy.on_add, host)
        self.assertRaises(NotImplementedError, policy.on_remove, host)

    def test_instance_check(self):
        self.assertRaises(TypeError, Cluster, load_balancing_policy=RoundRobinPolicy)


class RoundRobinPolicyTest(unittest.TestCase):

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
                self.assertEqual(sorted(qplan), list(hosts))

        threads = [Thread(target=check_query_plan) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def test_thread_safety_during_modification(self):
        hosts = range(100)
        policy = RoundRobinPolicy()
        policy.populate(None, hosts)

        errors = []

        def check_query_plan():
            try:
                for i in xrange(100):
                    list(policy.make_query_plan())
            except Exception as exc:
                errors.append(exc)

        def host_up():
            for i in xrange(1000):
                policy.on_up(randint(0, 99))

        def host_down():
            for i in xrange(1000):
                policy.on_down(randint(0, 99))

        threads = []
        for i in range(5):
            threads.append(Thread(target=check_query_plan))
            threads.append(Thread(target=host_up))
            threads.append(Thread(target=host_down))

        # make the GIL switch after every instruction, maximizing
        # the chance of race conditions
        check = six.PY2 or '__pypy__' in sys.builtin_module_names
        if check:
            original_interval = sys.getcheckinterval()
        else:
            original_interval = sys.getswitchinterval()

        try:
            if check:
                sys.setcheckinterval(0)
            else:
                sys.setswitchinterval(0.0001)
            map(lambda t: t.start(), threads)
            map(lambda t: t.join(), threads)
        finally:
            if check:
                sys.setcheckinterval(original_interval)
            else:
                sys.setswitchinterval(original_interval)

        if errors:
            self.fail("Saw errors: %s" % (errors,))

    def test_no_live_nodes(self):
        """
        Ensure query plan for a downed cluster will execute without errors
        """
        hosts = [0, 1, 2, 3]
        policy = RoundRobinPolicy()
        policy.populate(None, hosts)

        for i in range(4):
            policy.on_down(i)

        qplan = list(policy.make_query_plan())
        self.assertEqual(qplan, [])


class DCAwareRoundRobinPolicyTest(unittest.TestCase):

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
        policy.populate(Mock(), hosts)
        qplan = list(policy.make_query_plan())
        self.assertEqual(set(qplan[:2]), local_hosts)
        self.assertEqual(set(qplan[2:]), remote_hosts)

        # allow only one of the remote hosts to be used
        policy = DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=1)
        policy.populate(Mock(), hosts)
        qplan = list(policy.make_query_plan())
        self.assertEqual(set(qplan[:2]), local_hosts)

        used_remotes = set(qplan[2:])
        self.assertEqual(1, len(used_remotes))
        self.assertIn(qplan[2], remote_hosts)

        # allow no remote hosts to be used
        policy = DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=0)
        policy.populate(Mock(), hosts)
        qplan = list(policy.make_query_plan())
        self.assertEqual(2, len(qplan))
        self.assertEqual(local_hosts, set(qplan))

    def test_get_distance(self):
        policy = DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=0)
        host = Host("ip1", SimpleConvictionPolicy)
        host.set_location_info("dc1", "rack1")
        policy.populate(Mock(), [host])

        self.assertEqual(policy.distance(host), HostDistance.LOCAL)

        # used_hosts_per_remote_dc is set to 0, so ignore it
        remote_host = Host("ip2", SimpleConvictionPolicy)
        remote_host.set_location_info("dc2", "rack1")
        self.assertEqual(policy.distance(remote_host), HostDistance.IGNORED)

        # dc2 isn't registered in the policy's live_hosts dict
        policy.used_hosts_per_remote_dc = 1
        self.assertEqual(policy.distance(remote_host), HostDistance.IGNORED)

        # make sure the policy has both dcs registered
        policy.populate(Mock(), [host, remote_host])
        self.assertEqual(policy.distance(remote_host), HostDistance.REMOTE)

        # since used_hosts_per_remote_dc is set to 1, only the first
        # remote host in dc2 will be REMOTE, the rest are IGNORED
        second_remote_host = Host("ip3", SimpleConvictionPolicy)
        second_remote_host.set_location_info("dc2", "rack1")
        policy.populate(Mock(), [host, remote_host, second_remote_host])
        distances = set([policy.distance(remote_host), policy.distance(second_remote_host)])
        self.assertEqual(distances, set([HostDistance.REMOTE, HostDistance.IGNORED]))

    def test_status_updates(self):
        hosts = [Host(i, SimpleConvictionPolicy) for i in range(4)]
        for h in hosts[:2]:
            h.set_location_info("dc1", "rack1")
        for h in hosts[2:]:
            h.set_location_info("dc2", "rack1")

        policy = DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=1)
        policy.populate(Mock(), hosts)
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

        policy.on_down(new_local_host)
        policy.on_down(hosts[1])
        qplan = list(policy.make_query_plan())
        self.assertEqual(set(qplan), set([hosts[3], new_remote_host]))

        policy.on_down(new_remote_host)
        policy.on_down(hosts[3])
        qplan = list(policy.make_query_plan())
        self.assertEqual(qplan, [])

    def test_modification_during_generation(self):
        hosts = [Host(i, SimpleConvictionPolicy) for i in range(4)]
        for h in hosts[:2]:
            h.set_location_info("dc1", "rack1")
        for h in hosts[2:]:
            h.set_location_info("dc2", "rack1")

        policy = DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=3)
        policy.populate(Mock(), hosts)

        # The general concept here is to change thee internal state of the
        # policy during plan generation. In this case we use a grey-box
        # approach that changes specific things during known phases of the
        # generator.

        new_host = Host(4, SimpleConvictionPolicy)
        new_host.set_location_info("dc1", "rack1")

        # new local before iteration
        plan = policy.make_query_plan()
        policy.on_up(new_host)
        # local list is not bound yet, so we get to see that one
        self.assertEqual(len(list(plan)), 3 + 2)

        # remove local before iteration
        plan = policy.make_query_plan()
        policy.on_down(new_host)
        # local list is not bound yet, so we don't see it
        self.assertEqual(len(list(plan)), 2 + 2)

        # new local after starting iteration
        plan = policy.make_query_plan()
        next(plan)
        policy.on_up(new_host)
        # local list was is bound, and one consumed, so we only see the other original
        self.assertEqual(len(list(plan)), 1 + 2)

        # remove local after traversing available
        plan = policy.make_query_plan()
        for _ in range(3):
            next(plan)
        policy.on_down(new_host)
        # we should be past the local list
        self.assertEqual(len(list(plan)), 0 + 2)

        # REMOTES CHANGE
        new_host.set_location_info("dc2", "rack1")

        # new remote after traversing local, but not starting remote
        plan = policy.make_query_plan()
        for _ in range(2):
            next(plan)
        policy.on_up(new_host)
        # list is updated before we get to it
        self.assertEqual(len(list(plan)), 0 + 3)

        # remove remote after traversing local, but not starting remote
        plan = policy.make_query_plan()
        for _ in range(2):
            next(plan)
        policy.on_down(new_host)
        # list is updated before we get to it
        self.assertEqual(len(list(plan)), 0 + 2)

        # new remote after traversing local, and starting remote
        plan = policy.make_query_plan()
        for _ in range(3):
            next(plan)
        policy.on_up(new_host)
        # slice is already made, and we've consumed one
        self.assertEqual(len(list(plan)), 0 + 1)

        # remove remote after traversing local, and starting remote
        plan = policy.make_query_plan()
        for _ in range(3):
            next(plan)
        policy.on_down(new_host)
        # slice is created with all present, and we've consumed one
        self.assertEqual(len(list(plan)), 0 + 2)

        # local DC disappears after finishing it, but not starting remote
        plan = policy.make_query_plan()
        for _ in range(2):
            next(plan)
        policy.on_down(hosts[0])
        policy.on_down(hosts[1])
        # dict traversal starts as normal
        self.assertEqual(len(list(plan)), 0 + 2)
        policy.on_up(hosts[0])
        policy.on_up(hosts[1])

        # PYTHON-297 addresses the following cases, where DCs come and go
        # during generation
        # local DC disappears after finishing it, and starting remote
        plan = policy.make_query_plan()
        for _ in range(3):
            next(plan)
        policy.on_down(hosts[0])
        policy.on_down(hosts[1])
        # dict traversal has begun and consumed one
        self.assertEqual(len(list(plan)), 0 + 1)
        policy.on_up(hosts[0])
        policy.on_up(hosts[1])

        # remote DC disappears after finishing local, but not starting remote
        plan = policy.make_query_plan()
        for _ in range(2):
            next(plan)
        policy.on_down(hosts[2])
        policy.on_down(hosts[3])
        # nothing left
        self.assertEqual(len(list(plan)), 0 + 0)
        policy.on_up(hosts[2])
        policy.on_up(hosts[3])

        # remote DC disappears while traversing it
        plan = policy.make_query_plan()
        for _ in range(3):
            next(plan)
        policy.on_down(hosts[2])
        policy.on_down(hosts[3])
        # we continue with remainder of original list
        self.assertEqual(len(list(plan)), 0 + 1)
        policy.on_up(hosts[2])
        policy.on_up(hosts[3])

        another_host = Host(5, SimpleConvictionPolicy)
        another_host.set_location_info("dc3", "rack1")
        new_host.set_location_info("dc3", "rack1")

        # new DC while traversing remote
        plan = policy.make_query_plan()
        for _ in range(3):
            next(plan)
        policy.on_up(new_host)
        policy.on_up(another_host)
        # we continue with remainder of original list
        self.assertEqual(len(list(plan)), 0 + 1)

        # remote DC disappears after finishing it
        plan = policy.make_query_plan()
        for _ in range(3):
            next(plan)
        last_host_in_this_dc = next(plan)
        if last_host_in_this_dc in (new_host, another_host):
            down_hosts = [new_host, another_host]
        else:
            down_hosts = hosts[2:]
        for h in down_hosts:
            policy.on_down(h)
        # the last DC has two
        self.assertEqual(len(list(plan)), 0 + 2)

    def test_no_live_nodes(self):
        """
        Ensure query plan for a downed cluster will execute without errors
        """

        hosts = []
        for i in range(4):
            h = Host(i, SimpleConvictionPolicy)
            h.set_location_info("dc1", "rack1")
            hosts.append(h)

        policy = DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=1)
        policy.populate(Mock(), hosts)

        for host in hosts:
            policy.on_down(host)

        qplan = list(policy.make_query_plan())
        self.assertEqual(qplan, [])

    def test_no_nodes(self):
        """
        Ensure query plan for an empty cluster will execute without errors
        """

        policy = DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=1)
        policy.populate(None, [])

        qplan = list(policy.make_query_plan())
        self.assertEqual(qplan, [])

    def test_default_dc(self):
        host_local = Host(1, SimpleConvictionPolicy, 'local')
        host_remote = Host(2, SimpleConvictionPolicy, 'remote')
        host_none = Host(1, SimpleConvictionPolicy)

        # contact point is '1'
        cluster = Mock(contact_points_resolved=[1])

        # contact DC first
        policy = DCAwareRoundRobinPolicy()
        policy.populate(cluster, [host_none])
        self.assertFalse(policy.local_dc)
        policy.on_add(host_local)
        policy.on_add(host_remote)
        self.assertNotEqual(policy.local_dc, host_remote.datacenter)
        self.assertEqual(policy.local_dc, host_local.datacenter)

        # contact DC second
        policy = DCAwareRoundRobinPolicy()
        policy.populate(cluster, [host_none])
        self.assertFalse(policy.local_dc)
        policy.on_add(host_remote)
        policy.on_add(host_local)
        self.assertNotEqual(policy.local_dc, host_remote.datacenter)
        self.assertEqual(policy.local_dc, host_local.datacenter)

        # no DC
        policy = DCAwareRoundRobinPolicy()
        policy.populate(cluster, [host_none])
        self.assertFalse(policy.local_dc)
        policy.on_add(host_none)
        self.assertFalse(policy.local_dc)

        # only other DC
        policy = DCAwareRoundRobinPolicy()
        policy.populate(cluster, [host_none])
        self.assertFalse(policy.local_dc)
        policy.on_add(host_remote)
        self.assertFalse(policy.local_dc)


class TokenAwarePolicyTest(unittest.TestCase):

    def test_wrap_round_robin(self):
        cluster = Mock(spec=Cluster)
        cluster.metadata = Mock(spec=Metadata)
        hosts = [Host(str(i), SimpleConvictionPolicy) for i in range(4)]
        for host in hosts:
            host.set_up()

        def get_replicas(keyspace, packed_key):
            index = struct.unpack('>i', packed_key)[0]
            return list(islice(cycle(hosts), index, index + 2))

        cluster.metadata.get_replicas.side_effect = get_replicas

        policy = TokenAwarePolicy(RoundRobinPolicy())
        policy.populate(cluster, hosts)

        for i in range(4):
            query = Statement(routing_key=struct.pack('>i', i), keyspace='keyspace_name')
            qplan = list(policy.make_query_plan(None, query))

            replicas = get_replicas(None, struct.pack('>i', i))
            other = set(h for h in hosts if h not in replicas)
            self.assertEqual(replicas, qplan[:2])
            self.assertEqual(other, set(qplan[2:]))

        # Should use the secondary policy
        for i in range(4):
            qplan = list(policy.make_query_plan())

            self.assertEqual(set(qplan), set(hosts))

    def test_wrap_dc_aware(self):
        cluster = Mock(spec=Cluster)
        cluster.metadata = Mock(spec=Metadata)
        hosts = [Host(str(i), SimpleConvictionPolicy) for i in range(4)]
        for host in hosts:
            host.set_up()
        for h in hosts[:2]:
            h.set_location_info("dc1", "rack1")
        for h in hosts[2:]:
            h.set_location_info("dc2", "rack1")

        def get_replicas(keyspace, packed_key):
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
            query = Statement(routing_key=struct.pack('>i', i), keyspace='keyspace_name')
            qplan = list(policy.make_query_plan(None, query))
            replicas = get_replicas(None, struct.pack('>i', i))

            # first should be the only local replica
            self.assertIn(qplan[0], replicas)
            self.assertEqual(qplan[0].datacenter, "dc1")

            # then the local non-replica
            self.assertNotIn(qplan[1], replicas)
            self.assertEqual(qplan[1].datacenter, "dc1")

            # then one of the remotes (used_hosts_per_remote_dc is 1, so we
            # shouldn't see two remotes)
            self.assertEqual(qplan[2].datacenter, "dc2")
            self.assertEqual(3, len(qplan))

    class FakeCluster:
        def __init__(self):
            self.metadata = Mock(spec=Metadata)

    def test_get_distance(self):
        """
        Same test as DCAwareRoundRobinPolicyTest.test_get_distance()
        Except a FakeCluster is needed for the metadata variable and
        policy.child_policy is needed to change child policy settings
        """

        policy = TokenAwarePolicy(DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=0))
        host = Host("ip1", SimpleConvictionPolicy)
        host.set_location_info("dc1", "rack1")

        policy.populate(self.FakeCluster(), [host])

        self.assertEqual(policy.distance(host), HostDistance.LOCAL)

        # used_hosts_per_remote_dc is set to 0, so ignore it
        remote_host = Host("ip2", SimpleConvictionPolicy)
        remote_host.set_location_info("dc2", "rack1")
        self.assertEqual(policy.distance(remote_host), HostDistance.IGNORED)

        # dc2 isn't registered in the policy's live_hosts dict
        policy._child_policy.used_hosts_per_remote_dc = 1
        self.assertEqual(policy.distance(remote_host), HostDistance.IGNORED)

        # make sure the policy has both dcs registered
        policy.populate(self.FakeCluster(), [host, remote_host])
        self.assertEqual(policy.distance(remote_host), HostDistance.REMOTE)

        # since used_hosts_per_remote_dc is set to 1, only the first
        # remote host in dc2 will be REMOTE, the rest are IGNORED
        second_remote_host = Host("ip3", SimpleConvictionPolicy)
        second_remote_host.set_location_info("dc2", "rack1")
        policy.populate(self.FakeCluster(), [host, remote_host, second_remote_host])
        distances = set([policy.distance(remote_host), policy.distance(second_remote_host)])
        self.assertEqual(distances, set([HostDistance.REMOTE, HostDistance.IGNORED]))

    def test_status_updates(self):
        """
        Same test as DCAwareRoundRobinPolicyTest.test_status_updates()
        """

        hosts = [Host(i, SimpleConvictionPolicy) for i in range(4)]
        for h in hosts[:2]:
            h.set_location_info("dc1", "rack1")
        for h in hosts[2:]:
            h.set_location_info("dc2", "rack1")

        policy = TokenAwarePolicy(DCAwareRoundRobinPolicy("dc1", used_hosts_per_remote_dc=1))
        policy.populate(self.FakeCluster(), hosts)
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

        policy.on_down(new_local_host)
        policy.on_down(hosts[1])
        qplan = list(policy.make_query_plan())
        self.assertEqual(set(qplan), set([hosts[3], new_remote_host]))

        policy.on_down(new_remote_host)
        policy.on_down(hosts[3])
        qplan = list(policy.make_query_plan())
        self.assertEqual(qplan, [])

    def test_statement_keyspace(self):
        hosts = [Host(str(i), SimpleConvictionPolicy) for i in range(4)]
        for host in hosts:
            host.set_up()

        cluster = Mock(spec=Cluster)
        cluster.metadata = Mock(spec=Metadata)
        replicas = hosts[2:]
        cluster.metadata.get_replicas.return_value = replicas

        child_policy = Mock()
        child_policy.make_query_plan.return_value = hosts
        child_policy.distance.return_value = HostDistance.LOCAL

        policy = TokenAwarePolicy(child_policy)
        policy.populate(cluster, hosts)

        # no keyspace, child policy is called
        keyspace = None
        routing_key = 'routing_key'
        query = Statement(routing_key=routing_key)
        qplan = list(policy.make_query_plan(keyspace, query))
        self.assertEqual(hosts, qplan)
        self.assertEqual(cluster.metadata.get_replicas.call_count, 0)
        child_policy.make_query_plan.assert_called_once_with(keyspace, query)

        # working keyspace, no statement
        cluster.metadata.get_replicas.reset_mock()
        keyspace = 'working_keyspace'
        routing_key = 'routing_key'
        query = Statement(routing_key=routing_key)
        qplan = list(policy.make_query_plan(keyspace, query))
        self.assertEqual(replicas + hosts[:2], qplan)
        cluster.metadata.get_replicas.assert_called_with(keyspace, routing_key)

        # statement keyspace, no working
        cluster.metadata.get_replicas.reset_mock()
        working_keyspace = None
        statement_keyspace = 'statement_keyspace'
        routing_key = 'routing_key'
        query = Statement(routing_key=routing_key, keyspace=statement_keyspace)
        qplan = list(policy.make_query_plan(working_keyspace, query))
        self.assertEqual(replicas + hosts[:2], qplan)
        cluster.metadata.get_replicas.assert_called_with(statement_keyspace, routing_key)

        # both keyspaces set, statement keyspace used for routing
        cluster.metadata.get_replicas.reset_mock()
        working_keyspace = 'working_keyspace'
        statement_keyspace = 'statement_keyspace'
        routing_key = 'routing_key'
        query = Statement(routing_key=routing_key, keyspace=statement_keyspace)
        qplan = list(policy.make_query_plan(working_keyspace, query))
        self.assertEqual(replicas + hosts[:2], qplan)
        cluster.metadata.get_replicas.assert_called_with(statement_keyspace, routing_key)

    def test_shuffles_if_given_keyspace_and_routing_key(self):
        """
        Test to validate the hosts are shuffled when `shuffle_replicas` is truthy
        @since 3.8
        @jira_ticket PYTHON-676
        @expected_result shuffle should be called, because the keyspace and the
        routing key are set

        @test_category policy
        """
        self._assert_shuffle(keyspace='keyspace', routing_key='routing_key')

    def test_no_shuffle_if_given_no_keyspace(self):
        """
        Test to validate the hosts are not shuffled when no keyspace is provided
        @since 3.8
        @jira_ticket PYTHON-676
        @expected_result shuffle should be called, because keyspace is None

        @test_category policy
        """
        self._assert_shuffle(keyspace=None, routing_key='routing_key')

    def test_no_shuffle_if_given_no_routing_key(self):
        """
        Test to validate the hosts are not shuffled when no routing_key is provided
        @since 3.8
        @jira_ticket PYTHON-676
        @expected_result shuffle should be called, because routing_key is None

        @test_category policy
        """
        self._assert_shuffle(keyspace='keyspace', routing_key=None)

    @patch('cassandra.policies.shuffle')
    def _assert_shuffle(self, patched_shuffle, keyspace, routing_key):
        hosts = [Host(str(i), SimpleConvictionPolicy) for i in range(4)]
        for host in hosts:
            host.set_up()

        cluster = Mock(spec=Cluster)
        cluster.metadata = Mock(spec=Metadata)
        replicas = hosts[2:]
        cluster.metadata.get_replicas.return_value = replicas

        child_policy = Mock()
        child_policy.make_query_plan.return_value = hosts
        child_policy.distance.return_value = HostDistance.LOCAL

        policy = TokenAwarePolicy(child_policy, shuffle_replicas=True)
        policy.populate(cluster, hosts)

        cluster.metadata.get_replicas.reset_mock()
        child_policy.make_query_plan.reset_mock()
        query = Statement(routing_key=routing_key)
        qplan = list(policy.make_query_plan(keyspace, query))
        if keyspace is None or routing_key is None:
            self.assertEqual(hosts, qplan)
            self.assertEqual(cluster.metadata.get_replicas.call_count, 0)
            child_policy.make_query_plan.assert_called_once_with(keyspace, query)
            self.assertEqual(patched_shuffle.call_count, 0)
        else:
            self.assertEqual(set(replicas), set(qplan[:2]))
            self.assertEqual(hosts[:2], qplan[2:])
            child_policy.make_query_plan.assert_called_once_with(keyspace, query)
            self.assertEqual(patched_shuffle.call_count, 1)


class ConvictionPolicyTest(unittest.TestCase):
    def test_not_implemented(self):
        """
        Code coverage for interface-style base class
        """

        conviction_policy = ConvictionPolicy(1)
        self.assertRaises(NotImplementedError, conviction_policy.add_failure, 1)
        self.assertRaises(NotImplementedError, conviction_policy.reset)


class SimpleConvictionPolicyTest(unittest.TestCase):
    def test_basic_responses(self):
        """
        Code coverage for SimpleConvictionPolicy
        """

        conviction_policy = SimpleConvictionPolicy(1)
        self.assertEqual(conviction_policy.add_failure(1), True)
        self.assertEqual(conviction_policy.reset(), None)


class ReconnectionPolicyTest(unittest.TestCase):
    def test_basic_responses(self):
        """
        Code coverage for interface-style base class
        """

        policy = ReconnectionPolicy()
        self.assertRaises(NotImplementedError, policy.new_schedule)


class ConstantReconnectionPolicyTest(unittest.TestCase):

    def test_bad_vals(self):
        """
        Test initialization values
        """

        self.assertRaises(ValueError, ConstantReconnectionPolicy, -1, 0)

    def test_schedule(self):
        """
        Test ConstantReconnectionPolicy schedule
        """

        delay = 2
        max_attempts = 100
        policy = ConstantReconnectionPolicy(delay=delay, max_attempts=max_attempts)
        schedule = list(policy.new_schedule())
        self.assertEqual(len(schedule), max_attempts)
        for i, delay in enumerate(schedule):
            self.assertEqual(delay, delay)

    def test_schedule_negative_max_attempts(self):
        """
        Test how negative max_attempts are handled
        """

        delay = 2
        max_attempts = -100

        try:
            ConstantReconnectionPolicy(delay=delay, max_attempts=max_attempts)
            self.fail('max_attempts should throw ValueError when negative')
        except ValueError:
            pass

    def test_schedule_infinite_attempts(self):
        delay = 2
        max_attempts = None
        crp = ConstantReconnectionPolicy(delay=delay, max_attempts=max_attempts)
        # this is infinite. we'll just verify one more than default
        for _, d in zip(range(65), crp.new_schedule()):
            self.assertEqual(d, delay)


class ExponentialReconnectionPolicyTest(unittest.TestCase):

    def test_bad_vals(self):
        self.assertRaises(ValueError, ExponentialReconnectionPolicy, -1, 0)
        self.assertRaises(ValueError, ExponentialReconnectionPolicy, 0, -1)
        self.assertRaises(ValueError, ExponentialReconnectionPolicy, 9000, 1)
        self.assertRaises(ValueError, ExponentialReconnectionPolicy, 1, 2, -1)

    def test_schedule_no_max(self):
        base_delay = 2.0
        max_delay = 100.0
        test_iter = 10000
        policy = ExponentialReconnectionPolicy(base_delay=base_delay, max_delay=max_delay, max_attempts=None)
        sched_slice = list(islice(policy.new_schedule(), 0, test_iter))
        self.assertEqual(sched_slice[0], base_delay)
        self.assertEqual(sched_slice[-1], max_delay)
        self.assertEqual(len(sched_slice), test_iter)

    def test_schedule_with_max(self):
        base_delay = 2.0
        max_delay = 100.0
        max_attempts = 64
        policy = ExponentialReconnectionPolicy(base_delay=base_delay, max_delay=max_delay, max_attempts=max_attempts)
        schedule = list(policy.new_schedule())
        self.assertEqual(len(schedule), max_attempts)
        for i, delay in enumerate(schedule):
            if i == 0:
                self.assertEqual(delay, base_delay)
            elif i < 6:
                self.assertEqual(delay, schedule[i - 1] * 2)
            else:
                self.assertEqual(delay, max_delay)

    def test_schedule_exactly_one_attempt(self):
        base_delay = 2.0
        max_delay = 100.0
        max_attempts = 1
        policy = ExponentialReconnectionPolicy(
            base_delay=base_delay, max_delay=max_delay, max_attempts=max_attempts
        )
        self.assertEqual(len(list(policy.new_schedule())), 1)

    def test_schedule_overflow(self):
        """
        Test to verify an OverflowError is handled correctly
        in the ExponentialReconnectionPolicy
        @since 3.10
        @jira_ticket PYTHON-707
        @expected_result all numbers should be less than sys.float_info.max
        since that's the biggest max we can possibly have as that argument must be a float.
        Note that is possible for a float to be inf.

        @test_category policy
        """

        # This should lead to overflow
        # Note that this may not happen in the fist iterations
        # as sys.float_info.max * 2 = inf
        base_delay = sys.float_info.max - 1
        max_delay = sys.float_info.max
        max_attempts = 2**12
        policy = ExponentialReconnectionPolicy(base_delay=base_delay, max_delay=max_delay, max_attempts=max_attempts)
        schedule = list(policy.new_schedule())
        for number in schedule:
            self.assertLessEqual(number, sys.float_info.max)


ONE = ConsistencyLevel.ONE


class RetryPolicyTest(unittest.TestCase):

    def test_read_timeout(self):
        policy = RetryPolicy()

        # if this is the second or greater attempt, rethrow
        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=1, received_responses=2,
            data_retrieved=True, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        # if we didn't get enough responses, rethrow
        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=2, received_responses=1,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        # if we got enough responses, but also got a data response, rethrow
        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=2, received_responses=2,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        # we got enough responses but no data response, so retry
        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=2, received_responses=2,
            data_retrieved=False, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, ONE)

    def test_write_timeout(self):
        policy = RetryPolicy()

        # if this is the second or greater attempt, rethrow
        retry, consistency = policy.on_write_timeout(
            query=None, consistency=ONE, write_type=WriteType.SIMPLE,
            required_responses=1, received_responses=2, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        # if it's not a BATCH_LOG write, don't retry it
        retry, consistency = policy.on_write_timeout(
            query=None, consistency=ONE, write_type=WriteType.SIMPLE,
            required_responses=1, received_responses=2, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        # retry BATCH_LOG writes regardless of received responses
        retry, consistency = policy.on_write_timeout(
            query=None, consistency=ONE, write_type=WriteType.BATCH_LOG,
            required_responses=10000, received_responses=1, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, ONE)

    def test_unavailable(self):
        """
        Use the same tests for test_write_timeout, but ensure they only RETHROW
        """
        policy = RetryPolicy()

        retry, consistency = policy.on_unavailable(
            query=None, consistency=ONE,
            required_replicas=1, alive_replicas=2, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        retry, consistency = policy.on_unavailable(
            query=None, consistency=ONE,
            required_replicas=1, alive_replicas=2, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY_NEXT_HOST)
        self.assertEqual(consistency, ONE)

        retry, consistency = policy.on_unavailable(
            query=None, consistency=ONE,
            required_replicas=10000, alive_replicas=1, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY_NEXT_HOST)
        self.assertEqual(consistency, ONE)


class FallthroughRetryPolicyTest(unittest.TestCase):

    """
    Use the same tests for test_write_timeout, but ensure they only RETHROW
    """

    def test_read_timeout(self):
        policy = FallthroughRetryPolicy()

        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=1, received_responses=2,
            data_retrieved=True, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=2, received_responses=1,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=2, received_responses=2,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=2, received_responses=2,
            data_retrieved=False, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

    def test_write_timeout(self):
        policy = FallthroughRetryPolicy()

        retry, consistency = policy.on_write_timeout(
            query=None, consistency=ONE, write_type=WriteType.SIMPLE,
            required_responses=1, received_responses=2, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        retry, consistency = policy.on_write_timeout(
            query=None, consistency=ONE, write_type=WriteType.SIMPLE,
            required_responses=1, received_responses=2, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        retry, consistency = policy.on_write_timeout(
            query=None, consistency=ONE, write_type=WriteType.BATCH_LOG,
            required_responses=10000, received_responses=1, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

    def test_unavailable(self):
        policy = FallthroughRetryPolicy()

        retry, consistency = policy.on_unavailable(
            query=None, consistency=ONE,
            required_replicas=1, alive_replicas=2, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        retry, consistency = policy.on_unavailable(
            query=None, consistency=ONE,
            required_replicas=1, alive_replicas=2, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        retry, consistency = policy.on_unavailable(
            query=None, consistency=ONE,
            required_replicas=10000, alive_replicas=1, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)


class DowngradingConsistencyRetryPolicyTest(unittest.TestCase):

    def test_read_timeout(self):
        policy = DowngradingConsistencyRetryPolicy()

        # if this is the second or greater attempt, rethrow
        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=1, received_responses=2,
            data_retrieved=True, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        # if we didn't get enough responses, retry at a lower consistency
        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=4, received_responses=3,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, ConsistencyLevel.THREE)

        # if we didn't get enough responses, retry at a lower consistency
        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=3, received_responses=2,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, ConsistencyLevel.TWO)

        # retry consistency level goes down based on the # of recv'd responses
        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=3, received_responses=1,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, ConsistencyLevel.ONE)

        # if we got no responses, rethrow
        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=3, received_responses=0,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        # if we got enough response but no data, retry
        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=3, received_responses=3,
            data_retrieved=False, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, ONE)

        # if we got enough responses, but also got a data response, rethrow
        retry, consistency = policy.on_read_timeout(
            query=None, consistency=ONE, required_responses=2, received_responses=2,
            data_retrieved=True, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

    def test_write_timeout(self):
        policy = DowngradingConsistencyRetryPolicy()

        # if this is the second or greater attempt, rethrow
        retry, consistency = policy.on_write_timeout(
            query=None, consistency=ONE, write_type=WriteType.SIMPLE,
            required_responses=1, received_responses=2, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        for write_type in (WriteType.SIMPLE, WriteType.BATCH, WriteType.COUNTER):
            # ignore failures if at least one response (replica persisted)
            retry, consistency = policy.on_write_timeout(
                query=None, consistency=ONE, write_type=write_type,
                required_responses=1, received_responses=2, retry_num=0)
            self.assertEqual(retry, RetryPolicy.IGNORE)
            # retrhow if we can't be sure we have a replica
            retry, consistency = policy.on_write_timeout(
                query=None, consistency=ONE, write_type=write_type,
                required_responses=1, received_responses=0, retry_num=0)
            self.assertEqual(retry, RetryPolicy.RETHROW)

        # downgrade consistency level on unlogged batch writes
        retry, consistency = policy.on_write_timeout(
            query=None, consistency=ONE, write_type=WriteType.UNLOGGED_BATCH,
            required_responses=3, received_responses=1, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, ConsistencyLevel.ONE)

        # retry batch log writes at the same consistency level
        retry, consistency = policy.on_write_timeout(
            query=None, consistency=ONE, write_type=WriteType.BATCH_LOG,
            required_responses=3, received_responses=1, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, ONE)

        # timeout on an unknown write_type
        retry, consistency = policy.on_write_timeout(
            query=None, consistency=ONE, write_type=None,
            required_responses=1, received_responses=2, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

    def test_unavailable(self):
        policy = DowngradingConsistencyRetryPolicy()

        # if this is the second or greater attempt, rethrow
        retry, consistency = policy.on_unavailable(
            query=None, consistency=ONE, required_replicas=3, alive_replicas=1, retry_num=1)
        self.assertEqual(retry, RetryPolicy.RETHROW)
        self.assertEqual(consistency, None)

        # downgrade consistency on unavailable exceptions
        retry, consistency = policy.on_unavailable(
            query=None, consistency=ONE, required_replicas=3, alive_replicas=1, retry_num=0)
        self.assertEqual(retry, RetryPolicy.RETRY)
        self.assertEqual(consistency, ConsistencyLevel.ONE)


class WhiteListRoundRobinPolicyTest(unittest.TestCase):

    def test_hosts_with_hostname(self):
        hosts = ['localhost']
        policy = WhiteListRoundRobinPolicy(hosts)
        host = Host("127.0.0.1", SimpleConvictionPolicy)
        policy.populate(None, [host])

        qplan = list(policy.make_query_plan())
        self.assertEqual(sorted(qplan), [host])

        self.assertEqual(policy.distance(host), HostDistance.LOCAL)

class AddressTranslatorTest(unittest.TestCase):

    def test_identity_translator(self):
        IdentityTranslator()

    @patch('socket.getfqdn', return_value='localhost')
    def test_ec2_multi_region_translator(self, *_):
        ec2t = EC2MultiRegionTranslator()
        addr = '127.0.0.1'
        translated = ec2t.translate(addr)
        self.assertIsNot(translated, addr)  # verifies that the resolver path is followed
        self.assertEqual(translated, addr)  # and that it resolves to the same address
