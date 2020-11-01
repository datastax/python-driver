from unittest import TestCase

from cassandra.policies import HostStateListener
from tests.integration import get_node, use_cluster, local, TestCluster
from tests.integration.long.utils import decommission
from tests.util import wait_until


class StateListener(HostStateListener):
    def __init__(self):
        self.downed_host = None
        self.removed_host = None

    def on_remove(self, host):
        self.removed_host = host

    def on_up(self, host):
        pass

    def on_down(self, host):
        self.downed_host = host

    def on_add(self, host):
        pass


class TopologyChangeTests(TestCase):
    @local
    def test_removed_node_stops_reconnecting(self):
        """ Ensure we stop reconnecting after a node is removed. PYTHON-1181 """
        use_cluster("test_down_then_removed", [3], start=True)

        state_listener = StateListener()
        cluster = TestCluster()
        self.addCleanup(cluster.shutdown)
        cluster.register_listener(state_listener)
        session = cluster.connect(wait_for_all_pools=True)

        get_node(3).nodetool("disablebinary")

        wait_until(condition=lambda: state_listener.downed_host is not None, delay=2, max_attempts=50)
        self.assertTrue(state_listener.downed_host.is_currently_reconnecting())

        decommission(3)

        wait_until(condition=lambda: state_listener.removed_host is not None, delay=2, max_attempts=50)
        self.assertIs(state_listener.downed_host, state_listener.removed_host)  # Just a sanity check
        self.assertFalse(state_listener.removed_host.is_currently_reconnecting())
