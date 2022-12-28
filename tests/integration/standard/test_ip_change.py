import os
import logging
import unittest

from cassandra.cluster import ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy, ConstantReconnectionPolicy

from tests.integration import use_cluster, get_node, get_cluster, local, TestCluster
from tests.util import wait_until_not_raised

LOGGER = logging.getLogger(__name__)


def setup_module():
    os.environ['SCYLLA_EXT_OPTS'] = "--smp 2 --memory 2048M"
    use_cluster('test_ip_change', [3], start=True)


@local
class TestIpAddressChange(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.cluster = TestCluster(reconnection_policy=ConstantReconnectionPolicy(1))
        cls.session = cls.cluster.connect()

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    def test_change_address_during_live_session(self):
        node3 = get_node(3)

        LOGGER.debug("Stop node3")
        node3.stop()

        LOGGER.debug("Change IP address for node3")
        ip_prefix = get_cluster().get_ipprefix()
        new_ip = f'{ip_prefix}33'
        node3.set_configuration_options(values={'listen_address': new_ip, 'rpc_address': new_ip, 'api_address': new_ip})
        node3.network_interfaces = {k: (new_ip, v[1]) for k, v in node3.network_interfaces.items()}
        LOGGER.debug(f"Start node3 again with ip address {new_ip}")
        node3.start(wait_for_binary_proto=True)

        def new_address_found():
            addresses = [host.endpoint.address for host in self.cluster.metadata.all_hosts()]
            LOGGER.debug(addresses)
            assert new_ip in addresses

        wait_until_not_raised(new_address_found, 0.5, 100)

        new_node_only = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([new_ip]))
        self.cluster.add_execution_profile("new_node", new_node_only)
        local_info = self.session.execute("SELECT * FROM system.local", execution_profile="new_node").one()
        LOGGER.debug(local_info._asdict())
        assert local_info.broadcast_address == new_ip
