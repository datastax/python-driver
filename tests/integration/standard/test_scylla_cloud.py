import os.path
from unittest import TestCase
from ccmlib.utils.ssl_utils import generate_ssl_stores
from ccmlib.utils.sni_proxy import refresh_certs, get_cluster_info, start_sni_proxy, create_cloud_config

from tests.integration import use_cluster
from cassandra.cluster import Cluster, TwistedConnection
from cassandra.connection import SniEndPointFactory
from cassandra.io.asyncorereactor import AsyncoreConnection
from cassandra.io.libevreactor import LibevConnection
from cassandra.io.geventreactor import GeventConnection
from cassandra.io.eventletreactor import EventletConnection
from cassandra.io.asyncioreactor import AsyncioConnection

supported_connection_classes = [AsyncoreConnection, LibevConnection, TwistedConnection]
# need to run them with specific configuration like `gevent.monkey.patch_all()` or under async functions
unsupported_connection_classes = [GeventConnection, AsyncioConnection, EventletConnection]


class ScyllaCloudConfigTests(TestCase):
    def start_cluster_with_proxy(self):
        ccm_cluster = self.ccm_cluster
        generate_ssl_stores(ccm_cluster.get_path())
        ssl_port = 9142
        sni_port = 443
        ccm_cluster.set_configuration_options(dict(
            client_encryption_options=
            dict(require_client_auth=True,
                 truststore=os.path.join(ccm_cluster.get_path(), 'ccm_node.cer'),
                 certificate=os.path.join(ccm_cluster.get_path(), 'ccm_node.pem'),
                 keyfile=os.path.join(ccm_cluster.get_path(), 'ccm_node.key'),
                 enabled=True),
            native_transport_port_ssl=ssl_port))

        ccm_cluster._update_config()

        ccm_cluster.start(wait_for_binary_proto=True)

        nodes_info = get_cluster_info(ccm_cluster, port=ssl_port)
        refresh_certs(ccm_cluster, nodes_info)

        docker_id, listen_address, listen_port = \
            start_sni_proxy(ccm_cluster.get_path(), nodes_info=nodes_info, listen_port=sni_port)
        ccm_cluster.sni_proxy_docker_id = docker_id
        ccm_cluster.sni_proxy_listen_port = listen_port
        ccm_cluster._update_config()

        config_data_yaml, config_path_yaml = create_cloud_config(ccm_cluster.get_path(), listen_port)

        endpoint_factory = SniEndPointFactory(listen_address, port=int(listen_port),
                                              node_domain="cluster-id.scylla.com")

        return config_data_yaml, config_path_yaml, endpoint_factory

    def test_1_node_cluster(self):
        self.ccm_cluster = use_cluster("sni_proxy", [1], start=False)
        config_data_yaml, config_path_yaml, endpoint_factory = self.start_cluster_with_proxy()

        for config in [config_path_yaml, config_data_yaml]:
            for connection_class in supported_connection_classes:
                cluster = Cluster(scylla_cloud=config, connection_class=connection_class,
                                  endpoint_factory=endpoint_factory)
                with cluster.connect() as session:
                    res = session.execute("SELECT * FROM system.local")
                    assert res.all()

                    assert len(cluster.metadata._hosts) == 1
                    assert len(cluster.metadata._host_id_by_endpoint) == 1

    def test_3_node_cluster(self):
        self.ccm_cluster = use_cluster("sni_proxy", [3], start=False)
        config_data_yaml, config_path_yaml, endpoint_factory = self.start_cluster_with_proxy()

        for config in [config_path_yaml, config_data_yaml]:
            for connection_class in supported_connection_classes:
                cluster = Cluster(scylla_cloud=config, connection_class=connection_class,
                                  endpoint_factory=endpoint_factory)
                with cluster.connect() as session:
                    res = session.execute("SELECT * FROM system.local")
                    assert res.all()
                    assert len(cluster.metadata._hosts) == 3
                    assert len(cluster.metadata._host_id_by_endpoint) == 3
