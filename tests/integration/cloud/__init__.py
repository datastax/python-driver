# Copyright DataStax, Inc.
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
# limitations under the License
from cassandra.cluster import Cluster

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import os
import subprocess

from tests.integration import CLOUD_PROXY_PATH, USE_CASS_EXTERNAL


def setup_package():
    if CLOUD_PROXY_PATH and not USE_CASS_EXTERNAL:
        start_cloud_proxy()


def teardown_package():
    if not USE_CASS_EXTERNAL:
        stop_cloud_proxy()


class CloudProxyCluster(unittest.TestCase):

    creds_dir = os.path.join(os.path.abspath(CLOUD_PROXY_PATH or ''), 'certs/bundles/')
    creds = os.path.join(creds_dir, 'creds-v1.zip')
    creds_no_auth = os.path.join(creds_dir, 'creds-v1-wo-creds.zip')
    creds_unreachable = os.path.join(creds_dir, 'creds-v1-unreachable.zip')
    creds_invalid_ca = os.path.join(creds_dir, 'creds-v1-invalid-ca.zip')

    cluster, connect = None, False
    session = None

    @classmethod
    def connect(cls, creds, **kwargs):
        cloud_config = {
            'secure_connect_bundle': creds,
        }
        cls.cluster = Cluster(cloud=cloud_config, protocol_version=4, **kwargs)
        cls.session = cls.cluster.connect(wait_for_all_pools=True)

    def tearDown(self):
        if self.cluster:
            self.cluster.shutdown()


class CloudProxyServer(object):
    """
    Class for starting and stopping the proxy (sni_single_endpoint)
    """

    ccm_command = 'docker exec $(docker ps -a -q --filter ancestor=single_endpoint) ccm {}'

    def __init__(self, CLOUD_PROXY_PATH):
        self.CLOUD_PROXY_PATH = CLOUD_PROXY_PATH
        self.running = False

    def start(self):
        return_code = subprocess.call(
            ['REQUIRE_CLIENT_CERTIFICATE=true ./run.sh'],
            cwd=self.CLOUD_PROXY_PATH,
            shell=True)
        if return_code != 0:
            raise Exception("Error while starting proxy server")
        self.running = True

    def stop(self):
        if self.is_running():
            subprocess.call(
                ["docker kill $(docker ps -a -q --filter ancestor=single_endpoint)"],
                shell=True)
            self.running = False

    def is_running(self):
        return self.running

    def start_node(self, id):
        subcommand = 'node{} start --jvm_arg "-Ddse.product_type=DATASTAX_APOLLO" --root --wait-for-binary-proto'.format(id)
        subprocess.call(
            [self.ccm_command.format(subcommand)],
            shell=True)

    def stop_node(self, id):
        subcommand = 'node{} stop'.format(id)
        subprocess.call(
            [self.ccm_command.format(subcommand)],
            shell=True)


CLOUD_PROXY_SERVER = CloudProxyServer(CLOUD_PROXY_PATH)


def start_cloud_proxy():
    """
    Starts and waits for the proxy to run
    """
    CLOUD_PROXY_SERVER.stop()
    CLOUD_PROXY_SERVER.start()


def stop_cloud_proxy():
    CLOUD_PROXY_SERVER.stop()
