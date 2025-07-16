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
import unittest

from tests.integration import CASSANDRA_VERSION, SIMULACRON_JAR, PROTOCOL_VERSION
from tests.integration.simulacron.utils import (
    clear_queries,
    start_and_prime_singledc,
    stop_simulacron,
    start_and_prime_cluster_defaults,
)

from cassandra.cluster import Cluster

from packaging.version import Version


PROTOCOL_VERSION = min(4, PROTOCOL_VERSION)


def teardown_package():
    stop_simulacron()


class SimulacronBase(unittest.TestCase):
    def tearDown(self):
        clear_queries()
        stop_simulacron()


class SimulacronCluster(SimulacronBase):

    cluster, connect = None, True

    @classmethod
    def setUpClass(cls):
        if SIMULACRON_JAR is None or CASSANDRA_VERSION < Version("2.1"):
            return

        start_and_prime_singledc()
        if cls.connect:
            cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False)
            cls.session = cls.cluster.connect(wait_for_all_pools=True)

    @classmethod
    def tearDownClass(cls):
        if SIMULACRON_JAR is None or CASSANDRA_VERSION < Version("2.1"):
            return

        if cls.cluster:
            cls.cluster.shutdown()
        stop_simulacron()
