# Copyright 2013-2014 DataStax, Inc.
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

import logging
import os
import socket

from cassandra.cluster import Cluster, NoHostAvailable
from ccmlib import common
from ccmlib.cluster import Cluster as CCMCluster
from tests.integration import setup_package, teardown_package, PROTOCOL_VERSION, CASSANDRA_DIR, CASSANDRA_VERSION, path
from cassandra.io.asyncorereactor import AsyncoreConnection

try:
    from cassandra.io.libevreactor import LibevConnection
except ImportError:
    LibevConnection = None


try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

log = logging.getLogger(__name__)

# If more modules do IPV6 testing, this can be moved down to integration.__init__.
# For now, just keeping the clutter here
IPV6_CLUSTER_NAME = 'ipv6_test_cluster'


def setup_module(cls):
    validate_ccm_viable()
    validate_host_viable()
    cls.ccm_cluster = object()
    teardown_package()
    try:
        try:
            ccm_cluster = CCMCluster.load(path, IPV6_CLUSTER_NAME)
            log.debug("Found existing ccm test ipv6 cluster, clearing")
            ccm_cluster.clear()
        except Exception:
            log.debug("Creating new ccm test ipv6 cluster")
            if CASSANDRA_DIR:
                ccm_cluster = CCMCluster(path, IPV6_CLUSTER_NAME, cassandra_dir=CASSANDRA_DIR)
            else:
                ccm_cluster = CCMCluster(path, IPV6_CLUSTER_NAME, cassandra_version=CASSANDRA_VERSION)
            ccm_cluster.set_configuration_options({'start_native_transport': True})
            common.switch_cluster(path, IPV6_CLUSTER_NAME)
            ccm_cluster.populate(1, ipformat='::%d')

        log.debug("Starting ccm test cluster")
        ccm_cluster.start(wait_for_binary_proto=True)
    except Exception:
        log.exception("Failed to start ccm cluster:")
        raise

    log.debug("Switched to ipv6 cluster")
    cls.ccm_cluster = ccm_cluster


def teardown_module(cls):
    cls.ccm_cluster.stop()
    setup_package()


def validate_ccm_viable():
    try:
        common.normalize_interface(('::1', 0))
    except:
        raise unittest.SkipTest('this version of ccm does not support ipv6')


def validate_host_viable():
    # this is something ccm does when starting, but preemptively check to avoid
    # spinning up the cluster if it's not going to work
    try:
        common.check_socket_available(('::1', 9042))
    except:
        raise unittest.SkipTest('failed binding ipv6 loopback ::1 on 9042')


class IPV6ConnectionTest(object):

    connection_class = None

    def test_connect(self):
        cluster = Cluster(connection_class=self.connection_class, contact_points=['::1'], protocol_version=PROTOCOL_VERSION)
        session = cluster.connect()
        future = session.execute_async("SELECT * FROM system.local")
        future.result()
        self.assertEqual(future._current_host.address, '::1')
        cluster.shutdown()

    def test_error(self):
        cluster = Cluster(connection_class=self.connection_class, contact_points=['::1'], port=9043, protocol_version=PROTOCOL_VERSION)
        self.assertRaisesRegexp(NoHostAvailable, '\(\'Unable to connect.*%s.*::1\', 9043.*Connection refused.*' % os.errno.ECONNREFUSED, cluster.connect)

    def test_error_multiple(self):
        if len(socket.getaddrinfo('localhost', 9043, socket.AF_UNSPEC, socket.SOCK_STREAM)) < 2:
            raise unittest.SkipTest('localhost only resolves one address')
        cluster = Cluster(connection_class=self.connection_class, contact_points=['localhost'], port=9043, protocol_version=PROTOCOL_VERSION)
        self.assertRaisesRegexp(NoHostAvailable, '\(\'Unable to connect.*Tried connecting to \[\(.*\(.*\].*Last error', cluster.connect)


class LibevConnectionTests(IPV6ConnectionTest, unittest.TestCase):

    connection_class = LibevConnection

    @classmethod
    def setup_class(cls):
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed properly')


class AsyncoreConnectionTests(IPV6ConnectionTest, unittest.TestCase):

    connection_class = AsyncoreConnection
