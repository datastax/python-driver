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
# limitations under the License.
from concurrent.futures import ThreadPoolExecutor
import logging
import time
import uuid
from cassandra.protocol_features import ProtocolFeatures

from cassandra.shard_info import _ShardingInfo

import unittest
from threading import Thread, Event, Lock
from unittest.mock import Mock, NonCallableMagicMock, MagicMock, patch

from cassandra.cluster import Session, ShardAwareOptions
from cassandra.connection import Connection
from cassandra.pool import HostConnection
from cassandra.pool import Host, NoConnectionsAvailable
from cassandra.policies import HostDistance, SimpleConvictionPolicy
import pytest

from tests.unit.util import HashableMock

LOGGER = logging.getLogger(__name__)


class _PoolTests(unittest.TestCase):
    __test__ = False
    PoolImpl = None
    uses_single_connection = None

    def make_session(self):
        session = NonCallableMagicMock(spec=Session, keyspace='foobarkeyspace', _trash=[])
        session._signal_connection_failure.return_value = False
        session.is_shard_aware_disabled.return_value = False
        return session

    def test_borrow_and_return(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False, max_request_id=100)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        c, request_id = pool.borrow_connection(timeout=0.01)
        assert c is conn
        assert 1 == conn.in_flight
        conn.set_keyspace_blocking.assert_called_once_with('foobarkeyspace')

        pool.return_connection(conn)
        assert 0 == conn.in_flight
        if not self.uses_single_connection:
            assert conn not in pool._trash

    def test_failed_wait_for_connection(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False, max_request_id=100)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        assert 1 == conn.in_flight

        conn.in_flight = conn.max_request_id

        # we're already at the max number of requests for this connection,
        # so we this should fail
        with pytest.raises(NoConnectionsAvailable):
            pool.borrow_connection(0)

    def test_successful_wait_for_connection(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False, max_request_id=100,
                                    lock=Lock())
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        assert 1 == conn.in_flight

        def get_second_conn():
            c, request_id = pool.borrow_connection(1.0)
            assert conn is c
            pool.return_connection(c)

        t = Thread(target=get_second_conn)
        t.start()

        pool.return_connection(conn)
        t.join()
        assert 0 == conn.in_flight

    def test_spawn_when_at_max(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False, max_request_id=100)
        conn.max_request_id = 100
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        assert 1 == conn.in_flight

        # make this conn full
        conn.in_flight = conn.max_request_id

        # we don't care about making this borrow_connection call succeed for the
        # purposes of this test, as long as it results in a new connection
        # creation being scheduled
        with pytest.raises(NoConnectionsAvailable):
            pool.borrow_connection(0)
        if not self.uses_single_connection:
            session.submit.assert_called_once_with(pool._create_new_connection)

    def test_return_defunct_connection(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
                                    max_request_id=100, signaled_error=False)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        conn.is_defunct = True
        session._signal_connection_failure.return_value = False
        pool.return_connection(conn)

        # the connection should be closed a new creation scheduled
        assert session.submit.call_args
        assert not pool.is_shutdown

    def test_return_defunct_connection_on_down_host(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
                                    max_request_id=100, signaled_error=False,
                                    orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn
        session.cluster.shard_aware_options = ShardAwareOptions()

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        conn.is_defunct = True
        session._signal_connection_failure.return_value = True
        pool.return_connection(conn)

        # the connection should be closed and the pool should delegate down
        # handling back to the session.
        assert conn.close.call_args
        session._signal_connection_failure.assert_called_once_with(host, conn.last_error)
        session._handle_pool_down.assert_called_once_with(host, is_host_addition=False)
        assert pool.is_shutdown

    def test_return_closed_connection(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=True, max_request_id=100,
                                    signaled_error=False, orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        conn.is_closed = True
        session._signal_connection_failure.return_value = False
        pool.return_connection(conn)

        # a new creation should be scheduled
        assert session.submit.call_args
        assert not pool.is_shutdown

    def test_host_instantiations(self):
        """
        Ensure Host fails if not initialized properly
        """

        with pytest.raises(ValueError):
            Host(None, None, host_id=uuid.uuid4())
        with pytest.raises(ValueError):
            Host('127.0.0.1', None, host_id=uuid.uuid4())
        with pytest.raises(ValueError):
            Host(None, SimpleConvictionPolicy, host_id=uuid.uuid4())

    def test_host_equality(self):
        """
        Test host equality has correct logic
        """

        a = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.uuid4())
        b = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.uuid4())
        c = Host('127.0.0.2', SimpleConvictionPolicy, host_id=uuid.uuid4())

        assert a == b, 'Two Host instances should be equal when sharing.'
        assert a != c, 'Two Host instances should NOT be equal when using two different addresses.'
        assert b != c, 'Two Host instances should NOT be equal when using two different addresses.'


class HostConnectionTests(_PoolTests):
    __test__ = True
    PoolImpl = HostConnection
    uses_single_connection = True

    def test_session_level_shard_aware_disable_skips_fanout(self):
        host = Mock(spec=Host, address='ip1')
        host.sharding_info = None
        session = self.make_session()
        session.is_shard_aware_disabled.return_value = True

        connection = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                                  is_closed=False, max_request_id=100)
        connection.features = ProtocolFeatures(
            shard_id=0,
            sharding_info=_ShardingInfo(
                shard_id=0, shards_count=4, partitioner="",
                sharding_algorithm="", sharding_ignore_msb=0,
                shard_aware_port=19042, shard_aware_port_ssl=""),
            tablets_routing_v1=False)
        session.cluster.connection_factory.return_value = connection

        with patch.object(HostConnection, "_open_connections_for_all_shards") as open_shards:
            pool = HostConnection(host, HostDistance.LOCAL, session)

        open_shards.assert_not_called()
        assert pool.host.sharding_info is None

    def test_fast_shutdown(self):
        class MockSession(MagicMock):
            is_shutdown = False
            keyspace = "reprospace"

            def __init__(self, *args, **kwargs):
                super(MockSession, self).__init__(*args, **kwargs)
                self.cluster = MagicMock()
                self.connection_created = Event()
                self.cluster.executor = ThreadPoolExecutor(max_workers=2)
                self.cluster.signal_connection_failure = lambda *args, **kwargs: False
                self.cluster.connection_factory = self.mock_connection_factory
                self.connection_counter = 0

            def submit(self, fn, *args, **kwargs):
                LOGGER.info("Scheduling %s with args: %s, kwargs: %s", fn, args, kwargs)
                if not self.is_shutdown:
                    return self.cluster.executor.submit(fn, *args, **kwargs)

            def mock_connection_factory(self, *args, **kwargs):
                connection = HashableMock()
                connection.is_shutdown = False
                connection.is_defunct = False
                connection.is_closed = False
                connection.features = ProtocolFeatures(shard_id=self.connection_counter,
                                                       sharding_info=_ShardingInfo(shard_id=1, shards_count=14,
                                                                    partitioner="", sharding_algorithm="", sharding_ignore_msb=0,
                                                                    shard_aware_port="", shard_aware_port_ssl=""))
                self.connection_counter += 1
                self.connection_created.set()

                return connection

        for attempt_num in range(3):
            LOGGER.info("Testing fast shutdown %d / 3 times", attempt_num + 1)
            host = MagicMock()
            host.endpoint = "1.2.3.4"
            session = MockSession()

            pool = HostConnection(host=host, host_distance=HostDistance.REMOTE, session=session)
            LOGGER.info("Initialized pool %s", pool)

            # Wait for initial connection to be created (with timeout)
            if not session.connection_created.wait(timeout=2.0):
                pytest.fail("Initial connection failed to be created within 2 seconds")

            LOGGER.info("Connections: %s", pool._connections)

            # Shutdown the pool
            pool.shutdown()

            # Verify pool is shut down
            assert pool.is_shutdown, "Pool should be marked as shutdown"

            # Cleanup executor with proper wait
            session.cluster.executor.shutdown(wait=True)
