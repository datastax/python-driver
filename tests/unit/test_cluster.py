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

from mock import patch

from cassandra import ConsistencyLevel, DriverException, Timeout, Unavailable, RequestExecutionException, ReadTimeout, WriteTimeout, CoordinationFailure, ReadFailure, WriteFailure, FunctionFailure, AlreadyExists,\
    InvalidRequest, Unauthorized, AuthenticationFailed, OperationTimedOut, UnsupportedOperation, RequestValidationException, ConfigurationException
from cassandra.cluster import _Scheduler, Session, Cluster, _NOT_SET, default_lbp_factory, \
    ExecutionProfile, _ConfigMode, EXEC_PROFILE_DEFAULT, NoHostAvailable
from cassandra.policies import HostDistance, RetryPolicy, RoundRobinPolicy, \
    DowngradingConsistencyRetryPolicy, SimpleConvictionPolicy
from cassandra.query import SimpleStatement, named_tuple_factory, tuple_factory
from cassandra.pool import Host
from tests.unit.utils import mock_session_pools
from tests import connection_class


class ExceptionTypeTest(unittest.TestCase):

    def test_exception_types(self):
        """
        PYTHON-443
        Sanity check to ensure we don't unintentionally change class hierarchy of exception types
        """
        self.assertTrue(issubclass(Unavailable, DriverException))
        self.assertTrue(issubclass(Unavailable, RequestExecutionException))

        self.assertTrue(issubclass(ReadTimeout, DriverException))
        self.assertTrue(issubclass(ReadTimeout, RequestExecutionException))
        self.assertTrue(issubclass(ReadTimeout, Timeout))

        self.assertTrue(issubclass(WriteTimeout, DriverException))
        self.assertTrue(issubclass(WriteTimeout, RequestExecutionException))
        self.assertTrue(issubclass(WriteTimeout, Timeout))

        self.assertTrue(issubclass(CoordinationFailure, DriverException))
        self.assertTrue(issubclass(CoordinationFailure, RequestExecutionException))

        self.assertTrue(issubclass(ReadFailure, DriverException))
        self.assertTrue(issubclass(ReadFailure, RequestExecutionException))
        self.assertTrue(issubclass(ReadFailure, CoordinationFailure))

        self.assertTrue(issubclass(WriteFailure, DriverException))
        self.assertTrue(issubclass(WriteFailure, RequestExecutionException))
        self.assertTrue(issubclass(WriteFailure, CoordinationFailure))

        self.assertTrue(issubclass(FunctionFailure, DriverException))
        self.assertTrue(issubclass(FunctionFailure, RequestExecutionException))

        self.assertTrue(issubclass(RequestValidationException, DriverException))

        self.assertTrue(issubclass(ConfigurationException, DriverException))
        self.assertTrue(issubclass(ConfigurationException, RequestValidationException))

        self.assertTrue(issubclass(AlreadyExists, DriverException))
        self.assertTrue(issubclass(AlreadyExists, RequestValidationException))
        self.assertTrue(issubclass(AlreadyExists, ConfigurationException))

        self.assertTrue(issubclass(InvalidRequest, DriverException))
        self.assertTrue(issubclass(InvalidRequest, RequestValidationException))

        self.assertTrue(issubclass(Unauthorized, DriverException))
        self.assertTrue(issubclass(Unauthorized, RequestValidationException))

        self.assertTrue(issubclass(AuthenticationFailed, DriverException))

        self.assertTrue(issubclass(OperationTimedOut, DriverException))

        self.assertTrue(issubclass(UnsupportedOperation, DriverException))


class ClusterTest(unittest.TestCase):

    def test_invalid_contact_point_types(self):
        with self.assertRaises(ValueError):
            Cluster(contact_points=[None], protocol_version=4, connect_timeout=1)
        with self.assertRaises(TypeError):
            Cluster(contact_points="not a sequence", protocol_version=4, connect_timeout=1)

    def test_requests_in_flight_threshold(self):
        d = HostDistance.LOCAL
        mn = 3
        mx = 5
        c = Cluster(protocol_version=2)
        c.set_min_requests_per_connection(d, mn)
        c.set_max_requests_per_connection(d, mx)
        # min underflow, max, overflow
        for n in (-1, mx, 127):
            self.assertRaises(ValueError, c.set_min_requests_per_connection, d, n)
        # max underflow, under min, overflow
        for n in (0, mn, 128):
            self.assertRaises(ValueError, c.set_max_requests_per_connection, d, n)


class SchedulerTest(unittest.TestCase):
    # TODO: this suite could be expanded; for now just adding a test covering a ticket

    @patch('time.time', return_value=3)  # always queue at same time
    @patch('cassandra.cluster._Scheduler.run')  # don't actually run the thread
    def test_event_delay_timing(self, *_):
        """
        Schedule something with a time collision to make sure the heap comparison works

        PYTHON-473
        """
        sched = _Scheduler(None)
        sched.schedule(0, lambda: None)
        sched.schedule(0, lambda: None)  # pre-473: "TypeError: unorderable types: function() < function()"t


class SessionTest(unittest.TestCase):
    def setUp(self):
        if connection_class is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        connection_class.initialize_reactor()

    # TODO: this suite could be expanded; for now just adding a test covering a PR
    @mock_session_pools
    def test_default_serial_consistency_level(self, *_):
        """
        Make sure default_serial_consistency_level passes through to a query message.
        Also make sure Statement.serial_consistency_level overrides the default.

        PR #510
        """
        s = Session(Cluster(protocol_version=4), [Host("127.0.0.1", SimpleConvictionPolicy)])

        # default is None
        self.assertIsNone(s.default_serial_consistency_level)

        sentinel = 1001
        for cl in (None, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL, sentinel):
            s.default_serial_consistency_level = cl

            # default is passed through
            f = s.execute_async(query='')
            self.assertEqual(f.message.serial_consistency_level, cl)

            # any non-None statement setting takes precedence
            for cl_override in (ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
                f = s.execute_async(SimpleStatement(query_string='', serial_consistency_level=cl_override))
                self.assertEqual(s.default_serial_consistency_level, cl)
                self.assertEqual(f.message.serial_consistency_level, cl_override)


class ExecutionProfileTest(unittest.TestCase):
    def setUp(self):
        if connection_class is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        connection_class.initialize_reactor()

    def _verify_response_future_profile(self, rf, prof):
        self.assertEqual(rf._load_balancer, prof.load_balancing_policy)
        self.assertEqual(rf._retry_policy, prof.retry_policy)
        self.assertEqual(rf.message.consistency_level, prof.consistency_level)
        self.assertEqual(rf.message.serial_consistency_level, prof.serial_consistency_level)
        self.assertEqual(rf.timeout, prof.request_timeout)
        self.assertEqual(rf.row_factory, prof.row_factory)

    @mock_session_pools
    def test_default_exec_parameters(self):
        cluster = Cluster()
        self.assertEqual(cluster._config_mode, _ConfigMode.UNCOMMITTED)
        self.assertEqual(cluster.load_balancing_policy.__class__, default_lbp_factory().__class__)
        self.assertEqual(cluster.default_retry_policy.__class__, RetryPolicy)
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
        self.assertEqual(session.default_timeout, 10.0)
        self.assertEqual(session.default_consistency_level, ConsistencyLevel.LOCAL_ONE)
        self.assertEqual(session.default_serial_consistency_level, None)
        self.assertEqual(session.row_factory, named_tuple_factory)

    @mock_session_pools
    def test_default_legacy(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), default_retry_policy=DowngradingConsistencyRetryPolicy())
        self.assertEqual(cluster._config_mode, _ConfigMode.LEGACY)
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
        session.default_timeout = 3.7
        session.default_consistency_level = ConsistencyLevel.ALL
        session.default_serial_consistency_level = ConsistencyLevel.SERIAL
        rf = session.execute_async("query")
        expected_profile = ExecutionProfile(cluster.load_balancing_policy, cluster.default_retry_policy,
                                            session.default_consistency_level, session.default_serial_consistency_level,
                                            session.default_timeout, session.row_factory)
        self._verify_response_future_profile(rf, expected_profile)

    @mock_session_pools
    def test_default_profile(self):
        non_default_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(3)])
        cluster = Cluster(execution_profiles={'non-default': non_default_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        self.assertEqual(cluster._config_mode, _ConfigMode.PROFILES)

        default_profile = cluster.profile_manager.profiles[EXEC_PROFILE_DEFAULT]
        rf = session.execute_async("query")
        self._verify_response_future_profile(rf, default_profile)

        rf = session.execute_async("query", execution_profile='non-default')
        self._verify_response_future_profile(rf, non_default_profile)

    @mock_session_pools
    def test_statement_params_override_legacy(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), default_retry_policy=DowngradingConsistencyRetryPolicy())
        self.assertEqual(cluster._config_mode, _ConfigMode.LEGACY)
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        ss = SimpleStatement("query", retry_policy=DowngradingConsistencyRetryPolicy(),
                             consistency_level=ConsistencyLevel.ALL, serial_consistency_level=ConsistencyLevel.SERIAL)
        my_timeout = 1.1234

        self.assertNotEqual(ss.retry_policy.__class__, cluster.default_retry_policy)
        self.assertNotEqual(ss.consistency_level, session.default_consistency_level)
        self.assertNotEqual(ss._serial_consistency_level, session.default_serial_consistency_level)
        self.assertNotEqual(my_timeout, session.default_timeout)

        rf = session.execute_async(ss, timeout=my_timeout)
        expected_profile = ExecutionProfile(load_balancing_policy=cluster.load_balancing_policy, retry_policy=ss.retry_policy,
                                            request_timeout=my_timeout, consistency_level=ss.consistency_level,
                                            serial_consistency_level=ss._serial_consistency_level)
        self._verify_response_future_profile(rf, expected_profile)

    @mock_session_pools
    def test_statement_params_override_profile(self):
        non_default_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(3)])
        cluster = Cluster(execution_profiles={'non-default': non_default_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        self.assertEqual(cluster._config_mode, _ConfigMode.PROFILES)

        rf = session.execute_async("query", execution_profile='non-default')

        ss = SimpleStatement("query", retry_policy=DowngradingConsistencyRetryPolicy(),
                             consistency_level=ConsistencyLevel.ALL, serial_consistency_level=ConsistencyLevel.SERIAL)
        my_timeout = 1.1234

        self.assertNotEqual(ss.retry_policy.__class__, rf._load_balancer.__class__)
        self.assertNotEqual(ss.consistency_level, rf.message.consistency_level)
        self.assertNotEqual(ss._serial_consistency_level, rf.message.serial_consistency_level)
        self.assertNotEqual(my_timeout, rf.timeout)

        rf = session.execute_async(ss, timeout=my_timeout, execution_profile='non-default')
        expected_profile = ExecutionProfile(non_default_profile.load_balancing_policy, ss.retry_policy,
                                            ss.consistency_level, ss._serial_consistency_level, my_timeout, non_default_profile.row_factory)
        self._verify_response_future_profile(rf, expected_profile)

    @mock_session_pools
    def test_no_profile_with_legacy(self):
        # don't construct with both
        self.assertRaises(ValueError, Cluster, load_balancing_policy=RoundRobinPolicy(), execution_profiles={'a': ExecutionProfile()})
        self.assertRaises(ValueError, Cluster, default_retry_policy=DowngradingConsistencyRetryPolicy(), execution_profiles={'a': ExecutionProfile()})
        self.assertRaises(ValueError, Cluster, load_balancing_policy=RoundRobinPolicy(),
                          default_retry_policy=DowngradingConsistencyRetryPolicy(), execution_profiles={'a': ExecutionProfile()})

        # can't add after
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy())
        self.assertRaises(ValueError, cluster.add_execution_profile, 'name', ExecutionProfile())

        # session settings lock out profiles
        cluster = Cluster()
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
        for attr, value in (('default_timeout', 1),
                            ('default_consistency_level', ConsistencyLevel.ANY),
                            ('default_serial_consistency_level', ConsistencyLevel.SERIAL),
                            ('row_factory', tuple_factory)):
            cluster._config_mode = _ConfigMode.UNCOMMITTED
            setattr(session, attr, value)
            self.assertRaises(ValueError, cluster.add_execution_profile, 'name' + attr, ExecutionProfile())

        # don't accept profile
        self.assertRaises(ValueError, session.execute_async, "query", execution_profile='some name here')

    @mock_session_pools
    def test_no_legacy_with_profile(self):
        cluster_init = Cluster(execution_profiles={'name': ExecutionProfile()})
        cluster_add = Cluster()
        cluster_add.add_execution_profile('name', ExecutionProfile())
        # for clusters with profiles added either way...
        for cluster in (cluster_init, cluster_init):
            # don't allow legacy parameters set
            for attr, value in (('default_retry_policy', RetryPolicy()),
                                ('load_balancing_policy', default_lbp_factory())):
                self.assertRaises(ValueError, setattr, cluster, attr, value)
            session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
            for attr, value in (('default_timeout', 1),
                                ('default_consistency_level', ConsistencyLevel.ANY),
                                ('default_serial_consistency_level', ConsistencyLevel.SERIAL),
                                ('row_factory', tuple_factory)):
                self.assertRaises(ValueError, setattr, session, attr, value)

    @mock_session_pools
    def test_profile_name_value(self):

        internalized_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(3)])
        cluster = Cluster(execution_profiles={'by-name': internalized_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
        self.assertEqual(cluster._config_mode, _ConfigMode.PROFILES)

        rf = session.execute_async("query", execution_profile='by-name')
        self._verify_response_future_profile(rf, internalized_profile)

        by_value = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(3)])
        rf = session.execute_async("query", execution_profile=by_value)
        self._verify_response_future_profile(rf, by_value)

    @mock_session_pools
    def test_exec_profile_clone(self):

        cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(), 'one': ExecutionProfile()})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        profile_attrs = {'request_timeout': 1,
                         'consistency_level': ConsistencyLevel.ANY,
                         'serial_consistency_level': ConsistencyLevel.SERIAL,
                         'row_factory': tuple_factory,
                         'retry_policy': RetryPolicy(),
                         'load_balancing_policy': default_lbp_factory()}
        reference_attributes = ('retry_policy', 'load_balancing_policy')

        # default and one named
        for profile in (EXEC_PROFILE_DEFAULT, 'one'):
            active = cluster.profile_manager.profiles[profile]
            clone = session.execution_profile_clone_update(profile)
            self.assertIsNot(clone, active)

            all_updated = session.execution_profile_clone_update(clone, **profile_attrs)
            self.assertIsNot(all_updated, clone)
            for attr, value in profile_attrs.items():
                self.assertEqual(getattr(clone, attr), getattr(active, attr))
                if attr in reference_attributes:
                    self.assertIs(getattr(clone, attr), getattr(active, attr))
                self.assertNotEqual(getattr(all_updated, attr), getattr(active, attr))

        # cannot clone nonexistent profile
        self.assertRaises(ValueError, session.execution_profile_clone_update, 'DOES NOT EXIST', **profile_attrs)

    def test_no_profiles_same_name(self):
        # can override default in init
        cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(), 'one': ExecutionProfile()})

        # cannot update default
        self.assertRaises(ValueError, cluster.add_execution_profile, EXEC_PROFILE_DEFAULT, ExecutionProfile())

        # cannot update named init
        self.assertRaises(ValueError, cluster.add_execution_profile, 'one', ExecutionProfile())

        # can add new name
        cluster.add_execution_profile('two', ExecutionProfile())

        # cannot add a profile added dynamically
        self.assertRaises(ValueError, cluster.add_execution_profile, 'two', ExecutionProfile())

    @mock_session_pools
    def test_warning_on_no_lbp_with_contact_points(self):
        """
        Test that users are warned when they instantiate a Cluster object with
        contact points but no load-balancing policy.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result logs

        @test_category configuration
        """
        with patch('cassandra.cluster.log') as patched_logger:
            Cluster(contact_points=['1'])
        patched_logger.warn.assert_called_once()
        warning_message = patched_logger.warn.call_args[0][0]
        self.assertIn('no load_balancing_policy', warning_message)
        self.assertIn("contact_points = ['1']", warning_message)
        self.assertIn('lbp = None', warning_message)

    @mock_session_pools
    def test_no_warning_on_contact_points_with_lbp(self):
        """
        Test that users aren't warned when they instantiate a Cluster object
        with contact points and a load-balancing policy.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result no logs

        @test_category configuration
        """
        with patch('cassandra.cluster.log') as patched_logger:
            Cluster(contact_points=['1'], load_balancing_policy=object())
        patched_logger.warn.assert_not_called()
