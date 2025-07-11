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
import unittest

import logging
import socket

from unittest.mock import patch, Mock

from cassandra import ConsistencyLevel, DriverException, Timeout, Unavailable, RequestExecutionException, ReadTimeout, WriteTimeout, CoordinationFailure, ReadFailure, WriteFailure, FunctionFailure, AlreadyExists,\
    InvalidRequest, Unauthorized, AuthenticationFailed, OperationTimedOut, UnsupportedOperation, RequestValidationException, ConfigurationException, ProtocolVersion
from cassandra.cluster import _Scheduler, Session, Cluster, default_lbp_factory, \
    ExecutionProfile, _ConfigMode, EXEC_PROFILE_DEFAULT
from cassandra.pool import Host
from cassandra.policies import HostDistance, RetryPolicy, RoundRobinPolicy, DowngradingConsistencyRetryPolicy, SimpleConvictionPolicy
from cassandra.query import SimpleStatement, named_tuple_factory, tuple_factory
from tests.unit.utils import mock_session_pools
from tests import connection_class
import pytest


log = logging.getLogger(__name__)

class ExceptionTypeTest(unittest.TestCase):

    def test_exception_types(self):
        """
        PYTHON-443
        Sanity check to ensure we don't unintentionally change class hierarchy of exception types
        """
        assert issubclass(Unavailable, DriverException)
        assert issubclass(Unavailable, RequestExecutionException)

        assert issubclass(ReadTimeout, DriverException)
        assert issubclass(ReadTimeout, RequestExecutionException)
        assert issubclass(ReadTimeout, Timeout)

        assert issubclass(WriteTimeout, DriverException)
        assert issubclass(WriteTimeout, RequestExecutionException)
        assert issubclass(WriteTimeout, Timeout)

        assert issubclass(CoordinationFailure, DriverException)
        assert issubclass(CoordinationFailure, RequestExecutionException)

        assert issubclass(ReadFailure, DriverException)
        assert issubclass(ReadFailure, RequestExecutionException)
        assert issubclass(ReadFailure, CoordinationFailure)

        assert issubclass(WriteFailure, DriverException)
        assert issubclass(WriteFailure, RequestExecutionException)
        assert issubclass(WriteFailure, CoordinationFailure)

        assert issubclass(FunctionFailure, DriverException)
        assert issubclass(FunctionFailure, RequestExecutionException)

        assert issubclass(RequestValidationException, DriverException)

        assert issubclass(ConfigurationException, DriverException)
        assert issubclass(ConfigurationException, RequestValidationException)

        assert issubclass(AlreadyExists, DriverException)
        assert issubclass(AlreadyExists, RequestValidationException)
        assert issubclass(AlreadyExists, ConfigurationException)

        assert issubclass(InvalidRequest, DriverException)
        assert issubclass(InvalidRequest, RequestValidationException)

        assert issubclass(Unauthorized, DriverException)
        assert issubclass(Unauthorized, RequestValidationException)

        assert issubclass(AuthenticationFailed, DriverException)

        assert issubclass(OperationTimedOut, DriverException)

        assert issubclass(UnsupportedOperation, DriverException)


class ClusterTest(unittest.TestCase):

    def test_tuple_for_contact_points(self):
        cluster = Cluster(contact_points=[('localhost', 9045), ('127.0.0.2', 9046), '127.0.0.3'], port=9999)
        localhost_addr = set([addr[0] for addr in [t for (_,_,_,_,t) in socket.getaddrinfo("localhost",80)]])
        for cp in cluster.endpoints_resolved:
            if cp.address in localhost_addr:
                assert cp.port == 9045
            elif cp.address == '127.0.0.2':
                assert cp.port == 9046
            else:
                assert cp.address == '127.0.0.3'
                assert cp.port == 9999

    def test_invalid_contact_point_types(self):
        with pytest.raises(ValueError):
            Cluster(contact_points=[None], protocol_version=4, connect_timeout=1)
        with pytest.raises(TypeError):
            Cluster(contact_points="not a sequence", protocol_version=4, connect_timeout=1)

    def test_port_str(self):
        """Check port passed as tring is converted and checked properly"""
        cluster = Cluster(contact_points=['127.0.0.1'], port='1111')
        for cp in cluster.endpoints_resolved:
            if cp.address in ('::1', '127.0.0.1'):
                assert cp.port == 1111

        with pytest.raises(ValueError):
            cluster = Cluster(contact_points=['127.0.0.1'], port='string')


    def test_port_range(self):
        for invalid_port in [0, 65536, -1]:
            with pytest.raises(ValueError):
                cluster = Cluster(contact_points=['127.0.0.1'], port=invalid_port)


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
    def test_default_serial_consistency_level_ep(self, *_):
        """
        Make sure default_serial_consistency_level passes through to a query message using execution profiles.
        Also make sure Statement.serial_consistency_level overrides the default.

        PR #510
        """
        c = Cluster(protocol_version=4)
        s = Session(c, [Host("127.0.0.1", SimpleConvictionPolicy)])
        c.connection_class.initialize_reactor()

        # default is None
        default_profile = c.profile_manager.default
        assert default_profile.serial_consistency_level is None

        for cl in (None, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
            s.get_execution_profile(EXEC_PROFILE_DEFAULT).serial_consistency_level = cl

            # default is passed through
            f = s.execute_async(query='')
            assert f.message.serial_consistency_level == cl

            # any non-None statement setting takes precedence
            for cl_override in (ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
                f = s.execute_async(SimpleStatement(query_string='', serial_consistency_level=cl_override))
                assert default_profile.serial_consistency_level == cl
                assert f.message.serial_consistency_level == cl_override

    @mock_session_pools
    def test_default_serial_consistency_level_legacy(self, *_):
        """
        Make sure default_serial_consistency_level passes through to a query message using legacy settings.
        Also make sure Statement.serial_consistency_level overrides the default.

        PR #510
        """
        c = Cluster(protocol_version=4)
        s = Session(c, [Host("127.0.0.1", SimpleConvictionPolicy)])
        c.connection_class.initialize_reactor()
        # default is None
        assert s.default_serial_consistency_level is None

        # Should fail
        with pytest.raises(ValueError):
            s.default_serial_consistency_level = ConsistencyLevel.ANY
        with pytest.raises(ValueError):
            s.default_serial_consistency_level = 1001

        for cl in (None, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
            s.default_serial_consistency_level = cl

            # any non-None statement setting takes precedence
            for cl_override in (ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
                f = s.execute_async(SimpleStatement(query_string='', serial_consistency_level=cl_override))
                assert s.default_serial_consistency_level == cl
                assert f.message.serial_consistency_level == cl_override


class ProtocolVersionTests(unittest.TestCase):

    def test_protocol_downgrade_test(self):
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.DSE_V2)
        assert ProtocolVersion.DSE_V1 == lower
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.DSE_V1)
        assert ProtocolVersion.V5 == lower
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.V5)
        assert ProtocolVersion.V4 == lower
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.V4)
        assert ProtocolVersion.V3 == lower
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.V3)
        assert 0 == lower

        assert ProtocolVersion.uses_error_code_map(ProtocolVersion.DSE_V1)
        assert ProtocolVersion.uses_int_query_flags(ProtocolVersion.DSE_V1)

        assert not ProtocolVersion.uses_error_code_map(ProtocolVersion.V4)
        assert not ProtocolVersion.uses_int_query_flags(ProtocolVersion.V4)


class ExecutionProfileTest(unittest.TestCase):
    def setUp(self):
        if connection_class is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        connection_class.initialize_reactor()

    def _verify_response_future_profile(self, rf, prof):
        assert rf._load_balancer == prof.load_balancing_policy
        assert rf._retry_policy == prof.retry_policy
        assert rf.message.consistency_level == prof.consistency_level
        assert rf.message.serial_consistency_level == prof.serial_consistency_level
        assert rf.timeout == prof.request_timeout
        assert rf.row_factory == prof.row_factory

    @mock_session_pools
    def test_default_exec_parameters(self):
        cluster = Cluster()
        assert cluster._config_mode == _ConfigMode.UNCOMMITTED
        assert cluster.load_balancing_policy.__class__ == default_lbp_factory().__class__
        assert cluster.profile_manager.default.load_balancing_policy.__class__ == default_lbp_factory().__class__
        assert cluster.default_retry_policy.__class__ == RetryPolicy
        assert cluster.profile_manager.default.retry_policy.__class__ == RetryPolicy
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
        assert session.default_timeout == 10.0
        assert cluster.profile_manager.default.request_timeout == 10.0
        assert session.default_consistency_level == ConsistencyLevel.LOCAL_ONE
        assert cluster.profile_manager.default.consistency_level == ConsistencyLevel.LOCAL_ONE
        assert session.default_serial_consistency_level == None
        assert cluster.profile_manager.default.serial_consistency_level == None
        assert session.row_factory == named_tuple_factory
        assert cluster.profile_manager.default.row_factory == named_tuple_factory

    @mock_session_pools
    def test_default_legacy(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), default_retry_policy=DowngradingConsistencyRetryPolicy())
        assert cluster._config_mode == _ConfigMode.LEGACY
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
        non_default_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(2)])
        cluster = Cluster(execution_profiles={'non-default': non_default_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        assert cluster._config_mode == _ConfigMode.PROFILES

        default_profile = cluster.profile_manager.profiles[EXEC_PROFILE_DEFAULT]
        rf = session.execute_async("query")
        self._verify_response_future_profile(rf, default_profile)

        rf = session.execute_async("query", execution_profile='non-default')
        self._verify_response_future_profile(rf, non_default_profile)

        for name, ep in cluster.profile_manager.profiles.items():
            assert ep == session.get_execution_profile(name)

        # invalid ep
        with pytest.raises(ValueError):
            session.get_execution_profile('non-existent')

    def test_serial_consistency_level_validation(self):
        # should pass
        ep = ExecutionProfile(RoundRobinPolicy(), serial_consistency_level=ConsistencyLevel.SERIAL)
        ep = ExecutionProfile(RoundRobinPolicy(), serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL)

        # should not pass
        with pytest.raises(ValueError):
            ep = ExecutionProfile(RoundRobinPolicy(), serial_consistency_level=ConsistencyLevel.ANY)
        with pytest.raises(ValueError):
            ep = ExecutionProfile(RoundRobinPolicy(), serial_consistency_level=42)

    @mock_session_pools
    def test_statement_params_override_legacy(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), default_retry_policy=DowngradingConsistencyRetryPolicy())
        assert cluster._config_mode == _ConfigMode.LEGACY
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        ss = SimpleStatement("query", retry_policy=DowngradingConsistencyRetryPolicy(),
                             consistency_level=ConsistencyLevel.ALL, serial_consistency_level=ConsistencyLevel.SERIAL)
        my_timeout = 1.1234

        assert ss.retry_policy.__class__ != cluster.default_retry_policy
        assert ss.consistency_level != session.default_consistency_level
        assert ss._serial_consistency_level != session.default_serial_consistency_level
        assert my_timeout != session.default_timeout

        rf = session.execute_async(ss, timeout=my_timeout)
        expected_profile = ExecutionProfile(load_balancing_policy=cluster.load_balancing_policy, retry_policy=ss.retry_policy,
                                            request_timeout=my_timeout, consistency_level=ss.consistency_level,
                                            serial_consistency_level=ss._serial_consistency_level)
        self._verify_response_future_profile(rf, expected_profile)

    @mock_session_pools
    def test_statement_params_override_profile(self):
        non_default_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(2)])
        cluster = Cluster(execution_profiles={'non-default': non_default_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        assert cluster._config_mode == _ConfigMode.PROFILES

        rf = session.execute_async("query", execution_profile='non-default')

        ss = SimpleStatement("query", retry_policy=DowngradingConsistencyRetryPolicy(),
                             consistency_level=ConsistencyLevel.ALL, serial_consistency_level=ConsistencyLevel.SERIAL)
        my_timeout = 1.1234

        assert ss.retry_policy.__class__ != rf._load_balancer.__class__
        assert ss.consistency_level != rf.message.consistency_level
        assert ss._serial_consistency_level != rf.message.serial_consistency_level
        assert my_timeout != rf.timeout

        rf = session.execute_async(ss, timeout=my_timeout, execution_profile='non-default')
        expected_profile = ExecutionProfile(non_default_profile.load_balancing_policy, ss.retry_policy,
                                            ss.consistency_level, ss._serial_consistency_level, my_timeout, non_default_profile.row_factory)
        self._verify_response_future_profile(rf, expected_profile)

    @mock_session_pools
    def test_no_profile_with_legacy(self):
        # don't construct with both
        with pytest.raises(ValueError):
            Cluster(load_balancing_policy=RoundRobinPolicy(), execution_profiles={'a': ExecutionProfile()})
        with pytest.raises(ValueError):
            Cluster(default_retry_policy=DowngradingConsistencyRetryPolicy(), execution_profiles={'a': ExecutionProfile()})
        with pytest.raises(ValueError):
            Cluster(load_balancing_policy=RoundRobinPolicy(),
                          default_retry_policy=DowngradingConsistencyRetryPolicy(), execution_profiles={'a': ExecutionProfile()})

        # can't add after
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy())
        with pytest.raises(ValueError):
            cluster.add_execution_profile('name', ExecutionProfile())

        # session settings lock out profiles
        cluster = Cluster()
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
        for attr, value in (('default_timeout', 1),
                            ('default_consistency_level', ConsistencyLevel.ANY),
                            ('default_serial_consistency_level', ConsistencyLevel.SERIAL),
                            ('row_factory', tuple_factory)):
            cluster._config_mode = _ConfigMode.UNCOMMITTED
            setattr(session, attr, value)
            with pytest.raises(ValueError):
                cluster.add_execution_profile('name' + attr, ExecutionProfile())

        # don't accept profile
        with pytest.raises(ValueError):
            session.execute_async("query", execution_profile='some name here')

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
                with pytest.raises(ValueError):
                    setattr(cluster, attr, value)
            session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
            for attr, value in (('default_timeout', 1),
                                ('default_consistency_level', ConsistencyLevel.ANY),
                                ('default_serial_consistency_level', ConsistencyLevel.SERIAL),
                                ('row_factory', tuple_factory)):
                with pytest.raises(ValueError):
                    setattr(session, attr, value)

    @mock_session_pools
    def test_profile_name_value(self):

        internalized_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(2)])
        cluster = Cluster(execution_profiles={'by-name': internalized_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
        assert cluster._config_mode == _ConfigMode.PROFILES

        rf = session.execute_async("query", execution_profile='by-name')
        self._verify_response_future_profile(rf, internalized_profile)

        by_value = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(2)])
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
            active = session.get_execution_profile(profile)
            clone = session.execution_profile_clone_update(profile)
            assert clone is not active

            all_updated = session.execution_profile_clone_update(clone, **profile_attrs)
            assert all_updated is not clone
            for attr, value in profile_attrs.items():
                assert getattr(clone, attr) == getattr(active, attr)
                if attr in reference_attributes:
                    assert getattr(clone, attr) is getattr(active, attr)
                assert getattr(all_updated, attr) != getattr(active, attr)

        # cannot clone nonexistent profile
        with pytest.raises(ValueError):
            session.execution_profile_clone_update('DOES NOT EXIST', **profile_attrs)

    def test_no_profiles_same_name(self):
        # can override default in init
        cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(), 'one': ExecutionProfile()})

        # cannot update default
        with pytest.raises(ValueError):
            cluster.add_execution_profile(EXEC_PROFILE_DEFAULT, ExecutionProfile())

        # cannot update named init
        with pytest.raises(ValueError):
            cluster.add_execution_profile('one', ExecutionProfile())

        # can add new name
        cluster.add_execution_profile('two', ExecutionProfile())

        # cannot add a profile added dynamically
        with pytest.raises(ValueError):
            cluster.add_execution_profile('two', ExecutionProfile())

    def test_warning_on_no_lbp_with_contact_points_legacy_mode(self):
        """
        Test that users are warned when they instantiate a Cluster object in
        legacy mode with contact points but no load-balancing policy.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result logs

        @test_category configuration
        """
        self._check_warning_on_no_lbp_with_contact_points(
            cluster_kwargs={'contact_points': ['127.0.0.1']}
        )

    def test_warning_on_no_lbp_with_contact_points_profile_mode(self):
        """
        Test that users are warned when they instantiate a Cluster object in
        execution profile mode with contact points but no load-balancing
        policy.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result logs

        @test_category configuration
        """
        self._check_warning_on_no_lbp_with_contact_points(cluster_kwargs={
            'contact_points': ['127.0.0.1'],
            'execution_profiles': {EXEC_PROFILE_DEFAULT: ExecutionProfile()}
        })

    @mock_session_pools
    def _check_warning_on_no_lbp_with_contact_points(self, cluster_kwargs):
        with patch('cassandra.cluster.log') as patched_logger:
            Cluster(**cluster_kwargs)
        patched_logger.warning.assert_called_once()
        warning_message = patched_logger.warning.call_args[0][0]
        assert 'please specify a load-balancing policy' in warning_message
        assert "contact_points = ['127.0.0.1']" in warning_message

    def test_no_warning_on_contact_points_with_lbp_legacy_mode(self):
        """
        Test that users aren't warned when they instantiate a Cluster object
        with contact points and a load-balancing policy in legacy mode.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result no logs

        @test_category configuration
        """
        self._check_no_warning_on_contact_points_with_lbp({
            'contact_points': ['127.0.0.1'],
            'load_balancing_policy': object()
        })

    def test_no_warning_on_contact_points_with_lbp_profiles_mode(self):
        """
        Test that users aren't warned when they instantiate a Cluster object
        with contact points and a load-balancing policy in execution profile
        mode.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result no logs

        @test_category configuration
        """
        ep_with_lbp = ExecutionProfile(load_balancing_policy=object())
        self._check_no_warning_on_contact_points_with_lbp(cluster_kwargs={
            'contact_points': ['127.0.0.1'],
            'execution_profiles': {
                EXEC_PROFILE_DEFAULT: ep_with_lbp
            }
        })

    @mock_session_pools
    def _check_no_warning_on_contact_points_with_lbp(self, cluster_kwargs):
        """
        Test that users aren't warned when they instantiate a Cluster object
        with contact points and a load-balancing policy.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result no logs

        @test_category configuration
        """
        with patch('cassandra.cluster.log') as patched_logger:
            Cluster(**cluster_kwargs)
        patched_logger.warning.assert_not_called()

    @mock_session_pools
    def test_warning_adding_no_lbp_ep_to_cluster_with_contact_points(self):
        ep_with_lbp = ExecutionProfile(load_balancing_policy=object())
        cluster = Cluster(
            contact_points=['127.0.0.1'],
            execution_profiles={EXEC_PROFILE_DEFAULT: ep_with_lbp})
        with patch('cassandra.cluster.log') as patched_logger:
            cluster.add_execution_profile(
                name='no_lbp',
                profile=ExecutionProfile()
            )

        patched_logger.warning.assert_called_once()
        warning_message = patched_logger.warning.call_args[0][0]
        assert 'no_lbp' in warning_message
        assert 'trying to add' in warning_message
        assert 'please specify a load-balancing policy' in warning_message

    @mock_session_pools
    def test_no_warning_adding_lbp_ep_to_cluster_with_contact_points(self):
        ep_with_lbp = ExecutionProfile(load_balancing_policy=object())
        cluster = Cluster(
            contact_points=['127.0.0.1'],
            execution_profiles={EXEC_PROFILE_DEFAULT: ep_with_lbp})
        with patch('cassandra.cluster.log') as patched_logger:
            cluster.add_execution_profile(
                name='with_lbp',
                profile=ExecutionProfile(load_balancing_policy=Mock(name='lbp'))
            )

        patched_logger.warning.assert_not_called()
