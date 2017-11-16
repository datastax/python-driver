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

import os
import warnings
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa
import mock
import threading
from functools import partial
from concurrent.futures import Future

from cassandra import ConsistencyLevel

from cassandra.cqlengine import connection
from cassandra.cqlengine.management import create_keyspace_simple, CQLENG_ALLOW_SCHEMA_MANAGEMENT
import cassandra

from tests.integration import get_server_versions, use_single_node, PROTOCOL_VERSION, CASSANDRA_IP, set_default_cass_ip
DEFAULT_KEYSPACE = 'cqlengine_test'


CQL_SKIP_EXECUTE = bool(os.getenv('CQL_SKIP_EXECUTE', False))


def setup_package():
    warnings.simplefilter('always')  # for testing warnings, make sure all are let through
    os.environ[CQLENG_ALLOW_SCHEMA_MANAGEMENT] = '1'

    set_default_cass_ip()
    use_single_node()

    setup_connection(DEFAULT_KEYSPACE)
    create_keyspace_simple(DEFAULT_KEYSPACE, 1)


def teardown_package():
    connection.unregister_connection("default")

def is_prepend_reversed():
    # do we have https://issues.apache.org/jira/browse/CASSANDRA-8733 ?
    ver, _ = get_server_versions()
    return not (ver >= (2, 0, 13) or ver >= (2, 1, 3))


def setup_connection(keyspace_name):
    connection.setup([CASSANDRA_IP],
                     consistency=ConsistencyLevel.ONE,
                     protocol_version=PROTOCOL_VERSION,
                     default_keyspace=keyspace_name)


class StatementCounter(object):
    """
    Simple python object used to hold a count of the number of times
    the wrapped method has been invoked
    """
    def __init__(self, patched_func):
        self.func = patched_func
        self.counter = 0

    def wrapped_execute(self, *args, **kwargs):
        self.counter += 1
        return self.func(*args, **kwargs)

    def get_counter(self):
        return self.counter


def execute_count(expected):
    """
    A decorator used wrap cassandra.cqlengine.connection.execute_async. It counts the number of times this method is invoked
    then compares it to the number expected. If they don't match it throws an assertion error.
    This function can be disabled by running the test harness with the env variable CQL_SKIP_EXECUTE=1 set
    """
    def _management_execute(execute_async, *args, **kwargs):
        """This wrapper is required to exclude all management queries from the expected count"""
        return execute_async(*args, **kwargs).result()

    def innerCounter(fn):
        def wrapped_function(*args, **kwargs):
            # Create a counter monkey patch into cassandra.cqlengine.connection.execute_async
            original_execute_async = cassandra.cqlengine.connection.execute_async
            management_original_function = cassandra.cqlengine.management.execute
            cassandra.cqlengine.management.execute = partial(_management_execute, original_execute_async)
            count = StatementCounter(cassandra.cqlengine.connection.execute_async)
            # Monkey patch in our StatementCounter wrapper
            cassandra.cqlengine.connection.execute_async = count.wrapped_execute
            # Invoked the underlying unit test
            to_return = fn(*args, **kwargs)
            # Get the count from our monkey patched counter
            count.get_counter()
            # DeMonkey Patch our code
            cassandra.cqlengine.connection.execute_async = original_execute_async
            cassandra.cqlengine.management.execute = management_original_function
            # Check to see if we have a pre-existing test case to work from.
            if len(args) is 0:
                test_case = unittest.TestCase("__init__")
            else:
                test_case = args[0]
            # Check to see if the count is what you expect
            test_case.assertEqual(count.get_counter(), expected, msg="Expected number of cassandra.cqlengine.connection.execute_async calls ({0}) doesn't match actual number invoked ({1})".format(expected, count.get_counter()))
            return to_return
        # Name of the wrapped function must match the original or unittest will error out.
        wrapped_function.__name__ = fn.__name__
        wrapped_function.__doc__ = fn.__doc__
        # Escape hatch
        if(CQL_SKIP_EXECUTE):
            return fn
        else:
            return wrapped_function

    return innerCounter


class MockResponseFuture(Future):

    _result = None

    class MockSessionWithExecutor(object):

        def __init__(self):
            self.cluster = self
            self.executor = self

        def submit(self, fn, *args, **kwargs):
            fn(*args, **kwargs)

    class MockResultSet(object):
        _result = []
        has_more_pages = False

        def __iter__(self):
            return iter(self._result)

    def __init__(self):
        super(MockResponseFuture, self).__init__()
        self.set_result(MockResponseFuture.MockResultSet())
        self.session = MockResponseFuture.MockSessionWithExecutor()

    def add_callback(self, fn, *args, **kwargs):
        fn(self._result, *args, **kwargs)

    def add_callbacks(self, callback, errback,
                      callback_args=(), callback_kwargs=None,
                      errback_args=(), errback_kwargs=None):
        self.add_callback(callback, *callback_args, **(callback_kwargs or {}))

    def clear_callbacks(self):
        pass


def make_magic_mock_future(*args, **kwargs):
    mrf = MockResponseFuture()
    return mock.MagicMock(*args, return_value=mrf, **kwargs)


def mock_execute_async(wraps=False):
    if wraps:
        return mock.patch('cassandra.cqlengine.connection.execute_async',
                          wraps=cassandra.cqlengine.connection.execute_async)
    else:
        return mock.patch('cassandra.cqlengine.connection.execute_async',
                          new_callable=make_magic_mock_future)
