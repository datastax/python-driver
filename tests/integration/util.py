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

from tests.integration import PROTOCOL_VERSION
from functools import wraps
import time


def assert_quiescent_pool_state(test_case, cluster, wait=None):
    """
    Checking the quiescent pool state checks that none of the requests ids have
    been lost. However, the callback corresponding to a request_id is called
    before the request_id is returned back to the pool, therefore

    session.execute("SELECT * from system.local")
    assert_quiescent_pool_state(self, session.cluster)

    (with no wait) might fail because when execute comes back the request_id
    hasn't yet been returned to the pool, therefore the wait.
    """
    if wait is not None:
        time.sleep(wait)

    for session in cluster.sessions:
        pool_states = session.get_pool_state().values()
        test_case.assertTrue(pool_states)

        for state in pool_states:
            test_case.assertFalse(state['shutdown'])
            test_case.assertGreater(state['open_count'], 0)
            test_case.assertTrue(all((i == 0 for i in state['in_flights'])))

    for holder in cluster.get_connection_holders():
        for connection in holder.get_connections():
            # all ids are unique
            req_ids = connection.request_ids
            test_case.assertEqual(len(req_ids), len(set(req_ids)))
            test_case.assertEqual(connection.highest_request_id, len(req_ids) - 1)
            test_case.assertEqual(connection.highest_request_id, max(req_ids))
            if PROTOCOL_VERSION < 3:
                test_case.assertEqual(connection.highest_request_id, connection.max_request_id)


def wait_until(condition, delay, max_attempts):
    """
    Executes a function at regular intervals while the condition
    is false and the amount of attempts < maxAttempts.
    :param condition: a function
    :param delay: the delay in second
    :param max_attempts: the maximum number of attempts. So the timeout
                         of this function is delay*max_attempts
    """
    attempt = 0
    while not condition() and attempt < max_attempts:
        attempt += 1
        time.sleep(delay)

    if attempt >= max_attempts:
        raise Exception("Condition is still False after {} attempts.".format(max_attempts))


def wait_until_not_raised(condition, delay, max_attempts):
    """
    Executes a function at regular intervals while the condition
    doesn't raise an exception and the amount of attempts < maxAttempts.
    :param condition: a function
    :param delay: the delay in second
    :param max_attempts: the maximum number of attemps. So the timeout
                         of this function will be delay*max_attempts
    """
    def wrapped_condition():
        try:
            condition()
        except:
            return False

        return True

    attempt = 0
    while attempt < (max_attempts-1):
        attempt += 1
        if wrapped_condition():
            return

        time.sleep(delay)

    # last attempt, let the exception raise
    condition()


def late(seconds=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            time.sleep(seconds)
            func(*args, **kwargs)
        return wrapper
    return decorator
