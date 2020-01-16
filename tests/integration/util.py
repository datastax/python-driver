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
