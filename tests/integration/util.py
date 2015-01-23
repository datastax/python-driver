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

def assert_quiescent_pool_state(test_case, cluster):

    for session in cluster.sessions:
        pool_states = session.get_pool_state().values()
        test_case.assertTrue(pool_states)

        for state in pool_states:
            test_case.assertFalse(state['shutdown'])
            test_case.assertGreater(state['open_count'], 0)
            test_case.assertTrue(all((i == 0 for i in state['in_flights'])))
