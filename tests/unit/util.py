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


def check_sequence_consistency(unit_test, ordered_sequence, equal=False):
    for i, el in enumerate(ordered_sequence):
        for previous in ordered_sequence[:i]:
            _check_order_consistency(unit_test, previous, el, equal)
        for posterior in ordered_sequence[i + 1:]:
            _check_order_consistency(unit_test, el, posterior, equal)


def _check_order_consistency(unit_test, smaller, bigger, equal=False):
    unit_test.assertLessEqual(smaller, bigger)
    unit_test.assertGreaterEqual(bigger, smaller)
    if equal:
        unit_test.assertEqual(smaller, bigger)
    else:
        unit_test.assertNotEqual(smaller, bigger)
        unit_test.assertLess(smaller, bigger)
        unit_test.assertGreater(bigger, smaller)
