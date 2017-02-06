# Copyright 2013-2016 DataStax, Inc.
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

from tests.unit.cython.utils import cyimport, cythontest
types_testhelper = cyimport('tests.unit.cython.types_testhelper')

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa


class TypesTest(unittest.TestCase):

    @cythontest
    def test_datetype(self):
        types_testhelper.test_datetype(self.assertEqual)

    @cythontest
    def test_date_side_by_side(self):
        types_testhelper.test_date_side_by_side(self.assertEqual)
