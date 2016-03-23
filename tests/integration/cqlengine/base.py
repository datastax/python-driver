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
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import sys

from cassandra.cqlengine.connection import get_session

class BaseCassEngTestCase(unittest.TestCase):

    session = None

    def setUp(self):
        self.session = get_session()

    def assertHasAttr(self, obj, attr):
        self.assertTrue(hasattr(obj, attr),
                "{0} doesn't have attribute: {1}".format(obj, attr))

    def assertNotHasAttr(self, obj, attr):
        self.assertFalse(hasattr(obj, attr),
                "{0} shouldn't have the attribute: {1}".format(obj, attr))

    if sys.version_info > (3, 0):
        def assertItemsEqual(self, first, second, msg=None):
            return self.assertCountEqual(first, second, msg)
