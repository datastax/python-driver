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

from unittest import TestCase
from cassandra.cqlengine.operators import *

import six


class TestWhereOperators(TestCase):

    def test_symbol_lookup(self):
        """ tests where symbols are looked up properly """

        def check_lookup(symbol, expected):
            op = BaseWhereOperator.get_operator(symbol)
            self.assertEqual(op, expected)

        check_lookup('EQ', EqualsOperator)
        check_lookup('NE', NotEqualsOperator)
        check_lookup('IN', InOperator)
        check_lookup('GT', GreaterThanOperator)
        check_lookup('GTE', GreaterThanOrEqualOperator)
        check_lookup('LT', LessThanOperator)
        check_lookup('LTE', LessThanOrEqualOperator)
        check_lookup('CONTAINS', ContainsOperator)

    def test_operator_rendering(self):
        """ tests symbols are rendered properly """
        self.assertEqual("=", six.text_type(EqualsOperator()))
        self.assertEqual("!=", six.text_type(NotEqualsOperator()))
        self.assertEqual("IN", six.text_type(InOperator()))
        self.assertEqual(">", six.text_type(GreaterThanOperator()))
        self.assertEqual(">=", six.text_type(GreaterThanOrEqualOperator()))
        self.assertEqual("<", six.text_type(LessThanOperator()))
        self.assertEqual("<=", six.text_type(LessThanOrEqualOperator()))
        self.assertEqual("CONTAINS", six.text_type(ContainsOperator()))


