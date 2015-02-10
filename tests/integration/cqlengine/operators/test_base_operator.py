# Copyright 2015 DataStax, Inc.
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
from cassandra.cqlengine.operators import BaseQueryOperator, QueryOperatorException


class BaseOperatorTest(TestCase):

    def test_get_operator_cannot_be_called_from_base_class(self):
        with self.assertRaises(QueryOperatorException):
            BaseQueryOperator.get_operator('*')
