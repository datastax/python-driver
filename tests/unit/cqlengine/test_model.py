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

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import cassandra.cqlengine.models
from cassandra.cqlengine.models import ModelMetaClass, Model, columns
from cassandra.cqlengine.concurrent import CQLEngineFuture, CQLEngineFutureWaiter

from mock import patch, Mock
import warnings
import time
import random

class ToSubclassModel(Model):
    id = columns.UUID(primary_key=True)
    pass


class ModelTests(unittest.TestCase):
    def test_warning_when_subclassing(self):
        with patch('cassandra.cqlengine.models.warn') as patched_logger:
            for method in ModelMetaClass._warned_overridden_methods:
                type("new_class", (ToSubclassModel, ), {method: lambda self : ToSubclassModel.save(self)})
                patched_logger.assert_called_once()
                warning_message = patched_logger.call_args[0][0]
                self.assertIn('Consider overriding the async equivalent instead.', warning_message)
                self.assertIn(method, warning_message)
                patched_logger.reset_mock()
