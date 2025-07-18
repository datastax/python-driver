# -*- coding: utf-8 -*-
# # Copyright DataStax, Inc.
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

from cassandra.auth import PlainTextAuthenticator

import unittest


class TestPlainTextAuthenticator(unittest.TestCase):

    def test_evaluate_challenge_with_unicode_data(self):
        authenticator = PlainTextAuthenticator("johnӁ", "doeӁ")
        assert authenticator.evaluate_challenge(b'PLAIN-START') == "\x00johnӁ\x00doeӁ".encode('utf-8')
