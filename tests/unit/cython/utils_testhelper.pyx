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

import datetime

from cassandra.cython_utils cimport datetime_from_timestamp_in_us


def test_datetime_from_timestamp_in_us(assert_equal):
    assert_equal(datetime_from_timestamp_in_us(1454781157123456), datetime.datetime(2016, 2, 6, 17, 52, 37, 123456))
    assert_equal(datetime_from_timestamp_in_us(2177403010123456), datetime.datetime(2038, 12, 31, 10, 10, 10, 123456))
