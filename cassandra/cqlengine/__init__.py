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

import six


# Caching constants.
CACHING_ALL = "ALL"
CACHING_KEYS_ONLY = "KEYS_ONLY"
CACHING_ROWS_ONLY = "ROWS_ONLY"
CACHING_NONE = "NONE"


class CQLEngineException(Exception):
    pass


class ValidationError(CQLEngineException):
    pass


class UnicodeMixin(object):
    if six.PY3:
        __str__ = lambda x: x.__unicode__()
    else:
        __str__ = lambda x: six.text_type(x).encode('utf-8')
