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

from functools import wraps
import warnings

import cassandra.query

import logging
log = logging.getLogger(__name__)

_have_warned = False


def warn_once(f):

    @wraps(f)
    def new_f(*args, **kwargs):
        global _have_warned
        if not _have_warned:
            msg = "cassandra.decoder.%s has moved to cassandra.query.%s" % (f.__name__, f.__name__)
            warnings.warn(msg, DeprecationWarning)
            log.warning(msg)
            _have_warned = True
        return f(*args, **kwargs)

    return new_f

tuple_factory = warn_once(cassandra.query.tuple_factory)
"""
Deprecated: use :meth:`cassandra.query.tuple_factory()`
"""

named_tuple_factory = warn_once(cassandra.query.named_tuple_factory)
"""
Deprecated: use :meth:`cassandra.query.named_tuple_factory()`
"""

dict_factory = warn_once(cassandra.query.dict_factory)
"""
Deprecated: use :meth:`cassandra.query.dict_factory()`
"""

ordered_dict_factory = warn_once(cassandra.query.ordered_dict_factory)
"""
Deprecated: use :meth:`cassandra.query.ordered_dict_factory()`
"""
