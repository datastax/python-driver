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
import logging
import sys
import socket
import platform
import os

log = logging.getLogger()
log.setLevel('DEBUG')
# if nose didn't already attach a log handler, add one here
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(module)s:%(lineno)s]: %(message)s'))
    log.addHandler(handler)


def is_eventlet_monkey_patched():
    if 'eventlet.patcher' not in sys.modules:
        return False
    import eventlet.patcher
    return eventlet.patcher.is_monkey_patched('socket')


def is_gevent_monkey_patched():
    if 'gevent.monkey' not in sys.modules:
        return False
    import gevent.socket
    return socket.socket is gevent.socket.socket


def is_gevent_time_monkey_patched():
    import gevent.monkey
    return "time" in gevent.monkey.saved


def is_eventlet_time_monkey_patched():
    import eventlet
    return eventlet.patcher.is_monkey_patched('time')


def is_monkey_patched():
    return is_gevent_monkey_patched() or is_eventlet_monkey_patched()


EVENT_LOOP_MANAGER = os.getenv('EVENT_LOOP_MANAGER', "libev")
if "gevent" in EVENT_LOOP_MANAGER:
    import gevent.monkey
    gevent.monkey.patch_all()
    from cassandra.io.geventreactor import GeventConnection
    connection_class = GeventConnection
elif "eventlet" in EVENT_LOOP_MANAGER:
    from eventlet import monkey_patch
    monkey_patch()

    from cassandra.io.eventletreactor import EventletConnection
    connection_class = EventletConnection
elif "async" in EVENT_LOOP_MANAGER:
    from cassandra.io.asyncorereactor import AsyncoreConnection
    connection_class = AsyncoreConnection
elif "twisted" in EVENT_LOOP_MANAGER:
    from cassandra.io.twistedreactor import TwistedConnection
    connection_class = TwistedConnection

else:
    try:
        from cassandra.io.libevreactor import LibevConnection
        connection_class = LibevConnection
    except ImportError:
        connection_class = None


MONKEY_PATCH_LOOP = bool(os.getenv('MONKEY_PATCH_LOOP', False))

notwindows = unittest.skipUnless(not "Windows" in platform.system(), "This test is not adequate for windows")
notpypy = unittest.skipUnless(not platform.python_implementation() == 'PyPy', "This tests is not suitable for pypy")
notmonkeypatch = unittest.skipUnless(MONKEY_PATCH_LOOP, "Skipping this test because monkey patching is required")