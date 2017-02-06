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


import os
import select
import socket
import thread
import Queue
import threading
import __builtin__
import ssl
import time


def eventlet_un_patch_all():
    """
    A method to unpatch eventlet monkey patching used for the reactor tests
    """

    # These are the modules that are loaded by eventlet we reload them all
    modules_to_unpatch = [os, select, socket, thread, time, Queue, threading, ssl, __builtin__]
    for to_unpatch in modules_to_unpatch:
        reload(to_unpatch)


