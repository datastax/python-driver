DataStax Python Driver for Apache Cassandra (Beta)
==================================================
A Python client driver for Apache Cassandra.  This driver works exclusively
with the Cassandra Query Language v3 (CQL3) and Cassandra's native
protocol.  As such, only Cassandra 1.2+ is supported.

**Warning**

This driver is currently under heavy development, so the API and layout of
packages, modules, classes, and functions are subject to change.  There may
also be serious bugs, so usage in a production environment is *not*
recommended at this time.

* `JIRA <https://datastax-oss.atlassian.net/browse/PYTHON>`_
* `Mailing List <https://groups.google.com/a/lists.datastax.com/forum/#!forum/python-driver-user>`_
* IRC: #datastax-drivers on irc.freenode.net (you can use `freenode's web-based client <http://webchat.freenode.net/?channels=#datastax-drivers>`_)
* `API Documentation <http://datastax.github.io/python-driver/api/index.html>`_

Features to be Added
--------------------
* C extension for encoding/decoding messages
* Tracing support
* Connection pool metrics
* Authentication/security feature support
* Twisted, gevent support
* Python 3 support
* IPv6 Support

Installation
------------
A package hasn't been put on pypi yet, so for now, run:

    .. code-block:: bash

      $ sudo pip install futures  # install dependency
      $ sudo python setup.py install

libev support
^^^^^^^^^^^^^
The driver currently uses Python's ``asyncore`` module for its default
event loop.  For better performance, ``libev`` is also supported through
the ``pyev`` python wrapper.

If you're on Linux, you should be able to install libev
through a package manager.  For example, on Debian/Ubuntu:

    .. code-block:: bash

      $ sudo apt-get install libev

and then install ``pyev`` as follows:

    .. code-block:: bash

      $ sudo pip install pyev

If successful, you should be able to use the libev event loop by
doing the following

    .. code-block:: python

      >>> from cassandra.io.libevreactor import LibevConnection
      >>> from cassandra.cluster import Cluster

      >>> cluster = Cluster()
      >>> cluster.connection_class = LibevConnection
      >>> session = cluster.connect()

License
-------
Copyright 2013, DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
