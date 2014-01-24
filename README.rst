DataStax Python Driver for Apache Cassandra (Beta)
==================================================

.. image:: https://travis-ci.org/datastax/python-driver.png?branch=master
   :target: https://travis-ci.org/datastax/python-driver

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
* `Documentation <http://datastax.github.io/python-driver/index.html>`_

Features to be Added
--------------------
* C extension for encoding/decoding messages
* Twisted, gevent support
* Python 3 support
* IPv6 Support

Installation
------------
If you would like to use the optional C extensions, please follow
the instructions in the section below before installing the driver.

Installation through pip is recommended::

    $ pip install cassandra-driver --pre

If you want to install manually, you can instead do::

    $ pip install futures scales blist # install dependencies
    $ python setup.py install

C Extensions
^^^^^^^^^^^^
By default, two C extensions are compiled: one that adds support
for token-aware routing with the Murmur3Partitioner, and one that
allows you to use libev for the event loop, which improves performance.

When running setup.py, you can disable both with the ``--no-extensions``
option, or selectively disable one or the other with ``--no-murmur3`` and
``--no-libev``.

To compile the extenions, ensure that GCC and the Python headers are available.

On Ubuntu and Debian, this can be accomplished by running::

    $ sudo apt-get install build-essential python-dev

On RedHat and RedHat-based systems like CentOS and Fedora::

    $ sudo yum install gcc python-devel

On OS X, homebrew installations of Python should provide the necessary headers.

libev support
^^^^^^^^^^^^^
The driver currently uses Python's ``asyncore`` module for its default
event loop.  For better performance, ``libev`` is also supported through
a C extension.

If you're on Linux, you should be able to install libev
through a package manager.  For example, on Debian/Ubuntu::

    $ sudo apt-get install libev4 libev-dev

On RHEL/CentOS/Fedora::

    $ sudo yum install libev libev-devel

If you're on Mac OS X, you should be able to install libev
through `Homebrew <http://brew.sh/>`_. For example, on Mac OS X::

    $ brew install libev

If successful, you should be able to build and install the extension
(just using ``setup.py build`` or ``setup.py install``) and then use
the libev event loop by doing the following:

.. code-block:: python

    >>> from cassandra.io.libevreactor import LibevConnection
    >>> from cassandra.cluster import Cluster

    >>> cluster = Cluster()
    >>> cluster.connection_class = LibevConnection
    >>> session = cluster.connect()

Compression Support
^^^^^^^^^^^^^^^^^^^
Compression can optionally be used for communication between the driver and
Cassandra.  There are currently two supported compression algorithms:
snappy (in Cassandra 1.2+) and LZ4 (only in Cassandra 2.0+).  If either is
available for the driver and Cassandra also supports it, it will
be used automatically.

For lz4 support::

    $ pip install lz4

For snappy support::

    $ pip install python-snappy

(If using a Debian Linux derivative such as Ubuntu, it may be easier to
just run ``apt-get install python-snappy``.)

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
