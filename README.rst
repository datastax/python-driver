DataStax Python Driver for Apache Cassandra
===========================================

.. image:: https://travis-ci.org/datastax/python-driver.png?branch=master
   :target: https://travis-ci.org/datastax/python-driver

A Python client driver for Apache Cassandra.  This driver works exclusively
with the Cassandra Query Language v3 (CQL3) and Cassandra's native
protocol.  Cassandra versions 1.2 through 2.1 are supported.

The driver supports Python 2.6, 2.7, 3.3, and 3.4*.

* cqlengine component presently supports Python 2.7+

Installation
------------
Installation through pip is recommended::

    $ pip install cassandra-driver

For more complete installation instructions, see the
`installation guide <http://datastax.github.io/python-driver/installation.html>`_.

Documentation
-------------
The documentation can be found online `here <http://datastax.github.io/python-driver/index.html>`_.

A couple of links for getting up to speed:

* `Installation <http://datastax.github.io/python-driver/installation.html>`_
* `Getting started guide <http://datastax.github.io/python-driver/getting_started.html>`_
* `API docs <http://datastax.github.io/python-driver/api/index.html>`_
* `Performance tips <http://datastax.github.io/python-driver/performance.html>`_

Object Mapper
-------------
cqlengine (originally developed by Blake Eggleston and Jon Haddad, with contributions from the
community) is now maintained as an integral part of this package. Refer to
`documentation here <http://datastax.github.io/python-driver/object_mapper.html>`_.

Reporting Problems
------------------
Please report any bugs and make any feature requests on the
`JIRA <https://datastax-oss.atlassian.net/browse/PYTHON>`_ issue tracker.

If you would like to contribute, please feel free to open a pull request.

Getting Help
------------
Your two best options for getting help with the driver are the
`mailing list <https://groups.google.com/a/lists.datastax.com/forum/#!forum/python-driver-user>`_
and the IRC channel.

For IRC, use the #datastax-drivers channel on irc.freenode.net.  If you don't have an IRC client,
you can use `freenode's web-based client <http://webchat.freenode.net/?channels=#datastax-drivers>`_.

Features to be Added
--------------------
* C extension for encoding/decoding messages

License
-------
Copyright 2013-2015 DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
