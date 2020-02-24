Scylla Python Driver
====================

A modern, feature-rich and highly-tunable Python client library for Scylla Open Source (2.1+) and Apache Cassandra (2.1+) and
Scylla Enterprise (2018.1.x+) using exclusively Cassandra's binary protocol and Cassandra Query Language v3.

The driver supports Python versions 2.7, 3.4, 3.5, 3.6, 3.7 and 3.8.

.. **Note:** DataStax products do not support big-endian systems.

Features
--------
* Synchronous and Asynchronous APIs
* Simple, Prepared, and Batch statements 
* Asynchronous IO, parallel execution, request pipelining
* Connection pooling 
* Automatic node discovery
* Automatic reconnection 
* Configurable load balancing
* Concurrent execution utilities
* Object mapper
* Connecting to DataStax Apollo database (cloud)
* DSE Graph execution API
* DSE Geometric type serialization
* DSE PlainText and GSSAPI authentication

Installation
------------
Installation through pip is recommended::

    $ pip install cassandra-driver

For more complete installation instructions, see the installation guide.

Documentation
-------------
The documentation can be found within this repository.

Information includes: 

* Installation
* Getting started guide
* API docs 
* Performance tips 

Training
--------
The course `Using Scylla Drivers <https://university.scylladb.com/courses/using-scylla-drivers/lessons/coding-with-python/>`_ in `Scylla University <https://university.scylladb.com>`_  explains how to use drivers in different languages to interact with a Scylla cluster. 
The lesson, Coding with Python (link), goes over a sample application that, using the Python driver, interacts with a three-node Scylla cluster.
It connects to a Scylla cluster, displays the contents of a  table, inserts and deletes data, and shows the contents of the table after each action.
`Scylla University <https://university.scylladb.com>`_ includes other training material and online courses which will help you become a Scylla NoSQL database expert.


Object Mapper
-------------
cqlengine (originally developed by Blake Eggleston and Jon Haddad, with contributions from the
community) is now maintained as an integral part of this package. 

Contributing
------------
See CONTRIBUTING.md <https://github.com/scylladb/python-driver/blob/master/CONTRIBUTING.rst>`_.

Reporting Problems
------------------
Please report any bugs and make any feature requests by clicking the New Issue button in 
`Github <https://github.com/scylladb/python-driver/issues>`_.

If you would like to contribute, please feel free to send a pull request.

Getting Help
------------
Your best options for getting help with the driver are the
`mailing list <https://groups.google.com/forum/#!forum/scylladb-users>`_
and the Scylla Users `Slack channel <https://scylladb-users.slack.com>`_.

License
-------
Copyright DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
