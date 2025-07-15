Python Driver for Scylla and Apache Cassandra®
==============================================
A Python client driver for `Scylla <https://docs.scylladb.com>`_.
This driver works exclusively with the Cassandra Query Language v3 (CQL3)
and Cassandra's native protocol.

The driver supports Python 3.9-3.13.

This driver is open source under the
`Apache v2 License <http://www.apache.org/licenses/LICENSE-2.0.html>`_.
The source code for this driver can be found on `GitHub <http://github.com/scylladb/python-driver>`_.

Scylla Driver is a fork from `DataStax Python Driver <http://github.com/datastax/python-driver>`_, including some non-breaking changes for Scylla optimization, with more updates planned.

Contents
--------
:doc:`installation`
    How to install the driver.

:doc:`getting-started`
    A guide through the first steps of connecting to Scylla and executing queries

:doc:`scylla-specific`
    A list of feature available only on ``scylla-driver``

:doc:`execution-profiles`
    An introduction to a more flexible way of configuring request execution

:doc:`lwt`
    Working with results of conditional requests

:doc:`object-mapper`
    Introduction to the integrated object mapper, cqlengine

:doc:`performance`
    Tips for getting good performance.

:doc:`query-paging`
    Notes on paging large query results

:doc:`security`
    An overview of the security features of the driver

:doc:`user-defined-types`
    Working with Scylla's user-defined types (UDT)

:doc:`dates-and-times`
    Some discussion on the driver's approach to working with timestamp, date, time types

:doc:`scylla-cloud`
    Connect to ScyllaDB Cloud

:doc:`faq`
    A collection of Frequently Asked Questions

:doc:`api/index`
    The API documentation.

.. toctree::
   :hidden:

   api/index
   installation
   getting-started
   scylla-specific
   execution-profiles
   performance
   query-paging
   lwt
   security
   user-defined-types
   object-mapper
   dates-and-times
   scylla-cloud
   faq

Getting Help
------------
Visit the :doc:`FAQ section <faq>` in this documentation.

Please send questions to `ScyllaDB Community Forum <https://forum.scylladb.com/>`_
and the Scylla Users `Slack channel <https://scylladb-users.slack.com>`_.

Reporting Issues
----------------

Please report any bugs and make any feature requests on the `Github project issues <https://github.com/scylladb/python-driver/issues>`_


Copyright
---------

© 2013-2017 DataStax

© 2016, The Apache Software Foundation.
Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.
