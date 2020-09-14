DataStax Python Driver for Apache Cassandra®
============================================
A Python client driver for `Apache Cassandra® <http://cassandra.apache.org>`_.
This driver works exclusively with the Cassandra Query Language v3 (CQL3)
and Cassandra's native protocol.  Cassandra 2.1+ is supported, including DSE 4.7+.

The driver supports Python 2.7, 3.5, 3.6, 3.7 and 3.8.

This driver is open source under the
`Apache v2 License <http://www.apache.org/licenses/LICENSE-2.0.html>`_.
The source code for this driver can be found on `GitHub <http://github.com/datastax/python-driver>`_.

**Note:** DataStax products do not support big-endian systems.

Contents
--------
:doc:`installation`
    How to install the driver.

:doc:`getting_started`
    A guide through the first steps of connecting to Cassandra and executing queries

:doc:`execution_profiles`
    An introduction to a more flexible way of configuring request execution

:doc:`lwt`
    Working with results of conditional requests

:doc:`object_mapper`
    Introduction to the integrated object mapper, cqlengine

:doc:`performance`
    Tips for getting good performance.

:doc:`query_paging`
    Notes on paging large query results

:doc:`security`
    An overview of the security features of the driver

:doc:`upgrading`
    A guide to upgrading versions of the driver

:doc:`user_defined_types`
    Working with Cassandra 2.1's user-defined types

:doc:`dates_and_times`
    Some discussion on the driver's approach to working with timestamp, date, time types

:doc:`cloud`
    A guide to connecting to Datastax Astra

:doc:`geo_types`
    Working with DSE geometry types

:doc:`graph`
    Graph queries with the Core engine

:doc:`classic_graph`
    Graph queries with the Classic engine

:doc:`graph_fluent`
    DataStax Graph Fluent API

:doc:`CHANGELOG`
    Log of changes to the driver, organized by version.

:doc:`faq`
    A collection of Frequently Asked Questions

:doc:`api/index`
    The API documentation.

.. toctree::
   :hidden:

   api/index
   installation
   getting_started
   upgrading
   execution_profiles
   performance
   query_paging
   lwt
   security
   user_defined_types
   object_mapper
   geo_types
   graph
   classic_graph
   graph_fluent
   dates_and_times
   cloud
   faq

Getting Help
------------
Visit the :doc:`FAQ section <faq>` in this documentation.

Please send questions to the `mailing list <https://groups.google.com/a/lists.datastax.com/forum/#!forum/python-driver-user>`_.

Alternatively, you can use the `DataStax Community <https://community.datastax.com>`_.

Reporting Issues
----------------
Please report any bugs and make any feature requests on the
`JIRA <https://datastax-oss.atlassian.net/browse/PYTHON>`_ issue tracker.

If you would like to contribute, please feel free to open a pull request.
