Python Cassandra Driver
=======================
A Python client driver for `Apache Cassandra <http://cassandra.apache.org>`_.
This driver works exclusively with the Cassandra Query Language v3 (CQL3)
and Cassandra's native protocol.  Cassandra 2.1+ is supported.

The driver supports Python 2.7, 3.3, 3.4, 3.5, and 3.6.

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

:doc:`object_mapper`
    Introduction to the integrated object mapper, cqlengine

:doc:`api/index`
    The API documentation.

:doc:`upgrading`
    A guide to upgrading versions of the driver

:doc:`execution_profiles`
    An introduction to a more flexible way of configuring request execution

:doc:`performance`
    Tips for getting good performance.

:doc:`query_paging`
    Notes on paging large query results

:doc:`lwt`
    Working with results of conditional requests

:doc:`user_defined_types`
    Working with Cassandra 2.1's user-defined types

:doc:`security`
    An overview of the security features of the driver

:doc:`dates_and_times`
    Some discussion on the driver's approach to working with timestamp, date, time types

:doc:`faq`
    A collection of Frequently Asked Questions

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
   dates_and_times
   faq

Getting Help
------------
Visit the :doc:`FAQ section <faq>` in this documentation.

Please send questions to the `mailing list <https://groups.google.com/a/lists.datastax.com/forum/#!forum/python-driver-user>`_.

Alternatively, you can use the `#datastax-drivers` channel in the DataStax Acadamy Slack to ask questions in real time.

Reporting Issues
----------------
Please report any bugs and make any feature requests on the
`JIRA <https://datastax-oss.atlassian.net/browse/PYTHON>`_ issue tracker.

If you would like to contribute, please feel free to open a pull request.
