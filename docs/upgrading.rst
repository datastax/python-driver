Upgrading
=========

Upgrading to 2.0 from 1.x
-------------------------
Version 2.0 of the DataStax python driver for Apache Cassandra
includes some notable improvements over version 1.x.  This version
of the driver supports Cassandra 1.2, 2.0, and 2.1.  However, not
all features may be used with Cassandra 1.2, and some new features
in 2.1 are not yet supported.

Using the v2 Native Protocol
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default, the driver will attempt to use version 2 of Cassandra's
native protocol.  When working with Cassandra 1.2, you will need to
explicitly set the :attr:`~.Cluster.protocol_version` to 1:

.. code-block:: python

    from cassandra.cluster import Cluster

    cluster = Cluster(protocol_version=1)

Automatic Query Paging
^^^^^^^^^^^^^^^^^^^^^^
Version 2 of the native protocol adds support for automatic query
paging, which can make dealing with large result sets much simpler.

See :ref:`query-paging` for full details.

Protocol-Level Batch Statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
With version 1 of the native protocol, batching of statements required
using a `BATCH cql query <http://cassandra.apache.org/doc/cql3/CQL.html#batchStmt>`_.
With version 2 of the native protocol, you can now batch statements at
the protocol level. This allows you to use many different prepared
statements within a single batch.

See :class:`~.query.BatchStatement` for details and usage examples.

SASL-based Authentication
^^^^^^^^^^^^^^^^^^^^^^^^^
Also new in version 2 of the native protocol is SASL-based authentication.
See the section on :ref:`security` for details and examples.

Lightweight Transactions
^^^^^^^^^^^^^^^^^^^^^^^^
`Lightweight transactions <http://www.datastax.com/dev/blog/lightweight-transactions-in-cassandra-2-0>`_ are another new feature.  To use lightweight transactions, add ``IF`` clauses
to your CQL queries and set the :attr:`~.Statement.serial_consistency_level`
on your statements.

Deprecations
^^^^^^^^^^^^
The following functions have moved from ``cassandra.decoder`` to ``cassandra.query``.
The original functions have been left in place with a :exc:`DeprecationWarning` for
now:

* :attr:`cassandra.decoder.tuple_factory` has moved to
  :attr:`cassandra.query.tuple_factory`
* :attr:`cassandra.decoder.named_tuple_factory` has moved to
  :attr:`cassandra.query.named_tuple_factory
* :attr:`cassandra.decoder.dict_factory` has moved to
  :attr:`cassandra.query.dict_factory`
* :attr:`cassandra.decoder.ordered_dict_factory` has moved to
  :attr:`cassandra.query.ordered_dict_factory`

Dependency Changes
^^^^^^^^^^^^^^^^^^
The following dependencies have officially been made optional:

* ``scales``
* ``blist``

And one new dependency has been added (to enable Python 3 support):

* ``six``
