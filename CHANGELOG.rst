2.0.0b1
=======
In progress

Upgrading from 1.x
------------------
Cluster.shutdown() should always be called when you are done with a
Cluster instance.  If it is not called, there are no guarantees that the
driver will not hang.  However, if you *do* have a reproduceable case
where Cluster.shutdown() is not called and the driver hangs, please
report it so that we can attempt to fix it.

Features
--------
* Support v2 of Cassandra's native protocol, which includes the following
  new features:
  * Automatic query paging support
  * Protocol-level batch statements
  * Lightweight transactions
* Support for Python 3.3 and 3.4
* Allow a default query timeout to be set per-Session

Bug Fixes
---------
* Avoid errors during interpreter shutdown (the driver attempts to cleanup
  daemonized worker threads before interpreter shutdown)

Deprecations
------------
The following functions have moved from cassandra.decoder to cassandra.query.
The original functions have been left in place with a DeprecationWarning for
now:
* cassandra.decoder.tuple_factory has moved to cassandra.query.tuple_factory
* cassandra.decoder.named_tuple_factory has moved to cassandra.query.named_tuple_factory
* cassandra.decoder.dict_factory has moved to cassandra.query.dict_factory
* cassandra.decoder.ordered_dict_factory has moved to cassandra.query.ordered_dict_factory

1.1.2
=====
In Progress

Bug Fixes
---------
* Update token metadata (for TokenAware calculations) when a node is removed
  from the ring
* Fix file handle leak with gevent reactor due to blocking Greenlet kills when
  closing excess connections
* Avoid handling a node coming up multiple times due to a reconnection attempt
  succeeding close to the same time that an UP notification is pushed
* Fix duplicate node-up handling, which could result in multiple reconnectors
  being started as well as the executor threads becoming deadlocked, preventing
  future node up or node down handling from being executed.
* Handle exhausted ReconnectionPolicy schedule correctly

Other
-----
* Don't log at ERROR when a connection is closed during the startup
  communications

1.1.1
=====
April 16, 2014

Bug Fixes
---------
* Fix unconditional import of nose in setup.py (github #111)

1.1.0
=====
April 16, 2014

Features
--------
* Gevent is now supported through monkey-patching the stdlib (PYTHON-7,
  github issue #46)
* Support static columns in schemas, which are available starting in
  Cassandra 2.1. (github issue #91)
* Add debian packaging (github issue #101)
* Add utility methods for easy concurrent execution of statements. See
  the new cassandra.concurrent module. (github issue #7)

Bug Fixes
---------
* Correctly supply compaction and compression parameters in CREATE statements
  for tables when working with Cassandra 2.0+
* Lowercase boolean literals when generating schemas
* Ignore SSL_ERROR_WANT_READ and SSL_ERROR_WANT_WRITE socket errors.  Previously,
  these resulted in the connection being defuncted, but they can safely be
  ignored by the driver.
* Don't reconnect the control connection every time Cluster.connect() is
  called
* Avoid race condition that could leave ResponseFuture callbacks uncalled
  if the callback was added outside of the event loop thread (github issue #95)
* Properly escape keyspace name in Session.set_keyspace().  Previously, the
  keyspace name was quoted, but any quotes in the string were not escaped.
* Avoid adding hosts to the load balancing policy before their datacenter
  and rack information has been set, if possible.
* Avoid KeyError when updating metadata after droping a table (github issues
  #97, #98)
* Use tuples instead of sets for DCAwareLoadBalancingPolicy to ensure equal
  distribution of requests

Other
-----
* Don't ignore column names when parsing typestrings.  This is needed for
  user-defined type support.  (github issue #90)
* Better error message when libevwrapper is not found
* Only try to import scales when metrics are enabled (github issue #92)
* Cut down on the number of queries executing when a new Cluster
  connects and when the control connection has to reconnect (github issue #104,
  PYTHON-59)
* Issue warning log when schema versions do not match

1.0.2
=====
March 4, 2014

Bug Fixes
---------
* With asyncorereactor, correctly handle EAGAIN/EWOULDBLOCK when the message from
  Cassandra is a multiple of the read buffer size.  Previously, if no more data
  became available to read on the socket, the message would never be processed,
  resulting in an OperationTimedOut error.
* Double quote keyspace, table and column names that require them (those using
  uppercase characters or keywords) when generating CREATE statements through
  KeyspaceMetadata and TableMetadata.
* Decode TimestampType as DateType.  (Cassandra replaced DateType with
  TimestampType to fix sorting of pre-unix epoch dates in CASSANDRA-5723.)
* Handle latest table options when parsing the schema and generating
  CREATE statements.
* Avoid 'Set changed size during iteration' during query plan generation
  when hosts go up or down

Other
-----
* Remove ignored ``tracing_enabled`` parameter for ``SimpleStatement``.  The
  correct way to trace a query is by setting the ``trace`` argument to ``True``
  in ``Session.execute()`` and ``Session.execute_async()``.
* Raise TypeError instead of cassandra.query.InvalidParameterTypeError when
  a parameter for a prepared statement has the wrong type; remove
  cassandra.query.InvalidParameterTypeError.
* More consistent type checking for query parameters
* Add option to a return special object for empty string values for non-string
  columns

1.0.1
=====
Feb 19, 2014

Bug Fixes
---------
* Include table indexes in ``KeyspaceMetadata.export_as_string()``
* Fix broken token awareness on ByteOrderedPartitioner
* Always close socket when defuncting error'ed connections to avoid a potential
  file descriptor leak
* Handle "custom" types (such as the replaced DateType) correctly
* With libevreactor, correctly handle EAGAIN/EWOULDBLOCK when the message from
  Cassandra is a multiple of the read buffer size.  Previously, if no more data
  became available to read on the socket, the message would never be processed,
  resulting in an OperationTimedOut error.
* Don't break tracing when a Session's row_factory is not the default
  namedtuple_factory.
* Handle data that is already utf8-encoded for UTF8Type values
* Fix token-aware routing for tokens that fall before the first node token in
  the ring and tokens that exactly match a node's token
* Tolerate null source_elapsed values for Trace events.  These may not be
  set when events complete after the main operation has already completed.

Other
-----
* Skip sending OPTIONS message on connection creation if compression is
  disabled or not available and a CQL version has not been explicitly
  set
* Add details about errors and the last queried host to ``OperationTimedOut``

1.0.0 Final
===========
Jan 29, 2014

Bug Fixes
---------
* Prevent leak of Scheduler thread (even with proper shutdown)
* Correctly handle ignored hosts, which are common with the
  DCAwareRoundRobinPolicy
* Hold strong reference to prepared statement while executing it to avoid
  garbage collection
* Add NullHandler logging handler to the cassandra package to avoid
  warnings about there being no configured logger
* Fix bad handling of nodes that have been removed from the cluster
* Properly escape string types within cql collections
* Handle setting the same keyspace twice in a row
* Avoid race condition during schema agreement checks that could result
  in schema update queries returning before all nodes had seen the change
* Preserve millisecond-level precision in datetimes when performing inserts
  with simple (non-prepared) statements
* Properly defunct connections when libev reports an error by setting
  errno instead of simply logging the error
* Fix endless hanging of some requests when using the libev reactor
* Always start a reconnection process when we fail to connect to
  a newly bootstrapped node
* Generators map to CQL lists, not key sequences
* Always defunct connections when an internal operation fails
* Correctly break from handle_write() if nothing was sent (asyncore
  reactor only)
* Avoid potential double-erroring of callbacks when a connection
  becomes defunct

Features
--------
* Add default query timeout to ``Session``
* Add timeout parameter to ``Session.execute()``
* Add ``WhiteListRoundRobinPolicy`` as a load balancing policy option
* Support for consistency level ``LOCAL_ONE``
* Make the backoff for fetching traces exponentially increasing and
  configurable

Other
-----
* Raise Exception if ``TokenAwarePolicy`` is used against a cluster using the
  ``Murmur3Partitioner`` if the murmur3 C extension has not been compiled
* Add encoder mapping for ``OrderedDict``
* Use timeouts on all control connection queries
* Benchmark improvements, including command line options and eay
  multithreading support
* Reduced lock contention when using the asyncore reactor
* Warn when non-datetimes are used for 'timestamp' column values in
  prepared statements
* Add requirements.txt and test-requirements.txt
* TravisCI integration for running unit tests against Python 2.6,
  Python 2.7, and PyPy

1.0.0b7
=======
Nov 12, 2013

This release makes many stability improvements, especially around
prepared statements and node failure handling.  In particular,
several cases where a request would never be completed (and as a
result, leave the application hanging) have been resolved.

Features
--------
* Add `timeout` kwarg to ``ResponseFuture.result()``
* Create connection pools to all hosts in parallel when initializing
  new Sesssions.

Bug Fixes
---------
* Properly set exception on ResponseFuture when a query fails
  against all hosts
* Improved cleanup and reconnection efforts when reconnection fails
  on a node that has recently come up
* Use correct consistency level when retrying failed operations
  against a different host. (An invalid consistency level was being
  used, causing the retry to fail.)
* Better error messages for failed ``Session.prepare()`` opertaions
* Prepare new statements against all hosts in parallel (formerly
  sequential)
* Fix failure to save the new current keyspace on connections. (This
  could cause problems for prepared statements and lead to extra
  operations to continuously re-set the keyspace.)
* Avoid sharing ``LoadBalancingPolicies`` across ``Cluster`` instances. (When
  a second ``Cluster`` was connected, it effectively mark nodes down for the
  first ``Cluster``.)
* Better handling of failures during the re-preparation sequence for
  unrecognized prepared statements
* Throttle trashing of underutilized connections to avoid trashing newly
  created connections
* Fix race condition which could result in trashed connections being closed
  before the last operations had completed
* Avoid preparing statements on the event loop thread (which could lead to
  deadlock)
* Correctly mark up non-contact point nodes discovered by the control
  connection. (This lead to prepared statements not being prepared
  against those hosts, generating extra traffic later when the
  statements were executed and unrecognized.)
* Correctly handle large messages through libev
* Add timeout to schema agreement check queries
* More complete (and less contended) locking around manipulation of the
  pending message deque for libev connections

Other
-----
* Prepare statements in batches of 10. (When many prepared statements
  are in use, this allows the driver to start utilizing nodes that
  were restarted more quickly.)
* Better debug logging around connection management
* Don't retain unreferenced prepared statements in the local cache.
  (If many different prepared statements were created, this would
  increase memory usage and greatly increase the amount of time
  required to begin utilizing a node that was added or marked
  up.)

1.0.0b6
=======
Oct 22, 2013

Bug Fixes
---------
* Use lazy string formatting when logging
* Avoid several deadlock scenarios, especially when nodes go down
* Avoid trashing newly created connections due to insufficient traffic
* Gracefully handle un-handled Exceptions when erroring callbacks

Other
-----
* Node state listeners (which are called when a node is added, removed,
  goes down, or comes up) should now be registered through
  Cluster.register_listener() instead of through a host's HealthMonitor
  (which has been removed)


1.0.0b5
========
Oct 10, 2013

Features
--------
* SSL support

Bug Fixes
---------
* Avoid KeyError when building replica map for NetworkTopologyStrategy
* Work around python bug which causes deadlock when a thread imports
  the utf8 module
* Handle no blist library, which is not compatible with pypy
* Avoid deadlock triggered by a keyspace being set on a connection (which
  may happen automatically for new connections)

Other
-----
* Switch packaging from Distribute to setuptools, improved C extension
  support
* Use PEP 386 compliant beta and post-release versions

1.0.0-beta4
===========
Sep 24, 2013

Features
--------
* Handle new blob syntax in Cassandra 2.0 by accepting bytearray
  objects for blob values
* Add cql_version kwarg to Cluster.__init__

Bug Fixes
---------
* Fix KeyError when building token map with NetworkTopologyStrategy
  keyspaces (this prevented a Cluster from successfully connecting
  at all).
* Don't lose default consitency level from parent PreparedStatement
  when creating BoundStatements

1.0.0-beta3
===========
Sep 20, 2013

Features
--------
* Support for LZ4 compression (Cassandra 2.0+)
* Token-aware routing will now utilize all replicas for a query instead
  of just the first replica

Bug Fixes
---------
* Fix libev include path for CentOS
* Fix varint packing of the value 0
* Correctly pack unicode values
* Don't attempt to return failed connections to the pool when a final result
  is set
* Fix bad iteration of connection credentials
* Use blist's orderedset for set collections and OrderedDict for map
  collections so that Cassandra's ordering is preserved
* Fix connection failure on Windows due to unavailability of inet_pton
  and inet_ntop.  (Note that IPv6 inet_address values are still not
  supported on Windows.)
* Boolean constants shouldn't be surrounded by single quotes
* Avoid a potential loss of precision on float constants due to string
  formatting
* Actually utilize non-standard ports set on Cluster objects
* Fix export of schema as a set of CQL queries

Other
-----
* Use cStringIO for connection buffer for better performance
* Add __repr__ method for Statement classes
* Raise InvalidTypeParameterError when parameters of the wrong
  type are used with statements
* Make all tests compatible with Python 2.6
* Add 1s timeout for opening new connections

1.0.0-beta2
===========
Aug 19, 2013

Bug Fixes
---------
* Fix pip packaging

1.0.0-beta
==========
Aug 16, 2013

Initial release
