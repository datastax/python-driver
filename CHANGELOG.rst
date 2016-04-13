3.2.1
=====
April 13, 2016

* Introduced an update to allow deserializer compilation with recently released Cython 0.24

3.2.0
=====
April 12, 2016

Features
--------
* cqlengine: Warn on sync_schema type mismatch (PYTHON-260)
* cqlengine: Automatically defer fields with the '=' operator (and immutable values) in select queries (PYTHON-520)
* cqlengine: support non-equal conditions for LWT (PYTHON-528)
* cqlengine: sync_table should validate the primary key composition (PYTHON-532)
* cqlengine: token-aware routing for mapper statements (PYTHON-535)

Bug Fixes
---------
* Deleting a column in a lightweight transaction raises a SyntaxException #325 (PYTHON-249)
* cqlengine: make Token function works with named tables/columns #86 (PYTHON-272)
* comparing models with datetime fields fail #79 (PYTHON-273)
* cython date deserializer integer math should be aligned with CPython (PYTHON-480)
* db_field is not always respected with UpdateStatement (PYTHON-530)
* Sync_table fails on column.Set with secondary index (PYTHON-533)

3.1.1
=====
March 14, 2016

Bug Fixes
---------
* cqlengine: Fix performance issue related to additional "COUNT" queries (PYTHON-522)

3.1.0
=====
March 10, 2016

Features
--------
* Pass name of server auth class to AuthProvider (PYTHON-454)
* Surface schema agreed flag for DDL statements (PYTHON-458)
* Automatically convert float and int to Decimal on serialization (PYTHON-468)
* Eventlet Reactor IO improvement (PYTHON-495)
* Make pure Python ProtocolHandler available even when Cython is present (PYTHON-501)
* Optional Cython deserializer for bytes as bytearray (PYTHON-503)
* Add Session.default_serial_consistency_level (github #510)
* cqlengine: Expose prior state information via cqlengine LWTException (github #343, PYTHON-336)
* cqlengine: Collection datatype "contains" operators support (Cassandra 2.1) #278 (PYTHON-258)
* cqlengine: Add DISTINCT query operator (PYTHON-266)
* cqlengine: Tuple cqlengine api (PYTHON-306)
* cqlengine: Add support for UPDATE/DELETE ... IF EXISTS statements (PYTHON-432)
* cqlengine: Allow nested container types (PYTHON-478)
* cqlengine: Add ability to set query's fetch_size and limit (PYTHON-323)
* cqlengine: Internalize default keyspace from successive set_session (PYTHON-486)
* cqlengine: Warn when Model.create() on Counters (to be deprecated) (PYTHON-333)

Bug Fixes
---------
* Bus error (alignment issues) when running cython on some ARM platforms (PYTHON-450)
* Overflow when decoding large collections (cython) (PYTHON-459)
* Timer heap comparison issue with Python 3 (github #466)
* Cython deserializer date overflow at 2^31 - 1 (PYTHON-452)
* Decode error encountered when cython deserializing large map results (PYTHON-459)
* Don't require Cython for build if compiler or Python header not present (PYTHON-471)
* Unorderable types in task scheduling with Python 3 (h(PYTHON-473)
* cqlengine: Fix crash when updating a UDT column with a None value (github #467)
* cqlengine: Race condition in ..connection.execute with lazy_connect (PYTHON-310)
* cqlengine: doesn't support case sensitive column family names (PYTHON-337)
* cqlengine: UserDefinedType mandatory in create or update (PYTHON-344)
* cqlengine: db_field breaks UserType (PYTHON-346)
* cqlengine: UDT badly quoted (PYTHON-347)
* cqlengine: Use of db_field on primary key prevents querying except while tracing. (PYTHON-351)
* cqlengine: DateType.deserialize being called with one argument vs two (PYTHON-354)
* cqlengine: Querying without setting up connection now throws AttributeError and not CQLEngineException (PYTHON-395)
* cqlengine: BatchQuery multiple time executing execute statements. (PYTHON-445)
* cqlengine: Better error for management functions when no connection set (PYTHON-451)
* cqlengine: Handle None values for UDT attributes in cqlengine (PYTHON-470)
* cqlengine: Fix inserting None for model save (PYTHON-475)
* cqlengine: EQ doesn't map to a QueryOperator (setup race condition) (PYTHON-476)
* cqlengine: class.MultipleObjectsReturned has DoesNotExist as base class (PYTHON-489)
* cqlengine: Typo in cqlengine UserType __len__ breaks attribute assignment (PYTHON-502)


Other
-----

* cqlengine: a major improvement on queryset has been introduced. It
  is a lot more efficient to iterate large datasets: the rows are
  now fetched on demand using the driver pagination.

* cqlengine: the queryset len() and count() behaviors have changed. It
  now executes a "SELECT COUNT(*)" of the query rather than returning
  the size of the internal result_cache (loaded rows). On large
  queryset, you might want to avoid using them due to the performance
  cost. Note that trying to access objects using list index/slicing
  with negative indices also requires a count to be
  executed.



3.0.0
=====
November 24, 2015

Features
--------
* Support datetime.date objects as a DateType (PYTHON-212)
* Add Cluster.update_view_metadata (PYTHON-407)
* QueryTrace option to populate partial trace sessions (PYTHON-438)
* Attach column names to ResultSet (PYTHON-439)
* Change default consistency level to LOCAL_ONE

Bug Fixes
---------
* Properly SerDes nested collections when protocol_version < 3 (PYTHON-215)
* Evict UDTs from UserType cache on change (PYTHON-226)
* Make sure query strings are always encoded UTF-8 (PYTHON-334)
* Track previous value of columns at instantiation in CQLengine (PYTHON-348)
* UDT CQL encoding does not work for unicode values (PYTHON-353)
* NetworkTopologyStrategy#make_token_replica_map does not account for multiple racks in a DC (PYTHON-378)
* Cython integer overflow on decimal type deserialization (PYTHON-433)
* Query trace: if session hasn't been logged, query trace can throw exception (PYTHON-442)

3.0.0rc1
========
November 9, 2015

Features
--------
* Process Modernized Schema Tables for Cassandra 3.0 (PYTHON-276, PYTHON-408, PYTHON-400, PYTHON-422)
* Remove deprecated features (PYTHON-292)
* Don't assign trace data to Statements (PYTHON-318)
* Normalize results return (PYTHON-368)
* Process Materialized View Metadata/Events (PYTHON-371)
* Remove blist as soft dependency (PYTHON-385)
* Change default consistency level to LOCAL_QUORUM (PYTHON-416)
* Normalize CQL query/export in metadata model (PYTHON-405)

Bug Fixes
---------
* Implementation of named arguments bind is non-pythonic (PYTHON-178)
* CQL encoding is incorrect for NaN and Infinity floats (PYTHON-282)
* Protocol downgrade issue with C* 2.0.x, 2.1.x, and python3, with non-default logging (PYTHON-409)
* ValueError when accessing usertype with non-alphanumeric field names (PYTHON-413)
* NumpyProtocolHandler does not play well with PagedResult (PYTHON-430)

2.7.2
=====
Setpember 14, 2015

Bug Fixes
---------
* Resolve CQL export error for UDF with zero parameters (PYTHON-392)
* Remove futures dep. for Python 3 (PYTHON-393)
* Avoid Python closure in cdef (supports earlier Cython compiler) (PYTHON-396)
* Unit test runtime issues (PYTHON-397,398)

2.7.1
=====
August 25, 2015

Bug Fixes
---------
* Explicitly include extension source files in Manifest

2.7.0
=====
August 25, 2015

Cython is introduced, providing compiled extensions for core modules, and
extensions for optimized results deserialization.

Features
--------
* General Performance Improvements for Throughput (PYTHON-283)
* Improve synchronous request performance with Timers (PYTHON-108)
* Enable C Extensions for PyPy Runtime (PYTHON-357)
* Refactor SerDes functionality for pluggable interface (PYTHON-313)
* Cython SerDes Extension (PYTHON-377)
* Accept iterators/generators for execute_concurrent() (PYTHON-123)
* cythonize existing modules (PYTHON-342)
* Pure Python murmur3 implementation (PYTHON-363)
* Make driver tolerant of inconsistent metadata (PYTHON-370)

Bug Fixes
---------
* Drop Events out-of-order Cause KeyError on Processing (PYTHON-358)
* DowngradingConsistencyRetryPolicy doesn't check response count on write timeouts (PYTHON-338)
* Blocking connect does not use connect_timeout (PYTHON-381)
* Properly protect partition key in CQL export (PYTHON-375)
* Trigger error callbacks on timeout (PYTHON-294)

2.6.0
=====
July 20, 2015

Bug Fixes
---------
* Output proper CQL for compact tables with no clustering columns (PYTHON-360)

2.6.0c2
=======
June 24, 2015

Features
--------
* Automatic Protocol Version Downgrade (PYTHON-240)
* cqlengine Python 2.6 compatibility (PYTHON-288)
* Double-dollar string quote UDF body (PYTHON-345)
* Set models.DEFAULT_KEYSPACE when calling set_session (github #352)

Bug Fixes
---------
* Avoid stall while connecting to mixed version cluster (PYTHON-303)
* Make SSL work with AsyncoreConnection in python 2.6.9 (PYTHON-322)
* Fix Murmur3Token.from_key() on Windows (PYTHON-331)
* Fix cqlengine TimeUUID rounding error for Windows (PYTHON-341)
* Avoid invalid compaction options in CQL export for non-SizeTiered (PYTHON-352)

2.6.0c1
=======
June 4, 2015

This release adds support for Cassandra 2.2 features, including version
4 of the native protocol.

Features
--------
* Default load balancing policy to TokenAware(DCAware) (PYTHON-160)
* Configuration option for connection timeout (PYTHON-206)
* Support User Defined Function and Aggregate metadata in C* 2.2 (PYTHON-211)
* Surface request client in QueryTrace for C* 2.2+ (PYTHON-235)
* Implement new request failure messages in protocol v4+ (PYTHON-238)
* Metadata model now maps index meta by index name (PYTHON-241)
* Support new types in C* 2.2: date, time, smallint, tinyint (PYTHON-245, 295)
* cqle: add Double column type and remove Float overload (PYTHON-246)
* Use partition key column information in prepared response for protocol v4+ (PYTHON-277)
* Support message custom payloads in protocol v4+ (PYTHON-280, PYTHON-329)
* Deprecate refresh_schema and replace with functions for specific entities (PYTHON-291)
* Save trace id even when trace complete times out (PYTHON-302)
* Warn when registering client UDT class for protocol < v3 (PYTHON-305)
* Support client warnings returned with messages in protocol v4+ (PYTHON-315)
* Ability to distinguish between NULL and UNSET values in protocol v4+ (PYTHON-317)
* Expose CQL keywords in API (PYTHON-324)

Bug Fixes
---------
* IPv6 address support on Windows (PYTHON-20)
* Convert exceptions during automatic re-preparation to nice exceptions (PYTHON-207)
* cqle: Quote keywords properly in table management functions (PYTHON-244)
* Don't default to GeventConnection when gevent is loaded, but not monkey-patched (PYTHON-289)
* Pass dynamic host from SaslAuthProvider to SaslAuthenticator (PYTHON-300)
* Make protocol read_inet work for Windows (PYTHON-309)
* cqle: Correct encoding for nested types (PYTHON-311)
* Update list of CQL keywords used quoting identifiers (PYTHON-319)
* Make ConstantReconnectionPolicy work with infinite retries (github #327, PYTHON-325)
* Accept UUIDs with uppercase hex as valid in cqlengine (github #335)

2.5.1
=====
April 23, 2015

Bug Fixes
---------
* Fix thread safety in DC-aware load balancing policy (PYTHON-297)
* Fix race condition in node/token rebuild (PYTHON-298)
* Set and send serial consistency parameter (PYTHON-299)

2.5.0
=====
March 30, 2015

Features
--------
* Integrated cqlengine object mapping package
* Utility functions for converting timeuuids and datetime (PYTHON-99)
* Schema metadata fetch window randomized, config options added (PYTHON-202)
* Support for new Date and Time Cassandra types (PYTHON-190)

Bug Fixes
---------
* Fix index target for collection indexes (full(), keys()) (PYTHON-222)
* Thread exception during GIL cleanup (PYTHON-229)
* Workaround for rounding anomaly in datetime.utcfromtime (Python 3.4) (PYTHON-230)
* Normalize text serialization for lookup in OrderedMap (PYTHON-231)
* Support reading CompositeType data (PYTHON-234)
* Preserve float precision in CQL encoding (PYTHON-243)

2.1.4
=====
January 26, 2015

Features
--------
* SaslAuthenticator for Kerberos support (PYTHON-109)
* Heartbeat for network device keepalive and detecting failures on idle connections (PYTHON-197)
* Support nested, frozen collections for Cassandra 2.1.3+ (PYTHON-186)
* Schema agreement wait bypass config, new call for synchronous schema refresh (PYTHON-205)
* Add eventlet connection support (PYTHON-194)

Bug Fixes
---------
* Schema meta fix for complex thrift tables (PYTHON-191)
* Support for 'unknown' replica placement strategies in schema meta (PYTHON-192)
* Resolve stream ID leak on set_keyspace (PYTHON-195)
* Remove implicit timestamp scaling on serialization of numeric timestamps (PYTHON-204)
* Resolve stream id collision when using SASL auth (PYTHON-210)
* Correct unhexlify usage for user defined type meta in Python3 (PYTHON-208)

2.1.3
=====
December 16, 2014

Features
--------
* INFO-level log confirmation that a connection was opened to a node that was marked up (PYTHON-116)
* Avoid connecting to peer with incomplete metadata (PYTHON-163)
* Add SSL support to gevent reactor (PYTHON-174)
* Use control connection timeout in wait for schema agreement (PYTHON-175)
* Better consistency level representation in unavailable+timeout exceptions (PYTHON-180)
* Update schema metadata processing to accommodate coming schema modernization (PYTHON-185)

Bug Fixes
---------
* Support large negative timestamps on Windows (PYTHON-119)
* Fix schema agreement for clusters with peer rpc_addres 0.0.0.0 (PYTHON-166)
* Retain table metadata following keyspace meta refresh (PYTHON-173)
* Use a timeout when preparing a statement for all nodes (PYTHON-179)
* Make TokenAware routing tolerant of statements with no keyspace (PYTHON-181)
* Update add_collback to store/invoke multiple callbacks (PYTHON-182)
* Correct routing key encoding for composite keys (PYTHON-184)
* Include compression option in schema export string when disabled (PYTHON-187)

2.1.2
=====
October 16, 2014

Features
--------
* Allow DCAwareRoundRobinPolicy to be constructed without a local_dc, defaulting
  instead to the DC of a contact_point (PYTHON-126)
* Set routing key in BatchStatement.add() if none specified in batch (PYTHON-148)
* Improved feedback on ValueError using named_tuple_factory with invalid column names (PYTHON-122)

Bug Fixes
---------
* Make execute_concurrent compatible with Python 2.6 (PYTHON-159)
* Handle Unauthorized message on schema_triggers query (PYTHON-155)
* Pure Python sorted set in support of UDTs nested in collections (PYTON-167)
* Support CUSTOM index metadata and string export (PYTHON-165)

2.1.1
=====
September 11, 2014

Features
--------
* Detect triggers and include them in CQL queries generated to recreate
  the schema (github-189)
* Support IPv6 addresses (PYTHON-144) (note: basic functionality added; Windows
  platform not addressed (PYTHON-20))

Bug Fixes
---------
* Fix NetworkTopologyStrategy.export_for_schema (PYTHON-120)
* Keep timeout for paged results (PYTHON-150)

Other
-----
* Add frozen<> type modifier to UDTs and tuples to handle CASSANDRA-7857

2.1.0
=====
August 7, 2014

Bug Fixes
---------
* Correctly serialize and deserialize null values in tuples and
  user-defined types (PYTHON-110)
* Include additional header and lib dirs, allowing libevwrapper to build
  against Homebrew and Mac Ports installs of libev (PYTHON-112 and 804dea3)

2.1.0c1
=======
July 25, 2014

Bug Fixes
---------
* Properly specify UDTs for columns in CREATE TABLE statements
* Avoid moving retries to a new host when using request ID zero (PYTHON-88)
* Don't ignore fetch_size arguments to Statement constructors (github-151)
* Allow disabling automatic paging on a per-statement basis when it's
  enabled by default for the session (PYTHON-93)
* Raise ValueError when tuple query parameters for prepared statements
  have extra items (PYTHON-98)
* Correctly encode nested tuples and UDTs for non-prepared statements (PYTHON-100)
* Raise TypeError when a string is used for contact_points (github #164)
* Include User Defined Types in KeyspaceMetadata.export_as_string() (PYTHON-96)

Other
-----
* Return list collection columns as python lists instead of tuples
  now that tuples are a specific Cassandra type

2.1.0b1
=======
July 11, 2014

This release adds support for Cassandra 2.1 features, including version
3 of the native protocol.

Features
--------
* When using the v3 protocol, only one connection is opened per-host, and
  throughput is improved due to reduced pooling overhead and lock contention.
* Support for user-defined types (Cassandra 2.1+)
* Support for tuple type in (limited usage Cassandra 2.0.9, full usage
  in Cassandra 2.1)
* Protocol-level client-side timestamps (see Session.use_client_timestamp)
* Overridable type encoding for non-prepared statements (see Session.encoders)
* Configurable serial consistency levels for batch statements
* Use io.BytesIO for reduced CPU consumption (github #143)
* Support Twisted as a reactor. Note that a Twisted-compatible
  API is not exposed (so no Deferreds), this is just a reactor
  implementation. (github #135, PYTHON-8)

Bug Fixes
---------
* Fix references to xrange that do not go through "six" in libevreactor and
  geventreactor (github #138)
* Make BoundStatements inherit fetch_size from their parent
  PreparedStatement (PYTHON-80)
* Clear reactor state in child process after forking to prevent errors with
  multiprocessing when the parent process has connected a Cluster before
  forking (github #141)
* Don't share prepared statement lock across Cluster instances
* Format CompositeType and DynamicCompositeType columns correctly in
  CREATE TABLE statements.
* Fix cassandra.concurrent behavior when dealing with automatic paging
  (PYTHON-81)
* Properly defunct connections after protocol errors
* Avoid UnicodeDecodeError when query string is unicode (PYTHON-76)
* Correctly capture dclocal_read_repair_chance for tables and
  use it when generating CREATE TABLE statements (PYTHON-84)
* Avoid race condition with AsyncoreConnection that may cause messages
  to fail to be written until a new message is pushed
* Make sure cluster.metadata.partitioner and cluster.metadata.token_map
  are populated when all nodes in the cluster are included in the
  contact points (PYTHON-90)
* Make Murmur3 hash match Cassandra's hash for all values (PYTHON-89,
  github #147)
* Don't attempt to reconnect to hosts that should be ignored (according
  to the load balancing policy) when a notification is received that the
  host is down.
* Add CAS WriteType, avoiding KeyError on CAS write timeout (PYTHON-91)

2.0.2
=====
June 10, 2014

Bug Fixes
---------
* Add six to requirements.txt
* Avoid KeyError during schema refresh when a keyspace is dropped
  and TokenAwarePolicy is not in use
* Avoid registering multiple atexit cleanup functions when the
  asyncore event loop is restarted multiple times
* Delay initialization of reactors in order to avoid problems
  with shared state when using multiprocessing (PYTHON-60)
* Add python-six to debian dependencies, move python-blist to recommends
* Fix memory leak when libev connections are created and
  destroyed (github #93)
* Ensure token map is rebuilt when hosts are removed from the cluster

2.0.1
=====
May 28, 2014

Bug Fixes
---------
* Fix check for Cluster.is_shutdown in in @run_in_executor
  decorator

2.0.0
=====
May 28, 2014

Features
--------
* Make libev C extension Python3-compatible (PYTHON-70)
* Support v2 protocol authentication (PYTHON-73, github #125)

Bug Fixes
---------
* Fix murmur3 C extension compilation under Python3.4 (github #124)

Merged From 1.x
---------------

Features
^^^^^^^^
* Add Session.default_consistency_level (PYTHON-14)

Bug Fixes
^^^^^^^^^
* Don't strip trailing underscores from column names when using the
  named_tuple_factory (PYTHON-56)
* Ensure replication factors are ints for NetworkTopologyStrategy
  to avoid TypeErrors (github #120)
* Pass WriteType instance to RetryPolicy.on_write_timeout() instead
  of the string name of the write type. This caused write timeout
  errors to always be rethrown instead of retrying. (github #123)
* Avoid submitting tasks to the ThreadPoolExecutor after shutdown. With
  retries enabled, this could cause Cluster.shutdown() to hang under
  some circumstances.
* Fix unintended rebuild of token replica map when keyspaces are
  discovered (on startup), added, or updated and TokenAwarePolicy is not
  in use.
* Avoid rebuilding token metadata when cluster topology has not
  actually changed
* Avoid preparing queries for hosts that should be ignored (such as
  remote hosts when using the DCAwareRoundRobinPolicy) (PYTHON-75)

Other
^^^^^
* Add 1 second timeout to join() call on event loop thread during
  interpreter shutdown.  This can help to prevent the process from
  hanging during shutdown.

2.0.0b1
=======
May 6, 2014

Upgrading from 1.x
------------------
Cluster.shutdown() should always be called when you are done with a
Cluster instance.  If it is not called, there are no guarantees that the
driver will not hang.  However, if you *do* have a reproduceable case
where Cluster.shutdown() is not called and the driver hangs, please
report it so that we can attempt to fix it.

If you're using the 2.0 driver against Cassandra 1.2, you will need
to set your protocol version to 1.  For example:

    cluster = Cluster(..., protocol_version=1)

Features
--------
* Support v2 of Cassandra's native protocol, which includes the following
  new features: automatic query paging support, protocol-level batch statements,
  and lightweight transactions
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

Exceptions that were in cassandra.decoder have been moved to cassandra.protocol. If
you handle any of these exceptions, you must adjust the code accordingly.

1.1.2
=====
May 8, 2014

Features
--------
* Allow a specific compression type to be requested for communications with
  Cassandra and prefer lz4 if available

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
* Mke scales, blist optional dependencies

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
