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
