:orphan:

Cloud
-----
Connecting
==========
To connect to a DataStax Apollo cluster:

1. Download the secure connect bundle from your Apollo account.
2. Connect to your cluster with

.. code-block:: python

    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    cloud_config = {
        'secure_connect_bundle': '/path/to/secure-connect-dbname.zip'
    }
    auth_provider = PlainTextAuthProvider(username='user', password='pass')
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

Apollo Differences
==================
In most circumstances, the client code for interacting with an Apollo cluster will be the same as interacting with any other Cassandra cluster. The exceptions being:

* A cloud configuration must be passed to a :class:`~.Cluster` instance via the `cloud` attribute (as demonstrated above).
* An SSL connection will be established automatically. Manual SSL configuration is not allowed, and using `ssl_context` or `ssl_options` will result in an exception.
* A :class:`~.Cluster`'s `contact_points` attribute should not be used. The cloud config contains all of the necessary contact information.
* If a consistency level is not specified for an execution profile or query, then :attr:`.ConsistencyLevel.LOCAL_QUORUM` will be used as the default.


Limitations
===========

Event loops
^^^^^^^^^^^
Evenlet isn't yet supported for python 3.7+ due to an `issue in Eventlet <https://github.com/eventlet/eventlet/issues/526>`_.
