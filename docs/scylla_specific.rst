Scylla Specific Features
========================

Shard Awareness
---------------

**scylla-driver** is shard aware and contains extensions that work with the TokenAwarePolicy supported by Scylla 2.3 and onwards. Using this policy, the driver can select a connection to a particular shard based on the shard's token.
As a result, latency is significantly reduced because there is no need to pass data between the shards.

Details on the scylla cql protocol extensions
https://github.com/scylladb/scylla/blob/master/docs/protocol-extensions.md

For using it you only need to enable ``TokenAwarePolicy`` on the ``Cluster``

.. code:: python

    from cassandra.cluster import Cluster
    from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy

    cluster = Cluster(load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))


New Cluster Helpers
-------------------

* ``cluster.is_shard_aware()``

  New method available on ``Cluster`` allowing to check whether the remote cluster supports shard awareness (bool)

.. code:: python

    from cassandra.cluster import Cluster

    cluster = Cluster()
    session = cluster.connect()

    if cluster.is_shard_aware():
        print("connected to a scylla cluster")

* ``cluster.shard_aware_stats()``

  New method available on ``Cluster`` allowing to check the status of shard aware connections to all available hosts (dict)

.. code:: python

    from cassandra.cluster import Cluster

    cluster = Cluster()
    session = cluster.connect()

    stats = cluster.shard_aware_stats()
    if all([v["shards_count"] == v["connected"] for v in stats.values()]):
        print("successfully connected to all shards of all scylla nodes")


New Table Attributes
--------------------

* ``in_memory`` flag

  New flag available on ``TableMetadata.options`` to indicate that it is an `In Memory <https://docs.scylladb.com/using-scylla/in-memory/>`_ table

.. note::  in memory tables is a feature existing only in Scylla Enterprise

.. code:: python

    from cassandra.cluster import Cluster

    cluster = Cluster()
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS keyspace1
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS keyspace1.standard1 (
            key blob PRIMARY KEY,
            "C0" blob
        ) WITH in_memory=true AND compaction={'class': 'InMemoryCompactionStrategy'}
    """)

    cluster.refresh_table_metadata("keyspace1", "standard1")
    assert cluster.metadata.keyspaces["keyspace1"].tables["standard1"].options["in_memory"] == True
