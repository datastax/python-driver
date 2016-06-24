Execution Profiles (experimental)
=================================

Execution profiles are an experimental API aimed at making it easier to execute requests in different ways within
a single connected ``Session``. Execution profiles are being introduced to deal with the exploding number of
configuration options, especially as the database platform evolves more complex workloads.

The Execution Profile API is being introduced now, in an experimental capacity, in order to take advantage of it in
existing projects, and to gauge interest and feedback in the community. For now, the legacy configuration remains
intact, but legacy and Execution Profile APIs cannot be used simultaneously on the same client ``Cluster``.

This document explains how Execution Profiles relate to existing settings, and shows how to use the new profiles for
request execution.

Mapping Legacy Parameters to Profiles
-------------------------------------

Execution profiles can inherit from :class:`.cluster.ExecutionProfile`, and currently provide the following options,
previously input from the noted attributes:

- load_balancing_policy - :attr:`.Cluster.load_balancing_policy`
- request_timeout - :attr:`.Session.default_timeout`, optional :meth:`.Session.execute` parameter
- retry_policy - :attr:`.Cluster.default_retry_policy`, optional :attr:`.Statement.retry_policy` attribute
- consistency_level - :attr:`.Session.default_consistency_level`, optional :attr:`.Statement.consistency_level` attribute
- serial_consistency_level - :attr:`.Session.default_serial_consistency_level`, optional :attr:`.Statement.serial_consistency_level` attribute
- row_factory - :attr:`.Session.row_factory` attribute

When using the new API, these parameters can be defined by instances of :class:`.cluster.ExecutionProfile`.

Using Execution Profiles
------------------------
Default
~~~~~~~

.. code:: python

    from cassandra.cluster import Cluster
    cluster = Cluster()
    session = cluster.connect()
    local_query = 'SELECT rpc_address FROM system.local'
    for _ in cluster.metadata.all_hosts():
        print session.execute(local_query)[0]


.. parsed-literal::

    Row(rpc_address='127.0.0.2')
    Row(rpc_address='127.0.0.1')


The default execution profile is built from Cluster parameters and default Session attributes. This profile matches existing default
parameters.

Initializing cluster with profiles
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    from cassandra.cluster import ExecutionProfile
    from cassandra.policies import WhiteListRoundRobinPolicy

    node1_profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']))
    node2_profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.2']))

    profiles = {'node1': node1_profile, 'node2': node2_profile}
    session = Cluster(execution_profiles=profiles).connect()
    for _ in cluster.metadata.all_hosts():
        print session.execute(local_query, execution_profile='node1')[0]


.. parsed-literal::

    Row(rpc_address='127.0.0.1')
    Row(rpc_address='127.0.0.1')


.. code:: python

    for _ in cluster.metadata.all_hosts():
        print session.execute(local_query, execution_profile='node2')[0]


.. parsed-literal::

    Row(rpc_address='127.0.0.2')
    Row(rpc_address='127.0.0.2')


.. code:: python

    for _ in cluster.metadata.all_hosts():
        print session.execute(local_query)[0]


.. parsed-literal::

    Row(rpc_address='127.0.0.2')
    Row(rpc_address='127.0.0.1')

Note that, even when custom profiles are injected, the default ``TokenAwarePolicy(DCAwareRoundRobinPolicy())`` is still
present. To override the default, specify a policy with the :data:`~.cluster.EXEC_PROFILE_DEFAULT` key.

.. code:: python

    from cassandra.cluster import EXEC_PROFILE_DEFAULT
    profile = ExecutionProfile(request_timeout=30)
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})


Adding named profiles
~~~~~~~~~~~~~~~~~~~~~

New profiles can be added constructing from scratch, or deriving from default:

.. code:: python

    locked_execution = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']))
    node1_profile = 'node1_whitelist'
    cluster.add_execution_profile(node1_profile, locked_execution)
    
    for _ in cluster.metadata.all_hosts():
        print session.execute(local_query, execution_profile=node1_profile)[0]


.. parsed-literal::

    Row(rpc_address='127.0.0.1')
    Row(rpc_address='127.0.0.1')

See :meth:`.Cluster.add_execution_profile` for details and optional parameters.

Passing a profile instance without mapping
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We also have the ability to pass profile instances to be used for execution, but not added to the mapping:

.. code:: python

    from cassandra.query import tuple_factory
    
    tmp = session.execution_profile_clone_update('node1', request_timeout=100, row_factory=tuple_factory)

    print session.execute(local_query, execution_profile=tmp)[0]
    print session.execute(local_query, execution_profile='node1')[0]

.. parsed-literal::

    ('127.0.0.1',)
    Row(rpc_address='127.0.0.1')

The new profile is a shallow copy, so the ``tmp`` profile shares a load balancing policy with one managed by the cluster.
If reference objects are to be updated in the clone, one would typically set those attributes to a new instance.
