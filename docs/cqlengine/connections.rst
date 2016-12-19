==========================
Connections (experimental)
==========================

Connections are experimental and aimed to ease the use of multiple sessions with cqlengine. Connections can be set on a model class, per query or using a context manager.


Register a new connection
=========================

To use cqlengine, you need at least a default connection. If you initialize cqlengine's connections with with :func:`connection.setup <.connection.setup>`, a connection will be created automatically. If you want to use another cluster/session, you need to register a new cqlengine connection. You register a connection with :func:`~.connection.register_connection`:

    .. code-block:: python

        from cassandra.cqlengine import connection

        connection.setup(['127.0.0.1')
        connection.register_connection('cluster2', ['127.0.0.2'])

:func:`~.connection.register_connection` can take a list of hosts, as shown above, in which case it will create a connection with a new session. It can also take a `session` argument if you've already created a session:

    .. code-block:: python

        from cassandra.cqlengine import connection
        from cassandra.cluster import Cluster

        session = Cluster(['127.0.0.1']).connect()
        connection.register_connection('cluster3', session=session)


Change the default connection
=============================

You can change the default cqlengine connection on registration:

    .. code-block:: python

        from cassandra.cqlengine import connection

        connection.register_connection('cluster2', ['127.0.0.2'] default=True)

or on the fly using :func:`~.connection.set_default_connection`

    .. code-block:: python

        connection.set_default_connection('cluster2')

Unregister a connection
=======================

You can unregister a connection using :func:`~.connection.unregister_connection`:

    .. code-block:: python

        connection.unregister_connection('cluster2')

Management
==========

When using multiples connections, you also need to sync your models on all connections (and keyspaces) that you need operate on. Management commands have been improved to ease this part. Here is an example:

    .. code-block:: python

       from cassandra.cqlengine import management

       keyspaces = ['ks1', 'ks2']
       conns = ['cluster1', 'cluster2']

       # registers your connections
       # ...

       # create all keyspaces on all connections
       for ks in keyspaces:
           management.create_simple_keyspace(ks, connections=conns)

       # define your Automobile model
       # ...

       # sync your models
       management.sync_table(Automobile, keyspaces=keyspaces, connections=conns)


Connection Selection
====================

cqlengine will select the default connection, unless your specify a connection using one of the following methods.

Default Model Connection
------------------------

You can specify a default connection per model:

    .. code-block:: python

        class Automobile(Model):
            __keyspace__ = 'test'
            __connection__ = 'cluster2'
            manufacturer = columns.Text(primary_key=True)
            year = columns.Integer(primary_key=True)
            model = columns.Text(primary_key=True)

        print len(Automobile.objects.all())  # executed on the connection 'cluster2'

QuerySet and model instance
---------------------------

You can use the :attr:`using() <.query.ModelQuerySet.using>` method to select a connection (or keyspace):

    .. code-block:: python

        Automobile.objects.using(connection='cluster1').create(manufacturer='honda', year=2010, model='civic')
        q = Automobile.objects.filter(manufacturer='Tesla')
        autos = q.using(keyspace='ks2, connection='cluster2').all()

        for auto in autos:
            auto.using(connection='cluster1').save()

Context Manager
---------------

You can use the ContextQuery as well to select a connection:

    .. code-block:: python

        with ContextQuery(Automobile, connection='cluster1') as A:
            A.objects.filter(manufacturer='honda').all()  # executed on 'cluster1'


BatchQuery
----------

With a BatchQuery, you can select the connection with the context manager. Note that all operations in the batch need to use the same connection.

    .. code-block:: python

        with BatchQuery(connection='cluster1') as b:
            Automobile.objects.batch(b).create(manufacturer='honda', year=2010, model='civic')
