========================
Third party integrations
========================


Celery
------

Here's how, in substance, CQLengine can be plugged to `Celery
<http://celery.readthedocs.org/>`_:

.. code-block:: python

    from celery import Celery
    from celery.signals import worker_process_init, beat_init
    from cassandra.cqlengine import connection
    from cassandra.cqlengine.connection import (
        cluster as cql_cluster, session as cql_session)

    def cassandra_init(**kwargs):
        """ Initialize a clean Cassandra connection. """
        if cql_cluster is not None:
            cql_cluster.shutdown()
        if cql_session is not None:
            cql_session.shutdown()
        connection.setup()

    # Initialize worker context for both standard and periodic tasks.
    worker_process_init.connect(cassandra_init)
    beat_init.connect(cassandra_init)

    app = Celery()


uWSGI
-----

This is the code required for proper connection handling of CQLengine for a
`uWSGI <https://uwsgi-docs.readthedocs.org>`_-run application:

.. code-block:: python

    from cassandra.cqlengine import connection
    from cassandra.cqlengine.connection import (
        cluster as cql_cluster, session as cql_session)

    try:
        from uwsgidecorators import postfork
    except ImportError:
        # We're not in a uWSGI context, no need to hook Cassandra session
        # initialization to the postfork event.
        pass
    else:
        @postfork
        def cassandra_init(**kwargs):
            """ Initialize a new Cassandra session in the context.

            Ensures that a new session is returned for every new request.
            """
            if cql_cluster is not None:
                cql_cluster.shutdown()
            if cql_session is not None:
                cql_session.shutdown()
            connection.setup()
