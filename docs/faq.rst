Frequently Asked Questions
==========================

See also :doc:`cqlengine FAQ <cqlengine/faq>`

Why do connections or IO operations timeout in my WSGI application?
-------------------------------------------------------------------
Depending on your application process model, it may be forking after driver Session is created. Most IO reactors do not handle this, and problems will manifest as timeouts.

To avoid this, make sure to create sessions per process, after the fork. Using uWSGI and Flask for example:

.. code-block:: python

    from flask import Flask
    from uwsgidecorators import postfork
    from cassandra.cluster import Cluster

    session = None
    prepared = None

    @postfork
    def connect():
        global session, prepared
        session = Cluster().connect()
        prepared = session.prepare("SELECT release_version FROM system.local WHERE key=?")

    app = Flask(__name__)

    @app.route('/')
    def server_version():
        row = session.execute(prepared, ('local',))[0]
        return row.release_version

uWSGI provides a ``postfork`` hook you can use to create sessions and prepared statements after the child process forks.

How do I trace a request?
-------------------------
Request tracing can be turned on for any request by setting ``trace=True`` in :meth:`.Session.execute_async`. View the results by waiting on the future, then :meth:`.ResponseFuture.get_query_trace`.
Since tracing is done asynchronously to the request, this method polls until the trace is complete before querying data.

.. code-block:: python

    >>> future = session.execute_async("SELECT * FROM system.local", trace=True)
    >>> result = future.result()
    >>> trace = future.get_query_trace()
    >>> for e in trace.events:
    >>>     print e.source_elapsed, e.description

    0:00:00.000077 Parsing select * from system.local
    0:00:00.000153 Preparing statement
    0:00:00.000309 Computing ranges to query
    0:00:00.000368 Submitting range requests on 1 ranges with a concurrency of 1 (279.77142 rows per range expected)
    0:00:00.000422 Submitted 1 concurrent range requests covering 1 ranges
    0:00:00.000480 Executing seq scan across 1 sstables for (min(-9223372036854775808), min(-9223372036854775808))
    0:00:00.000669 Read 1 live and 0 tombstone cells
    0:00:00.000755 Scanned 1 rows and matched 1

``trace`` is a :class:`QueryTrace` object.

How do I determine the replicas for a query?
----------------------------------------------
With prepared statements, the replicas are obtained by ``routing_key``, based on current cluster token metadata:

.. code-block:: python

    >>> prepared = session.prepare("SELECT * FROM example.t WHERE key=?")
    >>> bound = prepared.bind((1,))
    >>> replicas = cluster.metadata.get_replicas(bound.keyspace, bound.routing_key)
    >>> for h in replicas:
    >>>   print h.address
    127.0.0.1
    127.0.0.2

``replicas`` is a list of :class:`Host` objects.

How does the driver manage request retries?
-------------------------------------------
By default, retries are managed by the :attr:`.Cluster.default_retry_policy` set on the session Cluster. It can also
be specialized per statement by setting :attr:`.Statement.retry_policy`.

Retries are presently attempted on the same coordinator, but this may change in the future.

Please see :class:`.policies.RetryPolicy` for further details.
