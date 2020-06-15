DataStax Graph Fluent API
=========================

The fluent API adds graph features to the core driver:

* A TinkerPop GraphTraversalSource builder to execute traversals on a DSE cluster
* The ability to execution traversal queries explicitly using execute_graph
* GraphSON serializers for all DSE Graph types.
* DSE Search predicates

The Graph fluent API depends on Apache TinkerPop and is not installed by default. Make sure
you have the Graph requirements are properly :ref:`installed <installation-datastax-graph>`.

You might be interested in reading the :doc:`DataStax Graph Getting Started documentation <graph>` to
understand the basics of creating a graph and its schema.

Graph Traversal Queries
~~~~~~~~~~~~~~~~~~~~~~~

The driver provides :meth:`.Session.execute_graph`, which allows users to execute traversal
query strings. Here is a simple example::

    session.execute_graph("g.addV('genre').property('genreId', 1).property('name', 'Action').next();")

Since graph queries can be very complex, working with strings is not very convenient and is
hard to maintain. This fluent API allows you to build Gremlin traversals and write your graph
queries directly in Python. These native traversal queries can be executed explicitly, with
a `Session` object, or implicitly::

    from cassandra.cluster import Cluster, EXEC_PROFILE_GRAPH_DEFAULT
    from cassandra.datastax.graph import GraphProtocol
    from cassandra.datastax.graph.fluent import DseGraph

    # Create an execution profile, using GraphSON3 for Core graphs
    ep_graphson3 = DseGraph.create_execution_profile(
        'my_core_graph_name',
        graph_protocol=GraphProtocol.GRAPHSON_3_0)
    cluster = Cluster(execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep_graphson3})
    session = cluster.connect()

    # Execute a fluent graph query
    g = DseGraph.traversal_source(session=session)
    g.addV('genre').property('genreId', 1).property('name', 'Action').next()

    # implicit execution caused by iterating over results
    for v in g.V().has('genre', 'name', 'Drama').in_('belongsTo').valueMap():
        print(v)

These :ref:`Python types <graph-types>` are also supported transparently::

    g.addV('person').property('name', 'Mike').property('birthday', datetime(1984, 3, 11)). \
        property('house_yard', Polygon(((30, 10), (40, 40), (20, 40), (10, 20), (30, 10)))

More readings about Gremlin:

* `DataStax Drivers Fluent API <https://www.datastax.com/dev/blog/datastax-drivers-fluent-apis-for-dse-graph-are-out>`_
* `gremlin-python documentation <http://tinkerpop.apache.org/docs/current/reference/#gremlin-python>`_

Configuring a Traversal Execution Profile
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The fluent api takes advantage of *configuration profiles* to allow
different execution configurations for the various query handlers. Graph traversal
execution requires a custom execution profile to enable Gremlin-bytecode as
query language. With Core graphs, it is important to use GraphSON3. Here is how
to accomplish this configuration:

.. code-block:: python

    from cassandra.cluster import Cluster, EXEC_PROFILE_GRAPH_DEFAULT
    from cassandra.datastax.graph import GraphProtocol
    from cassandra.datastax.graph.fluent import DseGraph

    # Using GraphSON3 as graph protocol is a requirement with Core graphs.
    ep = DseGraph.create_execution_profile(
        'graph_name',
        graph_protocol=GraphProtocol.GRAPHSON_3_0)

    # For Classic graphs, GraphSON1, GraphSON2 and GraphSON3 (DSE 6.8+) are supported.
    ep_classic = DseGraph.create_execution_profile('classic_graph_name')  # default is GraphSON2

    cluster = Cluster(execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep, 'classic': ep_classic})
    session = cluster.connect()

    g = DseGraph.traversal_source(session)  # Build the GraphTraversalSource
    print g.V().toList()  # Traverse the Graph

Note that the execution profile created with :meth:`DseGraph.create_execution_profile <.datastax.graph.fluent.DseGraph.create_execution_profile>` cannot
be used for any groovy string queries.

If you want to change execution property defaults, please see the :doc:`Execution Profile documentation <execution_profiles>`
for a more generalized discussion of the API. Graph traversal queries use the same execution profile defined for DSE graph. If you
need to change the default properties, please refer to the :doc:`DSE Graph query documentation page <graph>`

Explicit Graph Traversal Execution with a DSE Session
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Traversal queries can be executed explicitly using `session.execute_graph` or `session.execute_graph_async`. These functions
return results as DSE graph types. If you are familiar with DSE queries or need async execution, you might prefer that way.
Below is an example of explicit execution. For this example, assume the schema has been generated as above:

.. code-block:: python

    from cassandra.cluster import Cluster, EXEC_PROFILE_GRAPH_DEFAULT
    from cassandra.datastax.graph import GraphProtocol
    from cassandra.datastax.graph.fluent import DseGraph
    from pprint import pprint

    ep = DseGraph.create_execution_profile(
        'graph_name',
        graph_protocol=GraphProtocol.GRAPHSON_3_0)
    cluster = Cluster(execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep})
    session = cluster.connect()

    g = DseGraph.traversal_source(session=session)

Convert a traversal to a bytecode query for classic graphs::

    addV_query = DseGraph.query_from_traversal(
        g.addV('genre').property('genreId', 1).property('name', 'Action'),
        graph_protocol=GraphProtocol.GRAPHSON_3_0
    )
    v_query = DseGraph.query_from_traversal(
        g.V(),
        graph_protocol=GraphProtocol.GRAPHSON_3_0)

    for result in session.execute_graph(addV_query):
        pprint(result.value)
    for result in session.execute_graph(v_query):
        pprint(result.value)

Converting a traversal to a bytecode query for core graphs require some more work, because we
need the cluster context for UDT and tuple types:

.. code-block:: python
    context = {
        'cluster': cluster,
        'graph_name': 'the_graph_for_the_query'
    }
    addV_query = DseGraph.query_from_traversal(
        g.addV('genre').property('genreId', 1).property('name', 'Action'),
        graph_protocol=GraphProtocol.GRAPHSON_3_0,
        context=context
    )

    for result in session.execute_graph(addV_query):
        pprint(result.value)

Implicit Graph Traversal Execution with TinkerPop
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using the :class:`DseGraph <.datastax.graph.fluent.DseGraph>` class, you can build a GraphTraversalSource
that will execute queries on a DSE session without explicitly passing anything to
that session. We call this *implicit execution* because the `Session` is not
explicitly involved. Everything is managed internally by TinkerPop while
traversing the graph and the results are TinkerPop types as well.

Synchronous Example
-------------------

.. code-block:: python

    # Build the GraphTraversalSource
    g = DseGraph.traversal_source(session)
    # implicitly execute the query by traversing the TraversalSource
    g.addV('genre').property('genreId', 1).property('name', 'Action').next()

    # blocks until the query is completed and return the results
    results = g.V().toList()
    pprint(results)

Asynchronous Exemple
--------------------

You can execute a graph traversal query asynchronously by using `.promise()`. It returns a
python `Future <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future>`_.

.. code-block:: python

    # Build the GraphTraversalSource
    g = DseGraph.traversal_source(session)
    # implicitly execute the query by traversing the TraversalSource
    g.addV('genre').property('genreId', 1).property('name', 'Action').next()  # not async

    # get a future and wait
    future = g.V().promise()
    results = list(future.result())
    pprint(results)

    # or set a callback
    def cb(f):
        results = list(f.result())
        pprint(results)
    future = g.V().promise()
    future.add_done_callback(cb)
    # do other stuff...

Specify the Execution Profile explicitly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you don't want to change the default graph execution profile (`EXEC_PROFILE_GRAPH_DEFAULT`), you can register a new
one as usual and use it explicitly. Here is an example:

.. code-block:: python

    from cassandra.cluster import Cluster
    from cassandra.datastax.graph.fluent import DseGraph

    cluster = Cluster()
    ep = DseGraph.create_execution_profile('graph_name', graph_protocol=GraphProtocol.GRAPHSON_3_0)
    cluster.add_execution_profile('graph_traversal', ep)
    session = cluster.connect()

    g = DseGraph.traversal_source()
    query = DseGraph.query_from_traversal(g.V())
    session.execute_graph(query, execution_profile='graph_traversal')

You can also create multiple GraphTraversalSources and use them with
the same execution profile (for different graphs):

.. code-block:: python

    g_movies = DseGraph.traversal_source(session, graph_name='movies', ep)
    g_series = DseGraph.traversal_source(session, graph_name='series', ep)

    print(g_movies.V().toList())  # Traverse the movies Graph
    print(g_series.V().toList())  # Traverse the series Graph

Batch Queries
~~~~~~~~~~~~~

DSE Graph supports batch queries using a :class:`TraversalBatch <.datastax.graph.fluent.query.TraversalBatch>` object
instantiated with :meth:`DseGraph.batch <.datastax.graph.fluent.DseGraph.batch>`. A :class:`TraversalBatch <.datastax.graph.fluent.query.TraversalBatch>` allows
you to execute multiple graph traversals in a single atomic transaction. A 
traversal batch is executed with :meth:`.Session.execute_graph` or using 
:meth:`TraversalBatch.execute <.datastax.graph.fluent.query.TraversalBatch.execute>` if bounded to a DSE session. 

Either way you choose to execute the traversal batch, you need to configure 
the execution profile accordingly. Here is a example::

    from cassandra.cluster import Cluster
    from cassandra.datastax.graph.fluent import DseGraph

    ep = DseGraph.create_execution_profile(
        'graph_name',
        graph_protocol=GraphProtocol.GRAPHSON_3_0)
    cluster = Cluster(execution_profiles={'graphson3': ep})
    session = cluster.connect()

    g = DseGraph.traversal_source()

To execute the batch using :meth:`.Session.execute_graph`, you need to convert
the batch to a GraphStatement::

    batch = DseGraph.batch()

    batch.add(
        g.addV('genre').property('genreId', 1).property('name', 'Action'))
    batch.add(
        g.addV('genre').property('genreId', 2).property('name', 'Drama'))  # Don't use `.next()` with a batch

    graph_statement = batch.as_graph_statement(graph_protocol=GraphProtocol.GRAPHSON_3_0)
    graph_statement.is_idempotent = True  # configure any Statement parameters if needed...
    session.execute_graph(graph_statement, execution_profile='graphson3')

To execute the batch using :meth:`TraversalBatch.execute <.datastax.graph.fluent.query.TraversalBatch.execute>`, you need to bound the batch to a DSE session::

    batch = DseGraph.batch(session, 'graphson3')  # bound the session and execution profile

    batch.add(
        g.addV('genre').property('genreId', 1).property('name', 'Action'))
    batch.add(
        g.addV('genre').property('genreId', 2).property('name', 'Drama'))  # Don't use `.next()` with a batch

    batch.execute()

DSL (Domain Specific Languages)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

DSL are very useful to write better domain-specific APIs and avoiding
code duplication. Let's say we have a graph of `People` and we produce
a lot of statistics based on age. All graph traversal queries of our
application would look like::

  g.V().hasLabel("people").has("age", P.gt(21))...


which is not really verbose and quite annoying to repeat in a code base. Let's create a DSL::

  from gremlin_python.process.graph_traversal import GraphTraversal, GraphTraversalSource

  class MyAppTraversal(GraphTraversal):

    def younger_than(self, age):
        return self.has("age", P.lt(age))

    def older_than(self, age):
        return self.has("age", P.gt(age))


  class MyAppTraversalSource(GraphTraversalSource):

    def __init__(self, *args, **kwargs):
        super(MyAppTraversalSource, self).__init__(*args, **kwargs)
        self.graph_traversal = MyAppTraversal

    def people(self):
        return self.get_graph_traversal().V().hasLabel("people")

Now, we can use our DSL that is a lot cleaner::

  from cassandra.datastax.graph.fluent import DseGraph

  # ...
  g = DseGraph.traversal_source(session=session, traversal_class=MyAppTraversalsource)

  g.people().younger_than(21)...
  g.people().older_than(30)...

To see a more complete example of DSL, see the `Python killrvideo DSL app <https://github.com/datastax/graph-examples/tree/master/killrvideo/dsl/python>`_

Search
~~~~~~

DSE Graph can use search indexes that take advantage of DSE Search functionality for
efficient traversal queries. Here are the list of additional search predicates:

Text tokenization:

* :meth:`token <.datastax.graph.fluent.predicates.Search.token>`
* :meth:`token_prefix <.datastax.graph.fluent.predicates.Search.token_prefix>`
* :meth:`token_regex <.datastax.graph.fluent.predicates.Search.token_regex>`
* :meth:`token_fuzzy <.datastax.graph.fluent.predicates.Search.token_fuzzy>`

Text match:

* :meth:`prefix <.datastax.graph.fluent.predicates.Search.prefix>`
* :meth:`regex <.datastax.graph.fluent.predicates.Search.regex>`
* :meth:`fuzzy <.datastax.graph.fluent.predicates.Search.fuzzy>`
* :meth:`phrase <.datastax.graph.fluent.predicates.Search.phrase>`

Geo:

* :meth:`inside <.datastax.graph.fluent.predicates.Geo.inside>`

Create search indexes
---------------------

For text tokenization:

.. code-block:: python


    s.execute_graph("schema.vertexLabel('my_vertex_label').index('search').search().by('text_field').asText().add()")

For text match:

.. code-block:: python


    s.execute_graph("schema.vertexLabel('my_vertex_label').index('search').search().by('text_field').asString().add()")


For geospatial:

You can create a geospatial index on Point and LineString fields.

.. code-block:: python


    s.execute_graph("schema.vertexLabel('my_vertex_label').index('search').search().by('point_field').add()")


Using search indexes
--------------------

Token:

.. code-block:: python

    from cassandra.datastax.graph.fluent.predicates import Search
    # ...

    g = DseGraph.traversal_source()
    query = DseGraph.query_from_traversal(
        g.V().has('my_vertex_label','text_field', Search.token_regex('Hello.+World')).values('text_field'))
    session.execute_graph(query)

Text:

.. code-block:: python

    from cassandra.datastax.graph.fluent.predicates import Search
    # ...

    g = DseGraph.traversal_source()
    query = DseGraph.query_from_traversal(
        g.V().has('my_vertex_label','text_field', Search.prefix('Hello')).values('text_field'))
    session.execute_graph(query)

Geospatial:

.. code-block:: python

    from cassandra.datastax.graph.fluent.predicates import Geo
    from cassandra.util import Distance
    # ...

    g = DseGraph.traversal_source()
    query = DseGraph.query_from_traversal(
        g.V().has('my_vertex_label','point_field', Geo.inside(Distance(46, 71, 100)).values('point_field'))
    session.execute_graph(query)


For more details, please refer to the official `DSE Search Indexes Documentation <https://docs.datastax.com/en/dse/6.7/dse-admin/datastax_enterprise/search/searchReference.html>`_
