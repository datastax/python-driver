DataStax Classic Graph Queries
==============================

Getting Started
~~~~~~~~~~~~~~~

First, we need to create a graph in the system. To access the system API, we 
use the system execution profile ::

    from cassandra.cluster import Cluster, EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT

    cluster = Cluster()
    session = cluster.connect()

    graph_name = 'movies'
    session.execute_graph("system.graph(name).ifNotExists().engine(Classic).create()", {'name': graph_name},
                          execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)


To execute requests on our newly created graph, we need to setup an execution
profile. Additionally, we also need to set the schema_mode to `development` 
for the schema creation::


    from cassandra.cluster import Cluster, GraphExecutionProfile, EXEC_PROFILE_GRAPH_DEFAULT
    from cassandra.graph import GraphOptions

    graph_name = 'movies'
    ep = GraphExecutionProfile(graph_options=GraphOptions(graph_name=graph_name))

    cluster = Cluster(execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep})
    session = cluster.connect()
    
    session.execute_graph("schema.config().option('graph.schema_mode').set('development')")


We are ready to configure our graph schema. We will create a simple one for movies::

    # properties are used to define a vertex
    properties = """
    schema.propertyKey("genreId").Text().create();
    schema.propertyKey("personId").Text().create();
    schema.propertyKey("movieId").Text().create();
    schema.propertyKey("name").Text().create();
    schema.propertyKey("title").Text().create();
    schema.propertyKey("year").Int().create();
    schema.propertyKey("country").Text().create();
    """

    session.execute_graph(properties)  # we can execute multiple statements in a single request

    # A Vertex represents a "thing" in the world.
    vertices = """
    schema.vertexLabel("genre").properties("genreId","name").create();
    schema.vertexLabel("person").properties("personId","name").create();
    schema.vertexLabel("movie").properties("movieId","title","year","country").create();
    """

    session.execute_graph(vertices)

    # An edge represents a relationship between two vertices
    edges = """
    schema.edgeLabel("belongsTo").single().connection("movie","genre").create();
    schema.edgeLabel("actor").connection("movie","person").create();
    """

    session.execute_graph(edges)

    # Indexes to execute graph requests efficiently
    indexes = """
    schema.vertexLabel("genre").index("genresById").materialized().by("genreId").add();
    schema.vertexLabel("genre").index("genresByName").materialized().by("name").add();
    schema.vertexLabel("person").index("personsById").materialized().by("personId").add();
    schema.vertexLabel("person").index("personsByName").materialized().by("name").add();
    schema.vertexLabel("movie").index("moviesById").materialized().by("movieId").add();
    schema.vertexLabel("movie").index("moviesByTitle").materialized().by("title").add();
    schema.vertexLabel("movie").index("moviesByYear").secondary().by("year").add();
    """

Next, we'll add some data::

    session.execute_graph("""
    g.addV('genre').property('genreId', 1).property('name', 'Action').next();
    g.addV('genre').property('genreId', 2).property('name', 'Drama').next();
    g.addV('genre').property('genreId', 3).property('name', 'Comedy').next();
    g.addV('genre').property('genreId', 4).property('name', 'Horror').next();
    """)

    session.execute_graph("""
    g.addV('person').property('personId', 1).property('name', 'Mark Wahlberg').next();
    g.addV('person').property('personId', 2).property('name', 'Leonardo DiCaprio').next();
    g.addV('person').property('personId', 3).property('name', 'Iggy Pop').next();
    """)

    session.execute_graph("""
    g.addV('movie').property('movieId', 1).property('title', 'The Happening').
        property('year', 2008).property('country', 'United States').next();
    g.addV('movie').property('movieId', 2).property('title', 'The Italian Job').
        property('year', 2003).property('country', 'United States').next();

    g.addV('movie').property('movieId', 3).property('title', 'Revolutionary Road').
        property('year', 2008).property('country', 'United States').next();
    g.addV('movie').property('movieId', 4).property('title', 'The Man in the Iron Mask').
        property('year', 1998).property('country', 'United States').next();

    g.addV('movie').property('movieId', 5).property('title', 'Dead Man').
        property('year', 1995).property('country', 'United States').next();
    """)

Now that our genre, actor and movie vertices are added, we'll create the relationships (edges) between them::

    session.execute_graph("""
    genre_horror = g.V().hasLabel('genre').has('name', 'Horror').next();
    genre_drama = g.V().hasLabel('genre').has('name', 'Drama').next();
    genre_action = g.V().hasLabel('genre').has('name', 'Action').next();

    leo  = g.V().hasLabel('person').has('name', 'Leonardo DiCaprio').next();
    mark = g.V().hasLabel('person').has('name', 'Mark Wahlberg').next();
    iggy = g.V().hasLabel('person').has('name', 'Iggy Pop').next();

    the_happening = g.V().hasLabel('movie').has('title', 'The Happening').next();
    the_italian_job = g.V().hasLabel('movie').has('title', 'The Italian Job').next();
    rev_road = g.V().hasLabel('movie').has('title', 'Revolutionary Road').next();
    man_mask = g.V().hasLabel('movie').has('title', 'The Man in the Iron Mask').next();
    dead_man = g.V().hasLabel('movie').has('title', 'Dead Man').next();

    the_happening.addEdge('belongsTo', genre_horror);
    the_italian_job.addEdge('belongsTo', genre_action);
    rev_road.addEdge('belongsTo', genre_drama);
    man_mask.addEdge('belongsTo', genre_drama);
    man_mask.addEdge('belongsTo', genre_action);
    dead_man.addEdge('belongsTo', genre_drama);

    the_happening.addEdge('actor', mark);
    the_italian_job.addEdge('actor', mark);
    rev_road.addEdge('actor', leo);
    man_mask.addEdge('actor', leo);
    dead_man.addEdge('actor', iggy);
    """)

We are all set. You can now query your graph. Here are some examples::

    # Find all movies of the genre Drama
    for r in session.execute_graph("""
      g.V().has('genre', 'name', 'Drama').in('belongsTo').valueMap();"""):
        print(r)
    
    # Find all movies of the same genre than the movie 'Dead Man'
    for r in session.execute_graph("""
      g.V().has('movie', 'title', 'Dead Man').out('belongsTo').in('belongsTo').valueMap();"""):
        print(r)

    # Find all movies of Mark Wahlberg
    for r in session.execute_graph("""
      g.V().has('person', 'name', 'Mark Wahlberg').in('actor').valueMap();"""):
        print(r)

To see a more graph examples, see `DataStax Graph Examples <https://github.com/datastax/graph-examples/>`_.

Graph Types
~~~~~~~~~~~

Here are the supported graph types with their python representations:

==========   ================
DSE Graph    Python
==========   ================
boolean      bool
bigint       long, int (PY3)
int          int
smallint     int
varint       int
float        float
double       double
uuid         uuid.UUID
Decimal      Decimal
inet         str
timestamp    datetime.datetime
date         datetime.date
time         datetime.time
duration     datetime.timedelta
point        Point
linestring   LineString
polygon      Polygon
blob         bytearray, buffer (PY2), memoryview (PY3), bytes (PY3)
==========   ================

Graph Row Factory
~~~~~~~~~~~~~~~~~

By default (with :class:`.GraphExecutionProfile.row_factory` set to :func:`.graph.graph_object_row_factory`), known graph result
types are unpacked and returned as specialized types (:class:`.Vertex`, :class:`.Edge`). If the result is not one of these
types, a :class:`.graph.Result` is returned, containing the graph result parsed from JSON and removed from its outer dict.
The class has some accessor convenience methods for accessing top-level properties by name (`type`, `properties` above),
or lists by index::

    # dicts with `__getattr__` or `__getitem__`
    result = session.execute_graph("[[key_str: 'value', key_int: 3]]", execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)[0]  # Using system exec just because there is no graph defined
    result  # dse.graph.Result({u'key_str': u'value', u'key_int': 3})
    result.value  # {u'key_int': 3, u'key_str': u'value'} (dict)
    result.key_str  # u'value'
    result.key_int  # 3
    result['key_str']  # u'value'
    result['key_int']  # 3

    # lists with `__getitem__`
    result = session.execute_graph('[[0, 1, 2]]', execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)[0]
    result  # dse.graph.Result([0, 1, 2])
    result.value  # [0, 1, 2] (list)
    result[1]  # 1 (list[1])

You can use a different row factory by setting :attr:`.Session.default_graph_row_factory` or passing it to
:meth:`.Session.execute_graph`. For example, :func:`.graph.single_object_row_factory` returns the JSON result string`,
unparsed. :func:`.graph.graph_result_row_factory` returns parsed, but unmodified results (such that all metadata is retained,
unlike :func:`.graph.graph_object_row_factory`, which sheds some as attributes and properties are unpacked). These results
also provide convenience methods for converting to known types (:meth:`~.Result.as_vertex`, :meth:`~.Result.as_edge`, :meth:`~.Result.as_path`).

Vertex and Edge properties are never unpacked since their types are unknown. If you know your graph schema and want to
deserialize properties, use the :class:`.GraphSON1Deserializer`. It provides convenient methods to deserialize by types (e.g.
deserialize_date, deserialize_uuid, deserialize_polygon etc.) Example::

    # ...
    from cassandra.graph import GraphSON1Deserializer

    row = session.execute_graph("g.V().toList()")[0]
    value = row.properties['my_property_key'][0].value  # accessing the VertexProperty value
    value = GraphSON1Deserializer.deserialize_timestamp(value)

    print(value)  # 2017-06-26 08:27:05
    print(type(value))  # <type 'datetime.datetime'>


Named Parameters
~~~~~~~~~~~~~~~~

Named parameters are passed in a dict to :meth:`.cluster.Session.execute_graph`::

    result_set = session.execute_graph('[a, b]', {'a': 1, 'b': 2}, execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)
    [r.value for r in result_set]  # [1, 2]

All python types listed in `Graph Types`_ can be passed as named parameters and will be serialized
automatically to their graph representation:

Example::

    session.execute_graph("""
      g.addV('person').
      property('name', text_value).
      property('age', integer_value).
      property('birthday', timestamp_value).
      property('house_yard', polygon_value).toList()
    """, {
      'text_value': 'Mike Smith',
      'integer_value': 34,
      'timestamp_value': datetime.datetime(1967, 12, 30),
      'polygon_value': Polygon(((30, 10), (40, 40), (20, 40), (10, 20), (30, 10)))
    })


As with all Execution Profile parameters, graph options can be set in the cluster default (as shown in the first example)
or specified per execution::

    ep = session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT,
                                                graph_options=GraphOptions(graph_name='something-else'))
    session.execute_graph(statement, execution_profile=ep)

Using GraphSON2 Protocol
~~~~~~~~~~~~~~~~~~~~~~~~

The default graph protocol used is GraphSON1. However GraphSON1 may
cause problems of type conversion happening during the serialization
of the query to the DSE Graph server, or the deserialization of the
responses back from a string Gremlin query. GraphSON2 offers better
support for the complex data types handled by DSE Graph.

DSE >=5.0.4 now offers the possibility to use the GraphSON2 protocol
for graph queries. Enabling GraphSON2 can be done by `changing the
graph protocol of the execution profile` and `setting the graphson2 row factory`::

    from cassandra.cluster import Cluster, GraphExecutionProfile, EXEC_PROFILE_GRAPH_DEFAULT
    from cassandra.graph import GraphOptions, GraphProtocol, graph_graphson2_row_factory

    # Create a GraphSON2 execution profile
    ep = GraphExecutionProfile(graph_options=GraphOptions(graph_name='types',
                                                          graph_protocol=GraphProtocol.GRAPHSON_2_0),
                               row_factory=graph_graphson2_row_factory)

    cluster = Cluster(execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep})
    session = cluster.connect()
    session.execute_graph(...)

Using GraphSON2, all properties will be automatically deserialized to
its Python representation. Note that it may bring significant
behavioral change at runtime.

It is generally recommended to switch to GraphSON2 as it brings more
consistent support for complex data types in the Graph driver and will
be activated by default in the next major version (Python dse-driver
driver 3.0).
