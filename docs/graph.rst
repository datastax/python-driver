DataStax Graph Queries
======================

The driver executes graph queries over the Cassandra native protocol. Use
:meth:`.Session.execute_graph` or :meth:`.Session.execute_graph_async` for 
executing gremlin queries in DataStax Graph.

The driver defines three Execution Profiles suitable for graph execution:

* :data:`~.cluster.EXEC_PROFILE_GRAPH_DEFAULT`
* :data:`~.cluster.EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT`
* :data:`~.cluster.EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT`

See :doc:`getting_started` and :doc:`execution_profiles`
for more detail on working with profiles.

In DSE 6.8.0, the Core graph engine has been introduced and is now the default. It
provides a better unified multi-model, performance and scale. This guide
is for graphs that use the core engine. If you work with previous versions of 
DSE or existing graphs, see :doc:`classic_graph`.

Getting Started with Graph and the Core Engine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, we need to create a graph in the system. To access the system API, we 
use the system execution profile ::

    from cassandra.cluster import Cluster, EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT

    cluster = Cluster()
    session = cluster.connect()

    graph_name = 'movies'
    session.execute_graph("system.graph(name).create()", {'name': graph_name},
                          execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)


Graphs that use the core engine only support GraphSON3. Since they are Cassandra tables under
the hood, we can automatically configure the execution profile with the proper options
(row_factory and graph_protocol) when executing queries. You only need to make sure that
the `graph_name` is set and GraphSON3 will be automatically used::

    from cassandra.cluster import Cluster, GraphExecutionProfile, EXEC_PROFILE_GRAPH_DEFAULT

    graph_name = 'movies'
    ep = GraphExecutionProfile(graph_options=GraphOptions(graph_name=graph_name))
    cluster = Cluster(execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep})
    session = cluster.connect()
    session.execute_graph("g.addV(...)")


Note that this graph engine detection is based on the metadata. You might experience
some query errors if the graph has been newly created and is not yet in the metadata. This
would result to a badly configured execution profile. If you really want to avoid that,
configure your execution profile explicitly::

    from cassandra.cluster import Cluster, GraphExecutionProfile, EXEC_PROFILE_GRAPH_DEFAULT
    from cassandra.graph import GraphOptions, GraphProtocol, graph_graphson3_row_factory

    graph_name = 'movies'
    ep_graphson3 = GraphExecutionProfile(
        row_factory=graph_graphson3_row_factory,
        graph_options=GraphOptions(
            graph_protocol=GraphProtocol.GRAPHSON_3_0,
            graph_name=graph_name))

    cluster = Cluster(execution_profiles={'core': ep_graphson3})
    session = cluster.connect()
    session.execute_graph("g.addV(...)", execution_profile='core')


We are ready to configure our graph schema. We will create a simple one for movies::

    # A Vertex represents a "thing" in the world.
    # Create the genre vertex
    query = """
        schema.vertexLabel('genre')
            .partitionBy('genreId', Int)
            .property('name', Text)
            .create()
    """
    session.execute_graph(query)

    # Create the person vertex
    query = """
        schema.vertexLabel('person')
            .partitionBy('personId', Int)
            .property('name', Text)
            .create()
    """
    session.execute_graph(query)

    # Create the movie vertex
    query = """
        schema.vertexLabel('movie')
            .partitionBy('movieId', Int)
            .property('title', Text)
            .property('year', Int)
            .property('country', Text)
            .create()
    """
    session.execute_graph(query)

    # An edge represents a relationship between two vertices
    # Create our edges
    queries = """
    schema.edgeLabel('belongsTo').from('movie').to('genre').create();
    schema.edgeLabel('actor').from('movie').to('person').create();
    """
    session.execute_graph(queries)

    # Indexes to execute graph requests efficiently

    # If you have a node with the search workload enabled (solr), use the following:
    indexes = """
        schema.vertexLabel('genre').searchIndex()
            .by("name")
            .create();

        schema.vertexLabel('person').searchIndex()
            .by("name")
            .create();

        schema.vertexLabel('movie').searchIndex()
            .by('title')
            .by("year")
            .create();
    """
    session.execute_graph(indexes)

    # Otherwise, use secondary indexes:
    indexes = """
        schema.vertexLabel('genre')
            .secondaryIndex('by_genre')
            .by('name')
            .create()

        schema.vertexLabel('person')
            .secondaryIndex('by_name')
            .by('name')
            .create()

        schema.vertexLabel('movie')
            .secondaryIndex('by_title')
            .by('title')
            .create()
    """
    session.execute_graph(indexes)

Add some edge indexes (materialized views)::

    indexes = """
        schema.edgeLabel('belongsTo')
            .from('movie')
            .to('genre')
            .materializedView('movie__belongsTo__genre_by_in_genreId')
            .ifNotExists()
            .partitionBy(IN, 'genreId')
            .clusterBy(OUT, 'movieId', Asc)
            .create()

        schema.edgeLabel('actor')
            .from('movie')
            .to('person')
            .materializedView('movie__actor__person_by_in_personId')
            .ifNotExists()
            .partitionBy(IN, 'personId')
            .clusterBy(OUT, 'movieId', Asc)
            .create()
    """
    session.execute_graph(indexes)

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
        genre_horror = g.V().hasLabel('genre').has('name', 'Horror').id().next();
        genre_drama = g.V().hasLabel('genre').has('name', 'Drama').id().next();
        genre_action = g.V().hasLabel('genre').has('name', 'Action').id().next();

        leo  = g.V().hasLabel('person').has('name', 'Leonardo DiCaprio').id().next();
        mark = g.V().hasLabel('person').has('name', 'Mark Wahlberg').id().next();
        iggy = g.V().hasLabel('person').has('name', 'Iggy Pop').id().next();

        the_happening = g.V().hasLabel('movie').has('title', 'The Happening').id().next();
        the_italian_job = g.V().hasLabel('movie').has('title', 'The Italian Job').id().next();
        rev_road = g.V().hasLabel('movie').has('title', 'Revolutionary Road').id().next();
        man_mask = g.V().hasLabel('movie').has('title', 'The Man in the Iron Mask').id().next();
        dead_man = g.V().hasLabel('movie').has('title', 'Dead Man').id().next();

        g.addE('belongsTo').from(__.V(the_happening)).to(__.V(genre_horror)).next();
        g.addE('belongsTo').from(__.V(the_italian_job)).to(__.V(genre_action)).next();
        g.addE('belongsTo').from(__.V(rev_road)).to(__.V(genre_drama)).next();
        g.addE('belongsTo').from(__.V(man_mask)).to(__.V(genre_drama)).next();
        g.addE('belongsTo').from(__.V(man_mask)).to(__.V(genre_action)).next();
        g.addE('belongsTo').from(__.V(dead_man)).to(__.V(genre_drama)).next();

        g.addE('actor').from(__.V(the_happening)).to(__.V(mark)).next();
        g.addE('actor').from(__.V(the_italian_job)).to(__.V(mark)).next();
        g.addE('actor').from(__.V(rev_road)).to(__.V(leo)).next();
        g.addE('actor').from(__.V(man_mask)).to(__.V(leo)).next();
        g.addE('actor').from(__.V(dead_man)).to(__.V(iggy)).next();
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

Graph Types for the Core Engine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here are the supported graph types with their python representations:

============   =================
DSE Graph      Python Driver
============   =================
text           str
boolean        bool
bigint         long
int            int
smallint       int
varint         long
double         float
float          float
uuid           UUID
bigdecimal     Decimal
duration       Duration (cassandra.util)
inet           str or IPV4Address/IPV6Address (if available)
timestamp      datetime.datetime
date           datetime.date
time           datetime.time
polygon        Polygon
point          Point
linestring     LineString
blob           bytearray, buffer (PY2), memoryview (PY3), bytes (PY3)
list           list
map            dict
set            set or list
               (Can return a list due to numerical values returned by Java)
tuple          tuple
udt            class or namedtuple
============   =================

Named Parameters
~~~~~~~~~~~~~~~~

Named parameters are passed in a dict to :meth:`.cluster.Session.execute_graph`::

    result_set = session.execute_graph('[a, b]', {'a': 1, 'b': 2}, execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)
    [r.value for r in result_set]  # [1, 2]

All python types listed in `Graph Types for the Core Engine`_ can be passed as named parameters and will be serialized
automatically to their graph representation:

Example::

    session.execute_graph("""
      g.addV('person').
      property('name', text_value).
      property('age', integer_value).
      property('birthday', timestamp_value).
      property('house_yard', polygon_value).next()
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

CQL collections, Tuple and UDT
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a very interesting feature of the core engine: we can use all CQL data types, including
list, map, set, tuple and udt. Here is an example using all these types::

    query = """
        schema.type('address')
            .property('address', Text)
            .property('city', Text)
            .property('state', Text)
            .create();
    """
    session.execute_graph(query)

    # It works the same way than normal CQL UDT, so we
    # can create an udt class and register it
    class Address(object):
        def __init__(self, address, city, state):
            self.address = address
            self.city = city
            self.state = state

    session.cluster.register_user_type(graph_name, 'address', Address)

    query = """
        schema.vertexLabel('person')
            .partitionBy('personId', Int)
            .property('address', typeOf('address'))
            .property('friends', listOf(Text))
            .property('skills', setOf(Text))
            .property('scores', mapOf(Text, Int))
            .property('last_workout', tupleOf(Text, Date))
            .create()
    """
    session.execute_graph(query)

    # insertion example
    query = """
         g.addV('person')
            .property('personId', pid)
            .property('address', address)
            .property('friends', friends)
            .property('skills', skills)
            .property('scores', scores)
            .property('last_workout', last_workout)
            .next()
    """

    session.execute_graph(query, {
        'pid': 3,
        'address': Address('42 Smith St', 'Quebec', 'QC'),
        'friends': ['Al', 'Mike', 'Cathy'],
        'skills': {'food', 'fight', 'chess'},
        'scores': {'math': 98, 'french': 3},
        'last_workout': ('CrossFit', datetime.date(2018, 11, 20))
    })

Limitations
-----------

Since Python is not a strongly-typed language and the UDT/Tuple graphson representation is, you might 
get schema errors when trying to write numerical data. Example::

    session.execute_graph("""
        schema.vertexLabel('test_tuple').partitionBy('id', Int).property('t', tupleOf(Text, Bigint)).create()
    """)

    session.execute_graph("""
        g.addV('test_tuple').property('id', 0).property('t', t)
          """, 
          {'t': ('Test', 99))}
    )

    # error: [Invalid query] message="Value component 1 is of type int, not bigint"

This is because the server requires the client to include a GraphSON schema definition
with every UDT or tuple query. In the general case, the driver can't determine what Graph type
is meant by, e.g., an int value, and so it can't serialize the value with the correct type in the schema.
The driver provides some numerical type-wrapper factories that you can use to specify types:

* :func:`~.to_int`
* :func:`~.to_bigint`
* :func:`~.to_smallint`
* :func:`~.to_float`
* :func:`~.to_double`

Here's the working example of the case above::

    from cassandra.graph import to_bigint

     session.execute_graph("""
        g.addV('test_tuple').property('id', 0).property('t', t)
          """, 
          {'t': ('Test', to_bigint(99))}
    )

Continuous Paging
~~~~~~~~~~~~~~~~~

This is another nice feature that comes with the core engine: continuous paging with
graph queries. If all nodes of the cluster are >= DSE 6.8.0, it is automatically
enabled under the hood to get the best performance. If you want to explicitly
enable/disable it, you can do it through the execution profile::

    # Disable it
    ep = GraphExecutionProfile(..., continuous_paging_options=None))
    cluster = Cluster(execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep})

    # Enable with a custom max_pages option
    ep = GraphExecutionProfile(...,
        continuous_paging_options=ContinuousPagingOptions(max_pages=10)))
    cluster = Cluster(execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep})
