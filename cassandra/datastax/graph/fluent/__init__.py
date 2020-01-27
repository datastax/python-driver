# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import copy

from concurrent.futures import Future

HAVE_GREMLIN = False
try:
    import gremlin_python
    HAVE_GREMLIN = True
except ImportError:
    # gremlinpython is not installed.
    pass

if HAVE_GREMLIN:
    from gremlin_python.structure.graph import Graph
    from gremlin_python.driver.remote_connection import RemoteConnection, RemoteTraversal
    from gremlin_python.process.traversal import Traverser, TraversalSideEffects
    from gremlin_python.process.graph_traversal import GraphTraversal

    from cassandra.cluster import Session, GraphExecutionProfile, EXEC_PROFILE_GRAPH_DEFAULT
    from cassandra.datastax.graph import GraphOptions, GraphProtocol

    from cassandra.datastax.graph.fluent.serializers import (
        GremlinGraphSONReader,
        deserializers,
        gremlin_deserializers
    )
    from cassandra.datastax.graph.fluent.query import _DefaultTraversalBatch, _query_from_traversal

    log = logging.getLogger(__name__)

    __all__ = ['BaseGraphRowFactory', 'dse_graphson_reader', 'graphson_reader', 'graph_traversal_row_factory',
               'graph_traversal_dse_object_row_factory', 'DSESessionRemoteGraphConnection', 'DseGraph']

    # Create our custom GraphSONReader/Writer
    dse_graphson_reader = GremlinGraphSONReader(deserializer_map=deserializers)
    graphson_reader = GremlinGraphSONReader(deserializer_map=gremlin_deserializers)

    # Traversal result keys
    _bulk_key = 'bulk'
    _result_key = 'result'


    class BaseGraphRowFactory(object):
        """
        Base row factory for graph traversal. This class basically wraps a
        graphson reader function to handle additional features of Gremlin/DSE
        and is callable as a normal row factory.

        Currently supported:
          - bulk results

        :param graphson_reader: The function used to read the graphson.

        Use example::

            my_custom_row_factory = BaseGraphRowFactory(custom_graphson_reader.readObject)
        """

        def __init__(self, graphson_reader):
            self._graphson_reader = graphson_reader

        def __call__(self, column_names, rows):
            results = []

            for row in rows:
                parsed_row = self._graphson_reader(row[0])
                bulk = parsed_row.get(_bulk_key, 1)
                if bulk > 1:  # Avoid deepcopy call if bulk <= 1
                    results.extend([copy.deepcopy(parsed_row[_result_key])
                                    for _ in range(bulk - 1)])

                results.append(parsed_row[_result_key])

            return results


    graph_traversal_row_factory = BaseGraphRowFactory(graphson_reader.readObject)
    graph_traversal_row_factory.__doc__ = "Row Factory that returns the decoded graphson."

    graph_traversal_dse_object_row_factory = BaseGraphRowFactory(dse_graphson_reader.readObject)
    graph_traversal_dse_object_row_factory.__doc__ = "Row Factory that returns the decoded graphson as DSE types."


    class DSESessionRemoteGraphConnection(RemoteConnection):
        """
        A Tinkerpop RemoteConnection to execute traversal queries on DSE.

        :param session: A DSE session
        :param graph_name: (Optional) DSE Graph name.
        :param execution_profile: (Optional) Execution profile for traversal queries. Default is set to `EXEC_PROFILE_GRAPH_DEFAULT`.
        """

        session = None
        graph_name = None
        execution_profile = None

        def __init__(self, session, graph_name=None, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
            super(DSESessionRemoteGraphConnection, self).__init__(None, None)

            if not isinstance(session, Session):
                raise ValueError('A DSE Session must be provided to execute graph traversal queries.')

            self.session = session
            self.graph_name = graph_name
            self.execution_profile = execution_profile

        def _prepare_query(self, bytecode):
            query = DseGraph.query_from_traversal(bytecode)
            ep = self.session.execution_profile_clone_update(self.execution_profile,
                                                             row_factory=graph_traversal_row_factory)
            graph_options = ep.graph_options.copy()
            graph_options.graph_language = DseGraph.DSE_GRAPH_QUERY_LANGUAGE
            if self.graph_name:
                graph_options.graph_name = self.graph_name
            ep.graph_options = graph_options

            return query, ep

        @staticmethod
        def _handle_query_results(result_set, gremlin_future):
            try:
                traversers = [Traverser(t) for t in result_set]
                gremlin_future.set_result(
                    RemoteTraversal(iter(traversers), TraversalSideEffects())
                )
            except Exception as e:
                gremlin_future.set_exception(e)

        @staticmethod
        def _handle_query_error(response, gremlin_future):
            gremlin_future.set_exception(response)

        def submit(self, bytecode):
            # the only reason I don't use submitAsync here
            # is to avoid an unuseful future wrap
            query, ep = self._prepare_query(bytecode)

            traversers = self.session.execute_graph(query, execution_profile=ep)
            traversers = [Traverser(t) for t in traversers]
            return RemoteTraversal(iter(traversers), TraversalSideEffects())

        def submitAsync(self, bytecode):
            query, ep = self._prepare_query(bytecode)

            # to be compatible with gremlinpython, we need to return a concurrent.futures.Future
            gremlin_future = Future()
            response_future = self.session.execute_graph_async(query, execution_profile=ep)
            response_future.add_callback(self._handle_query_results, gremlin_future)
            response_future.add_errback(self._handle_query_error, gremlin_future)

            return gremlin_future

        def __str__(self):
            return "<DSESessionRemoteGraphConnection: graph_name='{0}'>".format(self.graph_name)

        __repr__ = __str__


    class DseGraph(object):
        """
        Dse Graph utility class for GraphTraversal construction and execution.
        """

        DSE_GRAPH_QUERY_LANGUAGE = 'bytecode-json'
        """
        Graph query language, Default is 'bytecode-json' (GraphSON).
        """

        @staticmethod
        def query_from_traversal(traversal):
            """
            From a GraphTraversal, return a query string based on the language specified in `DseGraph.DSE_GRAPH_QUERY_LANGUAGE`.

            :param traversal: The GraphTraversal object
            """

            if isinstance(traversal, GraphTraversal):
                for strategy in traversal.traversal_strategies.traversal_strategies:
                    rc = strategy.remote_connection
                    if (isinstance(rc, DSESessionRemoteGraphConnection) and
                            rc.session or rc.graph_name or rc.execution_profile):
                        log.warning("GraphTraversal session, graph_name and execution_profile are "
                                    "only taken into account when executed with TinkerPop.")

            return _query_from_traversal(traversal)

        @staticmethod
        def traversal_source(session=None, graph_name=None, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT,
                             traversal_class=None):
            """
            Returns a TinkerPop GraphTraversalSource binded to the session and graph_name if provided.

            :param session: (Optional) A DSE session
            :param graph_name: (Optional) DSE Graph name
            :param execution_profile: (Optional) Execution profile for traversal queries. Default is set to `EXEC_PROFILE_GRAPH_DEFAULT`.
            :param traversal_class: (Optional) The GraphTraversalSource class to use (DSL).

            .. code-block:: python

                from cassandra.cluster import Cluster
                from cassandra.datastax.graph.fluent import DseGraph

                c = Cluster()
                session = c.connect()

                g = DseGraph.traversal_source(session, 'my_graph')
                print g.V().valueMap().toList()

            """

            graph = Graph()
            traversal_source = graph.traversal(traversal_class)

            if session:
                traversal_source = traversal_source.withRemote(
                    DSESessionRemoteGraphConnection(session, graph_name, execution_profile))

            return traversal_source

        @staticmethod
        def create_execution_profile(graph_name):
            """
            Creates an ExecutionProfile for GraphTraversal execution. You need to register that execution profile to the
            cluster by using `cluster.add_execution_profile`.

            :param graph_name: The graph name
            """

            ep = GraphExecutionProfile(row_factory=graph_traversal_dse_object_row_factory,
                                       graph_options=GraphOptions(graph_name=graph_name,
                                                                  graph_language=DseGraph.DSE_GRAPH_QUERY_LANGUAGE,
                                                                  graph_protocol=GraphProtocol.GRAPHSON_2_0))
            return ep

        @staticmethod
        def batch(*args, **kwargs):
            """
            Returns the :class:`cassandra.datastax.graph.fluent.query.TraversalBatch` object allowing to
            execute multiple traversals in the same transaction.
            """
            return _DefaultTraversalBatch(*args, **kwargs)
