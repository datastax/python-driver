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
import json
from warnings import warn

import six

from cassandra import ConsistencyLevel
from cassandra.query import Statement, SimpleStatement
from cassandra.datastax.graph.types import Vertex, Edge, Path
from cassandra.datastax.graph.graphson import GraphSON2Reader

HAVE_GREMLIN = False
try:
    import gremlin_python
    HAVE_GREMLIN = True
except ImportError:
    # gremlinpython is not installed.
    pass


__all__ = [
    'GraphProtocol', 'GraphOptions', 'GraphStatement', 'SimpleGraphStatement',
    'single_object_row_factory', 'graph_result_row_factory', 'graph_object_row_factory',
    'graph_graphson2_row_factory', 'Result'
]

# (attr, description, server option)
_graph_options = (
    ('graph_name', 'name of the targeted graph.', 'graph-name'),
    ('graph_source', 'choose the graph traversal source, configured on the server side.', 'graph-source'),
    ('graph_language', 'the language used in the queries (default "gremlin-groovy")', 'graph-language'),
    ('graph_protocol', 'the graph protocol that the server should use for query results (default "graphson-1-0")', 'graph-results'),
    ('graph_read_consistency_level', '''read `dse.ConsistencyLevel <http://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/#cassandra.ConsistencyLevel>`_ for graph queries (if distinct from session default).
Setting this overrides the native `Statement.consistency_level <http://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/query/#cassandra.query.Statement.consistency_level>`_ for read operations from Cassandra persistence''', 'graph-read-consistency'),
    ('graph_write_consistency_level', '''write `dse.ConsistencyLevel <http://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/#cassandra.ConsistencyLevel>`_ for graph queries (if distinct from session default).
Setting this overrides the native `Statement.consistency_level <http://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/query/#cassandra.query.Statement.consistency_level>`_ for write operations to Cassandra persistence.''', 'graph-write-consistency')
)
_graph_option_names = tuple(option[0] for option in _graph_options)

# this is defined by the execution profile attribute, not in graph options
_request_timeout_key = 'request-timeout'

_graphson2_reader = GraphSON2Reader()


class GraphProtocol(object):

    GRAPHSON_1_0 = 'graphson-1.0'
    """
    GraphSON1
    """

    GRAPHSON_2_0 = 'graphson-2.0'
    """
    GraphSON2
    """


class GraphOptions(object):
    """
    Options for DSE Graph Query handler.
    """
    # See _graph_options map above for notes on valid options

    def __init__(self, **kwargs):
        self._graph_options = {}
        kwargs.setdefault('graph_source', 'g')
        kwargs.setdefault('graph_language', 'gremlin-groovy')
        kwargs.setdefault('graph_protocol', GraphProtocol.GRAPHSON_1_0)
        for attr, value in six.iteritems(kwargs):
            if attr not in _graph_option_names:
                warn("Unknown keyword argument received for GraphOptions: {0}".format(attr))
            setattr(self, attr, value)

    def copy(self):
        new_options = GraphOptions()
        new_options._graph_options = self._graph_options.copy()
        return new_options

    def update(self, options):
        self._graph_options.update(options._graph_options)

    def get_options_map(self, other_options=None):
        """
        Returns a map for these options updated with other options,
        and mapped to graph payload types.
        """
        options = self._graph_options.copy()
        if other_options:
            options.update(other_options._graph_options)

        # cls are special-cased so they can be enums in the API, and names in the protocol
        for cl in ('graph-write-consistency', 'graph-read-consistency'):
            cl_enum = options.get(cl)
            if cl_enum is not None:
                options[cl] = six.b(ConsistencyLevel.value_to_name[cl_enum])
        return options

    def set_source_default(self):
        """
        Sets ``graph_source`` to the server-defined default traversal source ('default')
        """
        self.graph_source = 'default'

    def set_source_analytics(self):
        """
        Sets ``graph_source`` to the server-defined analytic traversal source ('a')
        """
        self.graph_source = 'a'

    def set_source_graph(self):
        """
        Sets ``graph_source`` to the server-defined graph traversal source ('g')
        """
        self.graph_source = 'g'

    def set_graph_protocol(self, protocol):
        """
        Sets ``graph_protocol`` as server graph results format (See :class:`dse.graph.GraphProtocol`)
        """
        self.graph_protocol = protocol

    @property
    def is_default_source(self):
        return self.graph_source in (b'default', None)

    @property
    def is_analytics_source(self):
        """
        True if ``graph_source`` is set to the server-defined analytics traversal source ('a')
        """
        return self.graph_source == b'a'

    @property
    def is_graph_source(self):
        """
        True if ``graph_source`` is set to the server-defined graph traversal source ('g')
        """
        return self.graph_source == b'g'


for opt in _graph_options:

    def get(self, key=opt[2]):
        return self._graph_options.get(key)

    def set(self, value, key=opt[2]):
        if value is not None:
            # normalize text here so it doesn't have to be done every time we get options map
            if isinstance(value, six.text_type) and not isinstance(value, six.binary_type):
                value = six.b(value)
            self._graph_options[key] = value
        else:
            self._graph_options.pop(key, None)

    def delete(self, key=opt[2]):
        self._graph_options.pop(key, None)

    setattr(GraphOptions, opt[0], property(get, set, delete, opt[1]))


class GraphStatement(Statement):
    """ An abstract class representing a graph query."""

    @property
    def query(self):
        raise NotImplementedError()

    def __str__(self):
        return u'<GraphStatement query="{0}">'.format(self.query)
    __repr__ = __str__


class SimpleGraphStatement(GraphStatement, SimpleStatement):
    """
    Simple graph statement for :meth:`.Session.execute_graph`.
    Takes the same parameters as :class:`.SimpleStatement`.
    """
    @property
    def query(self):
        return self._query_string


def single_object_row_factory(column_names, rows):
    """
    returns the JSON string value of graph results
    """
    return [row[0] for row in rows]


def graph_result_row_factory(column_names, rows):
    """
    Returns a :class:`cassandra.graph.Result` object that can load graph results and produce specific types.
    The Result JSON is deserialized and unpacked from the top-level 'result' dict.
    """
    return [Result(json.loads(row[0])['result']) for row in rows]


def graph_object_row_factory(column_names, rows):
    """
    Like :func:`~.graph_result_row_factory`, except known element types (:class:`~.Vertex`, :class:`~.Edge`) are
    converted to their simplified objects. Some low-level metadata is shed in this conversion. Unknown result types are
    still returned as :class:`dse.graph.Result`.
    """
    return _graph_object_sequence(json.loads(row[0])['result'] for row in rows)


def _graph_object_sequence(objects):
    for o in objects:
        res = Result(o)
        if isinstance(o, dict):
            typ = res.value.get('type')
            if typ == 'vertex':
                res = res.as_vertex()
            elif typ == 'edge':
                res = res.as_edge()
        yield res


def graph_graphson2_row_factory(column_names, rows):
    """
    Row Factory that returns the decoded graphson as DSE types.
    """
    return [_graphson2_reader.read(row[0])['result'] for row in rows]


class Result(object):
    """
    Represents deserialized graph results.
    Property and item getters are provided for convenience.
    """

    value = None
    """
    Deserialized value from the result
    """

    def __init__(self, value):
        self.value = value

    def __getattr__(self, attr):
        if not isinstance(self.value, dict):
            raise ValueError("Value cannot be accessed as a dict")

        if attr in self.value:
            return self.value[attr]

        raise AttributeError("Result has no top-level attribute %r" % (attr,))

    def __getitem__(self, item):
        if isinstance(self.value, dict) and isinstance(item, six.string_types):
            return self.value[item]
        elif isinstance(self.value, list) and isinstance(item, int):
            return self.value[item]
        else:
            raise ValueError("Result cannot be indexed by %r" % (item,))

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "%s(%r)" % (Result.__name__, self.value)

    def __eq__(self, other):
        return self.value == other.value

    def as_vertex(self):
        """
        Return a :class:`Vertex` parsed from this result

        Raises TypeError if parsing fails (i.e. the result structure is not valid).
        """
        try:
            return Vertex(self.id, self.label, self.type, self.value.get('properties', {}))
        except (AttributeError, ValueError, TypeError):
            raise TypeError("Could not create Vertex from %r" % (self,))

    def as_edge(self):
        """
        Return a :class:`Edge` parsed from this result

        Raises TypeError if parsing fails (i.e. the result structure is not valid).
        """
        try:
            return Edge(self.id, self.label, self.type, self.value.get('properties', {}),
                        self.inV, self.inVLabel, self.outV, self.outVLabel)
        except (AttributeError, ValueError, TypeError):
            raise TypeError("Could not create Edge from %r" % (self,))

    def as_path(self):
        """
        Return a :class:`Path` parsed from this result

        Raises TypeError if parsing fails (i.e. the result structure is not valid).
        """
        try:
            return Path(self.labels, self.objects)
        except (AttributeError, ValueError, TypeError):
            raise TypeError("Could not create Path from %r" % (self,))


# Fluent API
if HAVE_GREMLIN:
    from gremlin_python.process.graph_traversal import GraphTraversal
    from gremlin_python.structure.io.graphsonV2d0 import GraphSONWriter

    from cassandra.datastax.graph.serializers import serializers

    log = logging.getLogger(__name__)

    graphson_writer = GraphSONWriter(serializer_map=serializers)


    def _query_from_traversal(traversal):
        """
        From a GraphTraversal, return a query string.

        :param traversal: The GraphTraversal object
        """
        try:
            query = graphson_writer.writeObject(traversal)
        except Exception:
            log.exception("Error preparing graphson traversal query:")
            raise

        return query


    class TraversalBatch(object):
        """
        A `TraversalBatch` is used to execute multiple graph traversals in a
        single transaction. If any traversal in the batch fails, the entire
        batch will fail to apply.

        If a TraversalBatch is bounded to a DSE session, it can be executed using
        `traversal_batch.execute()`.
        """

        _session = None
        _execution_profile = None

        def __init__(self, session=None, execution_profile=None):
            """
            :param session: (Optional) A DSE session
            :param execution_profile: (Optional) The execution profile to use for the batch execution
            """
            self._session = session
            self._execution_profile = execution_profile

        def add(self, traversal):
            """
            Add a traversal to the batch.

            :param traversal: A gremlin GraphTraversal
            """
            raise NotImplementedError()

        def add_all(self, traversals):
            """
            Adds a sequence of traversals to the batch.

            :param traversals: A sequence of gremlin GraphTraversal
            """
            raise NotImplementedError()

        def execute(self):
            """
            Execute the traversal batch if bounded to a `DSE Session`.
            """
            raise NotImplementedError()

        def as_graph_statement(self):
            """
            Return the traversal batch as GraphStatement.
            """
            raise NotImplementedError()

        def clear(self):
            """
            Clear a traversal batch for reuse.
            """
            raise NotImplementedError()

        def __len__(self):
            raise NotImplementedError()

        def __str__(self):
            return u'<TraversalBatch traversals={0}>'.format(len(self))

        __repr__ = __str__


    class _DefaultTraversalBatch(TraversalBatch):

        _traversals = None

        def __init__(self, *args, **kwargs):
            super(_DefaultTraversalBatch, self).__init__(*args, **kwargs)
            self._traversals = []

        @property
        def _query(self):
            return u"[{0}]".format(','.join(self._traversals))

        def add(self, traversal):
            if not isinstance(traversal, GraphTraversal):
                raise ValueError('traversal should be a gremlin GraphTraversal')

            query = _query_from_traversal(traversal)
            self._traversals.append(query)

            return self

        def add_all(self, traversals):
            for traversal in traversals:
                self.add(traversal)

        def as_graph_statement(self):
            return SimpleGraphStatement(self._query)

        def execute(self):
            if self._session is None:
                raise ValueError('A DSE Session must be provided to execute the traversal batch.')

            from cassandra.cluster import EXEC_PROFILE_GRAPH_DEFAULT
            execution_profile = self._execution_profile if self._execution_profile else EXEC_PROFILE_GRAPH_DEFAULT
            return self._session.execute_graph(self._query, execution_profile=execution_profile)

        def clear(self):
            del self._traversals[:]

        def __len__(self):
            return len(self._traversals)