from cassandra.query import SimpleStatement

from collections import namedtuple
import json
import six

# (attr, server option, description)
_graph_options = (
('graph_namespace', 'define the namespace the graph is defined with.', 'graph-keyspace'),
('graph_source', 'choose the graph traversal source, configured on the server side.', 'graph-source'),
('graph_language', 'the language used in the queries (default "gremlin-groovy"', 'graph-language'),
('graph_rebinding', 'name of the graph in the query (default "g")', 'graph-rebinding')
)


class GraphStatement(SimpleStatement):

    def __init__(self, *args, **kwargs):
        super(GraphStatement, self).__init__(*args, **kwargs)
        self._graph_options = {}

    def get_options_map(self, base_options):
        if self._graph_options:
            options = base_options.copy()
            options.update(self._graph_options)
            return options
        else:
            return base_options


for opt in _graph_options:

    key = opt[2]

    def get(self):
        return self._graph_options.get(key)

    def set(self, value):
        if value:
            self._graph_options[key] = value
        else:
            self._graph_options.pop(key)

    def delete(self):
        self._graph_options.pop(key)

    setattr(GraphStatement, opt[0], property(get, set, delete, opt[1]))


def single_object_row_factory(column_names, rows):
    # returns the string value of graph results
    return [row[0] for row in rows]


def graph_result_row_factory(column_names, rows):
    # Returns an object that can load graph results and produce specific types
    return [Result(row[0]) for row in rows]


class Result(object):

    value = None

    def __init__(self, json_value):
        self.value = json.loads(json_value)['result']

    def __getattr__(self, attr):
        if not isinstance(self.value, dict):
            raise ValueError("Value cannot be accessed as a dict")

        if attr in self.value:
            return self.value[attr]

        raise AttributeError("Result has no top-level attribute %r" % (attr,))

    def __getitem__(self, item):
        if isinstance(self.value, dict) and isinstance(key, six.string_types):
            return self.value[item]
        elif isinstance(self.value, list) and isinstance(key, int):
            raise TypeError("Key must be a string")
        else:
            raise ValueError("Result cannot be indexed by %r" %(item,))

    def __str__(self):
        return self.value

    def __repr__(self):
        return "%s(%r)" % (Result.__name__, json.dumps({'result': self.value}))


_NOT_SET = object()


class GraphSession(object):
    """
    A session object allowing to execute Gremlin queries against a DSEGraph cluster.
    """

    """
    The default options to be used for all graph statements executions. Is initialised with :
        - graph-source = 'default'
        - graph-language = 'gremlin-groovy'

    These options can be over redefined, or removed.
    """
    default_graph_options = None

    default_graph_row_factory = staticmethod(graph_result_row_factory)

    # Expose the session or private?
    session = None

    def __init__(self, session, default_graph_options=None):
        self.session = session
        self.default_graph_options = {'graph-source': b'default', 'graph-language': b'gremlin-groovy'}
        if default_graph_options:
            self.default_graph_options.update(default_graph_options)

    def execute(self, query, parameters=None, timeout=_NOT_SET, trace=False, row_factory=None):
        """
        Executes a Gremlin query string, a GraphStatement, or a BoundGraphStatement synchronously,
        and returns a GraphResultSet from this execution.
        """
        if isinstance(query, GraphStatement):
            options = query.get_options_map(self.default_graph_options)
        else:
            query = GraphStatement(query)
            options = self.default_graph_options

        graph_parameters = None
        if parameters:
            graph_parameters = self._transform_params(parameters)

        # TODO: pass down trace, timeout parameters
        # this is basically Session.execute_async, repeated here to customize the row factory. May want to add that
        # parameter to the session method
        if timeout is _NOT_SET:
            timeout = self.session.default_timeout
        future = self.session._create_response_future(query, parameters=None, trace=trace, custom_payload=options, timeout=timeout)
        future.message._query_params = graph_parameters
        future._protocol_handler = self.session.client_protocol_handler
        future.row_factory = row_factory or self.default_graph_row_factory
        future.send_request()
        return future.result()

    # this may go away if we change parameter encoding
    def _transform_params(self, parameters):
        if not isinstance(parameters, dict):
            raise Exception('The values parameter can only be a dictionary, unnamed parameters are not authorized in Gremlin queries.')
        return [json.dumps({'name': name, 'value': value}) for name, value in six.iteritems(parameters)]

