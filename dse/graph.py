from cassandra.query import SimpleStatement

import json
import six

# (attr, description, server option)
_graph_options = (
    ('graph_namespace', 'define the namespace the graph is defined with.', 'graph-keyspace'),
    ('graph_source', 'choose the graph traversal source, configured on the server side.', 'graph-source'),
    ('graph_language', 'the language used in the queries (default "gremlin-groovy"', 'graph-language'),
    ('graph_rebinding', 'name of the graph in the query (default "g")', 'graph-rebinding')
)


class GraphOptions(object):

    def __init__(self, **kwargs):
        self._graph_options = {}
        for attr, value in six.iteritems(kwargs):
            setattr(self, attr, value)

    def update(self, options):
        self._graph_options.update(options._graph_options)

    def get_options_map(self, base_options):
        """
        Returns a map for base_options updated with options set on this object, or
        base_options map if none were set.
        """
        if self._graph_options:
            options = base_options._graph_options.copy()
            options.update(self._graph_options)
            return options
        else:
            return base_options._graph_options


for opt in _graph_options:

    def get(self, key=opt[2]):
        return self._graph_options.get(key)

    def set(self, value, key=opt[2]):
        if value:
            if not isinstance(value, six.binary_type):
                value = six.b(value)
            self._graph_options[key] = value
        else:
            self._graph_options.pop(key)

    def delete(self, key=opt[2]):
        self._graph_options.pop(key)

    setattr(GraphOptions, opt[0], property(get, set, delete, opt[1]))


class GraphStatement(SimpleStatement):

    options = None
    """
    GraphOptions for this statement.
    Any attributes set here override the GraphSession defaults.
    """

    def __init__(self, *args, **kwargs):
        super(GraphStatement, self).__init__(*args, **kwargs)
        self.options = GraphOptions()


def single_object_row_factory(column_names, rows):
    """
    returns the JSON string value of graph results
    """
    return [row[0] for row in rows]


def graph_result_row_factory(column_names, rows):
    """
    Returns an object that can load graph results and produce specific types
    """
    return [Result(row[0]) for row in rows]


class Result(object):

    value = None
    """
    Deserialized value from the result
    """

    def __init__(self, json_value):
        self.value = json.loads(json_value)['result']

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
        return "%s(%r)" % (Result.__name__, json.dumps({'result': self.value}))


_NOT_SET = object()


class GraphSession(object):
    """
    A session wrapper for executing Gremlin queries against a DSE cluster
    """

    default_graph_options = None
    """
    Default options, initialized as follows if no additional options are provided
    in init:
    GraphOptions(graph_source=b'default',
                 graph_language=b'gremlin-groovy')
    """

    default_graph_row_factory = staticmethod(graph_result_row_factory)

    # Expose the session or private?
    session = None

    def __init__(self, session, default_graph_options=None):
        """
        :param session: Session to use for graph queries
        :param default_graph_options: a GraphOptions object; options are merged with built-in defaults
        """
        self.session = session
        self.default_graph_options = GraphOptions(graph_source=b'default',
                                                  graph_language=b'gremlin-groovy')
        if default_graph_options:
            self.default_graph_options.update(default_graph_options)

    def execute(self, query, parameters=None, timeout=_NOT_SET, trace=False, row_factory=None):
        """
        Executes a Gremlin query string, a GraphStatement synchronously,
        and returns a GraphResultSet from this execution.
        """
        if isinstance(query, GraphStatement):
            options = query.options.get_options_map(self.default_graph_options)
        else:
            query = GraphStatement(query)
            options = self.default_graph_options._graph_options

        graph_parameters = None
        if parameters:
            graph_parameters = self._transform_params(parameters)

        # TODO:
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
            raise ValueError('The parameters must be a dictionary. Unnamed parameters are not allowed.')
        return [json.dumps({'name': name, 'value': value}).encode('utf-8') for name, value in six.iteritems(parameters)]
