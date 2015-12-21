from cassandra.query import SimpleStatement

from collections import namedtuple
import json
import six

# (attr, server option, description)
_graph_options = (
('graph_keyspace', 'define the keyspace the graph is defined with.', 'graph-keyspace'),
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


class GraphResultSet(object):
    """
    A results set containing multiple GraphTraversalResult objects.
    """

    def __init__(self, result_set):
        self._wrapped_rs = result_set

    def __iter__(self):
        # Not great but doing iter(self._wrapped_rs); return self; Does not work-
        # -because of the paging and __getitem__ on the wrapped result set.
        return GraphResultSet(iter(self._wrapped_rs))

    def __getitem__(self, i):
        return self._generate_traversal_result_tuple(self._wrapped_rs[i].gremlin)

    def next(self):
        return self._generate_traversal_result_tuple(self._wrapped_rs.next().gremlin)

    __next__ = next

    def _generate_traversal_result_tuple(self, result):
        json_result = json.loads(result)['result']
        GraphTraversalResult = namedtuple('GraphTraversalResult', json_result.keys())
        return GraphTraversalResult(*json_result.values())


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

    # Expose the session or private?
    session = None

    def __init__(self, session, default_graph_options=None):
        self.session = session
        self.default_graph_options = {'graph-source': b'default', 'graph-language': b'gremlin-groovy'}
        if default_graph_options:
            self.default_graph_options.update(default_graph_options)

    def execute(self, query, parameters=None):
        """
        Executes a Gremlin query string, a GraphStatement, or a BoundGraphStatement synchronously, 
        and returns a GraphResultSet from this execution.
        """
        if isinstance(query, GraphStatement):
            return self._execute(query, query.get_options_map(self.default_graph_options), parameters)
        else:
            graph_statement = GraphStatement(query)
            return self._execute(graph_statement, self.default_graph_options, parameters)

    def _execute(self, statement, options, parameters):
        graph_parameters = None
        if parameters:
            graph_parameters = self._transform_params(parameters)
        return GraphResultSet(self.session.execute(statement, graph_parameters, custom_payload=options))

    # this may go away if we change parameter encoding
    def _transform_params(self, parameters):
        if not isinstance(parameters, dict):
            raise Exception('The values parameter can only be a dictionary, unnamed parameters are not authorized in Gremlin queries.')
        return [json.dumps({'name': name, 'value': value}) for name, value in six.iteritems(parameters)]
