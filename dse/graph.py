from cassandra.query import SimpleStatement
from collections import namedtuple

import json

class AbstractGraphStatement:
    """
    Parent implementation of the Graph specific statements. It encapsulate the necessary work
    to produce a statement recognizable to the DSEQueryHandler.
    Child implementations will contain the graph_options field and will have to call self.configure()
    before being executed. They also need to implement the configure_and_get_wrapped() method that
    will be called by the GraphSession to transparently execute the wrapped statement.
    """
    _wrapped = None
    graph_options = {}

    def __init__(self):
        pass

    def configure(self, session_graph_options):
        """
        Handles the general tranformations to apply on the wrapped statement.
        """
        merged_options = session_graph_options.copy()
        merged_options.update(self.graph_options)
        self._wrapped.custom_payload = merged_options

    def configure_and_get_wrapped(self, graph_options):
        raise NotImplementedError()


class GraphStatement(AbstractGraphStatement):
    """
    A simple, un-prepared graph query.
    """

    def __init__(self, query_string, graph_options=None, *args, **kwargs):
        AbstractGraphStatement.__init__(self)
        self.query_string = query_string
        self.args = args

    def configure_and_get_wrapped(self, session_graph_options):
        self._wrapped = SimpleStatement(self.query_string)
        self.configure(session_graph_options)
        return self._wrapped


class PreparedGraphStatement(object):
    """
    """

    def __init__(self, prepared_statement, graph_statement):
        self.ps = prepared_statement
        self.gs = graph_statement

    def bind(self, values=None):
        return BoundGraphStatement(self.ps, self.gs).bind(values)


class BoundGraphStatement(AbstractGraphStatement):
    """
    """

    prepared_statement = None
    graph_statement = None

    def __init__(self, prepared_statement, graph_statement):
        self.ps = prepared_statement
        self.gs = graph_statement
        self.values = None

    def bind(self, values_param=None):
        values = None
        if values_param is None:
            values = {}
        else:
            values = values_param

        if not isinstance(values, dict):
            # TODO: change the exception class
            raise Exception('The values parameter can only be a dict, unnamed parameters are not authorized in Gremlin queries.')
        self.values = values
        return self

    def configure_and_get_wrapped(self, graph_options):
        # Transform the dict of parameters in a [{'name':name, 'value':value}] as imposed in the specs
        values_list = []
        for key, value in self.values.iteritems():
            json_param = json.dumps({'name':key, 'value':value})
            values_list.append(json_param)

        self._wrapped = self.ps.bind(values_list)
        self.configure(graph_options)

        return self._wrapped


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

    def _generate_traversal_result_tuple(result):
        json_result = json.loads(result)['result']
        GraphTraversalResult = namedtuple('GraphTraversalResult', json_result.keys())
        return GraphTraversalResult(*json_result.values())

class GraphSession(object):
    """
    A session object allowing to execute Gremlin queries against a DSEGraph cluster.
    """

    default_graph_options = {'graph-source':b'default', 'graph-language':b'gremlin-groovy'}

    def __init__(self, session, default_graph_options=None):
        self.session = session
        if default_graph_options:
            self.default_graph_options.update(default_graph_options)

    def execute(self, query, *args, **kwargs):
        if isinstance(query, AbstractGraphStatement):
            return self._execute(query, args, kwargs)
        else:
            graph_statement = GraphStatement(query, args)
            return self._execute(graph_statement, args, kwargs)

    def _execute(self, graph_statement, *args, **kwargs):
        statement = graph_statement.configure_and_get_wrapped(self.default_graph_options)
        return GraphResultSet(self.session.execute(statement, args, kwargs))

    def prepare(self, query, *args, **kwargs):
        # Should maybe only accept GraphStatement instances instead of AbstractGraphStatement.
        if isinstance(query, AbstractGraphStatement):
            return self._prepare(query, args, kwargs)
        else:
            graph_statement = GraphStatement(query, args)
            return self._prepare(graph_statement, args, kwargs)

    def _prepare(self, graph_statement, *args, **kwargs):
        return PreparedGraphStatement(self.session.prepare(graph_statement.configure_and_get_wrapped(self.default_graph_options), args), graph_statement)

