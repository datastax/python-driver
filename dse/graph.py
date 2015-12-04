from cassandra.query import SimpleStatement
from collections import namedtuple

import json

class AbstractGraphStatement:
    """
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


class GraphResultSet(object):
    """
    """

    def __init__(self, result_set):
        self._wrapped_rs = result_set

    def __iter__(self):
        # Not great but doing iter(self._wrapped_rs); return self; Does not work-
        # -with the paging and __getitem__ on the wrapped result set.
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
    """

    default_graph_options = {'graph-source':b'default', 'graph-language':b'gremlin-groovy'}

    def __init__(self, session, default_graph_options=None):
        self.session = session
        if default_graph_options:
            self.default_graph_options.update(default_graph_options)

    def execute(self, query, *args, **kwargs):
        if (isinstance(query, AbstractGraphStatement)):
            return GraphResultSet(self.session.execute(query.configure_and_get_wrapped(self.default_graph_options)))
        else:
            return GraphResultSet(self.session.execute(query, custom_payload=self.default_graph_options))
