from cassandra.query import SimpleStatement
from collections import namedtuple

import json


class GraphStatement(SimpleStatement):
    """
    A simple, un-prepared graph query.
    """

    def __init__(self, query_string, *args, **kwargs):
        """
        """
        SimpleStatement.__init__(self, query_string, *args, **kwargs)
        self.graph_options = {}

class GraphResultSet(object):
    wrapped_rs = None

    def __init__(self, result_set):
        self.wrapped_rs = result_set

    def __iter__(self):
        # Not great but doing iter(self.wrapped_rs); return self; Does not work-
        # -with the paging and __getitem__ on the wrapped result set.
        return GraphResultSet(iter(self.wrapped_rs))

    def __getitem__(self, i):
        return self._generate_traversal_result_tuple(self.wrapped_rs[i].gremlin)

    def next(self):
        return self._generate_traversal_result_tuple(self.wrapped_rs.next().gremlin)

    __next__ = next

    def _generate_traversal_result_tuple(self, result):
        json_result = json.loads(result)['result']
        GraphTraversalResult = namedtuple('GraphTraversalResult', json_result.keys())
        return GraphTraversalResult(*json_result.values())

class GraphSession(object):

    default_graph_options = {'graph-source':b'default', 'graph-language':b'gremlin-groovy'}

    def __init__(self, session, default_graph_options=None):
        self.session = session
        if default_graph_options:
            self.default_graph_options.update(default_graph_options)

    def execute(self, query, *args, **kwargs):
        if (isinstance(query, GraphStatement)):
            z = self.default_graph_options.copy()
            z.update(query.graph_options)
            return GraphResultSet(self.session.execute(query, custom_payload=z))
        else:
            return GraphResultSet(self.session.execute(query, custom_payload=self.default_graph_options))
