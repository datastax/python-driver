from cassandra.query import SimpleStatement
from collections import namedtuple

import json

class AbstractGraphStatement:
    """
    Parent implementation of the Graph specific statements. It encapsulate the necessary work
    to produce a statement recognizable to the DSEQueryHandler.
    Child implementations will contain the graph_options field and will have to call self.configure()
    before being executed. They also need to implement the _configure_and_get_wrapped() method that
    will be called by the GraphSession to transparently execute the wrapped statement.
    """
    _wrapped = None

    """
    The graph specific options that will be included in the execution of the statement.
    The 4 available graph options are : 
        - graph-keyspace : define the keyspace the graph is defined with.
        - graph-source : choose the graph traversal source, configured on the server side.
        - graph-language : the language used in the queries.
        - graph-rebinding : rename the traversal source.
    """
    graph_options = {}

    def __init__(self):
        pass

    def _configure(self, session_graph_options):
        # Handles the general tranformations to apply on the wrapped statement.
        merged_options = session_graph_options.copy()
        merged_options.update(self.graph_options)
        self._wrapped.custom_payload = merged_options

    def _configure_and_get_wrapped(self, graph_options):
        raise NotImplementedError()


class GraphStatement(AbstractGraphStatement):
    """
    A simple, un-prepared graph query.
    """

    """
    The query string used in this statement.
    """
    query_string = None


    def __init__(self, query_string, graph_options=None):
        AbstractGraphStatement.__init__(self)
        self.query_string = query_string

    def _configure_and_get_wrapped(self, session_graph_options):
        self._wrapped = SimpleStatement(self.query_string)
        self._configure(session_graph_options)
        return self._wrapped


class PreparedGraphStatement(object):
    """
    A prepared graph statement returned from the execution of a GraphSession.prepare()
    method. This query corresponding to this statement is supposed to contain Gremlin 
    parameters that will eventually be bound.
    """

    _prepared_statement = None
    _graph_statement = None

    def __init__(self, prepared_statement, graph_statement):
        self._prepared_statement = prepared_statement
        self._graph_statement = graph_statement

    def bind(self, values=None):
        """
        Add parameters values to this prepared statement. This will create a BoundGraphStatement
        instance. It is possible to not provide values for now, but call bind() later on the 
        BoundGraphStatement.
        """
        return BoundGraphStatement(self._prepared_statement, self._graph_statement).bind(values)


class BoundGraphStatement(AbstractGraphStatement):
    """
    A statement bound with values resulting from the preparation of a simple graph statement.
    """

    _prepared_statement = None
    _graph_statement = None


    """
    The values of the parameters to be sent with the statement.
    """
    values = None

    def __init__(self, prepared_statement, graph_statement):
        self._prepared_statement = prepared_statement
        self._graph_statement = graph_statement
        self.values = None

        self.graph_options.update(graph_statement.graph_options)

    def bind(self, values_param=None):
        """
        Add values that values will be added to the execution of the previously prepared statement.
        The values bound can only be in the form of a dictionary, meaning that each Gremlin parameter
        must be named, and bound with its name as well.
        """
        values = None
        if values_param is None:
            values = {}
        else:
            values = values_param

        if not isinstance(values, dict):
            # TODO: change the exception class
            raise Exception('The values parameter can only be a dictionary, unnamed parameters are not authorized in Gremlin queries.')

        self.values = values
        return self

    def _configure_and_get_wrapped(self, graph_options):
        # Transform the dict of parameters in a [{'name':name, 'value':value}] as imposed in the specs
        values_list = []
        for key, value in self.values.iteritems():
            json_param = json.dumps({'name':key, 'value':value})
            values_list.append(json_param)

        self._wrapped = self._prepared_statement.bind(values_list)

        # We want to propagate the options from the original GraphStatement.
        self._configure(graph_options)

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
    default_graph_options = {'graph-source':b'default', 'graph-language':b'gremlin-groovy'}

    # Expose the session or private?
    session = None

    def __init__(self, session, default_graph_options=None):
        self.session = session
        if default_graph_options:
            self.default_graph_options.update(default_graph_options)

    def execute(self, query):
        """
        Executes a Gremlin query string, a GraphStatement, or a BoundGraphStatement synchronously, 
        and returns a GraphResultSet from this execution.
        """
        if isinstance(query, AbstractGraphStatement):
            return self._execute(query)
        else:
            graph_statement = GraphStatement(query)
            return self._execute(graph_statement)

    def _execute(self, graph_statement):
        statement = graph_statement._configure_and_get_wrapped(self.default_graph_options)
        return GraphResultSet(self.session.execute(statement))

    def prepare(self, query):
        """
        Prepares a Gremlin query string, or a GraphStatement. This returns a PreparedGraphStatement
        resulting of the Cassandra prepare phase.
        """
        # Should maybe only accept GraphStatement instances instead of AbstractGraphStatement.
        if isinstance(query, AbstractGraphStatement):
            return self._prepare(query)
        else:
            graph_statement = GraphStatement(query)
            return self._prepare(graph_statement)

    def _prepare(self, graph_statement):
        statement = graph_statement._configure_and_get_wrapped(self.default_graph_options)
        return PreparedGraphStatement(self.session.prepare(statement), graph_statement)

