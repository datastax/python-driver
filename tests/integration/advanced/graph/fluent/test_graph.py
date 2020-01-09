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


import sys

from concurrent.futures import Future

from cassandra.datastax.graph.fluent import DseGraph
from gremlin_python.process.graph_traversal import GraphTraversal, GraphTraversalSource
from gremlin_python.process.traversal import P
from tests.integration import DSE_VERSION, requiredse, greaterthanorequaldse60
from tests.integration.advanced import BasicGraphUnitTestCase, use_single_node_with_graph_and_solr, \
    use_single_node_with_graph, generate_classic, generate_line_graph, generate_multi_field_graph, \
    generate_large_complex_graph, generate_type_graph_schema, validate_classic_vertex, validate_classic_edge, \
    validate_generic_vertex_result_type, validate_classic_edge_properties, validate_line_edge, \
    validate_generic_edge_result_type, validate_path_result_type, TYPE_MAP


from gremlin_python.structure.graph import Edge as TravEdge
from gremlin_python.structure.graph import Vertex as TravVertex
from cassandra.graph import Vertex, Edge
from cassandra.util import Point, Polygon, LineString
import datetime
from six import string_types
import six
if six.PY3:
    import ipaddress


def setup_module():
    if DSE_VERSION:
        dse_options = {'graph': {'realtime_evaluation_timeout_in_seconds': 60}}
        use_single_node_with_graph(dse_options=dse_options)


def check_equality_base(testcase, original, read_value):
    if isinstance(original, float):
        testcase.assertAlmostEqual(original, read_value, delta=.01)
    elif six.PY3 and isinstance(original, ipaddress.IPv4Address):
        testcase.assertAlmostEqual(original, ipaddress.IPv4Address(read_value))
    elif six.PY3 and isinstance(original, ipaddress.IPv6Address):
        testcase.assertAlmostEqual(original, ipaddress.IPv6Address(read_value))
    else:
        testcase.assertEqual(original, read_value)


class AbstractTraversalTest():

    def test_basic_query(self):
        """
        Test to validate that basic graph queries works

        Creates a simple classic tinkerpot graph, and attempts to preform a basic query
        using Tinkerpop's GLV with both explicit and implicit execution
        ensuring that each one is correct. See reference graph here
        http://www.tinkerpop.com/docs/3.0.0.M1/

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result graph should generate and all vertices and edge results should be

        @test_category dse graph
        """


        g = self.fetch_traversal_source()
        generate_classic(self.session)
        traversal =g.V().has('name', 'marko').out('knows').values('name')
        results_list = self.execute_traversal(traversal)
        self.assertEqual(len(results_list), 2)
        self.assertIn('vadas', results_list)
        self.assertIn('josh', results_list)

    def test_classic_graph(self):
        """
        Test to validate that basic graph generation, and vertex and edges are surfaced correctly

        Creates a simple classic tinkerpot graph, and iterates over the the vertices and edges
        using Tinkerpop's GLV with both explicit and implicit execution
        ensuring that each one iscorrect. See reference graph here
        http://www.tinkerpop.com/docs/3.0.0.M1/

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result graph should generate and all vertices and edge results should be

        @test_category dse graph
        """

        generate_classic(self.session)
        g = self.fetch_traversal_source()
        traversal =  g.V()
        vert_list = self.execute_traversal(traversal)

        for vertex in vert_list:
            self._validate_classic_vertex(g, vertex)
        traversal =  g.E()
        edge_list = self.execute_traversal(traversal)
        for edge in edge_list:
            self._validate_classic_edge(g, edge)

    def test_graph_classic_path(self):
        """
        Test to validate that the path version of the result type is generated correctly. It also
        tests basic path results as that is not covered elsewhere

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result path object should be unpacked correctly including all nested edges and verticies
        @test_category dse graph
        """
        generate_classic(self.session)
        g = self.fetch_traversal_source()
        traversal = g.V().hasLabel('person').has('name', 'marko').as_('a').outE('knows').inV().as_('c', 'd').outE('created').as_('e', 'f', 'g').inV().path()
        path_list = self.execute_traversal(traversal)
        self.assertEqual(len(path_list), 2)
        for path in path_list:
            self._validate_path_result_type(g, path)


    def test_range_query(self):
        """
        Test to validate range queries are handled correctly.

        Creates a very large line graph script and executes it. Then proceeds to to a range
        limited query against it, and ensure that the results are formated correctly and that
        the result set is properly sized.

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result result set should be properly formated and properly sized

        @test_category dse graph
        """


        query_to_run = generate_line_graph(150)
        self.session.execute_graph(query_to_run)
        g = self.fetch_traversal_source()

        traversal = g.E().range(0,10)
        edges = self.execute_traversal(traversal)
        self.assertEqual(len(edges), 10)
        for edge in edges:
            self._validate_line_edge(g, edge)

    def test_result_types(self):
        """
        Test to validate that the edge and vertex version of results are constructed correctly.

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result edge/vertex result types should be unpacked correctly.
        @test_category dse graph
        """
        generate_multi_field_graph(self.session)  # TODO: we could just make a single vertex with properties of all types, or even a simple query that just uses a sequence of groovy expressions
        g = self.fetch_traversal_source()
        traversal = g.V()
        vertices = self.execute_traversal(traversal)
        for vertex in vertices:
            self._validate_type(g, vertex)

    def test_large_result_set(self):
        """
        Test to validate that large result sets return correctly.

        Creates a very large graph. Ensures that large result sets are handled appropriately.

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result when limits of result sets are hit errors should be surfaced appropriately

        @test_category dse graph
        """
        generate_large_complex_graph(self.session, 5000)
        g = self.fetch_traversal_source()
        traversal = g.V()
        vertices = self.execute_traversal(traversal)
        for vertex in vertices:
            self._validate_generic_vertex_result_type(g,vertex)

    def test_vertex_meta_properties(self):
        """
        Test verifying vertex property properties

        @since 1.0.0
        @jira_ticket PYTHON-641

        @test_category dse graph
        """
        s = self.session
        s.execute_graph("schema.propertyKey('k0').Text().ifNotExists().create();")
        s.execute_graph("schema.propertyKey('k1').Text().ifNotExists().create();")
        s.execute_graph("schema.propertyKey('key').Text().properties('k0', 'k1').ifNotExists().create();")
        s.execute_graph("schema.vertexLabel('MLP').properties('key').ifNotExists().create();")
        s.execute_graph("schema.config().option('graph.allow_scan').set('true');")
        v = s.execute_graph('''v = graph.addVertex('MLP')
                                 v.property('key', 'meta_prop', 'k0', 'v0', 'k1', 'v1')
                                 v''')[0]

        g = self.fetch_traversal_source()

        traversal = g.V()
        # This should contain key, and value where value is a property
        # This should be a vertex property and should contain sub properties
        results = self.execute_traversal(traversal)
        self._validate_meta_property(g, results[0])

    def test_vertex_multiple_properties(self):
        """
        Test verifying vertex property form for various Cardinality

        All key types are encoded as a list, regardless of cardinality

        Single cardinality properties have only one value -- the last one added

        Default is single (this is config dependent)

        @since 1.0.0
        @jira_ticket PYTHON-641

        @test_category dse graph
        """
        s = self.session
        s.execute_graph('''Schema schema = graph.schema();
                           schema.propertyKey('mult_key').Text().multiple().ifNotExists().create();
                           schema.propertyKey('single_key').Text().single().ifNotExists().create();
                           schema.vertexLabel('MPW1').properties('mult_key').ifNotExists().create();
                           schema.vertexLabel('MPW2').properties('mult_key').ifNotExists().create();
                           schema.vertexLabel('SW1').properties('single_key').ifNotExists().create();''')

        mpw1v = s.execute_graph('''v = graph.addVertex('MPW1')
                                 v.property('mult_key', 'value')
                                 v''')[0]

        mpw2v = s.execute_graph('''g.addV('MPW2').property('mult_key', 'value0').property('mult_key', 'value1')''')[0]

        g = self.fetch_traversal_source()
        traversal = g.V(mpw1v.id).properties()

        vertex_props = self.execute_traversal(traversal)

        self.assertEqual(len(vertex_props), 1)

        self.assertEqual(self.fetch_key_from_prop(vertex_props[0]), "mult_key")
        self.assertEqual(vertex_props[0].value, "value")

        # multiple_with_two_values
         #v = s.execute_graph('''g.addV(label, 'MPW2', 'mult_key', 'value0', 'mult_key', 'value1')''')[0]
        traversal = g.V(mpw2v.id).properties()

        vertex_props = self.execute_traversal(traversal)

        self.assertEqual(len(vertex_props), 2)
        self.assertEqual(self.fetch_key_from_prop(vertex_props[0]), 'mult_key')
        self.assertEqual(self.fetch_key_from_prop(vertex_props[1]), 'mult_key')
        self.assertEqual(vertex_props[0].value, 'value0')
        self.assertEqual(vertex_props[1].value, 'value1')

        # single_with_one_value
        v = s.execute_graph('''v = graph.addVertex('SW1')
                                 v.property('single_key', 'value')
                                 v''')[0]
        traversal = g.V(v.id).properties()
        vertex_props = self.execute_traversal(traversal)
        self.assertEqual(len(vertex_props), 1)
        self.assertEqual(self.fetch_key_from_prop(vertex_props[0]), "single_key")
        self.assertEqual(vertex_props[0].value, "value")


    def should_parse_meta_properties(self):
        g = self.fetch_traversal_source()
        g.addV("meta_v").property("meta_prop", "hello", "sub_prop", "hi", "sub_prop2", "hi2")


    def test_all_graph_types_with_schema(self):
        """
        Exhaustively goes through each type that is supported by dse_graph.
        creates a vertex for each type  using a dse-tinkerpop traversal,
        It then attempts to fetch it from the server and compares it to what was inserted
        Prime the graph with the correct schema first

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result inserted objects are equivalent to those retrieved

        @test_category dse graph
        """
        generate_type_graph_schema(self.session)
        # if result set is not parsed correctly this will throw an exception

        self._write_and_read_data_types()


    def test_all_graph_types_without_schema(self):
        """
        Exhaustively goes through each type that is supported by dse_graph.
        creates a vertex for each type  using a dse-tinkerpop traversal,
        It then attempts to fetch it from the server and compares it to what was inserted
        Do not prime the graph with the correct schema first

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result inserted objects are equivalent to those retrieved

        @test_category dse graph
        """

        # Prime graph using common utilites
        generate_type_graph_schema(self.session, prime_schema=False)
        self._write_and_read_data_types()

    def test_dsl(self):
        """
        The test creates a SocialTraversal and a SocialTraversalSource as part of
        a DSL. Then calls it's method and checks the results to verify
        we have the expected results

        @since @since 1.1.0a1
        @jira_ticket PYTHON-790
        @expected_result only the vertex corresponding to marko is in the result

        @test_category dse graph
        """
        class SocialTraversal(GraphTraversal):
            def knows(self, person_name):
                return self.out("knows").hasLabel("person").has("name", person_name).in_()

        class SocialTraversalSource(GraphTraversalSource):
            def __init__(self, *args, **kwargs):
                super(SocialTraversalSource, self).__init__(*args, **kwargs)
                self.graph_traversal = SocialTraversal

            def people(self, *names):
                return self.get_graph_traversal().V().has("name", P.within(*names))

        generate_classic(self.session)
        g = self.fetch_traversal_source(traversal_class=SocialTraversalSource)

        traversal = g.people("marko", "albert").knows("vadas")
        results = self.execute_traversal(traversal)

        self.assertEqual(len(results), 1)
        only_vertex = results[0]
        self._validate_classic_vertex(g, only_vertex)

    def test_bulked_results(self):
        """
        Send a query expecting a bulked result and the driver "undoes"
        the bulk and returns the expected list

        @since 1.1.0a1
        @jira_ticket PYTHON-771
        @expected_result the expanded list

        @test_category dse graph
        """
        generate_classic(self.session)
        g = self.fetch_traversal_source()
        barrier_traversal = g.E().label().barrier()
        results = self.execute_traversal(barrier_traversal)
        self.assertEqual(["created", "created", "created", "created", "knows", "knows"], results)

    def _write_and_read_data_types(self):
        g = self.fetch_traversal_source()
        for key in TYPE_MAP.keys():
            vertex_label = generate_type_graph_schema.single_vertex
            property_name = key + "value"
            data_value = TYPE_MAP[key][1]

            write_traversal = g.addV(vertex_label).property(property_name, data_value)
            self.execute_traversal(write_traversal)

            read_traversal = g.V().hasLabel(vertex_label).has(property_name).values()
            results = self.execute_traversal(read_traversal)

            self._check_equality(g, data_value, results[0])

    def fetch_edge_props(self, g, edge):
        edge_props = g.E(edge.id).properties().toList()
        return edge_props

    def fetch_vertex_props(self, g, vertex):

        vertex_props = g.V(vertex.id).properties().toList()
        return vertex_props

    def _check_equality(self, g, original, read_value):
        return check_equality_base(self, original, read_value)


@requiredse
class BaseImplicitExecutionTest(BasicGraphUnitTestCase):
    """
    This test class will execute all tests of the AbstractTraversalTestClass using implicit execution
    This all traversal will be run directly using toList()
    """
    def setUp(self):
        super(BaseImplicitExecutionTest, self).setUp()
        if DSE_VERSION:
            self.ep = DseGraph().create_execution_profile(self.graph_name)
            self.cluster.add_execution_profile(self.graph_name, self.ep)

    def fetch_key_from_prop(self, property):
        return property.key

    def fetch_traversal_source(self, **kwargs):
        return DseGraph().traversal_source(self.session, self.graph_name, execution_profile=self.ep, **kwargs)

    def execute_traversal(self, traversal):
        return traversal.toList()

    def _validate_classic_vertex(self, g, vertex):
        # Checks the properties on a classic vertex for correctness
        vertex_props = self.fetch_vertex_props(g, vertex)
        vertex_prop_keys = [vp.key for vp in vertex_props]
        self.assertEqual(len(vertex_prop_keys), 2)
        self.assertIn('name', vertex_prop_keys)
        self.assertTrue('lang' in vertex_prop_keys or 'age' in vertex_prop_keys)

    def _validate_generic_vertex_result_type(self,g, vertex):
        # Checks a vertex object for it's generic properties
        properties = self.fetch_vertex_props(g, vertex)
        for attr in ('id', 'label'):
            self.assertIsNotNone(getattr(vertex, attr))
        self.assertTrue( len(properties)>2)

    def _validate_classic_edge_properties(self, g, edge):
        # Checks the properties on a classic edge for correctness
        edge_props = self.fetch_edge_props(g, edge)
        edge_prop_keys = [ep.key for ep in edge_props]
        self.assertEqual(len(edge_prop_keys), 1)
        self.assertIn('weight', edge_prop_keys)

    def _validate_classic_edge(self, g, edge):
        self._validate_generic_edge_result_type(edge)
        self._validate_classic_edge_properties(g, edge)

    def _validate_line_edge(self, g, edge):
        self._validate_generic_edge_result_type(edge)
        edge_props = self.fetch_edge_props(g, edge)
        edge_prop_keys = [ep.key for ep in edge_props]
        self.assertEqual(len(edge_prop_keys), 1)
        self.assertIn('distance', edge_prop_keys)

    def _validate_generic_edge_result_type(self, edge):
        self.assertIsInstance(edge, TravEdge)

        for attr in ('outV', 'inV', 'label', 'id'):
            self.assertIsNotNone(getattr(edge, attr))

    def _validate_path_result_type(self, g, objects_path):
        for obj in objects_path:
            if isinstance(obj, TravEdge):
                self._validate_classic_edge(g, obj)
            elif isinstance(obj, TravVertex):
                self._validate_classic_vertex(g, obj)
            else:
                self.fail("Invalid object found in path " + str(obj.type))

    def _validate_meta_property(self, g, vertex):
        meta_props =  g.V(vertex.id).properties().toList()
        self.assertEqual(len(meta_props), 1)
        meta_prop = meta_props[0]
        self.assertEqual(meta_prop.value,"meta_prop")
        self.assertEqual(meta_prop.key,"key")

        nested_props = vertex_props = g.V(vertex.id).properties().properties().toList()
        self.assertEqual(len(nested_props), 2)
        for nested_prop in nested_props:
            self.assertTrue(nested_prop.key in ['k0', 'k1'])
            self.assertTrue(nested_prop.value in ['v0', 'v1'])

    def _validate_type(self, g,  vertex):
        props = self.fetch_vertex_props(g, vertex)
        for prop in props:
            value = prop.value
            key = prop.key
            _validate_prop(key, value, self)


@requiredse
class ImplicitExecutionTest(BaseImplicitExecutionTest, AbstractTraversalTest):
    pass


@requiredse
class ImplicitAsyncExecutionTest(BaseImplicitExecutionTest):
    """
    Test to validate that the traversal async execution works properly.

    @since 3.21.0
    @jira_ticket PYTHON-1129

    @test_category dse graph
    """

    def _validate_results(self, results):
        results = list(results)
        self.assertEqual(len(results), 2)
        self.assertIn('vadas', results)
        self.assertIn('josh', results)

    def test_promise(self):
        generate_classic(self.session)
        g = self.fetch_traversal_source()
        traversal_future = g.V().has('name', 'marko').out('knows').values('name').promise()
        self._validate_results(traversal_future.result())

    def test_promise_error_is_propagated(self):
        generate_classic(self.session)
        g = DseGraph().traversal_source(self.session, 'wrong_graph', execution_profile=self.ep)
        traversal_future = g.V().has('name', 'marko').out('knows').values('name').promise()
        with self.assertRaises(Exception):
            traversal_future.result()

    def test_promise_callback(self):
        generate_classic(self.session)
        g = self.fetch_traversal_source()
        future = Future()

        def cb(f):
            future.set_result(f.result())

        traversal_future = g.V().has('name', 'marko').out('knows').values('name').promise()
        traversal_future.add_done_callback(cb)
        self._validate_results(future.result())

    def test_promise_callback_on_error(self):
        generate_classic(self.session)
        g = DseGraph().traversal_source(self.session, 'wrong_graph', execution_profile=self.ep)
        future = Future()

        def cb(f):
            try:
                f.result()
            except Exception as e:
                future.set_exception(e)

        traversal_future = g.V().has('name', 'marko').out('knows').values('name').promise()
        traversal_future.add_done_callback(cb)
        with self.assertRaises(Exception):
            future.result()


@requiredse
class ExplicitExecutionBase(BasicGraphUnitTestCase):
    def setUp(self):
        super(ExplicitExecutionBase, self).setUp()
        if DSE_VERSION:
            self.ep = DseGraph().create_execution_profile(self.graph_name)
            self.cluster.add_execution_profile(self.graph_name, self.ep)

    def fetch_traversal_source(self, **kwargs):
        return DseGraph().traversal_source(self.session, self.graph_name, **kwargs)

    def execute_traversal(self, traversal):
        query = DseGraph.query_from_traversal(traversal)
        #Use an ep that is configured with the correct row factory, and bytecode-json language flat set
        result_set = self.session.execute_graph(query, execution_profile=self.ep)
        return list(result_set)


@requiredse
class ExplicitExecutionTest(ExplicitExecutionBase, AbstractTraversalTest):
    """
    This test class will execute all tests of the AbstractTraversalTestClass using Explicit execution
    All queries will be run by converting them to byte code, and calling execute graph explicitly with a generated ep.
    """
    def fetch_key_from_prop(self, property):
        return property.label

    def _validate_classic_vertex(self, g, vertex):
        validate_classic_vertex(self, vertex)

    def _validate_generic_vertex_result_type(self,g, vertex):
        validate_generic_vertex_result_type(self, vertex)

    def _validate_classic_edge_properties(self, g, edge):
        validate_classic_edge_properties(self, edge)

    def _validate_classic_edge(self, g, edge):
        validate_classic_edge(self, edge)

    def _validate_line_edge(self, g, edge):
        validate_line_edge(self, edge)

    def _validate_generic_edge_result_type(self, edge):
        validate_generic_edge_result_type(self, edge)

    def _validate_type(self, g,  vertex):
        for key in vertex.properties:
            value =  vertex.properties[key][0].value
            _validate_prop(key, value, self)

    def _validate_path_result_type(self, g, path_obj):
        # This pre-processing is due to a change in TinkerPop
        # properties are not returned automatically anymore
        # with some queries.
        for obj in path_obj.objects:
            if not obj.properties:
                props = []
                if isinstance(obj, Edge):
                    obj.properties = {
                        p['key']: p['value']
                        for p in self.fetch_edge_props(g, obj)
                    }
                elif isinstance(obj, Vertex):
                    obj.properties = {
                        p['label']: p['value']
                        for p in self.fetch_vertex_props(g, obj)
                    }

        validate_path_result_type(self, path_obj)

    def _validate_meta_property(self, g, vertex):

        self.assertEqual(len(vertex.properties), 1)
        self.assertEqual(len(vertex.properties['key']), 1)
        p = vertex.properties['key'][0]
        self.assertEqual(p.label, 'key')
        self.assertEqual(p.value, 'meta_prop')
        self.assertEqual(p.properties, {'k0': 'v0', 'k1': 'v1'})


def _validate_prop(key, value, unittest):
    if any(key.startswith(t) for t in ('int', 'short')):
        typ = int

    elif any(key.startswith(t) for t in ('long',)):
        if sys.version_info >= (3, 0):
            typ = int
        else:
            typ = long
    elif any(key.startswith(t) for t in ('float', 'double')):
        typ = float
    elif any(key.startswith(t) for t in ('polygon',)):
        typ = Polygon
    elif any(key.startswith(t) for t in ('point',)):
        typ = Point
    elif any(key.startswith(t) for t in ('Linestring',)):
        typ = LineString
    elif any(key.startswith(t) for t in ('neg',)):
        typ = string_types
    elif any(key.startswith(t) for t in ('date',)):
        typ = datetime.date
    elif any(key.startswith(t) for t in ('time',)):
        typ = datetime.time
    else:
        unittest.fail("Received unexpected type: %s" % key)
    unittest.assertIsInstance(value, typ)


@requiredse
@greaterthanorequaldse60
class BatchStatementTests(ExplicitExecutionBase):

    def setUp(self):
        super(BatchStatementTests, self).setUp()
        self.g = self.fetch_traversal_source()

        if hasattr(self, "batch"):
            self.batch.clear()
        else:
            self.batch = DseGraph.batch(session=self.session, execution_profile=self.ep)

    def test_batch_with_schema(self):
        """
        Sends a Batch statement and verifies it has succeeded with a schema created

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        generate_type_graph_schema(self.session)
        self._send_batch_and_read_results()

    def test_batch_without_schema(self):
        """
        Sends a Batch statement and verifies it has succeeded without a schema created

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        generate_type_graph_schema(self.session)
        self._send_batch_and_read_results()

    def test_batch_with_schema_add_all(self):
        """
        Sends a Batch statement and verifies it has succeeded with a schema created.
        Uses :method:`dse_graph.query._BatchGraphStatement.add_all` to add the statements
        instead of :method:`dse_graph.query._BatchGraphStatement.add`

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        generate_type_graph_schema(self.session)
        self._send_batch_and_read_results(add_all=True)

    def test_batch_without_schema_add_all(self):
        """
        Sends a Batch statement and verifies it has succeeded without a schema created
        Uses :method:`dse_graph.query._BatchGraphStatement.add_all` to add the statements
        instead of :method:`dse_graph.query._BatchGraphStatement.add`

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        generate_type_graph_schema(self.session, prime_schema=False)
        self._send_batch_and_read_results(add_all=True)

    def test_only_graph_traversals_are_accepted(self):
        """
        Verifies that ValueError is risen if the parameter add is not a traversal

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        self.assertRaises(ValueError, self.batch.add, '{"@value":{"step":[["addV","poc_int"],'
                                                      '["property","bigint1value",{"@value":12,"@type":"g:Int32"}]]},'
                                                      '"@type":"g:Bytecode"}')
        another_batch = DseGraph.batch()
        self.assertRaises(ValueError, self.batch.add, another_batch)

    def _send_batch_and_read_results(self, add_all=False):
        # For each supported type fetch create a vetex containing that type
        vertex_label = generate_type_graph_schema.single_vertex
        traversals = []
        for key in TYPE_MAP.keys():
            property_name = key + "value"
            traversal = self.g.addV(vertex_label).property(property_name, TYPE_MAP[key][1])
            if not add_all:
                self.batch.add(traversal)
            traversals.append(traversal)

        if add_all:
            self.batch.add_all(traversals)

        self.assertEqual(len(TYPE_MAP), len(self.batch))

        self.batch.execute()

        traversal = self.g.V()
        vertices = self.execute_traversal(traversal)

        self.assertEqual(len(vertices), len(TYPE_MAP), "g.V() returned {}".format(vertices))

        # Iterate over all the vertices and check that they match the original input
        for vertex in vertices:
            key = list(vertex.properties.keys())[0].replace("value", "")
            original = TYPE_MAP[key][1]
            self._check_equality(self.g, original, vertex)

    def _check_equality(self,g, original, vertex):
        for key in vertex.properties:
            value = vertex.properties[key][0].value
            check_equality_base(self, original, value)
