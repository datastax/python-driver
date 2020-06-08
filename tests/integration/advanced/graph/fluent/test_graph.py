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
import datetime
import six
import time
from collections import namedtuple
from concurrent.futures import Future
from packaging.version import Version

from cassandra import cluster
from cassandra.cluster import ContinuousPagingOptions
from cassandra.datastax.graph.fluent import DseGraph
from cassandra.graph import Vertex, Edge, VertexProperty, GraphProtocol
from cassandra.util import Point, Polygon, LineString

from gremlin_python.process.graph_traversal import GraphTraversal, GraphTraversalSource
from gremlin_python.process.traversal import P
from gremlin_python.structure.graph import Edge as TravEdge
from gremlin_python.structure.graph import Vertex as TravVertex, VertexProperty as TravVertexProperty

from tests.util import wait_until_not_raised
from tests.integration import DSE_VERSION, greaterthanorequaldse68
from tests.integration.advanced.graph import GraphUnitTestCase, \
    ClassicGraphSchema, CoreGraphSchema, \
    validate_classic_vertex, validate_classic_edge, validate_generic_vertex_result_type, \
    validate_classic_edge_properties, validate_line_edge, \
    validate_generic_edge_result_type, validate_path_result_type, VertexLabel, \
    GraphTestConfiguration, BasicGraphUnitTestCase
from tests.integration import greaterthanorequaldse60, requiredse

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa


import ipaddress


def check_equality_base(testcase, original, read_value):
    if isinstance(original, float):
        testcase.assertAlmostEqual(original, read_value, delta=.01)
    elif isinstance(original, ipaddress.IPv4Address):
        testcase.assertAlmostEqual(original, ipaddress.IPv4Address(read_value))
    elif isinstance(original, ipaddress.IPv6Address):
        testcase.assertAlmostEqual(original, ipaddress.IPv6Address(read_value))
    else:
        testcase.assertEqual(original, read_value)


def create_traversal_profiles(cluster, graph_name):
    ep_graphson2 = DseGraph().create_execution_profile(graph_name,
                                                            graph_protocol=GraphProtocol.GRAPHSON_2_0)
    ep_graphson3 = DseGraph().create_execution_profile(graph_name,
                                                            graph_protocol=GraphProtocol.GRAPHSON_3_0)

    cluster.add_execution_profile('traversal_graphson2', ep_graphson2)
    cluster.add_execution_profile('traversal_graphson3', ep_graphson3)

    return ep_graphson2, ep_graphson3


class _AbstractTraversalTest(GraphUnitTestCase):

    def setUp(self):
        super(_AbstractTraversalTest, self).setUp()
        self.ep_graphson2, self.ep_graphson3 = create_traversal_profiles(self.cluster, self.graph_name)

    def _test_basic_query(self, schema, graphson):
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

        g = self.fetch_traversal_source(graphson)
        self.execute_graph(schema.fixtures.classic(), graphson)
        traversal = g.V().has('name', 'marko').out('knows').values('name')
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 2)
        self.assertIn('vadas', results_list)
        self.assertIn('josh', results_list)

    def _test_classic_graph(self, schema, graphson):
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

        self.execute_graph(schema.fixtures.classic(), graphson)
        ep = self.get_execution_profile(graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.V()
        vert_list = self.execute_traversal(traversal, graphson)

        for vertex in vert_list:
            schema.ensure_properties(self.session, vertex, execution_profile=ep)
            self._validate_classic_vertex(g, vertex)
        traversal = g.E()
        edge_list = self.execute_traversal(traversal, graphson)
        for edge in edge_list:
            schema.ensure_properties(self.session, edge, execution_profile=ep)
            self._validate_classic_edge(g, edge)

    def _test_graph_classic_path(self, schema, graphson):
        """
        Test to validate that the path version of the result type is generated correctly. It also
        tests basic path results as that is not covered elsewhere

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result path object should be unpacked correctly including all nested edges and vertices
        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.classic(), graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.V().hasLabel('person').has('name', 'marko').as_('a').outE('knows').inV().as_('c', 'd').outE('created').as_('e', 'f', 'g').inV().path()
        path_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(path_list), 2)
        for path in path_list:
            self._validate_path_result_type(g, path)

    def _test_range_query(self, schema, graphson):
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

        self.execute_graph(schema.fixtures.line(150), graphson)
        ep = self.get_execution_profile(graphson)
        g = self.fetch_traversal_source(graphson)

        traversal = g.E().range(0, 10)
        edges = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(edges), 10)
        for edge in edges:
            schema.ensure_properties(self.session, edge, execution_profile=ep)
            self._validate_line_edge(g, edge)

    def _test_result_types(self, schema, graphson):
        """
        Test to validate that the edge and vertex version of results are constructed correctly.

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result edge/vertex result types should be unpacked correctly.
        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.line(150), graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.V()
        vertices = self.execute_traversal(traversal, graphson)
        for vertex in vertices:
            self._validate_type(g, vertex)

    def _test_large_result_set(self, schema, graphson):
        """
        Test to validate that large result sets return correctly.

        Creates a very large graph. Ensures that large result sets are handled appropriately.

        @since 1.0.0
        @jira_ticket PYTHON-641
        @expected_result when limits of result sets are hit errors should be surfaced appropriately

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.large(), graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.V()
        vertices = self.execute_traversal(traversal, graphson)
        for vertex in vertices:
            self._validate_generic_vertex_result_type(g, vertex)

    def _test_vertex_meta_properties(self, schema, graphson):
        """
        Test verifying vertex property properties

        @since 1.0.0
        @jira_ticket PYTHON-641

        @test_category dse graph
        """
        if schema is not ClassicGraphSchema:
            raise unittest.SkipTest('skipped because multiple properties are only supported with classic graphs')

        s = self.session
        s.execute_graph("schema.propertyKey('k0').Text().ifNotExists().create();")
        s.execute_graph("schema.propertyKey('k1').Text().ifNotExists().create();")
        s.execute_graph("schema.propertyKey('key').Text().properties('k0', 'k1').ifNotExists().create();")
        s.execute_graph("schema.vertexLabel('MLP').properties('key').ifNotExists().create();")
        s.execute_graph("schema.config().option('graph.allow_scan').set('true');")
        v = s.execute_graph('''v = graph.addVertex('MLP')
                                 v.property('key', 'meta_prop', 'k0', 'v0', 'k1', 'v1')
                                 v''')[0]

        g = self.fetch_traversal_source(graphson)

        traversal = g.V()
        # This should contain key, and value where value is a property
        # This should be a vertex property and should contain sub properties
        results = self.execute_traversal(traversal, graphson)
        self._validate_meta_property(g, results[0])

    def _test_vertex_multiple_properties(self, schema, graphson):
        """
        Test verifying vertex property form for various Cardinality

        All key types are encoded as a list, regardless of cardinality

        Single cardinality properties have only one value -- the last one added

        Default is single (this is config dependent)

        @since 1.0.0
        @jira_ticket PYTHON-641

        @test_category dse graph
        """
        if schema is not ClassicGraphSchema:
            raise unittest.SkipTest('skipped because multiple properties are only supported with classic graphs')

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

        g = self.fetch_traversal_source(graphson)
        traversal = g.V(mpw1v.id).properties()

        vertex_props = self.execute_traversal(traversal, graphson)

        self.assertEqual(len(vertex_props), 1)

        self.assertEqual(self.fetch_key_from_prop(vertex_props[0]), "mult_key")
        self.assertEqual(vertex_props[0].value, "value")

        # multiple_with_two_values
        #v = s.execute_graph('''g.addV(label, 'MPW2', 'mult_key', 'value0', 'mult_key', 'value1')''')[0]
        traversal = g.V(mpw2v.id).properties()

        vertex_props = self.execute_traversal(traversal, graphson)

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
        vertex_props = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(vertex_props), 1)
        self.assertEqual(self.fetch_key_from_prop(vertex_props[0]), "single_key")
        self.assertEqual(vertex_props[0].value, "value")

    def should_parse_meta_properties(self):
        g = self.fetch_traversal_source()
        g.addV("meta_v").property("meta_prop", "hello", "sub_prop", "hi", "sub_prop2", "hi2")

    def _test_all_graph_types_with_schema(self, schema, graphson):
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
        self._write_and_read_data_types(schema, graphson)

    def _test_all_graph_types_without_schema(self, schema, graphson):
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
        if schema is not ClassicGraphSchema:
            raise unittest.SkipTest('schema-less is only for classic graphs')
        self._write_and_read_data_types(schema, graphson, use_schema=False)

    def _test_dsl(self, schema, graphson):
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

        self.execute_graph(schema.fixtures.classic(), graphson)
        if schema is CoreGraphSchema:
            self.execute_graph("""
            schema.edgeLabel('knows').from('person').to('person').materializedView('person__knows__person_by_in_name').
            ifNotExists().partitionBy('in_name').clusterBy('out_name', Asc).create()
            """, graphson)
            time.sleep(1)  # give some time to the MV to be populated
        g = self.fetch_traversal_source(graphson, traversal_class=SocialTraversalSource)

        traversal = g.people("marko", "albert").knows("vadas")
        results = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results), 1)
        only_vertex = results[0]
        schema.ensure_properties(self.session, only_vertex,
                                 execution_profile=self.get_execution_profile(graphson))
        self._validate_classic_vertex(g, only_vertex)

    def _test_bulked_results(self, schema, graphson):
        """
        Send a query expecting a bulked result and the driver "undoes"
        the bulk and returns the expected list

        @since 1.1.0a1
        @jira_ticket PYTHON-771
        @expected_result the expanded list

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.classic(), graphson)
        g = self.fetch_traversal_source(graphson)
        barrier_traversal = g.E().label().barrier()
        results = self.execute_traversal(barrier_traversal, graphson)
        self.assertEqual(sorted(["created", "created", "created", "created", "knows", "knows"]), sorted(results))

    def _test_udt_with_classes(self, schema, graphson):
        class Address(object):

            def __init__(self, address, city, state):
                self.address = address
                self.city = city
                self.state = state

            def __eq__(self, other):
                return self.address == other.address and self.city == other.city and self.state == other.state

        class AddressWithTags(object):

            def __init__(self, address, city, state, tags):
                self.address = address
                self.city = city
                self.state = state
                self.tags = tags

            def __eq__(self, other):
                return (self.address == other.address and self.city == other.city
                        and self.state == other.state and self.tags == other.tags)

        class ComplexAddress(object):

            def __init__(self, address, address_tags, city, state, props):
                self.address = address
                self.address_tags = address_tags
                self.city = city
                self.state = state
                self.props = props

            def __eq__(self, other):
                return (self.address == other.address and self.address_tags == other.address_tags
                        and self.city == other.city and self.state == other.state
                        and self.props == other.props)

        class ComplexAddressWithOwners(object):

            def __init__(self, address, address_tags, city, state, props, owners):
                self.address = address
                self.address_tags = address_tags
                self.city = city
                self.state = state
                self.props = props
                self.owners = owners

            def __eq__(self, other):
                return (self.address == other.address and self.address_tags == other.address_tags
                        and self.city == other.city and self.state == other.state
                        and self.props == other.props and self.owners == other.owners)

        self.__test_udt(schema, graphson, Address, AddressWithTags, ComplexAddress, ComplexAddressWithOwners)

    def _test_udt_with_namedtuples(self, schema, graphson):
        AddressTuple = namedtuple('Address', ('address', 'city', 'state'))
        AddressWithTagsTuple = namedtuple('AddressWithTags', ('address', 'city', 'state', 'tags'))
        ComplexAddressTuple = namedtuple('ComplexAddress', ('address', 'address_tags', 'city', 'state', 'props'))
        ComplexAddressWithOwnersTuple = namedtuple('ComplexAddressWithOwners', ('address', 'address_tags', 'city',
                                                                                'state', 'props', 'owners'))

        self.__test_udt(schema, graphson, AddressTuple, AddressWithTagsTuple,
                        ComplexAddressTuple, ComplexAddressWithOwnersTuple)

    def _write_and_read_data_types(self, schema, graphson, use_schema=True):
        g = self.fetch_traversal_source(graphson)
        ep = self.get_execution_profile(graphson)
        for data in six.itervalues(schema.fixtures.datatypes()):
            typ, value, deserializer = data
            vertex_label = VertexLabel([typ])
            property_name = next(six.iterkeys(vertex_label.non_pk_properties))
            if use_schema or schema is CoreGraphSchema:
                schema.create_vertex_label(self.session, vertex_label, execution_profile=ep)

            write_traversal = g.addV(str(vertex_label.label)).property('pkid', vertex_label.id).\
                property(property_name, value)
            self.execute_traversal(write_traversal, graphson)

            read_traversal = g.V().hasLabel(str(vertex_label.label)).has(property_name).properties()
            results = self.execute_traversal(read_traversal, graphson)

            for result in results:
                if result.label == 'pkid':
                    continue
                self._check_equality(g, value, result.value)

    def __test_udt(self, schema, graphson, address_class, address_with_tags_class,
                   complex_address_class, complex_address_with_owners_class):
        if schema is not CoreGraphSchema or DSE_VERSION < Version('6.8'):
            raise unittest.SkipTest("Graph UDT is only supported with DSE 6.8+ and Core graphs.")

        ep = self.get_execution_profile(graphson)

        Address = address_class
        AddressWithTags = address_with_tags_class
        ComplexAddress = complex_address_class
        ComplexAddressWithOwners = complex_address_with_owners_class

        # setup udt
        self.session.execute_graph("""
                schema.type('address').property('address', Text).property('city', Text).property('state', Text).create();
                schema.type('addressTags').property('address', Text).property('city', Text).property('state', Text).
                    property('tags', setOf(Text)).create();
                schema.type('complexAddress').property('address', Text).property('address_tags', frozen(typeOf('addressTags'))).
                    property('city', Text).property('state', Text).property('props', mapOf(Text, Int)).create();
                schema.type('complexAddressWithOwners').property('address', Text).
                    property('address_tags', frozen(typeOf('addressTags'))).
                    property('city', Text).property('state', Text).property('props', mapOf(Text, Int)).
                    property('owners', frozen(listOf(tupleOf(Text, Int)))).create();
                """, execution_profile=ep)

        # wait max 10 seconds to get the UDT discovered.
        wait_until_not_raised(
            lambda: self.session.cluster.register_user_type(self.graph_name, 'address', Address),
            1, 10)
        wait_until_not_raised(
            lambda: self.session.cluster.register_user_type(self.graph_name, 'addressTags', AddressWithTags),
            1, 10)
        wait_until_not_raised(
            lambda: self.session.cluster.register_user_type(self.graph_name, 'complexAddress', ComplexAddress),
            1, 10)
        wait_until_not_raised(
            lambda: self.session.cluster.register_user_type(self.graph_name, 'complexAddressWithOwners', ComplexAddressWithOwners),
            1, 10)

        data = {
            "udt1": ["typeOf('address')", Address('1440 Rd Smith', 'Quebec', 'QC')],
            "udt2": ["tupleOf(typeOf('address'), Text)", (Address('1440 Rd Smith', 'Quebec', 'QC'), 'hello')],
            "udt3": ["tupleOf(frozen(typeOf('address')), Text)", (Address('1440 Rd Smith', 'Quebec', 'QC'), 'hello')],
            "udt4": ["tupleOf(tupleOf(Int, typeOf('address')), Text)",
                      ((42, Address('1440 Rd Smith', 'Quebec', 'QC')), 'hello')],
            "udt5": ["tupleOf(tupleOf(Int, typeOf('addressTags')), Text)",
                     ((42, AddressWithTags('1440 Rd Smith', 'Quebec', 'QC', {'t1', 't2'})), 'hello')],
            "udt6": ["tupleOf(tupleOf(Int, typeOf('complexAddress')), Text)",
                     ((42, ComplexAddress('1440 Rd Smith',
                                          AddressWithTags('1440 Rd Smith', 'Quebec', 'QC', {'t1', 't2'}),
                                          'Quebec', 'QC', {'p1': 42, 'p2': 33})), 'hello')],
            "udt7": ["tupleOf(tupleOf(Int, frozen(typeOf('complexAddressWithOwners'))), Text)",
                     ((42, ComplexAddressWithOwners(
                           '1440 Rd Smith',
                           AddressWithTags('1440 CRd Smith', 'Quebec', 'QC', {'t1', 't2'}),
                           'Quebec', 'QC', {'p1': 42, 'p2': 33}, [('Mike', 43), ('Gina', 39)])
                       ), 'hello')]
        }

        g = self.fetch_traversal_source(graphson)
        for typ, value in six.itervalues(data):
            vertex_label = VertexLabel([typ])
            property_name = next(six.iterkeys(vertex_label.non_pk_properties))
            schema.create_vertex_label(self.session, vertex_label, execution_profile=ep)

            write_traversal = g.addV(str(vertex_label.label)).property('pkid', vertex_label.id). \
                property(property_name, value)
            self.execute_traversal(write_traversal, graphson)

            #vertex = list(schema.add_vertex(self.session, vertex_label, property_name, value, execution_profile=ep))[0]
            #vertex_properties = list(schema.get_vertex_properties(
            #    self.session, vertex, execution_profile=ep))

            read_traversal = g.V().hasLabel(str(vertex_label.label)).has(property_name).properties()
            vertex_properties = self.execute_traversal(read_traversal, graphson)

            self.assertEqual(len(vertex_properties), 2)  # include pkid
            for vp in vertex_properties:
                if vp.label == 'pkid':
                    continue

                self.assertIsInstance(vp, (VertexProperty, TravVertexProperty))
                self.assertEqual(vp.label, property_name)
                self.assertEqual(vp.value, value)

    @staticmethod
    def fetch_edge_props(g, edge):
        edge_props = g.E(edge.id).properties().toList()
        return edge_props

    @staticmethod
    def fetch_vertex_props(g, vertex):

        vertex_props = g.V(vertex.id).properties().toList()
        return vertex_props

    def _check_equality(self, g, original, read_value):
        return check_equality_base(self, original, read_value)


@requiredse
class BaseImplicitExecutionTest(GraphUnitTestCase):
    """
    This test class will execute all tests of the AbstractTraversalTestClass using implicit execution
    This all traversal will be run directly using toList()
    """
    def setUp(self):
        super(BaseImplicitExecutionTest, self).setUp()
        if DSE_VERSION:
            self.ep = DseGraph().create_execution_profile(self.graph_name)
            self.cluster.add_execution_profile(self.graph_name, self.ep)

    @staticmethod
    def fetch_key_from_prop(property):
        return property.key

    def fetch_traversal_source(self, graphson, **kwargs):
        ep = self.get_execution_profile(graphson, traversal=True)
        return DseGraph().traversal_source(self.session, self.graph_name, execution_profile=ep, **kwargs)

    def execute_traversal(self, traversal, graphson=None):
        return traversal.toList()

    def _validate_classic_vertex(self, g, vertex):
        # Checks the properties on a classic vertex for correctness
        vertex_props = self.fetch_vertex_props(g, vertex)
        vertex_prop_keys = [vp.key for vp in vertex_props]
        self.assertEqual(len(vertex_prop_keys), 2)
        self.assertIn('name', vertex_prop_keys)
        self.assertTrue('lang' in vertex_prop_keys or 'age' in vertex_prop_keys)

    def _validate_generic_vertex_result_type(self, g, vertex):
        # Checks a vertex object for it's generic properties
        properties = self.fetch_vertex_props(g, vertex)
        for attr in ('id', 'label'):
            self.assertIsNotNone(getattr(vertex, attr))
        self.assertTrue(len(properties) > 2)

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
        meta_props = g.V(vertex.id).properties().toList()
        self.assertEqual(len(meta_props), 1)
        meta_prop = meta_props[0]
        self.assertEqual(meta_prop.value, "meta_prop")
        self.assertEqual(meta_prop.key, "key")

        nested_props = g.V(vertex.id).properties().properties().toList()
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
@GraphTestConfiguration.generate_tests(traversal=True)
class ImplicitExecutionTest(BaseImplicitExecutionTest, _AbstractTraversalTest):
    def _test_iterate_step(self, schema, graphson):
        """
        Test to validate that the iterate() step work on all dse versions.
        @jira_ticket PYTHON-1155
        @expected_result iterate step works
        @test_category dse graph
        """

        g = self.fetch_traversal_source(graphson)
        self.execute_graph(schema.fixtures.classic(), graphson)
        g.addV('person').property('name', 'Person1').iterate()


@requiredse
@GraphTestConfiguration.generate_tests(traversal=True)
class ImplicitAsyncExecutionTest(BaseImplicitExecutionTest):
    """
    Test to validate that the traversal async execution works properly.

    @since 3.21.0
    @jira_ticket PYTHON-1129

    @test_category dse graph
    """

    def setUp(self):
        super(ImplicitAsyncExecutionTest, self).setUp()
        self.ep_graphson2, self.ep_graphson3 = create_traversal_profiles(self.cluster, self.graph_name)


    def _validate_results(self, results):
        results = list(results)
        self.assertEqual(len(results), 2)
        self.assertIn('vadas', results)
        self.assertIn('josh', results)

    def _test_promise(self, schema, graphson):
        self.execute_graph(schema.fixtures.classic(), graphson)
        g = self.fetch_traversal_source(graphson)
        traversal_future = g.V().has('name', 'marko').out('knows').values('name').promise()
        self._validate_results(traversal_future.result())

    def _test_promise_error_is_propagated(self, schema, graphson):
        self.execute_graph(schema.fixtures.classic(), graphson)
        g = DseGraph().traversal_source(self.session, 'wrong_graph', execution_profile=self.ep)
        traversal_future = g.V().has('name', 'marko').out('knows').values('name').promise()
        with self.assertRaises(Exception):
            traversal_future.result()

    def _test_promise_callback(self, schema, graphson):
        self.execute_graph(schema.fixtures.classic(), graphson)
        g = self.fetch_traversal_source(graphson)
        future = Future()

        def cb(f):
            future.set_result(f.result())

        traversal_future = g.V().has('name', 'marko').out('knows').values('name').promise()
        traversal_future.add_done_callback(cb)
        self._validate_results(future.result())

    def _test_promise_callback_on_error(self, schema, graphson):
        self.execute_graph(schema.fixtures.classic(), graphson)
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


class ExplicitExecutionBase(GraphUnitTestCase):

    def fetch_traversal_source(self, graphson, **kwargs):
        ep = self.get_execution_profile(graphson, traversal=True)
        return DseGraph().traversal_source(self.session, self.graph_name, execution_profile=ep, **kwargs)

    def execute_traversal(self, traversal, graphson):
        ep = self.get_execution_profile(graphson, traversal=True)
        ep = self.session.get_execution_profile(ep)
        context = None
        if graphson == GraphProtocol.GRAPHSON_3_0:
            context = {
                'cluster': self.cluster,
                'graph_name': ep.graph_options.graph_name.decode('utf-8') if ep.graph_options.graph_name else None
            }
        query = DseGraph.query_from_traversal(traversal, graphson, context=context)
        # Use an ep that is configured with the correct row factory, and bytecode-json language flat set
        result_set = self.execute_graph(query, graphson, traversal=True)
        return list(result_set)


@requiredse
@GraphTestConfiguration.generate_tests(traversal=True)
class ExplicitExecutionTest(ExplicitExecutionBase, _AbstractTraversalTest):
    """
    This test class will execute all tests of the AbstractTraversalTestClass using Explicit execution
    All queries will be run by converting them to byte code, and calling execute graph explicitly with a generated ep.
    """
    @staticmethod
    def fetch_key_from_prop(property):
        return property.label

    def _validate_classic_vertex(self, g, vertex):
        validate_classic_vertex(self, vertex)

    def _validate_generic_vertex_result_type(self, g, vertex):
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
            value = vertex.properties[key][0].value
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
                        p.key: p.value
                        for p in self.fetch_edge_props(g, obj)
                    }
                elif isinstance(obj, Vertex):
                    obj.properties = {
                        p.label: p.value
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
    if key == 'index':
        return

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
        typ = six.string_types
    elif any(key.startswith(t) for t in ('date',)):
        typ = datetime.date
    elif any(key.startswith(t) for t in ('time',)):
        typ = datetime.time
    else:
        unittest.fail("Received unexpected type: %s" % key)
    unittest.assertIsInstance(value, typ)


@greaterthanorequaldse60
@GraphTestConfiguration.generate_tests(traversal=True)
class BatchStatementTests(ExplicitExecutionBase):

    def setUp(self):
        super(BatchStatementTests, self).setUp()
        self.ep_graphson2, self.ep_graphson3 = create_traversal_profiles(self.cluster, self.graph_name)

    def _test_batch_with_schema(self, schema, graphson):
        """
        Sends a Batch statement and verifies it has succeeded with a schema created

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        self._send_batch_and_read_results(schema, graphson)

    def _test_batch_without_schema(self, schema, graphson):
        """
        Sends a Batch statement and verifies it has succeeded without a schema created

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        if schema is not ClassicGraphSchema:
            raise unittest.SkipTest('schema-less is only for classic graphs')
        self._send_batch_and_read_results(schema, graphson, use_schema=False)

    def _test_batch_with_schema_add_all(self, schema, graphson):
        """
        Sends a Batch statement and verifies it has succeeded with a schema created.
        Uses :method:`dse_graph.query._BatchGraphStatement.add_all` to add the statements
        instead of :method:`dse_graph.query._BatchGraphStatement.add`

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        self._send_batch_and_read_results(schema, graphson, add_all=True)

    def _test_batch_without_schema_add_all(self, schema, graphson):
        """
        Sends a Batch statement and verifies it has succeeded without a schema created
        Uses :method:`dse_graph.query._BatchGraphStatement.add_all` to add the statements
        instead of :method:`dse_graph.query._BatchGraphStatement.add`

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        if schema is not ClassicGraphSchema:
            raise unittest.SkipTest('schema-less is only for classic graphs')
        self._send_batch_and_read_results(schema, graphson, add_all=True, use_schema=False)

    def test_only_graph_traversals_are_accepted(self):
        """
        Verifies that ValueError is risen if the parameter add is not a traversal

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        batch = DseGraph.batch()
        self.assertRaises(ValueError, batch.add, '{"@value":{"step":[["addV","poc_int"],'
                                                 '["property","bigint1value",{"@value":12,"@type":"g:Int32"}]]},'
                                                 '"@type":"g:Bytecode"}')
        another_batch = DseGraph.batch()
        self.assertRaises(ValueError, batch.add, another_batch)

    def _send_batch_and_read_results(self, schema, graphson, add_all=False, use_schema=True):
        traversals = []
        datatypes = schema.fixtures.datatypes()
        values = {}
        g = self.fetch_traversal_source(graphson)
        ep = self.get_execution_profile(graphson)
        batch = DseGraph.batch(session=self.session,
                               execution_profile=self.get_execution_profile(graphson, traversal=True))
        for data in six.itervalues(datatypes):
            typ, value, deserializer = data
            vertex_label = VertexLabel([typ])
            property_name = next(six.iterkeys(vertex_label.non_pk_properties))
            values[property_name] = value
            if use_schema or schema is CoreGraphSchema:
                schema.create_vertex_label(self.session, vertex_label, execution_profile=ep)

            traversal = g.addV(str(vertex_label.label)).property('pkid', vertex_label.id).property(property_name, value)
            if not add_all:
                batch.add(traversal)
            traversals.append(traversal)

        if add_all:
            batch.add_all(traversals)

        self.assertEqual(len(datatypes), len(batch))

        batch.execute()

        vertices = self.execute_traversal(g.V(), graphson)
        self.assertEqual(len(vertices), len(datatypes), "g.V() returned {}".format(vertices))

        # Iterate over all the vertices and check that they match the original input
        for vertex in vertices:
            schema.ensure_properties(self.session, vertex, execution_profile=ep)
            key = [k for k in list(vertex.properties.keys()) if k != 'pkid'][0].replace("value", "")
            original = values[key]
            self._check_equality(original, vertex)

    def _check_equality(self, original, vertex):
        for key in vertex.properties:
            if key == 'pkid':
                continue
            value = vertex.properties[key].value \
                if isinstance(vertex.properties[key], VertexProperty) else vertex.properties[key][0].value
            check_equality_base(self, original, value)


class ContinuousPagingOptionsForTests(ContinuousPagingOptions):
    def __init__(self,
                 page_unit=ContinuousPagingOptions.PagingUnit.ROWS, max_pages=1,  # max_pages=1
                 max_pages_per_second=0, max_queue_size=4):
        super(ContinuousPagingOptionsForTests, self).__init__(page_unit, max_pages, max_pages_per_second,
                                                              max_queue_size)


def reset_paging_options():
    cluster.ContinuousPagingOptions = ContinuousPagingOptions


@greaterthanorequaldse68
@GraphTestConfiguration.generate_tests(schema=CoreGraphSchema)
class GraphPagingTest(GraphUnitTestCase):

    def setUp(self):
        super(GraphPagingTest, self).setUp()
        self.addCleanup(reset_paging_options)
        self.ep_graphson2, self.ep_graphson3 = create_traversal_profiles(self.cluster, self.graph_name)

    def _setup_data(self, schema, graphson):
        self.execute_graph(
            "schema.vertexLabel('person').ifNotExists().partitionBy('name', Text).property('age', Int).create();",
            graphson)
        for i in range(100):
            self.execute_graph("g.addV('person').property('name', 'batman-{}')".format(i), graphson)

    def _test_cont_paging_is_enabled_by_default(self, schema, graphson):
        """
        Test that graph paging is automatically enabled with a >=6.8 cluster.

        @jira_ticket PYTHON-1045
        @expected_result the default continuous paging options are used

        @test_category dse graph
        """
        # with traversals... I don't have access to the response future... so this is a hack to ensure paging is on
        cluster.ContinuousPagingOptions = ContinuousPagingOptionsForTests
        ep = self.get_execution_profile(graphson, traversal=True)
        self._setup_data(schema, graphson)
        self.session.default_fetch_size = 10
        g = DseGraph.traversal_source(self.session, execution_profile=ep)
        results = g.V().toList()
        self.assertEqual(len(results), 10)  # only 10 results due to our hack

    def _test_cont_paging_can_be_disabled(self, schema, graphson):
        """
        Test that graph paging can be disabled.

        @jira_ticket PYTHON-1045
        @expected_result the default continuous paging options are not used

        @test_category dse graph
        """
        # with traversals... I don't have access to the response future... so this is a hack to ensure paging is on
        cluster.ContinuousPagingOptions = ContinuousPagingOptionsForTests
        ep = self.get_execution_profile(graphson, traversal=True)
        ep = self.session.execution_profile_clone_update(ep, continuous_paging_options=None)
        self._setup_data(schema, graphson)
        self.session.default_fetch_size = 10
        g = DseGraph.traversal_source(self.session, execution_profile=ep)
        results = g.V().toList()
        self.assertEqual(len(results), 100)  # 100 results since paging is disabled

    def _test_cont_paging_with_custom_options(self, schema, graphson):
        """
        Test that we can specify custom paging options.

        @jira_ticket PYTHON-1045
        @expected_result we get only the desired number of results

        @test_category dse graph
        """
        ep = self.get_execution_profile(graphson, traversal=True)
        ep = self.session.execution_profile_clone_update(ep,
                                                         continuous_paging_options=ContinuousPagingOptions(max_pages=1))
        self._setup_data(schema, graphson)
        self.session.default_fetch_size = 10
        g = DseGraph.traversal_source(self.session, execution_profile=ep)
        results = g.V().toList()
        self.assertEqual(len(results), 10)  # only 10 results since paging is disabled
