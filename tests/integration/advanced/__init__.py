# Copyright DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from six.moves.urllib.request import build_opener, Request, HTTPHandler
import sys
import re
import os
import time
from os.path import expanduser
from uuid import UUID
from decimal import Decimal
from ccmlib import common
import datetime
import six
from packaging.version import Version

from cassandra.cluster import Cluster, EXEC_PROFILE_GRAPH_DEFAULT, EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT

from tests.integration import PROTOCOL_VERSION, DSE_VERSION, get_server_versions, BasicKeyspaceUnitTestCase, \
    drop_keyspace_shutdown_cluster, get_node, USE_CASS_EXTERNAL
from tests.integration import use_singledc, use_single_node, wait_for_node_socket
from cassandra.protocol import ServerError
from cassandra.util import Point, LineString, Polygon
from cassandra.graph import Edge, Vertex, Path
from cassandra.graph import GraphSON1Deserializer
from cassandra.graph.graphson import InetTypeIO
from cassandra.datastax.graph.query import _graphson2_reader
from cassandra.cluster import (GraphAnalyticsExecutionProfile, GraphExecutionProfile,
                               EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT, default_lbp_factory)
from cassandra.policies import DSELoadBalancingPolicy
from cassandra.graph.query import GraphOptions, GraphProtocol, graph_graphson2_row_factory

home = expanduser('~')


# Home directory of the Embedded Apache Directory Server to use
ADS_HOME = os.getenv('ADS_HOME', home)
MAKE_STRICT = "schema.config().option('graph.schema_mode').set('production')"
MAKE_NON_STRICT = "schema.config().option('graph.schema_mode').set('development')"
ALLOW_SCANS = "schema.config().option('graph.allow_scan').set('true')"

# A map of common types and their corresponding groovy declaration for use in schema creation and insertion
MAX_LONG = 9223372036854775807
MIN_LONG = -9223372036854775808
ZERO_LONG = 0

if sys.version_info < (3, 0):
    MAX_LONG = long(MAX_LONG)
    MIN_LONG = long(MIN_LONG)
    ZERO_LONG = long(ZERO_LONG)

deserializers = GraphSON1Deserializer()._deserializers

TYPE_MAP = {"point1": ["Point()", Point(.5, .13), GraphSON1Deserializer.deserialize_point],
            "point2": ["Point()", Point(-5, .0), GraphSON1Deserializer.deserialize_point],

            "linestring1": ["Linestring()", LineString(((1.0, 2.0), (3.0, 4.0), (-89.0, 90.0))),
                                GraphSON1Deserializer.deserialize_linestring],
            "polygon1": ["Polygon()", Polygon([(10.0, 10.0), (80.0, 10.0), (80., 88.0), (10., 89.0), (10., 10.0)],
                                              [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)],
                                               [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]]),
                                GraphSON1Deserializer.deserialize_polygon],
            "smallint1": ["Smallint()", 1, GraphSON1Deserializer.deserialize_smallint],
            "varint1": ["Varint()", 2147483647, GraphSON1Deserializer.deserialize_varint],

            "bigint1": ["Bigint()", MAX_LONG, GraphSON1Deserializer.deserialize_bigint],
            "bigint2": ["Bigint()", MIN_LONG, GraphSON1Deserializer.deserialize_bigint],
            "bigint3": ["Bigint()", ZERO_LONG, GraphSON1Deserializer.deserialize_bigint],

            "int1": ["Int()", 100, GraphSON1Deserializer.deserialize_int],
            "float1": ["Float()", .5, GraphSON1Deserializer.deserialize_float],
            "double1": ["Double()", .3415681, GraphSON1Deserializer.deserialize_double],
            "uuid1": ["Uuid()", UUID('12345678123456781234567812345678'), GraphSON1Deserializer.deserialize_uuid],
            "decimal1": ["Decimal()", Decimal(10), GraphSON1Deserializer.deserialize_decimal],
            "blob1": ["Blob()",  bytearray(b"Hello World"), GraphSON1Deserializer.deserialize_blob],

            "timestamp1": ["Timestamp()", datetime.datetime.now().replace(microsecond=0),
                                GraphSON1Deserializer.deserialize_timestamp],
            "timestamp2": ["Timestamp()", datetime.datetime.max.replace(microsecond=0),
                                GraphSON1Deserializer.deserialize_timestamp],
            # These are valid values but are pending for DSP-14093 to be fixed
            #"timestamp3": ["Timestamp()", datetime.datetime(159, 1, 1, 23, 59, 59),
            #                                 GraphSON1TypeDeserializer.deserialize_timestamp],
            #"timestamp4": ["Timestamp()", datetime.datetime.min,
            #                                            GraphSON1TypeDeserializer.deserialize_timestamp],

            "duration1": ["Duration()", datetime.timedelta(1, 16, 0),
                                GraphSON1Deserializer.deserialize_duration],
            "duration2": ["Duration()", datetime.timedelta(days=1, seconds=16, milliseconds=15),
                                GraphSON1Deserializer.deserialize_duration],
            }


if six.PY2:
    TYPE_MAP["blob3"] = ["Blob()",  buffer(b"Hello World"), GraphSON1Deserializer.deserialize_blob]

    TYPE_MAP["inet1"] = ["Inet()", "127.0.0.1", GraphSON1Deserializer.deserialize_inet]
    TYPE_MAP["inet2"] = ["Inet()", "2001:db8:85a3:8d3:1319:8a2e:370:7348", GraphSON1Deserializer.deserialize_inet]

else:
    TYPE_MAP["blob4"] = ["Blob()", bytes(b"Hello World Again"), GraphSON1Deserializer.deserialize_blob]
    TYPE_MAP["blob5"] = ["Blob()", memoryview(b"And Again Hello World"), GraphSON1Deserializer.deserialize_blob]

    import ipaddress
    deserializer_plus_to_ipaddressv4 = lambda x: ipaddress.IPv4Address(GraphSON1Deserializer.deserialize_inet(x))
    deserializer_plus_to_ipaddressv6 = lambda x: ipaddress.IPv6Address(GraphSON1Deserializer.deserialize_inet(x))

    def generic_ip_deserializer(string_ip_adress):
        if ":" in string_ip_adress:
            return deserializer_plus_to_ipaddressv6(string_ip_adress)
        return deserializer_plus_to_ipaddressv4(string_ip_adress)

    class GenericIpAddressIO(InetTypeIO):
            @classmethod
            def deserialize(cls, value, reader=None):
                return generic_ip_deserializer(value)

    _graphson2_reader.deserializers[GenericIpAddressIO.graphson_type] = GenericIpAddressIO

    TYPE_MAP["inet1"] = ["Inet()", ipaddress.IPv4Address("127.0.0.1"), deserializer_plus_to_ipaddressv4]
    TYPE_MAP["inet2"] = ["Inet()", ipaddress.IPv6Address("2001:db8:85a3:8d3:1319:8a2e:370:7348"),
                         deserializer_plus_to_ipaddressv6]

if DSE_VERSION >= Version("5.1"):
    TYPE_MAP["datetime1"]= ["Date()", datetime.date.today(), GraphSON1Deserializer.deserialize_date]
    TYPE_MAP["time1"] = ["Time()", datetime.time(12, 6, 12, 444), GraphSON1Deserializer.deserialize_time]
    TYPE_MAP["time2"] = ["Time()", datetime.time(12, 6, 12), GraphSON1Deserializer.deserialize_time]
    TYPE_MAP["time3"] = ["Time()", datetime.time(12, 6), GraphSON1Deserializer.deserialize_time]
    TYPE_MAP["time4"] = ["Time()", datetime.time.min, GraphSON1Deserializer.deserialize_time]
    TYPE_MAP["time5"] = ["Time()", datetime.time.max, GraphSON1Deserializer.deserialize_time]
    TYPE_MAP["blob2"] = ["Blob()", bytearray(b"AKDLIElksadlaswqA" * 100000), GraphSON1Deserializer.deserialize_blob]
    TYPE_MAP["datetime1"]= ["Date()", datetime.date.today(), GraphSON1Deserializer.deserialize_date]
    TYPE_MAP["datetime2"]= ["Date()", datetime.date(159, 1, 3), GraphSON1Deserializer.deserialize_date]
    TYPE_MAP["datetime3"]= ["Date()", datetime.date.min, GraphSON1Deserializer.deserialize_date]
    TYPE_MAP["datetime4"]= ["Date()", datetime.date.max, GraphSON1Deserializer.deserialize_date]
    TYPE_MAP["time1"] = ["Time()", datetime.time(12, 6, 12, 444), GraphSON1Deserializer.deserialize_time]
    TYPE_MAP["time2"] = ["Time()", datetime.time(12, 6, 12), GraphSON1Deserializer.deserialize_time]
    TYPE_MAP["time3"] = ["Time()", datetime.time(12, 6), GraphSON1Deserializer.deserialize_time]
    TYPE_MAP["time4"] = ["Time()", datetime.time.min, GraphSON1Deserializer.deserialize_time]
    TYPE_MAP["time5"] = ["Time()", datetime.time.max, GraphSON1Deserializer.deserialize_time]
    TYPE_MAP["blob2"] = ["Blob()", bytearray(b"AKDLIElksadlaswqA" * 100000), GraphSON1Deserializer.deserialize_blob]

def find_spark_master(session):

    # Itterate over the nodes the one with port 7080 open is the spark master
    for host in session.hosts:
        ip = host.address
        port = 7077
        spark_master = (ip, port)
        if common.check_socket_listening(spark_master, timeout=3):
            return spark_master[0]
    return None


def wait_for_spark_workers(num_of_expected_workers, timeout):
    """
    This queries the spark master and checks for the expected number of workers
    """
    start_time = time.time()
    while True:
        opener = build_opener(HTTPHandler)
        request = Request("http://{0}:7080".format(DSE_IP))
        request.get_method = lambda: 'GET'
        connection = opener.open(request)
        match = re.search('Alive Workers:.*(\d+)</li>', connection.read().decode('utf-8'))
        num_workers = int(match.group(1))
        if num_workers == num_of_expected_workers:
            match = True
            break
        elif time.time() - start_time > timeout:
            match = True
            break
        time.sleep(1)
    return match


def use_single_node_with_graph(start=True, options={}, dse_options={}):
    use_single_node(start=start, workloads=['graph'], configuration_options=options, dse_options=dse_options)


def use_single_node_with_graph_and_spark(start=True, options={}):
    use_single_node(start=start, workloads=['graph', 'spark'], configuration_options=options)


def use_single_node_with_graph_and_solr(start=True, options={}):
    use_single_node(start=start, workloads=['graph', 'solr'], configuration_options=options)


def use_singledc_wth_graph(start=True):
    use_singledc(start=start, workloads=['graph'])


def use_singledc_wth_graph_and_spark(start=True):
    use_cluster_with_graph(3)


def use_cluster_with_graph(num_nodes):
    """
    This is a  work around to account for the fact that spark nodes will conflict over master assignment
    when started all at once.
    """
    if USE_CASS_EXTERNAL:
        set_default_dse_ip()
        return

    # Create the cluster but don't start it.
    use_singledc(start=False, workloads=['graph', 'spark'])
    # Start first node.
    get_node(1).start(wait_for_binary_proto=True)
    # Wait binary protocol port to open
    wait_for_node_socket(get_node(1), 120)
    # Wait for spark master to start up
    spark_master_http = ("localhost", 7080)
    common.check_socket_listening(spark_master_http, timeout=60)
    tmp_cluster = Cluster(protocol_version=PROTOCOL_VERSION)

    # Start up remaining nodes.
    try:
        session = tmp_cluster.connect()
        statement = "ALTER KEYSPACE dse_leases WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': '%d'}" % (num_nodes)
        session.execute(statement)
    finally:
        tmp_cluster.shutdown()

    for i in range(1, num_nodes+1):
        if i is not 1:
            node = get_node(i)
            node.start(wait_for_binary_proto=True)
            wait_for_node_socket(node, 120)

    # Wait for workers to show up as Alive on master
    wait_for_spark_workers(3, 120)


def reset_graph(session, graph_name):
        session.execute_graph('system.graph(name).ifNotExists().create()', {'name': graph_name},
                              execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)
        wait_for_graph_inserted(session, graph_name)


def wait_for_graph_inserted(session, graph_name):
        count = 0
        exists = session.execute_graph('system.graph(name).exists()', {'name': graph_name},
                              execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)[0].value
        while not exists and count < 50:
            time.sleep(1)
            exists = session.execute_graph('system.graph(name).exists()', {'name': graph_name},
                              execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)[0].value
        return exists


class BasicGraphUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This is basic graph unit test case that provides various utility methods that can be leveraged for testcase setup and tear
    down
    """
    @property
    def graph_name(self):
        return self._testMethodName.lower()

    def session_setup(self):
        lbp = DSELoadBalancingPolicy(default_lbp_factory())

        ep_graphson2 = GraphExecutionProfile(
            request_timeout=60,
            load_balancing_policy=lbp,
            graph_options=GraphOptions(
                graph_name=self.graph_name,
                graph_protocol=GraphProtocol.GRAPHSON_2_0
            ),
            row_factory=graph_graphson2_row_factory)

        ep_graphson1 = GraphExecutionProfile(
            request_timeout=60,
            load_balancing_policy=lbp,
            graph_options=GraphOptions(
                graph_name=self.graph_name
            )
        )

        ep_analytics = GraphAnalyticsExecutionProfile(
            request_timeout=60,
            load_balancing_policy=lbp,
            graph_options = GraphOptions(
                graph_language=b'gremlin-groovy',
                graph_name=self.graph_name
            )
        )

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION,
                               execution_profiles={
                                   EXEC_PROFILE_GRAPH_DEFAULT: ep_graphson1,
                                   EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT: ep_analytics,
                                   "graphson2": ep_graphson2
                               })
        self.session = self.cluster.connect()
        self.ks_name = self._testMethodName.lower()
        self.cass_version, self.cql_version = get_server_versions()

    def setUp(self):
        self.session_setup()
        self.reset_graph()

        self.clear_schema()

    def tearDown(self):
        self.cluster.shutdown()

    def clear_schema(self):
        self.session.execute_graph('schema.clear()')

    def reset_graph(self):
        reset_graph(self.session, self.graph_name)


    def wait_for_graph_inserted(self):
        wait_for_graph_inserted(self.session, self.graph_name)


class BasicSharedGraphUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This is basic graph unit test case that provides various utility methods that can be leveraged for testcase setup and tear
    down
    """

    @classmethod
    def session_setup(cls):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cls.session = cls.cluster.connect()
        cls.ks_name = cls.__name__.lower()
        cls.cass_version, cls.cql_version = get_server_versions()
        cls.graph_name = cls.__name__.lower()

    @classmethod
    def setUpClass(cls):
        cls.session_setup()
        cls.reset_graph()
        profiles = cls.cluster.profile_manager.profiles
        profiles[EXEC_PROFILE_GRAPH_DEFAULT].request_timeout = 60
        profiles[EXEC_PROFILE_GRAPH_DEFAULT].graph_options.graph_name = cls.graph_name
        profiles[EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT].request_timeout = 60
        profiles[EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT].graph_options.graph_name = cls.graph_name
        cls.clear_schema()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.shutdown()

    @classmethod
    def clear_schema(self):
        self.session.execute_graph('schema.clear()')

    @classmethod
    def reset_graph(self):
        reset_graph(self.session, self.graph_name)

    def wait_for_graph_inserted(self):
        wait_for_graph_inserted(self.session, self.graph_name)


def fetchCustomGeoType(type):
    if type.lower().startswith("point"):
        return getPointType()
    elif type.lower().startswith("line"):
        return getLineType()
    elif type.lower().startswith("poly"):
        return getPolygonType()
    else:
        return None


geo_condition = DSE_VERSION < Version('5.1')
def getPointType():
    if geo_condition:
        return "Point()"

    return "Point().withGeoBounds()"

def getPointTypeWithBounds(lowerX, lowerY, upperX, upperY):
    if geo_condition:
        return "Point()"

    return "Point().withBounds({0}, {1}, {2}, {3})".format(lowerX, lowerY, upperX, upperY)

def getLineType():
    if geo_condition:
        return "Linestring()"

    return "Linestring().withGeoBounds()"

def getPolygonType():
    if geo_condition:
        return "Polygon()"

    return "Polygon().withGeoBounds()"



class BasicGeometricUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This base test class is used by all the geomteric tests. It contains class level teardown and setup
    methods. It also contains the test fixtures used by those tests
    """
    @classmethod
    def common_dse_setup(cls, rf, keyspace_creation=True):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cls.session = cls.cluster.connect()
        cls.ks_name = cls.__name__.lower()
        if keyspace_creation:
            cls.create_keyspace(rf)
        cls.cass_version, cls.cql_version = get_server_versions()
        cls.session.set_keyspace(cls.ks_name)

    @classmethod
    def setUpClass(cls):
        cls.common_dse_setup(1)
        cls.initalizeTables()

    @classmethod
    def tearDownClass(cls):
        drop_keyspace_shutdown_cluster(cls.ks_name, cls.session, cls.cluster)

    @classmethod
    def initalizeTables(cls):
        udt_type = "CREATE TYPE udt1 (g {0})".format(cls.cql_type_name)
        large_table = "CREATE TABLE tbl (k uuid PRIMARY KEY, g {0}, l list<{0}>, s set<{0}>, m0 map<{0},int>, m1 map<int,{0}>, t tuple<{0},{0},{0}>, u frozen<udt1>)".format(cls.cql_type_name)
        simple_table = "CREATE TABLE tblpk (k {0} primary key, v int)".format( cls.cql_type_name)
        cluster_table = "CREATE TABLE tblclustering (k0 int, k1 {0}, v int, primary key (k0, k1))".format(cls.cql_type_name)
        cls.session.execute(udt_type)
        cls.session.execute(large_table)
        cls.session.execute(simple_table)
        cls.session.execute(cluster_table)


def generate_line_graph(length):
        query_parts = []
        query_parts.append(ALLOW_SCANS+';')
        query_parts.append("schema.propertyKey('index').Int().ifNotExists().create();")
        query_parts.append("schema.propertyKey('distance').Int().ifNotExists().create();")
        query_parts.append("schema.vertexLabel('lp').properties('index').ifNotExists().create();")
        query_parts.append("schema.edgeLabel('goesTo').properties('distance').connection('lp', 'lp').ifNotExists().create();")
        for index in range(0, length):
            query_parts.append('''Vertex vertex{0} = graph.addVertex(label, 'lp', 'index', {0}); '''.format(index))
            if index is not 0:
                query_parts.append('''vertex{0}.addEdge('goesTo', vertex{1}, 'distance', 5); '''.format(index-1, index))
        final_graph_generation_statement = "".join(query_parts)
        return final_graph_generation_statement


def generate_classic(session):
    to_run = [MAKE_STRICT, ALLOW_SCANS, '''schema.propertyKey('name').Text().ifNotExists().create();
            schema.propertyKey('age').Int().ifNotExists().create();
            schema.propertyKey('lang').Text().ifNotExists().create();
            schema.propertyKey('weight').Float().ifNotExists().create();
            schema.vertexLabel('person').properties('name', 'age').ifNotExists().create();
            schema.vertexLabel('software').properties('name', 'lang').ifNotExists().create();
            schema.edgeLabel('created').properties('weight').connection('person', 'software').ifNotExists().create();
            schema.edgeLabel('created').connection('software', 'software').add();
            schema.edgeLabel('knows').properties('weight').connection('person', 'person').ifNotExists().create();''',
            '''Vertex marko = graph.addVertex(label, 'person', 'name', 'marko', 'age', 29);
            Vertex vadas = graph.addVertex(label, 'person', 'name', 'vadas', 'age', 27);
            Vertex lop = graph.addVertex(label, 'software', 'name', 'lop', 'lang', 'java');
            Vertex josh = graph.addVertex(label, 'person', 'name', 'josh', 'age', 32);
            Vertex ripple = graph.addVertex(label, 'software', 'name', 'ripple', 'lang', 'java');
            Vertex peter = graph.addVertex(label, 'person', 'name', 'peter', 'age', 35);
            marko.addEdge('knows', vadas, 'weight', 0.5f);
            marko.addEdge('knows', josh, 'weight', 1.0f);
            marko.addEdge('created', lop, 'weight', 0.4f);
            josh.addEdge('created', ripple, 'weight', 1.0f);
            josh.addEdge('created', lop, 'weight', 0.4f);
            peter.addEdge('created', lop, 'weight', 0.2f);''']

    for run in to_run:
        succeed = False
        count = 0
        # Retry up to 10 times this is an issue for
        # Graph Mult-NodeClusters
        while count < 10 and not succeed:
            try:
                session.execute_graph(run)
                succeed = True
            except (ServerError):
                print("error creating classic graph retrying")
                time.sleep(.5)
            count += 1


def generate_multi_field_graph(session):
        to_run = [ALLOW_SCANS,
                  '''schema.propertyKey('shortvalue').Smallint().ifNotExists().create();
                     schema.vertexLabel('shortvertex').properties('shortvalue').ifNotExists().create();
                     short s1 = 5000; graph.addVertex(label, "shortvertex", "shortvalue", s1);''',
                  '''schema.propertyKey('intvalue').Int().ifNotExists().create();
                     schema.vertexLabel('intvertex').properties('intvalue').ifNotExists().create();
                     int i1 = 1000000000; graph.addVertex(label, "intvertex", "intvalue", i1);''',
                  '''schema.propertyKey('intvalue2').Int().ifNotExists().create();
                     schema.vertexLabel('intvertex2').properties('intvalue2').ifNotExists().create();
                     Integer i2 = 100000000; graph.addVertex(label, "intvertex2", "intvalue2", i2);''',
                  '''schema.propertyKey('longvalue').Bigint().ifNotExists().create();
                     schema.vertexLabel('longvertex').properties('longvalue').ifNotExists().create();
                     long l1 = 9223372036854775807; graph.addVertex(label, "longvertex", "longvalue", l1);''',
                  '''schema.propertyKey('longvalue2').Bigint().ifNotExists().create();
                     schema.vertexLabel('longvertex2').properties('longvalue2').ifNotExists().create();
                     Long l2 = 100000000000000000L; graph.addVertex(label, "longvertex2", "longvalue2", l2);''',
                  '''schema.propertyKey('floatvalue').Float().ifNotExists().create();
                     schema.vertexLabel('floatvertex').properties('floatvalue').ifNotExists().create();
                     float f1 = 3.5f; graph.addVertex(label, "floatvertex", "floatvalue", f1);''',
                  '''schema.propertyKey('doublevalue').Double().ifNotExists().create();
                     schema.vertexLabel('doublevertex').properties('doublevalue').ifNotExists().create();
                     double d1 = 3.5e40; graph.addVertex(label, "doublevertex", "doublevalue", d1);''',
                  '''schema.propertyKey('doublevalue2').Double().ifNotExists().create();
                     schema.vertexLabel('doublevertex2').properties('doublevalue2').ifNotExists().create();
                     Double d2 = 3.5e40d; graph.addVertex(label, "doublevertex2", "doublevalue2", d2);''']


        for run in to_run:
            session.execute_graph(run)

        if DSE_VERSION >= Version('5.1'):
            to_run_51=['''schema.propertyKey('datevalue1').Date().ifNotExists().create();
                     schema.vertexLabel('datevertex1').properties('datevalue1').ifNotExists().create();''',
                       '''schema.propertyKey('negdatevalue2').Date().ifNotExists().create();
                     schema.vertexLabel('negdatevertex2').properties('negdatevalue2').ifNotExists().create();''']
            for i in range(1,4):
                to_run_51.append('''schema.propertyKey('timevalue{0}').Time().ifNotExists().create();
                     schema.vertexLabel('timevertex{0}').properties('timevalue{0}').ifNotExists().create();'''.format(i))

            for run in to_run_51:
                session.execute_graph(run)

            session.execute_graph('''graph.addVertex(label, "datevertex1", "datevalue1", date1);''',
                                  {'date1': '1999-07-29' })
            session.execute_graph('''graph.addVertex(label, "negdatevertex2", "negdatevalue2", date2);''',
                                  {'date2': '-1999-07-28' })

            session.execute_graph('''graph.addVertex(label, "timevertex1", "timevalue1", time1);''',
                                  {'time1': '14:02'})
            session.execute_graph('''graph.addVertex(label, "timevertex2", "timevalue2", time2);''',
                                  {'time2': '14:02:20'})
            session.execute_graph('''graph.addVertex(label, "timevertex3", "timevalue3", time3);''',
                                  {'time3': '14:02:20.222'})


def generate_type_graph_schema(session, prime_schema=True):
    """
    This method will prime the schema for all types in the TYPE_MAP
    """
    session.execute_graph(ALLOW_SCANS)
    if(prime_schema):
        create_vertex= "schema.vertexLabel('{0}').ifNotExists().create();".\
            format(generate_type_graph_schema.single_vertex)
        session.execute_graph(create_vertex)
        for key in TYPE_MAP.keys():
            prop_type = fetchCustomGeoType(key)
            if prop_type is None:
                prop_type=TYPE_MAP[key][0]
            vertex_label = key
            prop_name = key+"value"
            insert_string = ""
            insert_string += "schema.propertyKey('{0}').{1}.ifNotExists().create();".format(prop_name, prop_type)
            insert_string += "schema.vertexLabel('{}').properties('{}').add();".\
                format(generate_type_graph_schema.single_vertex, prop_name)
            session.execute_graph(insert_string)
    else:
        session.execute_graph(MAKE_NON_STRICT)
generate_type_graph_schema.single_vertex = "single_vertex_label"

def generate_address_book_graph(session, size):
    to_run = [ALLOW_SCANS,
              "schema.propertyKey('name').Text().create()\n" +
              "schema.propertyKey('pointPropWithBoundsWithSearchIndex')." + getPointTypeWithBounds(-100, -100, 100, 100) + ".create()\n" +
              "schema.propertyKey('pointPropWithBounds')." + getPointTypeWithBounds(-100, -100, 100, 100) + ".create()\n" +
              "schema.propertyKey('pointPropWithGeoBoundsWithSearchIndex')." + getPointType() + ".create()\n" +
              "schema.propertyKey('pointPropWithGeoBounds')." + getPointType() + ".create()\n" +
              "schema.propertyKey('city').Text().create()\n" +
              "schema.propertyKey('state').Text().create()\n" +
              "schema.propertyKey('description').Text().create()\n" +
              "schema.vertexLabel('person').properties('name', 'city', 'state', 'description', 'pointPropWithBoundsWithSearchIndex', 'pointPropWithBounds', 'pointPropWithGeoBoundsWithSearchIndex', 'pointPropWithGeoBounds').create()",
              "schema.vertexLabel('person').index('searchPointWithBounds').secondary().by('pointPropWithBounds').add()",
              "schema.vertexLabel('person').index('searchPointWithGeoBounds').secondary().by('pointPropWithGeoBounds').add()",

              "g.addV('person').property('name', 'Paul Thomas Joe').property('city', 'Rochester').property('state', 'MN').property('pointPropWithBoundsWithSearchIndex', Geo.point(-92.46295, 44.0234)).property('pointPropWithBounds', Geo.point(-92.46295, 44.0234)).property('pointPropWithGeoBoundsWithSearchIndex', Geo.point(-92.46295, 44.0234)).property('pointPropWithGeoBounds', Geo.point(-92.46295, 44.0234)).property('description', 'Lives by the hospital')",
              "g.addV('person').property('name', 'George Bill Steve').property('city', 'Minneapolis').property('state', 'MN').property('pointPropWithBoundsWithSearchIndex', Geo.point(-93.266667, 44.093333)).property('pointPropWithBounds', Geo.point(-93.266667, 44.093333)).property('pointPropWithGeoBoundsWithSearchIndex', Geo.point(-93.266667, 44.093333)).property('pointPropWithGeoBounds', Geo.point(-93.266667, 44.093333)).property('description', 'A cold dude')",
              "g.addV('person').property('name', 'James Paul Smith').property('city', 'Chicago').property('state', 'IL').property('pointPropWithBoundsWithSearchIndex', Geo.point(-87.684722, 41.836944)).property('description', 'Likes to hang out')",
              "g.addV('person').property('name', 'Jill Alice').property('city', 'Atlanta').property('state', 'GA').property('pointPropWithBoundsWithSearchIndex', Geo.point(-84.39, 33.755)).property('description', 'Enjoys a nice cold coca cola')",
              ]

    if not Version('5.0') <= DSE_VERSION < Version('5.1'):
        to_run.append("schema.vertexLabel('person').index('search').search().by('pointPropWithBoundsWithSearchIndex').withError(0.00001, 0.0).by('pointPropWithGeoBoundsWithSearchIndex').withError(0.00001, 0.0).add()")

    for run in to_run:
        session.execute_graph(run)


def generate_large_complex_graph(session, size):
        to_run = '''schema.config().option('graph.schema_mode').set('development');
            int size = 2000;
            List ids = new ArrayList();
            schema.propertyKey('ts').Int().single().ifNotExists().create();
            schema.propertyKey('sin').Int().single().ifNotExists().create();
            schema.propertyKey('cos').Int().single().ifNotExists().create();
            schema.propertyKey('ii').Int().single().ifNotExists().create();
            schema.vertexLabel('lcg').properties('ts', 'sin', 'cos', 'ii').ifNotExists().create();
            schema.edgeLabel('linked').connection('lcg', 'lcg').ifNotExists().create();
            Vertex v = graph.addVertex(label, 'lcg');
            v.property("ts", 100001);
            v.property("sin", 0);
            v.property("cos", 1);
            v.property("ii", 0);
            ids.add(v.id());
            Random rand = new Random();
            for (int ii = 1; ii < size; ii++) {
                v = graph.addVertex(label, 'lcg');
                v.property("ii", ii);
                v.property("ts", 100001 + ii);
                v.property("sin", Math.sin(ii/5.0));
                v.property("cos", Math.cos(ii/5.0));
                Vertex u = g.V(ids.get(rand.nextInt(ids.size()))).next();
                v.addEdge("linked", u);
                ids.add(u.id());
                ids.add(v.id());
            }
            g.V().count();'''
        prof = session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT, request_timeout=32)
        session.execute_graph(to_run, execution_profile=prof)


def validate_classic_vertex(test, vertex):
        vertex_props = vertex.properties.keys()
        test.assertEqual(len(vertex_props), 2)
        test.assertIn('name', vertex_props)
        test.assertTrue('lang' in vertex_props or 'age' in vertex_props)


def validate_classic_vertex_return_type(test, vertex):
    validate_generic_vertex_result_type(vertex)
    vertex_props = vertex.properties
    test.assertIn('name', vertex_props)
    test.assertTrue('lang' in vertex_props or 'age' in vertex_props)


def validate_generic_vertex_result_type(test, vertex):
    test.assertIsInstance(vertex, Vertex)
    for attr in ('id', 'type', 'label', 'properties'):
        test.assertIsNotNone(getattr(vertex, attr))


def validate_classic_edge_properties(test, edge_properties):
    test.assertEqual(len(edge_properties.keys()), 1)
    test.assertIn('weight', edge_properties)
    test.assertIsInstance(edge_properties, dict)


def validate_classic_edge(test, edge):
    validate_generic_edge_result_type(test, edge)
    validate_classic_edge_properties(test, edge.properties)


def validate_line_edge(test, edge):
    validate_generic_edge_result_type(test, edge)
    edge_props = edge.properties
    test.assertEqual(len(edge_props.keys()), 1)
    test.assertIn('distance', edge_props)


def validate_generic_edge_result_type(test, edge):
    test.assertIsInstance(edge, Edge)
    for attr in ('properties', 'outV', 'outVLabel', 'inV', 'inVLabel', 'label', 'type', 'id'):
        test.assertIsNotNone(getattr(edge, attr))


def validate_path_result_type(test, path):
    test.assertIsInstance(path, Path)
    test.assertIsNotNone(path.labels)
    for obj in path.objects:
        if isinstance(obj, Edge):
            validate_classic_edge(test, obj)
        elif isinstance(obj, Vertex):
            validate_classic_vertex(test, obj)
        else:
            test.fail("Invalid object found in path " + str(object.type))
