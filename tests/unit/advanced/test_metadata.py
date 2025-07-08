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
import datetime
import unittest

from cassandra.metadata import (
    KeyspaceMetadata, TableMetadataDSE68,
    VertexMetadata, EdgeMetadata, SchemaParserV22, _SchemaParser
)
from cassandra.protocol import ResultMessage, RESULT_KIND_ROWS


class GraphMetadataToCQLTests(unittest.TestCase):

    def _create_edge_metadata(self, partition_keys=['pk1'], clustering_keys=['c1']):
        return EdgeMetadata(
            'keyspace', 'table', 'label', 'from_table', 'from_label',
            partition_keys, clustering_keys, 'to_table', 'to_label',
            partition_keys, clustering_keys)

    def _create_vertex_metadata(self, label_name='label'):
        return VertexMetadata('keyspace', 'table', label_name)

    def _create_keyspace_metadata(self, graph_engine):
        return KeyspaceMetadata(
            'keyspace', True, 'org.apache.cassandra.locator.SimpleStrategy',
            {'replication_factor': 1}, graph_engine=graph_engine)

    def _create_table_metadata(self, with_vertex=False, with_edge=False):
        tm = TableMetadataDSE68('keyspace', 'table')
        if with_vertex:
            tm.vertex = self._create_vertex_metadata() if with_vertex is True else with_vertex
        elif with_edge:
            tm.edge = self._create_edge_metadata() if with_edge is True else with_edge

        return tm

    def test_keyspace_no_graph_engine(self):
        km = self._create_keyspace_metadata(None)
        assert km.graph_engine == None
        self.assertNotIn(
            "graph_engine",
            km.as_cql_query()
        )

    def test_keyspace_with_graph_engine(self):
        graph_engine = 'Core'
        km = self._create_keyspace_metadata(graph_engine)
        assert km.graph_engine == graph_engine
        cql = km.as_cql_query()
        self.assertIn(
            "graph_engine",
            cql
        )
        self.assertIn(
            "Core",
            cql
        )

    def test_table_no_vertex_or_edge(self):
        tm = self._create_table_metadata()
        assert tm.vertex is None
        assert tm.edge is None
        cql = tm.as_cql_query()
        self.assertNotIn("VERTEX LABEL", cql)
        self.assertNotIn("EDGE LABEL", cql)

    def test_table_with_vertex(self):
        tm = self._create_table_metadata(with_vertex=True)
        self.assertIsInstance(tm.vertex, VertexMetadata)
        assert tm.edge is None
        cql = tm.as_cql_query()
        self.assertIn("VERTEX LABEL", cql)
        self.assertNotIn("EDGE LABEL", cql)

    def test_table_with_edge(self):
        tm = self._create_table_metadata(with_edge=True)
        assert tm.vertex is None
        self.assertIsInstance(tm.edge, EdgeMetadata)
        cql = tm.as_cql_query()
        self.assertNotIn("VERTEX LABEL", cql)
        self.assertIn("EDGE LABEL", cql)
        self.assertIn("FROM from_label", cql)
        self.assertIn("TO to_label", cql)

    def test_vertex_with_label(self):
        tm = self. _create_table_metadata(with_vertex=True)
        self.assertTrue(tm.as_cql_query().endswith('VERTEX LABEL label'))

    def test_edge_single_partition_key_and_clustering_key(self):
        tm = self._create_table_metadata(with_edge=True)
        self.assertIn(
            'FROM from_label(pk1, c1)',
            tm.as_cql_query()
        )

    def test_edge_multiple_partition_keys(self):
        edge = self._create_edge_metadata(partition_keys=['pk1', 'pk2'])
        tm = self. _create_table_metadata(with_edge=edge)
        self.assertIn(
            'FROM from_label((pk1, pk2), ',
            tm.as_cql_query()
        )

    def test_edge_no_clustering_keys(self):
        edge = self._create_edge_metadata(clustering_keys=[])
        tm = self. _create_table_metadata(with_edge=edge)
        self.assertIn(
            'FROM from_label(pk1) ',
            tm.as_cql_query()
        )

    def test_edge_multiple_clustering_keys(self):
        edge = self._create_edge_metadata(clustering_keys=['c1', 'c2'])
        tm = self. _create_table_metadata(with_edge=edge)
        self.assertIn(
            'FROM from_label(pk1, c1, c2) ',
            tm.as_cql_query()
        )

    def test_edge_multiple_partition_and_clustering_keys(self):
        edge = self._create_edge_metadata(partition_keys=['pk1', 'pk2'],
                                          clustering_keys=['c1', 'c2'])
        tm = self. _create_table_metadata(with_edge=edge)
        self.assertIn(
            'FROM from_label((pk1, pk2), c1, c2) ',
            tm.as_cql_query()
        )


class SchemaParsersTests(unittest.TestCase):
    def test_metadata_query_metadata_timeout(self):
        class FakeConnection:
            def __init__(self):
                self.queries = []

            def wait_for_responses(self, *msgs, **kwargs):
                self.queries.extend(msgs)
                local_response = ResultMessage(kind=RESULT_KIND_ROWS)
                local_response.column_names = []
                local_response.parsed_rows = []

                return [[local_response, local_response] for _ in msgs]

        for schemaClass in get_all_schema_parser_classes(_SchemaParser):
            conn = FakeConnection()
            p = schemaClass(conn, 2.0, 1000, None)
            p._query_all()

            for q in conn.queries:
                if "USING TIMEOUT" in q.query:
                    self.fail(f"<{schemaClass.__name__}> query `{q.query}` contains `USING TIMEOUT`, while should not")

            conn = FakeConnection()
            p = schemaClass(conn, 2.0, 1000, datetime.timedelta(seconds=2))
            p._query_all()

            for q in conn.queries:
                if "USING TIMEOUT 2000ms" not in q.query:
                    self.fail(f"{schemaClass.__name__} query `{q.query}` does not contain `USING TIMEOUT 2000ms`")


def get_all_schema_parser_classes(cl):
    for child in cl.__subclasses__():
        if not child.__name__.startswith('SchemaParser') or child.__module__ != 'cassandra.metadata':
            continue
        yield child
        for c in get_all_schema_parser_classes(child):
            yield c
