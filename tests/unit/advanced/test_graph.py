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

import warnings
import json

import unittest

from cassandra import ConsistencyLevel
from cassandra.policies import RetryPolicy
from cassandra.graph import (SimpleGraphStatement, GraphOptions, GraphProtocol, Result,
                       graph_result_row_factory, single_object_row_factory,
                       Vertex, Edge, Path, VertexProperty)
from cassandra.datastax.graph.query import _graph_options
from tests.util import assertRegex

class GraphResultTests(unittest.TestCase):

    _values = (None, 1, 1.2, True, False, [1, 2, 3], {'x': 1, 'y': 2})

    def test_result_value(self):
        for v in self._values:
            result = self._make_result(v)
            assert result.value == v

    def test_result_attr(self):
        # value is not a dict
        result = self._make_result(123)
        with self.assertRaises(ValueError):
            result.something

        expected = {'a': 1, 'b': 2}
        result = self._make_result(expected)
        assert result.a == 1
        assert result.b == 2
        with self.assertRaises(AttributeError):
            result.not_present

    def test_result_item(self):
        # value is not a dict, list
        result = self._make_result(123)
        with self.assertRaises(ValueError):
            result['something']
        with self.assertRaises(ValueError):
            result[0]

        # dict key access
        expected = {'a': 1, 'b': 2}
        result = self._make_result(expected)
        assert result['a'] == 1
        assert result['b'] == 2
        with self.assertRaises(KeyError):
            result['not_present']
        with self.assertRaises(ValueError):
            result[0]

        # list index access
        expected = [0, 1]
        result = self._make_result(expected)
        assert result[0] == 0
        assert result[1] == 1
        with self.assertRaises(IndexError):
            result[2]
        with self.assertRaises(ValueError):
            result['something']

    def test_as_vertex(self):
        prop_name = 'name'
        prop_val = 'val'
        vertex_dict = {'id': object(),
                       'label': object(),
                       'type': 'vertex',
                       'properties': {prop_name: [{'value': prop_val, 'whatever': object()}]}
                      }
        required_attrs = [k for k in vertex_dict if k != 'properties']
        result = self._make_result(vertex_dict)
        vertex = result.as_vertex()
        for attr in required_attrs:
            assert getattr(vertex, attr) == vertex_dict[attr]
        assert len(vertex.properties) == 1
        assert vertex.properties[prop_name][0].value == prop_val

        # no props
        modified_vertex_dict = vertex_dict.copy()
        del modified_vertex_dict['properties']
        vertex = self._make_result(modified_vertex_dict).as_vertex()
        assert vertex.properties == {}

        # wrong 'type'
        modified_vertex_dict = vertex_dict.copy()
        modified_vertex_dict['type'] = 'notavertex'
        result = self._make_result(modified_vertex_dict)
        self.assertRaises(TypeError, result.as_vertex)

        # missing required properties
        for attr in required_attrs:
            modified_vertex_dict = vertex_dict.copy()
            del modified_vertex_dict[attr]
            result = self._make_result(modified_vertex_dict)
            self.assertRaises(TypeError, result.as_vertex)

    def test_as_edge(self):
        prop_name = 'name'
        prop_val = 'val'
        edge_dict = {'id': object(),
                     'label': object(),
                     'type': 'edge',
                     'inV': object(),
                     'inVLabel': object(),
                     'outV': object(),
                     'outVLabel': object(),
                     'properties': {prop_name: prop_val}
                    }
        required_attrs = [k for k in edge_dict if k != 'properties']
        result = self._make_result(edge_dict)
        edge = result.as_edge()
        for attr in required_attrs:
            assert getattr(edge, attr) == edge_dict[attr]
        assert len(edge.properties) == 1
        assert edge.properties[prop_name] == prop_val

        # no props
        modified_edge_dict = edge_dict.copy()
        del modified_edge_dict['properties']
        edge = self._make_result(modified_edge_dict).as_edge()
        assert edge.properties == {}

        # wrong 'type'
        modified_edge_dict = edge_dict.copy()
        modified_edge_dict['type'] = 'notanedge'
        result = self._make_result(modified_edge_dict)
        self.assertRaises(TypeError, result.as_edge)

        # missing required properties
        for attr in required_attrs:
            modified_edge_dict = edge_dict.copy()
            del modified_edge_dict[attr]
            result = self._make_result(modified_edge_dict)
            self.assertRaises(TypeError, result.as_edge)

    def test_as_path(self):
        vertex_dict = {'id': object(),
                       'label': object(),
                       'type': 'vertex',
                       'properties': {'name': [{'value': 'val', 'whatever': object()}]}
                       }
        edge_dict = {'id': object(),
                     'label': object(),
                     'type': 'edge',
                     'inV': object(),
                     'inVLabel': object(),
                     'outV': object(),
                     'outVLabel': object(),
                     'properties': {'name': 'val'}
                     }
        path_dict = {'labels': [['a', 'b'], ['c']],
                     'objects': [vertex_dict, edge_dict]
                    }
        result = self._make_result(path_dict)
        path = result.as_path()
        assert path.labels == path_dict['labels']

        # make sure inner objects are bound correctly
        assert isinstance(path.objects[0], Vertex)
        assert isinstance(path.objects[1], Edge)

        # missing required properties
        for attr in path_dict:
            modified_path_dict = path_dict.copy()
            del modified_path_dict[attr]
            result = self._make_result(modified_path_dict)
            self.assertRaises(TypeError, result.as_path)

    def test_str(self):
        for v in self._values:
            assert str(self._make_result(v)) == str(v)

    def test_repr(self):
        for v in self._values:
            result = self._make_result(v)
            assert eval(repr(result)) == result

    def _make_result(self, value):
        # direct pass-through now
        return Result(value)


class GraphTypeTests(unittest.TestCase):
    # see also: GraphResultTests.test_as_*

    def test_vertex_str_repr(self):
        prop_name = 'name'
        prop_val = 'val'
        kwargs = {'id': 'id_val', 'label': 'label_val', 'type': 'vertex', 'properties': {prop_name: [{'value': prop_val}]}}
        vertex = Vertex(**kwargs)
        transformed = kwargs.copy()
        transformed['properties'] = {prop_name: [VertexProperty(prop_name, prop_val)]}
        assert eval(str(vertex)) == transformed
        assert eval(repr(vertex)) == vertex

    def test_edge_str_repr(self):
        prop_name = 'name'
        prop_val = 'val'
        kwargs = {'id': 'id_val', 'label': 'label_val', 'type': 'edge',
                  'inV': 'inV_val', 'inVLabel': 'inVLabel_val',
                  'outV': 'outV_val', 'outVLabel': 'outVLabel_val',
                  'properties': {prop_name: prop_val}}
        edge = Edge(**kwargs)
        assert eval(str(edge)) == kwargs
        assert eval(repr(edge)) == edge

    def test_path_str_repr(self):
        kwargs = {'labels': [['a', 'b'], ['c']], 'objects': range(10)}
        path = Path(**kwargs)
        transformed = kwargs.copy()
        transformed['objects'] = [Result(o) for o in kwargs['objects']]
        assert eval(str(path)) == transformed
        assert eval(repr(path)) == path


class GraphOptionTests(unittest.TestCase):

    opt_mapping = dict((t[0], t[2]) for t in _graph_options if not
        (t[0].endswith('consistency_level') or  # cl excluded from general tests because it requires mapping to names
         t[0] == 'graph_protocol'))  # default is None

    api_params = dict((p, str(i)) for i, p in enumerate(opt_mapping))

    def test_init(self):
        opts = GraphOptions(**self.api_params)
        self._verify_api_params(opts, self.api_params)
        self._verify_api_params(GraphOptions(), {
            'graph_source': 'g',
            'graph_language': 'gremlin-groovy'
        })

    def test_with_graph_protocol(self):
        opts = GraphOptions(graph_protocol='graphson-2-0')
        assert opts._graph_options == {
            'graph-source': b'g',
            'graph-language': b'gremlin-groovy',
            'graph-results': b'graphson-2-0'
        }

    def test_init_unknown_kwargs(self):
        with warnings.catch_warnings(record=True) as w:
            GraphOptions(unknown_param=42)
        assert len(w) == 1
        assertRegex(str(w[0].message), r"^Unknown keyword.*GraphOptions.*")

    def test_update(self):
        opts = GraphOptions(**self.api_params)
        new_params = dict((k, str(int(v) + 1)) for k, v in self.api_params.items())
        opts.update(GraphOptions(**new_params))
        self._verify_api_params(opts, new_params)

    def test_get_options(self):
        # nothing set --> base map
        base = GraphOptions(**self.api_params)
        assert GraphOptions().get_options_map(base) == base._graph_options

        # something set overrides
        kwargs = self.api_params.copy()  # this test concept got strange after we added default values for a couple GraphOption attrs
        kwargs['graph_name'] = 'unit_test'
        other = GraphOptions(**kwargs)
        options = base.get_options_map(other)
        updated = self.opt_mapping['graph_name']
        assert options[updated] == b'unit_test'
        for name in (n for n in self.opt_mapping.values() if n != updated):
            assert options[name] == base._graph_options[name]

        # base unchanged
        self._verify_api_params(base, self.api_params)

    def test_set_attr(self):
        expected = 'test@@@@'
        opts = GraphOptions(graph_name=expected)
        assert opts.graph_name == expected.encode()
        expected = 'somethingelse####'
        opts.graph_name = expected
        assert opts.graph_name == expected.encode()

        # will update options with set value
        another = GraphOptions()
        assert another.graph_name is None
        another.update(opts)
        assert another.graph_name == expected.encode()

        opts.graph_name = None
        assert opts.graph_name is None
        # will not update another with its set-->unset value
        another.update(opts)
        assert another.graph_name == expected.encode()  # remains unset
        opt_map = another.get_options_map(opts)
        assert opt_map == another._graph_options

    def test_del_attr(self):
        opts = GraphOptions(**self.api_params)
        test_params = self.api_params.copy()
        del test_params['graph_source']
        del opts.graph_source
        self._verify_api_params(opts, test_params)

    def _verify_api_params(self, opts, api_params):
        assert len(opts._graph_options) == len(api_params)
        for name, value in api_params.items():
            try:
                value = value.encode()
            except:
                pass  # already bytes
            assert getattr(opts, name) == value
            assert opts._graph_options[self.opt_mapping[name]] == value

    def test_consistency_levels(self):
        read_cl = ConsistencyLevel.ONE
        write_cl = ConsistencyLevel.LOCAL_QUORUM

        # set directly
        opts = GraphOptions(graph_read_consistency_level=read_cl, graph_write_consistency_level=write_cl)
        assert opts.graph_read_consistency_level == read_cl
        assert opts.graph_write_consistency_level == write_cl

        # mapping from base
        opt_map = opts.get_options_map()
        assert opt_map['graph-read-consistency'] == ConsistencyLevel.value_to_name[read_cl].encode()
        assert opt_map['graph-write-consistency'] == ConsistencyLevel.value_to_name[write_cl].encode()

        # empty by default
        new_opts = GraphOptions()
        opt_map = new_opts.get_options_map()
        assert 'graph-read-consistency' not in opt_map
        assert 'graph-write-consistency' not in opt_map

        # set from other
        opt_map = new_opts.get_options_map(opts)
        assert opt_map['graph-read-consistency'] == ConsistencyLevel.value_to_name[read_cl].encode()
        assert opt_map['graph-write-consistency'] == ConsistencyLevel.value_to_name[write_cl].encode()

    def test_graph_source_convenience_attributes(self):
        opts = GraphOptions()
        assert opts.graph_source == b'g'
        assert not opts.is_analytics_source
        assert opts.is_graph_source
        assert not opts.is_default_source

        opts.set_source_default()
        assert opts.graph_source is not None
        assert not opts.is_analytics_source
        assert not opts.is_graph_source
        assert opts.is_default_source

        opts.set_source_analytics()
        assert opts.graph_source is not None
        assert opts.is_analytics_source
        assert not opts.is_graph_source
        assert not opts.is_default_source

        opts.set_source_graph()
        assert opts.graph_source is not None
        assert not opts.is_analytics_source
        assert opts.is_graph_source
        assert not opts.is_default_source

class GraphStatementTests(unittest.TestCase):

    def test_init(self):
        # just make sure Statement attributes are accepted
        kwargs = {'query_string': object(),
                  'retry_policy': RetryPolicy(),
                  'consistency_level': object(),
                  'fetch_size': object(),
                  'keyspace': object(),
                  'custom_payload': object()}
        statement = SimpleGraphStatement(**kwargs)
        for k, v in kwargs.items():
            assert getattr(statement, k) is v

        # but not a bogus parameter
        kwargs['bogus'] = object()
        self.assertRaises(TypeError, SimpleGraphStatement, **kwargs)


class GraphRowFactoryTests(unittest.TestCase):

    def test_object_row_factory(self):
        col_names = []  # unused
        rows = [object() for _ in range(10)]
        assert single_object_row_factory(col_names, ((o,) for o in rows)) == rows

    def test_graph_result_row_factory(self):
        col_names = []  # unused
        rows = [json.dumps({'result': i}) for i in range(10)]
        results = graph_result_row_factory(col_names, ((o,) for o in rows))
        for i, res in enumerate(results):
            assert isinstance(res, Result)
            assert res.value == i
