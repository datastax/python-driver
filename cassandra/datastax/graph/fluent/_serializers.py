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

from collections import OrderedDict

import six

from gremlin_python.structure.io.graphsonV2d0 import (
    GraphSONReader,
    GraphSONUtil,
    VertexDeserializer,
    VertexPropertyDeserializer,
    PropertyDeserializer,
    EdgeDeserializer,
    PathDeserializer
)

from cassandra.datastax.graph.graphson import (
    GraphSON2Serializer,
    GraphSON2Deserializer
)

from cassandra.datastax.graph.fluent.predicates import GeoP, TextDistanceP
from cassandra.util import Distance


__all__ = ['GremlinGraphSONReader', 'GeoPSerializer', 'TextDistancePSerializer',
           'DistanceIO', 'gremlin_deserializers', 'deserializers', 'serializers']


class _GremlinGraphSONTypeSerializer(object):

    @classmethod
    def dictify(cls, v, _):
        return GraphSON2Serializer.serialize(v)


class _GremlinGraphSONTypeDeserializer(object):

    deserializer = None

    def __init__(self, deserializer):
        self.deserializer = deserializer

    def objectify(self, v, reader):
        return self.deserializer.deserialize(v, reader=reader)


def _make_gremlin_deserializer(graphson_type):
    return _GremlinGraphSONTypeDeserializer(
        GraphSON2Deserializer.get_deserializer(graphson_type.graphson_type)
    )


class GremlinGraphSONReader(GraphSONReader):
    """Gremlin GraphSONReader Adapter, required to use gremlin types"""

    def deserialize(self, obj):
        return self.toObject(obj)


class GeoPSerializer(object):
    @classmethod
    def dictify(cls, p, writer):
        out = {
            "predicateType": "Geo",
            "predicate": p.operator,
            "value": [writer.toDict(p.value), writer.toDict(p.other)] if p.other is not None else writer.toDict(p.value)
        }
        return GraphSONUtil.typedValue("P", out, prefix='dse')


class TextDistancePSerializer(object):
    @classmethod
    def dictify(cls, p, writer):
        out = {
            "predicate": p.operator,
            "value": {
                'query': writer.toDict(p.value),
                'distance': writer.toDict(p.distance)
            }
        }
        return GraphSONUtil.typedValue("P", out)


class DistanceIO(object):
    @classmethod
    def dictify(cls, v, _):
        return GraphSONUtil.typedValue('Distance', six.text_type(v), prefix='dse')


serializers = OrderedDict([
    (t, _GremlinGraphSONTypeSerializer)
    for t in six.iterkeys(GraphSON2Serializer.get_type_definitions())
])

# Predicates
serializers.update(OrderedDict([
    (Distance, DistanceIO),
    (GeoP, GeoPSerializer),
    (TextDistanceP, TextDistancePSerializer)
]))

deserializers = {
    k: _make_gremlin_deserializer(v)
    for k, v in six.iteritems(GraphSON2Deserializer.get_type_definitions())
}

deserializers.update({
    "dse:Distance": DistanceIO,
})

gremlin_deserializers = deserializers.copy()
gremlin_deserializers.update({
    'g:Vertex': VertexDeserializer,
    'g:VertexProperty': VertexPropertyDeserializer,
    'g:Edge': EdgeDeserializer,
    'g:Property': PropertyDeserializer,
    'g:Path': PathDeserializer
})
