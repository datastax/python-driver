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
import base64
import uuid
import re
import json
from decimal import Decimal
from collections import OrderedDict

import six

if six.PY3:
    import ipaddress

from cassandra.util import Polygon, Point, LineString
from cassandra.datastax.graph.types import Vertex, VertexProperty, Edge, Path

__all__ = ['GraphSON1Serializer', 'GraphSON1Deserializer', 'GraphSON1TypeDeserializer',
           'GraphSON2Serializer', 'GraphSON2Deserializer',
           'GraphSON2Reader', 'BooleanTypeIO', 'Int16TypeIO', 'Int32TypeIO', 'DoubleTypeIO',
           'FloatTypeIO', 'UUIDTypeIO', 'BigDecimalTypeIO', 'DurationTypeIO', 'InetTypeIO',
           'InstantTypeIO', 'LocalDateTypeIO', 'LocalTimeTypeIO', 'Int64TypeIO', 'BigIntegerTypeIO',
           'LocalDateTypeIO', 'PolygonTypeIO', 'PointTypeIO', 'LineStringTypeIO', 'BlobTypeIO']

"""
Supported types:

DSE Graph      GraphSON 2.0     Python Driver
------------ | -------------- | ------------
text         | ------         | str
boolean      | g:Boolean      | bool
bigint       | g:Int64        | long
int          | g:Int32        | int
double       | g:Double       | float
float        | g:Float        | float
uuid         | g:UUID         | UUID
bigdecimal   | gx:BigDecimal  | Decimal
duration     | gx:Duration    | timedelta
inet         | gx:InetAddress | str (unicode), IPV4Address/IPV6Address (PY3)
timestamp    | gx:Instant     | datetime.datetime
date         | gx:LocalDate   | datetime.date
time         | gx:LocalTime   | datetime.time
smallint     | gx:Int16       | int
varint       | gx:BigInteger  | long
date         | gx:LocalDate   | Date
polygon      | dse:Polygon    | Polygon
point        | dse:Point      | Point
linestring   | dse:LineString | LineString
blob         | dse:Blob       | bytearray, buffer (PY2), memoryview (PY3), bytes (PY3)
"""

MAX_INT32 = 2 ** 32 - 1
MIN_INT32 = -2 ** 31


class _GraphSONTypeType(type):
    """GraphSONType metaclass, required to create a class property."""

    @property
    def graphson_type(cls):
        return "{0}:{1}".format(cls.prefix, cls.graphson_base_type)


@six.add_metaclass(_GraphSONTypeType)
class GraphSONTypeIO(object):
    """Represent a serializable GraphSON type"""

    prefix = 'g'
    graphson_base_type = None

    @classmethod
    def serialize(cls, value):
        return six.text_type(value)

    @classmethod
    def deserialize(cls, value, reader=None):
        return value

    @classmethod
    def get_specialized_serializer(cls, value):
        return cls


class BooleanTypeIO(GraphSONTypeIO):
    graphson_base_type = 'Boolean'

    @classmethod
    def serialize(cls, value):
        return bool(value)


class IntegerTypeIO(GraphSONTypeIO):

    @classmethod
    def serialize(cls, value):
        return value

    @classmethod
    def get_specialized_serializer(cls, value):
        if type(value) in six.integer_types and (value > MAX_INT32 or value < MIN_INT32):
            return Int64TypeIO

        return Int32TypeIO


class Int16TypeIO(IntegerTypeIO):
    prefix = 'gx'
    graphson_base_type = 'Int16'


class Int32TypeIO(IntegerTypeIO):
    graphson_base_type = 'Int32'


class Int64TypeIO(IntegerTypeIO):
    graphson_base_type = 'Int64'

    @classmethod
    def deserialize(cls, value, reader=None):
        if six.PY3:
            return value
        return long(value)


class FloatTypeIO(GraphSONTypeIO):
    graphson_base_type = 'Float'

    @classmethod
    def deserialize(cls, value, reader=None):
        return float(value)


class DoubleTypeIO(FloatTypeIO):
    graphson_base_type = 'Double'


class BigIntegerTypeIO(IntegerTypeIO):
    prefix = 'gx'
    graphson_base_type = 'BigInteger'


class LocalDateTypeIO(GraphSONTypeIO):
    FORMAT = '%Y-%m-%d'

    prefix = 'gx'
    graphson_base_type = 'LocalDate'

    @classmethod
    def serialize(cls, value):
        return value.isoformat()

    @classmethod
    def deserialize(cls, value, reader=None):
        try:
            return datetime.datetime.strptime(value, cls.FORMAT).date()
        except ValueError:
            # negative date
            return value

    @classmethod
    def get_specialized_serializer(cls, value):
        if isinstance(value, datetime.datetime):
            return InstantTypeIO

        return cls


class InstantTypeIO(GraphSONTypeIO):
    prefix = 'gx'
    graphson_base_type = 'Instant'

    @classmethod
    def serialize(cls, value):
        if isinstance(value, datetime.datetime):
            value = datetime.datetime(*value.utctimetuple()[:6]).replace(microsecond=value.microsecond)
        else:
            value = datetime.datetime.combine(value, datetime.datetime.min.time())

        return "{0}Z".format(value.isoformat())

    @classmethod
    def deserialize(cls, value, reader=None):
        try:
            d = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            d = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
        return d


class LocalTimeTypeIO(GraphSONTypeIO):
    FORMATS = [
        '%H:%M',
        '%H:%M:%S',
        '%H:%M:%S.%f'
    ]

    prefix = 'gx'
    graphson_base_type = 'LocalTime'

    @classmethod
    def serialize(cls, value):
        return value.strftime(cls.FORMATS[2])

    @classmethod
    def deserialize(cls, value, reader=None):
        dt = None
        for f in cls.FORMATS:
            try:
                dt = datetime.datetime.strptime(value, f)
                break
            except ValueError:
                continue

        if dt is None:
            raise ValueError('Unable to decode LocalTime: {0}'.format(value))

        return dt.time()


class BlobTypeIO(GraphSONTypeIO):
    prefix = 'dse'
    graphson_base_type = 'Blob'

    @classmethod
    def serialize(cls, value):
        value = base64.b64encode(value)
        if six.PY3:
            value = value.decode('utf-8')
        return value

    @classmethod
    def deserialize(cls, value, reader=None):
        return bytearray(base64.b64decode(value))


class UUIDTypeIO(GraphSONTypeIO):
    graphson_base_type = 'UUID'

    @classmethod
    def deserialize(cls, value, reader=None):
        return uuid.UUID(value)


class BigDecimalTypeIO(GraphSONTypeIO):
    prefix = 'gx'
    graphson_base_type = 'BigDecimal'

    @classmethod
    def deserialize(cls, value, reader=None):
        return Decimal(value)


class DurationTypeIO(GraphSONTypeIO):
    prefix = 'gx'
    graphson_base_type = 'Duration'

    _duration_regex = re.compile(r"""
        ^P((?P<days>\d+)D)?
        T((?P<hours>\d+)H)?
        ((?P<minutes>\d+)M)?
        ((?P<seconds>[0-9.]+)S)?$
    """, re.VERBOSE)
    _duration_format = "P{days}DT{hours}H{minutes}M{seconds}S"

    _seconds_in_minute = 60
    _seconds_in_hour = 60 * _seconds_in_minute
    _seconds_in_day = 24 * _seconds_in_hour

    @classmethod
    def serialize(cls, value):
        total_seconds = int(value.total_seconds())
        days, total_seconds = divmod(total_seconds, cls._seconds_in_day)
        hours, total_seconds = divmod(total_seconds, cls._seconds_in_hour)
        minutes, total_seconds = divmod(total_seconds, cls._seconds_in_minute)
        total_seconds += value.microseconds / 1e6

        return cls._duration_format.format(
            days=int(days), hours=int(hours), minutes=int(minutes), seconds=total_seconds
        )

    @classmethod
    def deserialize(cls, value, reader=None):
        duration = cls._duration_regex.match(value)
        if duration is None:
            raise ValueError('Invalid duration: {0}'.format(value))

        duration = {k: float(v) if v is not None else 0
                    for k, v in six.iteritems(duration.groupdict())}
        return datetime.timedelta(days=duration['days'], hours=duration['hours'],
                                  minutes=duration['minutes'], seconds=duration['seconds'])


class PointTypeIO(GraphSONTypeIO):
    prefix = 'dse'
    graphson_base_type = 'Point'

    @classmethod
    def deserialize(cls, value, reader=None):
        return Point.from_wkt(value)


class LineStringTypeIO(GraphSONTypeIO):
    prefix = 'dse'
    graphson_base_type = 'LineString'

    @classmethod
    def deserialize(cls, value, reader=None):
        return LineString.from_wkt(value)


class PolygonTypeIO(GraphSONTypeIO):
    prefix = 'dse'
    graphson_base_type = 'Polygon'

    @classmethod
    def deserialize(cls, value, reader=None):
        return Polygon.from_wkt(value)


class InetTypeIO(GraphSONTypeIO):
    prefix = 'gx'
    graphson_base_type = 'InetAddress'


class VertexTypeIO(GraphSONTypeIO):
    graphson_base_type = 'Vertex'

    @classmethod
    def deserialize(cls, value, reader=None):
        vertex = Vertex(id=reader.deserialize(value["id"]),
                        label=value["label"] if "label" in value else "vertex",
                        type='vertex',
                        properties={})
        # avoid the properties processing in Vertex.__init__
        vertex.properties = reader.deserialize(value.get('properties', {}))
        return vertex


class VertexPropertyTypeIO(GraphSONTypeIO):
    graphson_base_type = 'VertexProperty'

    @classmethod
    def deserialize(cls, value, reader=None):
        return VertexProperty(label=value['label'],
                              value=reader.deserialize(value["value"]),
                              properties=reader.deserialize(value.get('properties', {})))


class EdgeTypeIO(GraphSONTypeIO):
    graphson_base_type = 'Edge'

    @classmethod
    def deserialize(cls, value, reader=None):
        in_vertex = Vertex(id=reader.deserialize(value["inV"]),
                           label=value['inVLabel'],
                           type='vertex',
                           properties={})
        out_vertex = Vertex(id=reader.deserialize(value["outV"]),
                            label=value['outVLabel'],
                            type='vertex',
                            properties={})
        return Edge(
            id=reader.deserialize(value["id"]),
            label=value["label"] if "label" in value else "vertex",
            type='edge',
            properties=reader.deserialize(value.get("properties", {})),
            inV=in_vertex,
            inVLabel=value['inVLabel'],
            outV=out_vertex,
            outVLabel=value['outVLabel']
        )


class PropertyTypeIO(GraphSONTypeIO):
    graphson_base_type = 'Property'

    @classmethod
    def deserialize(cls, value, reader=None):
        return {value["key"]: reader.deserialize(value["value"])}


class PathTypeIO(GraphSONTypeIO):
    graphson_base_type = 'Path'

    @classmethod
    def deserialize(cls, value, reader=None):
        labels = [set(label) for label in value['labels']]
        objects = [reader.deserialize(obj) for obj in value['objects']]
        p = Path(labels, [])
        p.objects = objects  # avoid the object processing in Path.__init__
        return p


class _BaseGraphSONSerializer(object):

    _serializers = OrderedDict()

    @classmethod
    def register(cls, type, serializer):
        cls._serializers[type] = serializer

    @classmethod
    def get_type_definitions(cls):
        return cls._serializers.copy()

    @classmethod
    def get_serializer(cls, value):
        """
        Get the serializer for a python object.

        :param value: The python object.
        """

        # The serializer matching logic is as follow:
        # 1. Try to find the python type by direct access.
        # 2. Try to find the first serializer by class inheritance.
        # 3. If no serializer found, return the raw value.

        # Note that when trying to find the serializer by class inheritance,
        # the order that serializers are registered is important. The use of
        # an OrderedDict is to avoid the difference between executions.
        serializer = None
        try:
            serializer = cls._serializers[type(value)]
        except KeyError:
            for key, serializer_ in cls._serializers.items():
                if isinstance(value, key):
                    serializer = serializer_
                    break

        if serializer:
            # A serializer can have specialized serializers (e.g for Int32 and Int64, so value dependant)
            serializer = serializer.get_specialized_serializer(value)

        return serializer

    @classmethod
    def serialize(cls, value):
        """
        Serialize a python object to graphson.

        :param value: The python object to serialize.
        """
        serializer = cls.get_serializer(value)
        if serializer:
            return serializer.serialize(value)

        return value


class GraphSON1Serializer(_BaseGraphSONSerializer):
    """
    Serialize python objects to graphson types.
    """

    # When we fall back to a superclass's serializer, we iterate over this map.
    # We want that iteration order to be consistent, so we use an OrderedDict,
    # not a dict.
    _serializers = OrderedDict([
        (bool, BooleanTypeIO),
        (bytearray, BlobTypeIO),
        (Decimal, BigDecimalTypeIO),
        (datetime.date, LocalDateTypeIO),
        (datetime.time, LocalTimeTypeIO),
        (datetime.timedelta, DurationTypeIO),
        (uuid.UUID, UUIDTypeIO),
        (Polygon, PolygonTypeIO),
        (Point, PointTypeIO),
        (LineString, LineStringTypeIO)
    ])


if six.PY2:
    GraphSON1Serializer.register(buffer, BlobTypeIO)
else:
    GraphSON1Serializer.register(memoryview, BlobTypeIO)
    GraphSON1Serializer.register(bytes, BlobTypeIO)
    GraphSON1Serializer.register(ipaddress.IPv4Address, InetTypeIO)
    GraphSON1Serializer.register(ipaddress.IPv6Address, InetTypeIO)


class _BaseGraphSONDeserializer(object):

    _deserializers = {}

    @classmethod
    def get_type_definitions(cls):
        return cls._deserializers.copy()

    @classmethod
    def register(cls, graphson_type, serializer):
        cls._deserializers[graphson_type] = serializer

    @classmethod
    def get_deserializer(cls, graphson_type):
        try:
            return cls._deserializers[graphson_type]
        except KeyError:
            raise ValueError('Invalid `graphson_type` specified: {}'.format(graphson_type))

    @classmethod
    def deserialize(cls, graphson_type, value):
        """
        Deserialize a `graphson_type` value to a python object.

        :param graphson_base_type: The graphson graphson_type. e.g. 'gx:Instant'
        :param value: The graphson value to deserialize.
        """
        return cls.get_deserializer(graphson_type).deserialize(value)


class GraphSON1Deserializer(_BaseGraphSONDeserializer):
    """
    Deserialize graphson1 types to python objects.
    """
    _TYPES = [UUIDTypeIO, BigDecimalTypeIO, InstantTypeIO, BlobTypeIO,
              PointTypeIO, LineStringTypeIO, PolygonTypeIO, LocalDateTypeIO,
              LocalTimeTypeIO, DurationTypeIO, InetTypeIO]

    _deserializers = {
        t.graphson_type: t
        for t in _TYPES
    }

    @classmethod
    def deserialize_date(cls, value):
        return cls._deserializers[LocalDateTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_time(cls, value):
        return cls._deserializers[LocalTimeTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_timestamp(cls, value):
        return cls._deserializers[InstantTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_duration(cls, value):
        return cls._deserializers[DurationTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_int(cls, value):
        return int(value)

    deserialize_smallint = deserialize_int

    deserialize_varint = deserialize_int

    @classmethod
    def deserialize_bigint(cls, value):
        if six.PY3:
            return cls.deserialize_int(value)
        return long(value)

    @classmethod
    def deserialize_double(cls, value):
        return float(value)

    deserialize_float = deserialize_double

    @classmethod
    def deserialize_uuid(cls, value):
        return cls._deserializers[UUIDTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_decimal(cls, value):
        return cls._deserializers[BigDecimalTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_blob(cls, value):
        return cls._deserializers[BlobTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_point(cls, value):
        return cls._deserializers[PointTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_linestring(cls, value):
        return cls._deserializers[LineStringTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_polygon(cls, value):
        return cls._deserializers[PolygonTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_inet(cls, value):
        return value

    @classmethod
    def deserialize_boolean(cls, value):
        return value


# Remove in the next major
GraphSON1TypeDeserializer = GraphSON1Deserializer
GraphSON1TypeSerializer = GraphSON1Serializer


class GraphSON2Serializer(_BaseGraphSONSerializer):
    TYPE_KEY = "@type"
    VALUE_KEY = "@value"

    _serializers = GraphSON1Serializer.get_type_definitions()

    @classmethod
    def serialize(cls, value):
        """
        Serialize a type to GraphSON2.

        e.g {'@type': 'gx:Duration', '@value': 'P2DT4H'}

        :param value: The python object to serialize.
        """
        serializer = cls.get_serializer(value)
        if not serializer:
            # if no serializer found, we can't type it. `value` will be jsonized as string.
            return value

        value = serializer.serialize(value)
        out = {cls.TYPE_KEY: serializer.graphson_type}
        if value is not None:
            out[cls.VALUE_KEY] = value

        return out


GraphSON2Serializer.register(int, IntegerTypeIO)
if six.PY2:
    GraphSON2Serializer.register(long, IntegerTypeIO)


class GraphSON2Deserializer(_BaseGraphSONDeserializer):

    _TYPES = GraphSON1Deserializer._TYPES + [
        Int16TypeIO, Int32TypeIO, Int64TypeIO, DoubleTypeIO, FloatTypeIO,
        BigIntegerTypeIO, VertexTypeIO, VertexPropertyTypeIO, EdgeTypeIO,
        PathTypeIO, PropertyTypeIO]

    _deserializers = {
        t.graphson_type: t
        for t in _TYPES
    }


class GraphSON2Reader(object):
    """
    GraphSON2 Reader that parse json and deserialize to python objects.
    """

    def __init__(self, extra_deserializer_map=None):
        """
        :param extra_deserializer_map: map from GraphSON type tag to deserializer instance implementing `deserialize`
        """
        self.deserializers = GraphSON2Deserializer.get_type_definitions()
        if extra_deserializer_map:
            self.deserializers.update(extra_deserializer_map)

    def read(self, json_data):
        """
        Read and deserialize ``json_data``.
        """
        return self.deserialize(json.loads(json_data))

    def deserialize(self, obj):
        """
        Deserialize GraphSON type-tagged dict values into objects mapped in self.deserializers
        """
        if isinstance(obj, dict):
            try:
                des = self.deserializers[obj[GraphSON2Serializer.TYPE_KEY]]
                return des.deserialize(obj[GraphSON2Serializer.VALUE_KEY], self)
            except KeyError:
                pass
            # list and map are treated as normal json objs (could be isolated deserializers)
            return {self.deserialize(k): self.deserialize(v) for k, v in six.iteritems(obj)}
        elif isinstance(obj, list):
            return [self.deserialize(o) for o in obj]
        else:
            return obj
