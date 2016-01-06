from six.moves import range
import struct
from cassandra.cqltypes import CassandraType
from dse.marshal import point_be, point_le, circle_be, circle_le
from dse.util import Point, Circle, LineString, Polygon


class PointType(CassandraType):
    typename = 'org.apache.cassandra.db.marshal.PointType'

    @staticmethod
    def deserialize(byts, protocol_version):
        is_little_endian = bool(byts[0])
        point = point_le if is_little_endian else point_be
        return Point(*point.unpack_from(byts, 5))  # ofs = endian byte + int type


class CircleType(CassandraType):
    typename = 'org.apache.cassandra.db.marshal.CircleType'

    @staticmethod
    def deserialize(byts, protocol_version):
        is_little_endian = bool(byts[0])
        circle = circle_le if is_little_endian else circle_be
        return Circle(*circle.unpack_from(byts, 5))


class LineStringType(CassandraType):
    typename = 'org.apache.cassandra.db.marshal.LineStringType'

    @staticmethod
    def deserialize(byts, protocol_version):
        is_little_endian = bool(byts[0])
        point = point_le if is_little_endian else point_be
        points = ((point.unpack_from(byts, offset) for offset in range(1 + 4 + 4, len(byts), point.size)))  # start = endian + int type + int count
        return LineString(points)


class PolygonType(CassandraType):
    typename = 'org.apache.cassandra.db.marshal.PolygonType'

    @staticmethod
    def deserialize(byts, protocol_version):
        is_little_endian = bool(byts[0])
        if is_little_endian:
            int_fmt = '<i'
            point = point_le
        else:
            int_fmt = '>i'
            point = point_be
        p = 5
        ring_count = struct.unpack_from(int_fmt, byts, p)[0]
        p += 4
        rings = []
        for _ in range(ring_count):
            point_count = struct.unpack_from(int_fmt, byts, p)[0]
            p += 4
            end = p + point_count * point.size
            rings.append([point.unpack_from(byts, offset) for offset in range(p, end, point.size)])
            p = end
        return Polygon(rings)
