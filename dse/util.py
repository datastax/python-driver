from itertools import chain

class Point(object):

    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __str__(self):
        return "POINT(%f %f)" % (self.x, self.y)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.x, self.y)


class Circle(object):

    def __init__(self, x, y, r):
        self.x = x
        self.y = y
        self.r = r

    def __str__(self):
        return "CIRCLE((%f %f) %f)" % (self.x, self.y, self.r)

    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__, self.x, self.y, self.r)


class LineString(object):

    def __init__(self, coords):
        self.coords = list(coords)

    def __str__(self):
        return "LINESTRING(%s)" % ', '.join("%f %f" % (x, y) for x, y in self.coords)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.coords)


class _LinearRing(object):
    # no validation, no implicit closing; just used for poly composition, to
    # mimic that of shapely.geometry.Polygon
    def __init__(self, coords):
        self.coords = list(coords)

    def __str__(self):
        return "LINEARRING(%s)" % ', '.join("%f %f" % (x, y) for x, y in self.coords)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.coords)


class Polygon(object):

    def __init__(self, exterior, interiors=None):
        self.exterior = _LinearRing(exterior)
        self.interiors = [_LinearRing(e) for e in interiors] if interiors else []

    def __str__(self):
        rings = (ring.coords for ring in chain((self.exterior,), self.interiors))
        rings = ("(%s)" % ', '.join("%s %s" % (x, y) for x, y in ring) for ring in rings)
        return "POLYGON(%s)" % ', '.join(rings)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.exterior.coords, [ring.coords for ring in self.interiors])
