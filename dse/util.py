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

    def __init__(self, points):
        self.points = list(points)

    def __str__(self):
        return "LINESTRING(%s)" % ', '.join("%f %f" % (x, y) for x, y in self.points)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.points)


class Polygon(object):

    def __init__(self, rings):
        self.rings = list(rings)

    def __str__(self):
        rings = ("(%s)" % ', '.join("%s %s" % (x, y) for x, y in ring) for ring in self.rings)
        return "POLYGON(%s)" % ', '.join(rings)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.rings)
