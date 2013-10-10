try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

try:
    import gevent.monkey
    gevent.monkey.patch_all()
except ImportError:
    pass

try:
    from cassandra.io.geventreactor import GeventConnection
except ImportError:
    GeventConnection = None

from .test_connection import ConnectionTest


class GeventConnectionTest(ConnectionTest, unittest.TestCase):

    klass = GeventConnection

    @classmethod
    def setup_class(cls):
        if GeventConnection is None:
            raise unittest.SkipTest('gevent does not appear to be installed properly')
