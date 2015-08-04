from tests.unit.cython.utils import cyimport, cythontest
bytesio_testhelper = cyimport('tests.unit.cython.bytesio_testhelper')

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa


class BytesIOTest(unittest.TestCase):
    """Test Cython BytesIO proxy"""

    @cythontest
    def test_reading(self):
        bytesio_testhelper.test_read1(self.assertEqual, self.assertRaises)
        bytesio_testhelper.test_read2(self.assertEqual, self.assertRaises)
        bytesio_testhelper.test_read3(self.assertEqual, self.assertRaises)

    @cythontest
    def test_reading_error(self):
        bytesio_testhelper.test_read_eof(self.assertEqual, self.assertRaises)
