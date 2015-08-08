from cassandra.cython_deps import HAVE_CYTHON, HAVE_NUMPY

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

def cyimport(import_path):
    """
    Import a Cython module if available, otherwise return None
    (and skip any relevant tests).
    """
    try:
        return __import__(import_path, fromlist=True)
    except ImportError:
        if HAVE_CYTHON:
            raise
        return None


# @cythontest
# def test_something(self): ...
cythontest = unittest.skipUnless(HAVE_CYTHON, 'Cython is not available')
numpytest  = unittest.skipUnless(HAVE_CYTHON and HAVE_NUMPY, 'NumPy is not available')