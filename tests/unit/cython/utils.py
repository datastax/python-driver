try:
    import tests.unit.cython.dummy_module
except ImportError:
    have_cython = False
else:
    have_cython = True

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
        if have_cython:
            raise
        return None

# @cythontest
# def test_something(self): ...
cythontest = unittest.skipUnless(have_cython, 'Cython is not available')
