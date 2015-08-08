try:
    from cassandra.rowparser import make_recv_results_rows
    HAVE_CYTHON = True
except ImportError:
    HAVE_CYTHON = False

try:
    import numpy
    HAVE_NUMPY = True
except ImportError:
    HAVE_NUMPY = False
