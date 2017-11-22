import re

import cassandra
from cassandra.protocol import cython_protocol_handler


def name_from_module(module):
    return module.__name__.split(".")[1]


def module_to_cythonized_file(module):
    return name_from_module(module) + ".so"


def assert_module_file(module):
    cython_module_file = module_to_cythonized_file(module)
    cython_module_name = name_from_module(module)

    # This would match for example
    # site-packages/cassandra/cluster.so
    if module.__file__.endswith(cython_module_file):
        return

    # This would match
    # site-packages/cassandra/cluster.cpython-34m.so
    if re.match(r'.*{}.*\.so$'.format(cython_module_name), module.__file__):
        return

    raise AssertionError("File being used is  {}, "
                         "it should have matched with {}".format(module.__file__, cython_module_file))


def load_module(module_name):
    __import__("cassandra." + module_name)
    return eval("cassandra." + module_name)

########################################
# Verify cython extensions
########################################

# Files that will be cythonized
# This modules should be imported from a file finished in .so
cython_candidates = ['cluster', 'concurrent', 'connection', 'cqltypes', 'metadata',
                                     'pool', 'protocol', 'query', 'util']
# Cython files
#type_codes is missing from here, not sure why but it looks like it's never cythonized
cython_files = ['bytesio', 'cython_marshal', 'cython_utils', 'deserializers', 'ioutils', 'obj_parser',
                'parsing', 'row_parser'] # 'type_codes']

files_to_check = cython_candidates + cython_files

modules_to_assert = (load_module(module) for module in cython_candidates)

for module in modules_to_assert:
    assert_module_file(module)

from cassandra.obj_parser import ListParser, LazyParser
ProtocolHandler = cython_protocol_handler(ListParser())
LazyProtocolHandler = cython_protocol_handler(LazyParser())


########################################
# Verify numpy extensions
########################################
try:
    import numpy
    HAVE_NUMPY = True
except ImportError:
    HAVE_NUMPY = False

if HAVE_NUMPY:
    assert_module_file(load_module("numpy_parser"))
    from cassandra.numpy_parser import NumpyParser
    NumpyProtocolHandler = cython_protocol_handler(NumpyParser())


########################################
# Verify murmur3
########################################
from cassandra.cmurmur3 import murmur3
assert murmur3("key") == -6847573755651342660


########################################
# Verify libev
########################################
import cassandra.io.libevwrapper as libev
loop = libev.Loop()