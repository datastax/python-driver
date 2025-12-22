# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys

from pathlib import Path
from setuptools import setup, Extension
try:
    import tomllib as toml
except ImportError:
    import tomli as toml

# ========================== General purpose vals to describe our current platform ==========================
is_windows = sys.platform.startswith('win32')
is_macos = sys.platform.startswith('darwin')
is_pypy = "PyPy" in sys.version
is_supported_platform = sys.platform != "cli" and not sys.platform.startswith("java")
is_supported_arch = sys.byteorder != "big"

# ========================== A few upfront checks ==========================
platform_unsupported_msg = \
"""
===============================================================================
The optional C extensions are not supported on this platform.
===============================================================================
"""

arch_unsupported_msg = \
"""
===============================================================================
The optional C extensions are not supported on big-endian systems.
===============================================================================
"""

pypy_unsupported_msg = \
"""
=================================================================================
Some optional C extensions are not supported in PyPy. Only murmur3 will be built.
=================================================================================
"""

if is_pypy:
    sys.stderr.write(pypy_unsupported_msg)
if not is_supported_platform:
    sys.stderr.write(platform_unsupported_msg)
elif not is_supported_arch:
    sys.stderr.write(arch_unsupported_msg)

# ========================== Extensions ==========================
pyproject_toml = Path(__file__).parent / "pyproject.toml"
with open(pyproject_toml,"rb") as f:
    pyproject_data = toml.load(f)
driver_project_data = pyproject_data["tool"]["cassandra-driver"]

murmur3_ext = Extension('cassandra.cmurmur3', sources=['cassandra/cmurmur3.c'])

DEFAULT_LIBEV_INCLUDES = ['/usr/include/libev', '/usr/local/include', '/opt/local/include', '/usr/include']
DEFAULT_LIBEV_LIBS = ['/usr/local/lib', '/opt/local/lib', '/usr/lib64']
libev_includes = driver_project_data["libev-includes"] or DEFAULT_LIBEV_INCLUDES
libev_libs = driver_project_data["libev-libs"] or DEFAULT_LIBEV_LIBS
if is_macos:
    libev_includes.extend(['/opt/homebrew/include', os.path.expanduser('~/homebrew/include')])
    libev_libs.extend(['/opt/homebrew/lib'])
libev_ext = Extension('cassandra.io.libevwrapper',
                      sources=['cassandra/io/libevwrapper.c'],
                      include_dirs=libev_includes,
                      libraries=['ev'],
                      library_dirs=libev_libs)

try_murmur3 = driver_project_data["build-murmur3-extension"] and is_supported_platform and is_supported_arch
try_libev = driver_project_data["build-libev-extension"] and is_supported_platform and is_supported_arch
try_cython = driver_project_data["build-cython-extensions"] and is_supported_platform and is_supported_arch and not is_pypy

build_concurrency = driver_project_data["build-concurrency"]

def build_extension_list():

    rv = []

    if try_murmur3:
        sys.stderr.write("Appending murmur extension %s\n" % murmur3_ext)
        rv.append(murmur3_ext)

    if try_libev:
        sys.stderr.write("Appending libev extension %s\n" % libev_ext)
        rv.append(libev_ext)

    if try_cython:
        sys.stderr.write("Trying Cython builds in order to append Cython extensions\n")
        try:
            from Cython.Build import cythonize
            cython_candidates = ['cluster', 'concurrent', 'connection', 'cqltypes', 'metadata',
                                 'pool', 'protocol', 'query', 'util']
            compile_args = [] if is_windows else ['-Wno-unused-function']
            rv.extend(cythonize(
                    [Extension('cassandra.%s' % m, ['cassandra/%s.py' % m],
                                extra_compile_args=compile_args)
                        for m in cython_candidates],
                    nthreads=build_concurrency,
                    exclude_failures=True))

            rv.extend(cythonize(Extension("*", ["cassandra/*.pyx"], extra_compile_args=compile_args),
                                          nthreads=build_concurrency))
        except Exception as exc:
            sys.stderr.write("Failed to cythonize one or more modules. These will not be compiled as extensions (optional).\n")
            sys.stderr.write("Cython error: %s\n" % exc)
    
    return rv

# ========================== And finally setup() itself ==========================
setup(
    ext_modules = build_extension_list()
)