# Copyright 2013-2015 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
import os
import sys
import warnings

if __name__ == '__main__' and sys.argv[1] == "gevent_nosetests":
    print("Running gevent tests")
    from gevent.monkey import patch_all
    patch_all()

if __name__ == '__main__' and sys.argv[1] == "eventlet_nosetests":
    print("Running eventlet tests")
    from eventlet import monkey_patch
    monkey_patch()

import ez_setup
ez_setup.use_setuptools()

from setuptools import setup
from distutils.command.build_ext import build_ext
from distutils.core import Extension
from distutils.errors import (CCompilerError, DistutilsPlatformError,
                              DistutilsExecError)
from distutils.cmd import Command

try:
    import subprocess
    has_subprocess = True
except ImportError:
    has_subprocess = False

from cassandra import __version__

long_description = ""
with open("README.rst") as f:
    long_description = f.read()


try:
    from nose.commands import nosetests
except ImportError:
    gevent_nosetests = None
    eventlet_nosetests = None
else:
    class gevent_nosetests(nosetests):
        description = "run nosetests with gevent monkey patching"

    class eventlet_nosetests(nosetests):
        description = "run nosetests with eventlet monkey patching"

has_cqlengine = False
if __name__ == '__main__' and sys.argv[1] == "install":
    try:
        import cqlengine
        has_cqlengine = True
    except ImportError:
        pass

PROFILING = False


class DocCommand(Command):

    description = "generate or test documentation"

    user_options = [("test", "t",
                     "run doctests instead of generating documentation")]

    boolean_options = ["test"]

    def initialize_options(self):
        self.test = False

    def finalize_options(self):
        pass

    def run(self):
        if self.test:
            path = "docs/_build/doctest"
            mode = "doctest"
        else:
            path = "docs/_build/%s" % __version__
            mode = "html"

            try:
                os.makedirs(path)
            except:
                pass

        if has_subprocess:
            try:
                output = subprocess.check_output(
                    ["sphinx-build", "-b", mode, "docs", path],
                    stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as exc:
                raise RuntimeError("Documentation step '%s' failed: %s: %s" % (mode, exc, exc.output))
            else:
                print(output)

            print("")
            print("Documentation step '%s' performed, results here:" % mode)
            print("   file://%s/%s/index.html" % (os.path.dirname(os.path.realpath(__file__)), path))


class BuildFailed(Exception):

    def __init__(self, ext):
        self.ext = ext


murmur3_ext = Extension('cassandra.cmurmur3',
                        sources=['cassandra/cmurmur3.c'])

libev_ext = Extension('cassandra.io.libevwrapper',
                      sources=['cassandra/io/libevwrapper.c'],
                      include_dirs=['/usr/include/libev', '/usr/local/include', '/opt/local/include'],
                      libraries=['ev'],
                      library_dirs=['/usr/local/lib', '/opt/local/lib'])

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


is_windows = os.name == 'nt'

is_pypy = "PyPy" in sys.version
if is_pypy:
    sys.stderr.write(pypy_unsupported_msg)

is_supported_platform = sys.platform != "cli" and not sys.platform.startswith("java")
is_supported_arch = sys.byteorder != "big"
if not is_supported_platform:
    sys.stderr.write(platform_unsupported_msg)
elif not is_supported_arch:
    sys.stderr.write(arch_unsupported_msg)

try_extensions = "--no-extensions" not in sys.argv and is_supported_platform and is_supported_arch
try_murmur3 = try_extensions and "--no-murmur3" not in sys.argv
try_libev = try_extensions and "--no-libev" not in sys.argv and not is_pypy and not is_windows
try_cython = try_extensions and "--no-cython" not in sys.argv and not is_pypy and not os.environ.get('CASS_DRIVER_NO_CYTHON')

sys.argv = [a for a in sys.argv if a not in ("--no-murmur3", "--no-libev", "--no-cython", "--no-extensions")]

build_concurrency = int(os.environ.get('CASS_DRIVER_BUILD_CONCURRENCY', '0'))


class build_extensions(build_ext):

    error_message = """
===============================================================================
WARNING: could not compile %s.

The C extensions are not required for the driver to run, but they add support
for token-aware routing with the Murmur3Partitioner.

On Windows, make sure Visual Studio or an SDK is installed, and your environment
is configured to build for the appropriate architecture (matching your Python runtime).
This is often a matter of using vcvarsall.bat from your install directory, or running
from a command prompt in the Visual Studio Tools Start Menu.
===============================================================================
""" if is_windows else """
===============================================================================
WARNING: could not compile %s.

The C extensions are not required for the driver to run, but they add support
for libev and token-aware routing with the Murmur3Partitioner.

Linux users should ensure that GCC and the Python headers are available.

On Ubuntu and Debian, this can be accomplished by running:

    $ sudo apt-get install build-essential python-dev

On RedHat and RedHat-based systems like CentOS and Fedora:

    $ sudo yum install gcc python-devel

On OSX, homebrew installations of Python should provide the necessary headers.

libev Support
-------------
For libev support, you will also need to install libev and its headers.

On Debian/Ubuntu:

    $ sudo apt-get install libev4 libev-dev

On RHEL/CentOS/Fedora:

    $ sudo yum install libev libev-devel

On OSX, via homebrew:

    $ brew install libev

===============================================================================
    """

    def run(self):
        try:
            self._setup_extensions()
            build_ext.run(self)
        except DistutilsPlatformError as exc:
            sys.stderr.write('%s\n' % str(exc))
            warnings.warn(self.error_message % "C extensions.")

    def build_extensions(self):
        if build_concurrency > 1:
            self.check_extensions_list(self.extensions)

            import multiprocessing.pool
            multiprocessing.pool.ThreadPool(processes=build_concurrency).map(self.build_extension, self.extensions)
        else:
            build_ext.build_extensions(self)

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except (CCompilerError, DistutilsExecError,
                DistutilsPlatformError, IOError) as exc:
            sys.stderr.write('%s\n' % str(exc))
            name = "The %s extension" % (ext.name,)
            warnings.warn(self.error_message % (name,))

    def _setup_extensions(self):
        # We defer extension setup until this command to leveraage 'setup_requires' pulling in Cython before we
        # attempt to import anything
        self.extensions = []

        if try_murmur3:
            self.extensions.append(murmur3_ext)

        if try_libev:
            self.extensions.append(libev_ext)

        if try_cython:
            try:
                from Cython.Build import cythonize
                cython_candidates = ['cluster', 'concurrent', 'connection', 'cqltypes', 'metadata',
                                     'pool', 'protocol', 'query', 'util']
                compile_args = [] if is_windows else ['-Wno-unused-function']
                self.extensions.extend(cythonize(
                    [Extension('cassandra.%s' % m, ['cassandra/%s.py' % m],
                               extra_compile_args=compile_args)
                        for m in cython_candidates],
                    nthreads=build_concurrency,
                    exclude_failures=True))
                self.extensions.extend(cythonize("cassandra/*.pyx", nthreads=build_concurrency))
            except Exception:
                sys.stderr.write("Cython is not available. Not compiling core driver files as extensions (optional).")


def run_setup(extensions):

    kw = {'cmdclass': {'doc': DocCommand}}
    if gevent_nosetests is not None:
        kw['cmdclass']['gevent_nosetests'] = gevent_nosetests

    if eventlet_nosetests is not None:
        kw['cmdclass']['eventlet_nosetests'] = eventlet_nosetests

    kw['cmdclass']['build_ext'] = build_extensions
    kw['ext_modules'] = [Extension('DUMMY', [])]  # dummy extension makes sure build_ext is called for install

    if try_cython:
        kw['setup_requires'] = ['Cython']

    dependencies = ['futures', 'six >=1.6']

    setup(
        name='cassandra-driver',
        version=__version__,
        description='Python driver for Cassandra',
        long_description=long_description,
        url='http://github.com/datastax/python-driver',
        author='Tyler Hobbs',
        author_email='tyler@datastax.com',
        packages=['cassandra', 'cassandra.io', 'cassandra.cqlengine'],
        keywords='cassandra,cql,orm',
        include_package_data=True,
        install_requires=dependencies,
        tests_require=['nose', 'mock<=1.0.1', 'PyYAML', 'pytz', 'sure'],
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: Apache Software License',
            'Natural Language :: English',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: Implementation :: CPython',
            'Programming Language :: Python :: Implementation :: PyPy',
            'Topic :: Software Development :: Libraries :: Python Modules'
        ],
        **kw)

run_setup(None)

if has_cqlengine:
    warnings.warn("\n#######\n'cqlengine' package is present on path: %s\n"
                  "cqlengine is now an integrated sub-package of this driver.\n"
                  "It is recommended to remove this package to reduce the chance for conflicting usage" % cqlengine.__file__)
