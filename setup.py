# Copyright 2013-2014 DataStax, Inc.
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
import sys

import ez_setup
ez_setup.use_setuptools()

if __name__ == '__main__' and sys.argv[1] == "gevent_nosetests":
    from gevent.monkey import patch_all
    patch_all()

from setuptools import setup
from distutils.command.build_ext import build_ext
from distutils.core import Extension
from distutils.errors import (CCompilerError, DistutilsPlatformError,
                              DistutilsExecError)
from distutils.cmd import Command


import os
import warnings

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
else:
    class gevent_nosetests(nosetests):
        description = "run nosetests with gevent monkey patching"


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
            print("   %s/" % path)


class BuildFailed(Exception):

    def __init__(self, ext):
        self.ext = ext


murmur3_ext = Extension('cassandra.murmur3',
                        sources=['cassandra/murmur3.c'])

libev_ext = Extension('cassandra.io.libevwrapper',
                      sources=['cassandra/io/libevwrapper.c'],
                      include_dirs=['/usr/include/libev'],
                      libraries=['ev'])


class build_extensions(build_ext):

    error_message = """
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
            build_ext.run(self)
        except DistutilsPlatformError as exc:
            sys.stderr.write('%s\n' % str(exc))
            warnings.warn(self.error_message % "C extensions.")

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except (CCompilerError, DistutilsExecError,
                DistutilsPlatformError, IOError) as exc:
            sys.stderr.write('%s\n' % str(exc))
            name = "The %s extension" % (ext.name,)
            warnings.warn(self.error_message % (name,))
            raise BuildFailed(ext)


def run_setup(extensions):
    kw = {'cmdclass': {'doc': DocCommand}}
    if gevent_nosetests is not None:
        kw['cmdclass']['gevent_nosetests'] = gevent_nosetests

    if extensions:
        kw['cmdclass']['build_ext'] = build_extensions
        kw['ext_modules'] = extensions

    dependencies = ['futures', 'six >=1.6']

    setup(
        name='cassandra-driver',
        version=__version__,
        description='Python driver for Cassandra',
        long_description=long_description,
        url='http://github.com/datastax/python-driver',
        author='Tyler Hobbs',
        author_email='tyler@datastax.com',
        packages=['cassandra', 'cassandra.io'],
        include_package_data=True,
        install_requires=dependencies,
        tests_require=['nose', 'mock', 'PyYAML', 'pytz'],
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

extensions = [murmur3_ext, libev_ext]
if "--no-extensions" in sys.argv:
    sys.argv = [a for a in sys.argv if a != "--no-extensions"]
    extensions = []
elif "--no-murmur3" in sys.argv:
    sys.argv = [a for a in sys.argv if a != "--no-murmur3"]
    extensions.remove(murmur3_ext)
elif "--no-libev" in sys.argv:
    sys.argv = [a for a in sys.argv if a != "--no-libev"]
    extensions.remove(libev_ext)


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

if extensions:
    if (sys.platform.startswith("java")
            or sys.platform == "cli"
            or "PyPy" in sys.version):
        sys.stderr.write(platform_unsupported_msg)
        extensions = ()
    elif sys.byteorder == "big":
        sys.stderr.write(arch_unsupported_msg)
        extensions = ()

while True:
    # try to build as many of the extensions as we can
    try:
        run_setup(extensions)
    except BuildFailed as failure:
        extensions.remove(failure.ext)
    else:
        break
