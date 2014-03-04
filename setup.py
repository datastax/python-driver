import platform
import os
import sys
import warnings

try:
    import subprocess
    has_subprocess = True
except ImportError:
    has_subprocess = False

import ez_setup
ez_setup.use_setuptools()

from setuptools import setup
from distutils.command.build_ext import build_ext
from distutils.core import Extension
from distutils.errors import (CCompilerError, DistutilsPlatformError,
                              DistutilsExecError)
from distutils.cmd import Command

from cassandra import __version__

long_description = ""
with open("README.rst") as f:
    long_description = f.read()


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
                print output

            print ""
            print "Documentation step '%s' performed, results here:" % mode
            print "   %s/" % path


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
    if extensions:
        kw['cmdclass']['build_ext'] = build_extensions
        kw['ext_modules'] = extensions

    dependencies = ['futures', 'scales', 'blist']
    if platform.python_implementation() != "CPython":
        dependencies.remove('blist')

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
        tests_require=['nose', 'mock', 'ccm', 'unittest2', 'PyYAML'],
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: Apache Software License',
            'Natural Language :: English',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
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
