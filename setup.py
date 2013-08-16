import os
import sys
import warnings

try:
    import subprocess
    has_subprocess = True
except ImportError:
    has_subprocess = False

from distribute_setup import use_setuptools
use_setuptools()

from setuptools import setup, Feature
from distutils.command.build_ext import build_ext
from distutils.core import Extension
from distutils.errors import (CCompilerError, DistutilsPlatformError,
                              DistutilsExecError)
from distutils.cmd import Command

from cassandra import __version__


class doc(Command):

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
            except subprocess.CalledProcessError, exc:
                raise RuntimeError("Documentation step '%s' failed: %s: %s" % (mode, exc, exc.output))
            else:
                print output

            print ""
            print "Documentation step '%s' performed, results here:" % mode
            print "   %s/" % path


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
        except DistutilsPlatformError, exc:
            sys.stderr.write('%s\n' % str(exc))
            warnings.warn(self.error_message % "C extensions.")

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except (CCompilerError, DistutilsExecError,
                DistutilsPlatformError, IOError), exc:
            sys.stderr.write('%s\n' % str(exc))
            name = "The %s extension" % (ext.name,)
            warnings.warn(self.error_message % (name,))

murmur3_ext = Feature(
    "Token-aware routing support for Murmur3",
    standard=True,
    ext_modules=[Extension('cassandra.murmur3',
                           sources=['cassandra/murmur3.c'])])

libev_support = Feature(
    "Libev event loop support",
    standard=True,
    ext_modules=[Extension('cassandra.io.libevwrapper',
                           libraries=['ev'],
                           sources=['cassandra/io/libevwrapper.c'])])

features = {
    "murmur3": murmur3_ext,
    "libev": libev_support,
}

if "--no-extensions" in sys.argv:
    sys.argv = [a for a in sys.argv if a != "--no-extensions"]
    features = {}
elif "--no-murmur3" in sys.argv:
    sys.argv = [a for a in sys.argv if a != "--no-murmur3"]
    features.pop("murmur3")
elif "--no-libev" in sys.argv:
    sys.argv = [a for a in sys.argv if a != "--no-libev"]
    features.pop("libev")

if (sys.platform.startswith("java")
        or sys.platform == "cli"
        or "PyPy" in sys.version):
    sys.stderr.write("""
===============================================================================
The optional C extensions are not supported on this platform.
===============================================================================
    """)
    features = {}
elif sys.byteorder == "big":
    sys.stderr.write("""
===============================================================================
The optional C extensions are not supported on big-endian systems.
===============================================================================
    """)
    features = {}

setup(
    name='cassandra',
    version=__version__,
    description='Python driver for Cassandra',
    author='Tyler Hobbs',
    author_email='tyler@datastax.com',
    packages=["cassandra", "cassandra.io"],
    features=features,
    install_requires=['futures', 'scales'],
    tests_require=['nose', 'mock', 'ccm'],
    cmdclass={"build_ext": build_extensions,
              "doc": doc},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]

)
