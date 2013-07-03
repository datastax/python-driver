import os
try:
    import subprocess
    has_subprocess = True
except ImportError:
    has_subprocess = False

from distutils.core import setup, Extension
from distutils.cmd import Command

from cassandra import __version__

murmur3 = Extension('cassandra.murmur3', sources=['cassandra/murmur3.c'])

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


setup(
    name='cassandra',
    version=__version__,
    description='Python driver for Cassandra',
    packages=["cassandra", "cassandra.io"],
    ext_modules=[murmur3],
    install_requires=['futures'],
    test_requires=['nose', 'mock'],
    cmdclass={"doc": doc},
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
