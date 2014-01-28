Installation
============

Supported Platforms
-------------------
Python 2.6 and 2.7 are supported.  Both CPython (the standard Python
implementation) and `PyPy <http://pypy.org>`_ are supported and tested
against.

Linux, OSX, and Windows are supported.

Support for Python 3 is planned.

Installation through pip
------------------------
`pip <https://pypi.python.org/pypi/pip>`_ is the suggested tool for installing
packages.  It will handle installing all python dependencies for the driver at
the same time as the driver itself.  To install the driver::

    pip install cassandra-driver

You can use ``pip install --pre cassandra-driver`` if you need to install a beta version.

Manual Installation
-------------------
You can always install the driver directly from a source checkout or tarball.
When installing manually, ensure the python dependencies are already
installed. You can find the list of dependencies in
`requirements.txt <https://github.com/datastax/python-driver/blob/master/requirements.txt>`_.

Once the dependencies are installed, simply run::

    python setup.py install

(Optional) Non-python Dependencies
----------------------------------
The driver has several **optional** features that have non-Python dependencies.

C Extensions
^^^^^^^^^^^^
By default, two C extensions are compiled: one that adds support
for token-aware routing with the ``Murmur3Partitioner``, and one that
allows you to use `libev <http://software.schmorp.de/pkg/libev.html>`_
for the event loop, which improves performance.

When installing manually through setup.py, you can disable both with
the ``--no-extensions`` option, or selectively disable one or the other
with ``--no-murmur3`` and ``--no-libev``.

To compile the extenions, ensure that GCC and the Python headers are available.

On Ubuntu and Debian, this can be accomplished by running::

    $ sudo apt-get install gcc python-dev

On RedHat and RedHat-based systems like CentOS and Fedora::

    $ sudo yum install gcc python-devel

On OS X, homebrew installations of Python should provide the necessary headers.

libev support
^^^^^^^^^^^^^
The driver currently uses Python's ``asyncore`` module for its default
event loop.  For better performance, ``libev`` is also supported through
a C extension.

If you're on Linux, you should be able to install libev
through a package manager.  For example, on Debian/Ubuntu::

    $ sudo apt-get install libev4 libev-dev

On RHEL/CentOS/Fedora::

    $ sudo yum install libev libev-devel

If you're on Mac OS X, you should be able to install libev
through `Homebrew <http://brew.sh/>`_. For example, on Mac OS X::

    $ brew install libev

If successful, you should be able to build and install the extension
(just using ``setup.py build`` or ``setup.py install``) and then use
the libev event loop by doing the following:

.. code-block:: python

    >>> from cassandra.io.libevreactor import LibevConnection
    >>> from cassandra.cluster import Cluster

    >>> cluster = Cluster()
    >>> cluster.connection_class = LibevConnection
    >>> session = cluster.connect()

Compression Support
^^^^^^^^^^^^^^^^^^^
Compression can optionally be used for communication between the driver and
Cassandra.  There are currently two supported compression algorithms:
snappy (in Cassandra 1.2+) and LZ4 (only in Cassandra 2.0+).  If either is
available for the driver and Cassandra also supports it, it will
be used automatically.

For lz4 support::

    $ pip install lz4

For snappy support::

    $ pip install python-snappy

(If using a Debian Linux derivative such as Ubuntu, it may be easier to
just run ``apt-get install python-snappy``.)
