Contributing
============

Contributions are welcome in the form of bug reports or pull requests.

Bug Reports
-----------
Quality bug reports are welcome at the `Scylla Python Driver Github <https://github.com/scylladb/python-driver/issues>`_.

There are plenty of `good resources <http://www.drmaciver.com/2013/09/how-to-submit-a-decent-bug-report/>`_ describing how to create
good bug reports. They will not be repeated in detail here, but in general, the bug report include where appropriate:

* relevant software versions (Python runtime, driver version, cython version, server version)
* details for how to produce (e.g. a test script or written procedure)
  * any effort to isolate the issue in reproduction is much-appreciated
* stack trace from a crashed runtime

Pull Requests
-------------
If you're able to fix a bug yourself, you can `fork the repository <https://help.github.com/articles/fork-a-repo/>`_ and submit a `Pull Request <https://help.github.com/articles/using-pull-requests/>`_ with the fix.
Please include tests demonstrating the issue and fix. For examples of how to run the tests, consult the further parts of this document.

Design and Implementation Guidelines
------------------------------------
- We have integrations (notably Cassandra cqlsh) that require pure Python and minimal external dependencies. We try to avoid new external dependencies. Where compiled extensions are concerned, there should always be a pure Python fallback implementation.
- This project follows `semantic versioning <http://semver.org/>`_, so breaking API changes will only be introduced in major versions.
- Legacy ``cqlengine`` has varying degrees of overreaching client-side validation. Going forward, we will avoid client validation where server feedback is adequate and not overly expensive.
- When writing tests, try to achieve maximal coverage in unit tests (where it is faster to run across many runtimes). Integration tests are good for things where we need to test server interaction, or where it is important to test across different server versions (emulating in unit tests would not be effective).

Dev setup
=========

We recommend using `uv` tool for running tests, linters and basically everything else,
since it makes Python tooling ecosystem mostly usable.
To install it, see instructions at https://docs.astral.sh/uv/getting-started/installation/
The rest of this document assumes you have `uv` installed.

It is also strongly recommended to use C/C++-caching tool like ccache or sccache.
When modifying driver files, rebuilding Cython modules is often necessary.
Without caching, each such rebuild may take over a minute. Caching usually brings it
down to about 2-3 seconds.

Building the Docs
=================

To build and preview the documentation for the ScyllaDB Python driver locally, you must first manually install `python-driver`.
This is necessary for autogenerating the reference documentation of the driver.
You can find detailed instructions on how to install the driver in the `Installation guide <https://python-driver.docs.scylladb.com/stable/installation.html#manual-installation>`_.

After installing the driver, you can build the documentation:
- Make sure you have Python version compatible with docs. You can see supported version in ``docs/pyproject.toml`` - look for ``python`` in ``tool.poetry.dependencies`` section.
- Install poetry: ``pip install poetry``
- To preview docs in your browser: ``make -C docs preview``

Tests
=====

Running Unit Tests
------------------
Unit tests can be run like so::

    uv run pytest tests/unit
    EVENT_LOOP_MANAGER=gevent uv run pytest tests/unit/io/test_geventreactor.py
    EVENT_LOOP_MANAGER=eventlet uv run pytest tests/unit/io/test_eventletreactor.py

You can run a specific test method like so::

    uv run pytest tests/unit/test_connection.py::ConnectionTest::test_bad_protocol_version

Running Integration Tests
-------------------------
In order to run integration tests, you must specify a version to run using either of:
* ``SCYLLA_VERSION`` e.g. ``release:2025.2``
* ``CASSANDRA_VERSION``
environment variable::

    SCYLLA_VERSION="release:2025.2" uv run pytest tests/integration/standard tests/integration/cqlengine/

Or you can specify a scylla/cassandra directory (to test unreleased versions)::

    SCYLLA_VERSION=/path/to/scylla uv run pytest tests/integration/standard/

Specifying the usage of an already running Scylla cluster
------------------------------------------------------------
The test will start the appropriate Scylla clusters when necessary  but if you don't want this to happen because a Scylla cluster is already running the flag ``USE_CASS_EXTERNAL`` can be used, for example::

    USE_CASS_EXTERNAL=1 SCYLLA_VERSION='release:5.1' uv run pytest tests/integration/standard

Specify a Protocol Version for Tests
------------------------------------
The protocol version defaults to:
- 4 for Scylla >= 3.0 and Scylla Enterprise > 2019.
- 3 for older versions of Scylla
- 5 for Cassandra >= 4.0, 4 for Cassandra >= 2.2, 3 for Cassandra >= 2.1, 2 for Cassandra >= 2.0
You can overwrite it with the ``PROTOCOL_VERSION`` environment variable::

    PROTOCOL_VERSION=3 SCYLLA_VERSION="release:5.1" uv run pytest tests/integration/standard tests/integration/cqlengine/

Seeing Test Logs in Real Time
-----------------------------
Sometimes it's useful to output logs for the tests as they run::

    uv run pytest -s tests/unit/

Use tee to capture logs and see them on your terminal::

    uv run pytest -s tests/unit/ 2>&1 | tee test.log


Running the Benchmarks
======================
There needs to be a version of Scyll running locally so before running the benchmarks, if ccm is installed:

	uv run ccm create benchmark_cluster --scylla -v release:2025.2 -n 1 -s

To run the benchmarks, pick one of the files under the ``benchmarks/`` dir and run it::

    uv run benchmarks/future_batches.py

There are a few options.  Use ``--help`` to see them all::

    uv run benchmarks/future_batches.py --help
