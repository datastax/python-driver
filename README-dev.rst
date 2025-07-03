Releasing
=========
* Run the tests and ensure they all pass
* Update the version in ``cassandra/__init__.py``
* Add the new version in ``docs/conf.py`` (variables: ``TAGS``, ``LATEST_VERSION``, ``DEPRECATED_VERSIONS``).
   * For patch version releases (like ``3.26.8-scylla -> 3.26.9-scylla``) replace the old version with new one in ``TAGS`` and update ``LATEST_VERSION``.
   * For minor version releases (like ``3.26.9-scylla -> 3.27.0-scylla``) add new version to ``TAGS``, update ``LATEST_VERSION`` and add previous minor version to ``DEPRECATED_VERSIONS``.
* Commit the version changes, e.g. ``git commit -m 'Release 3.26.9'``
* Tag the release.  For example: ``git tag -a 3.26.9-scylla -m 'Release 3.26.9'``
* Push the tag and new ``master`` SIMULTANEOUSLY: ``git push --atomic origin master v6.0.21-scylla``
* Now new version and its docs should be automatically published. Check `PyPI <https://pypi.org/project/scylla-driver/#history>`_ and `docs <https://python-driver.docs.scylladb.com/stable/>`_ to make sure its there.
* If you didn't push branch and tag simultaneously (or doc publishing failed for other reason) then restart the relevant job from GitHub Actions UI.
* Publish a GitHub Release and a post on community forum.

Building the Docs
=================

To build and preview the documentation for the ScyllaDB Python driver locally, you must first manually install `python-driver`.
This is necessary for autogenerating the reference documentation of the driver.
You can find detailed instructions on how to install the driver in the `Installation guide <https://python-driver.docs.scylladb.com/stable/installation.html#manual-installation>`_.

After installing the driver, you can build the documentation:
- Make sure you have Python version compatible with docs. You can see supported version in ``docs/pyproject.toml`` - look for ``python`` in ``tool.poetry.dependencies`` section.
- Install poetry: ``pip install poetry``
- To preview docs in your browser: ``make -C docs preview``

Tooling
=======

We recommend using `uv` tool for running tests, linters and basically everything else,
since it makes Python tooling ecosystem mostly usable.
To install it, see instructions at https://docs.astral.sh/uv/getting-started/installation/
The rest of this document assumes you have `uv` installed.

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
* ``SCYLLA_VERSION`` e.g. ``release:5.1``
* ``CASSANDRA_VERSION``
environment variable::

    SCYLLA_VERSION="release:5.1" uv run pytest tests/integration/standard tests/integration/cqlengine/

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
There needs to be a version of cassandra running locally so before running the benchmarks, if ccm is installed:

	uv run ccm create benchmark_cluster -v 3.0.1 -n 1 -s

To run the benchmarks, pick one of the files under the ``benchmarks/`` dir and run it::

    uv run benchmarks/future_batches.py

There are a few options.  Use ``--help`` to see them all::

    uv run benchmarks/future_batches.py --help
