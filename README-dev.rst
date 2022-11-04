Releasing
=========
* Run the tests and ensure they all pass
* Update CHANGELOG.rst
  * Check for any missing entries
  * Add today's date to the release section
* Update the version in ``cassandra/__init__.py``
  * For beta releases, use a version like ``(2, 1, '0b1')``
  * For release candidates, use a version like ``(2, 1, '0rc1')``
  * When in doubt, follow PEP 440 versioning
* Add the new version in ``docs.yaml``
* Commit the changelog and version changes, e.g. ``git commit -m'version 1.0.0'``
* Tag the release.  For example: ``git tag -a 1.0.0 -m 'version 1.0.0'``
* Push the tag and new ``master``: ``git push origin 1.0.0 ; git push origin master``
* Update the `python-driver` submodule of `python-driver-wheels`,
  commit then push. This will trigger TravisCI and the wheels building.
* For a GA release, upload the package to pypi::

    # Clean the working directory
    python setup.py clean
    rm dist/*

    # Build the source distribution
    python setup.py sdist

    # Download all wheels from the jfrog repository and copy them in
    # the dist/ directory
    cp /path/to/wheels/*.whl dist/

    # Upload all files
    twine upload dist/*

* On pypi, make the latest GA the only visible version
* Update the docs (see below)
* Append a 'postN' string to the version tuple in ``cassandra/__init__.py``
  so that it looks like ``(x, y, z, 'postN')``

  * After a beta or rc release, this should look like ``(2, 1, '0b1', 'post0')``

* After the release has been tagged, add a section to docs.yaml with the new tag ref::

    versions:
      - name: <version name>
        ref: <release tag>

* Commit and push
* Update 'cassandra-test' branch to reflect new release

    * this is typically a matter of merging or rebasing onto master
    * test and push updated branch to origin

* Update the JIRA versions: https://datastax-oss.atlassian.net/plugins/servlet/project-config/PYTHON/versions

  * add release dates and set version as "released"

* Make an announcement on the mailing list

Building the Docs
=================

*Note*: The docs build instructions have been tested with Sphinx 2.4.4 and Fedora 32.

To build and preview the theme locally, you will need to install the following software:

- `Git <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`_
- `Python 3.7 <https://www.python.org/downloads/>`_
- `pip <https://pip.pypa.io/en/stable/installing/>`_

Run the following command to build the docs.

.. code:: console

    cd docs
    make preview

Once the command completes processing, open http://127.0.0.1:5500/ with your preferred browser.

Building multiple documentation versions
========================================

Build docs for all the versions.

```
cd docs
make multiversion
```
 Then, open ``docs/_build/dirhtml/<version>/index.html`` with your preferred browser.

**NOTE:** If you only can see docs generated for the master branch, try to run ``git fetch --tags`` to download the latest tags from remote.

Tests
=====

Running Unit Tests
------------------
Unit tests can be run like so::

    python -m pytest --import-mode append tests/unit -k 'not (test_connection_initialization or test_cloud)'
    EVENT_LOOP_MANAGER=gevent python -m pytest --import-mode append tests/unit/io/test_geventreactor.py
    EVENT_LOOP_MANAGER=eventlet python -m pytest --import-mode append tests/unit/io/test_eventletreactor.py

You can run a specific test method like so::

    python -m pytest tests/unit/test_connection.py::ConnectionTest::test_bad_protocol_version

Running Integration Tests
-------------------------
In order to run integration tests, you must specify a version to run using the ``CASSANDRA_VERSION`` or ``DSE_VERSION`` environment variable::

    CASSANDRA_VERSION=2.0.9 nosetests -w tests/integration/standard

Or you can specify a cassandra directory (to test unreleased versions)::

    CASSANDRA_DIR=/home/thobbs/cassandra nosetests -w tests/integration/standard/

Specifying the usage of an already running Cassandra cluster
------------------------------------------------------------
The test will start the appropriate Cassandra clusters when necessary  but if you don't want this to happen because a Cassandra cluster is already running the flag ``USE_CASS_EXTERNAL`` can be used, for example::

    USE_CASS_EXTERNAL=1 CASSANDRA_VERSION=2.0.9 nosetests -w tests/integration/standard

Specify a Protocol Version for Tests
------------------------------------
The protocol version defaults to 1 for cassandra 1.2 and 2 otherwise.  You can explicitly set
it with the ``PROTOCOL_VERSION`` environment variable::

    PROTOCOL_VERSION=3 nosetests -w tests/integration/standard

Seeing Test Logs in Real Time
-----------------------------
Sometimes it's useful to output logs for the tests as they run::

    python -m pytest -s tests/unit/

Use tee to capture logs and see them on your terminal::

    python -m pytest -s tests/unit/ 2>&1 | tee test.log

Testing Multiple Python Versions
--------------------------------
If you want to test all of python 2.7, 3.5, 3.6, 3.7, and pypy, use tox (this is what
TravisCI runs)::

    tox

By default, tox only runs the unit tests.

Running the Benchmarks
======================
There needs to be a version of cassandra running locally so before running the benchmarks, if ccm is installed:
	
	ccm create benchmark_cluster -v 3.0.1 -n 1 -s

To run the benchmarks, pick one of the files under the ``benchmarks/`` dir and run it::

    python benchmarks/future_batches.py

There are a few options.  Use ``--help`` to see them all::

    python benchmarks/future_batches.py --help

Packaging for Cassandra
=======================
A source distribution is included in Cassandra, which uses the driver internally for ``cqlsh``.
To package a released version, checkout the tag and build a source zip archive::

    python setup.py sdist --formats=zip

If packaging a pre-release (untagged) version, it is useful to include a commit hash in the archive
name to specify the built version::

    python setup.py egg_info -b-`git rev-parse --short HEAD` sdist --formats=zip

The file (``dist/scylla-driver-<version spec>.zip``) is packaged with Cassandra in ``cassandra/lib/scylla-driver-internal-only*zip``.

Releasing an EAP
================

An EAP release is only uploaded on a private server and it is not published on pypi.

* Clean the environment::

    python setup.py clean

* Package the source distribution::

    python setup.py sdist

* Test the source distribution::

    pip install dist/scylla-driver-<version>.tar.gz

* Upload the package on the EAP download server.
* Build the documentation::

    python setup.py doc

* Upload the docs on the EAP download server.

Adding a New Python Runtime Support
===================================

* Add the new python version to our jenkins image:
  https://github.com/riptano/openstack-jenkins-drivers/

* Add the new python version in job-creator:
  https://github.com/riptano/job-creator/

* Run the tests and ensure they all pass
  * also test all event loops

* Update the wheels building repo to support that version:
  https://github.com/riptano/python-dse-driver-wheels
