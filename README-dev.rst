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
* Upload the package to pypi::

    python setup.py register
    python setup.py sdist upload

* On pypi, make the latest GA the only visible version
* Update the docs (see below)
* Append a 'postN' string to the version tuple in ``cassandra/__init__.py``
  so that it looks like ``(x, y, z, 'postN')``

  * After a beta or rc release, this should look like ``(2, 1, '0b1', 'post0')``

* Commit and push
* Update 'cassandra-test' branch to reflect new release

    * this is typically a matter of merging or rebasing onto master
    * test and push updated branch to origin

* Update the JIRA versions: https://datastax-oss.atlassian.net/plugins/servlet/project-config/PYTHON/versions

  * add release dates and set version as "released"

* Make an announcement on the mailing list

Building the Docs
=================
Sphinx is required to build the docs. You probably want to install through apt,
if possible::

    sudo apt-get install python-sphinx

pip may also work::

    sudo pip install -U Sphinx

To build the docs, run::

    python setup.py doc

To upload the docs, checkout the ``gh-pages`` branch and copy the entire
contents all of ``docs/_build/X.Y.Z/*`` into the root of the ``gh-pages`` branch
and then push that branch to github.

For example::

    git checkout 1.0.0
    python setup.py doc
    git checkout gh-pages
    cp -R docs/_build/1.0.0/* .
    git add --update  # add modified files
    # Also make sure to add any new documentation files!
    git commit -m 'Update docs (version 1.0.0)'
    git push origin gh-pages

If docs build includes errors, those errors may not show up in the next build unless
you have changed the files with errors.  It's good to occassionally clear the build
directory and build from scratch::

    rm -rf docs/_build/*

Running the Tests
=================
In order for the extensions to be built and used in the test, run::

    python setup.py nosetests

You can run a specific test module or package like so::

    python setup.py nosetests -w tests/unit/

You can run a specific test method like so::

    python setup.py nosetests -w tests/unit/test_connection.py:ConnectionTest.test_bad_protocol_version

Seeing Test Logs in Real Time
-----------------------------
Sometimes it's useful to output logs for the tests as they run::

    python setup.py nosetests -w tests/unit/ --nocapture --nologcapture

Use tee to capture logs and see them on your terminal::

    python setup.py nosetests -w tests/unit/ --nocapture --nologcapture 2>&1 | tee test.log

Specifying a Cassandra Version for Integration Tests
----------------------------------------------------
You can specify a cassandra version with the ``CASSANDRA_VERSION`` environment variable::

    CASSANDRA_VERSION=2.0.9 python setup.py nosetests -w tests/integration/standard

You can also specify a cassandra directory (to test unreleased versions)::

    CASSANDRA_DIR=/home/thobbs/cassandra python setup.py nosetests -w tests/integration/standard

Specifying the usage of an already running Cassandra cluster
----------------------------------------------------
The test will start the appropriate Cassandra clusters when necessary  but if you don't want this to happen because a Cassandra cluster is already running the flag ``USE_CASS_EXTERNAL`` can be used, for example: 

	USE_CASS_EXTERNAL=1 python setup.py nosetests -w tests/integration/standard

Specify a Protocol Version for Tests
------------------------------------
The protocol version defaults to 1 for cassandra 1.2 and 2 otherwise.  You can explicitly set
it with the ``PROTOCOL_VERSION`` environment variable::

    PROTOCOL_VERSION=3 python setup.py nosetests -w tests/integration/standard

Testing Multiple Python Versions
--------------------------------
If you want to test all of python 2.7, 3.3, 3.4 and pypy, use tox (this is what
TravisCI runs)::

    tox

By default, tox only runs the unit tests because I haven't put in the effort
to get the integration tests to run on TravicCI.  However, the integration
tests should work locally.  To run them, edit the following line in tox.ini::

    commands = {envpython} setup.py build_ext --inplace nosetests --verbosity=2 tests/unit/

and change ``tests/unit/`` to ``tests/``.

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

The file (``dist/cassandra-driver-<version spec>.zip``) is packaged with Cassandra in ``cassandra/lib/cassandra-driver-internal-only*zip``.
