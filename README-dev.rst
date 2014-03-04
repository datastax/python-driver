Releasing
=========
* Run the tests and ensure they all pass
* Update CHANGELOG.rst
* Update the version in ``cassandra/__init__.py``
* Commit the changelog and version changes
* Tag the release.  For example: ``git tag -a 1.0.0 -m 'version 1.0.0'``
* Push the commit and tag: ``git push --tags origin master``
* Upload the package to pypi::

    python setup.py register
    python setup.py sdist upload

* Update the docs (see below)
* Append a 'post' string to the version tuple in ``cassandra/__init__.py``
  so that it looks like ``(x, y, z, 'post')``
* Commit and push

Building the Docs
=================
Sphinx is required to build the docs. You probably want to install through apt,
if possible::

    sudo apt-get install python-sphinx

pip may also work::

    sudo pip install -U Sphinx

To build the docs, run::

    python setup.py doc

To upload the docs, checkout the ``gh-pages`` branch (it's usually easier to
clone a second copy of this repo and leave it on that branch) and copy the entire
contents all of ``docs/_build/X.Y.Z/*`` into the root of the ``gh-pages`` branch
and then push that branch to github.

For example::

    python setup.py doc
    cp -R docs/_build/1.0.0-beta1/* ~/python-driver-docs/
    cd ~/python-driver-docs
    git push origin gh-pages

Running the Tests
=================
In order for the extensions to be built and used in the test, run::

    python setup.py nosetests

You can run a specific test module or package like so::

    python setup.py nosetests -w tests/unit/

If you want to test all of python 2.6, 2.7, and pypy, use tox (this is what
TravisCI runs)::

    tox

By default, tox only runs the unit tests because I haven't put in the effort
to get the integration tests to run on TravicCI.  However, the integration
tests should work locally.  To run them, edit the following line in tox.ini::

    commands = {envpython} setup.py build_ext --inplace nosetests --verbosity=2 tests/unit/

and change ``tests/unit/`` to ``tests/``.
