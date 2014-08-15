==================
Development
==================

Tests are run using Travis CI using a Matrix to test different Cassandra and Python versions.  Only Pull Requests that have passed the entire matrix will be considered for merge into the main codebase.

Python versions:

- 2.7
- 3.4

Cassandra vesions:

- 1.2 (protocol_version 1)
- 2.0 (protocol_version 2)
- 2.1 (upcoming, protocol_version 3)


Testing Locally
=================

Before testing, you'll need to set an environment variable to the version of Cassandra that's being tested.  The version cooresponds to the <Major><Minor> release, so for example if you're testing against Cassandra 2.1, you'd set the following:

    .. code-block::bash

        export CASSANDRA_VERSION=20

At the command line, execute:

    .. code-block::bash

        bin/test.py

This is a wrapper for nose that also sets up the database connection.




