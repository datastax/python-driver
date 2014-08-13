#!/usr/bin/env python

import os
import nose
import sys

sys.path.append("")

# setup cassandra
from cqlengine import connection

if os.environ.get('CASSANDRA_TEST_HOST'):
    CASSANDRA_TEST_HOST = os.environ['CASSANDRA_TEST_HOST']
else:
    CASSANDRA_TEST_HOST = 'localhost'

protocol_version = int(os.environ.get("CASSANDRA_PROTOCOL_VERSION", 2))

connection.setup([CASSANDRA_TEST_HOST], protocol_version=protocol_version, default_keyspace='cqlengine_test')

try:
    c_version = os.environ["CASSANDRA_VERSION"]
except:
    print "CASSANDRA_VERSION must be set as an environment variable. One of (12, 20, 21)"
    raise

nose.main()
