#http://pypi.python.org/pypi/cql/1.0.4
#http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2 /
#http://cassandra.apache.org/doc/cql/CQL.html

from collections import namedtuple
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, Statement

try:
    import Queue as queue
except ImportError:
    # python 3
    import queue

import logging

from cqlengine.exceptions import CQLEngineException
from cassandra import ConsistencyLevel
from cqlengine.statements import BaseCQLStatement
from cassandra.query import dict_factory

LOG = logging.getLogger('cqlengine.cql')

class CQLConnectionError(CQLEngineException): pass

Host = namedtuple('Host', ['name', 'port'])

cluster = None
session = None
default_consistency_level = None

def setup(
        hosts,
        default_keyspace=None,
        consistency=ConsistencyLevel.ONE,
        **kwargs):
    """
    Records the hosts and connects to one of them

    :param hosts: list of hosts, see http://datastax.github.io/python-driver/api/cassandra/cluster.html
    :type hosts: list
    :param default_keyspace: The default keyspace to use
    :type default_keyspace: str
    :param consistency: The global consistency level
    :type consistency: int
    """
    global cluster, session, default_consistency_level

    if 'username' in kwargs or 'password' in kwargs:
        raise CQLEngineException("Username & Password are now handled by using the native driver's auth_provider")

    if default_keyspace:
        from cqlengine import models
        models.DEFAULT_KEYSPACE = default_keyspace

    default_consistency_level = consistency
    cluster = Cluster(hosts, **kwargs)
    session = cluster.connect()
    session.row_factory = dict_factory

def execute(query, params=None, consistency_level=None):

    if consistency_level is None:
        consistency_level = default_consistency_level

    if isinstance(query, Statement):
        pass

    elif isinstance(query, BaseCQLStatement):
        params = query.get_context()
        query = str(query)
        query = SimpleStatement(query, consistency_level=consistency_level)

    elif isinstance(query, basestring):
        query = SimpleStatement(query, consistency_level=consistency_level)


    params = params or {}
    result = session.execute(query, params)

    return result


def get_session():
    return session

def get_cluster():
    return cluster
