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

from cqlengine.statements import BaseCQLStatement
from cassandra.query import dict_factory

LOG = logging.getLogger('cqlengine.cql')

class CQLConnectionError(CQLEngineException): pass

Host = namedtuple('Host', ['name', 'port'])

cluster = None
session = None


class CQLConnectionError(CQLEngineException): pass


def setup(
        hosts,
        default_keyspace=None,
        consistency='ONE',
        timeout=None,
        **kwargs):
    """
    Records the hosts and connects to one of them

    :param hosts: list of hosts, strings in the <hostname>:<port>, or just <hostname>
    :type hosts: list
    :param username: The cassandra username
    :type username: str
    :param password: The cassandra password
    :type password: str
    :param default_keyspace: The default keyspace to use
    :type default_keyspace: str
    :param consistency: The global consistency level
    :type consistency: str
    :param timeout: The connection timeout in milliseconds
    :type timeout: int or long

    """
    global cluster, session

    if 'username' in kwargs or 'password' in kwargs:
        raise CQLEngineException("Username & Password are now handled by using the native driver's auth_provider")

    cluster = Cluster(hosts, **kwargs)
    session = cluster.connect()
    session.row_factory = dict_factory

def execute(query, params=None, consistency_level=None):
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
