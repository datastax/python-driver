#http://pypi.python.org/pypi/cql/1.0.4
#http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2 /
#http://cassandra.apache.org/doc/cql/CQL.html

from collections import namedtuple
from cassandra.cluster import Cluster

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

_max_connections = 10

# global connection pool
connection_pool = None
cluster = None
session = None


class CQLConnectionError(CQLEngineException): pass


class RowResult(tuple):
    pass

QueryResult = namedtuple('RowResult', ('columns', 'results'))


def _column_tuple_factory(colnames, values):
    return tuple(colnames), [RowResult(v) for v in values]


def setup(
        hosts,
        username=None,
        password=None,
        max_connections=10,
        default_keyspace=None,
        consistency='ONE',
        timeout=None):
    """
    Records the hosts and connects to one of them

    :param hosts: list of hosts, strings in the <hostname>:<port>, or just <hostname>
    :type hosts: list
    :param username: The cassandra username
    :type username: str
    :param password: The cassandra password
    :type password: str
    :param max_connections: The maximum number of connections to service
    :type max_connections: int or long
    :param default_keyspace: The default keyspace to use
    :type default_keyspace: str
    :param consistency: The global consistency level
    :type consistency: str
    :param timeout: The connection timeout in milliseconds
    :type timeout: int or long

    """
    global _max_connections, connection_pool, cluster, session

    cluster = Cluster(hosts)
    session = cluster.connect()
    session.row_factory = dict_factory


def execute(query, params=None, consistency_level=None):

    raise Exception("shut up")

    if isinstance(query, BaseCQLStatement):
        params = query.get_context()
        query = str(query)

    params = params or {}
    if consistency_level is None:
        consistency_level = connection_pool._consistency
    return connection_pool.execute(query, params, consistency_level)

    #return execute_native(query, params, consistency_level)


def execute_native(query, params=None, consistency_level=None):
    # TODO use consistency level
    if isinstance(query, BaseCQLStatement):
        params = query.get_context()
        query = str(query)

    params = params or {}
    result = session.execute(query, params)

    return result


def get_session():
    return session

def get_cluster():
    return cluster
