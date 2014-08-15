#http://pypi.python.org/pypi/cql/1.0.4
#http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2 /
#http://cassandra.apache.org/doc/cql/CQL.html

from collections import namedtuple
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import SimpleStatement, Statement
import six

try:
    import Queue as queue
except ImportError:
    # python 3
    import queue

import logging

from cqlengine.exceptions import CQLEngineException, UndefinedKeyspaceException
from cassandra import ConsistencyLevel
from cqlengine.statements import BaseCQLStatement
from cassandra.query import dict_factory

LOG = logging.getLogger('cqlengine.cql')

class CQLConnectionError(CQLEngineException): pass

Host = namedtuple('Host', ['name', 'port'])

cluster = None
session = None
lazy_connect_args = None
default_consistency_level = None

def setup(
        hosts,
        default_keyspace,
        consistency=ConsistencyLevel.ONE,
        lazy_connect=False,
        retry_connect=False,
        **kwargs):
    """
    Records the hosts and connects to one of them

    :param hosts: list of hosts, see http://datastax.github.io/python-driver/api/cassandra/cluster.html
    :type hosts: list
    :param default_keyspace: The default keyspace to use
    :type default_keyspace: str
    :param consistency: The global consistency level
    :type consistency: int
    :param lazy_connect: True if should not connect until first use
    :type lazy_connect: bool
    :param retry_connect: bool
    :param retry_connect: True if we should retry to connect even if there was a connection failure initially
    """
    global cluster, session, default_consistency_level, lazy_connect_args

    if 'username' in kwargs or 'password' in kwargs:
        raise CQLEngineException("Username & Password are now handled by using the native driver's auth_provider")

    if not default_keyspace:
        raise UndefinedKeyspaceException()

    from cqlengine import models
    models.DEFAULT_KEYSPACE = default_keyspace

    default_consistency_level = consistency
    if lazy_connect:
        kwargs['default_keyspace'] = default_keyspace
        kwargs['consistency'] = consistency
        kwargs['lazy_connect'] = False
        kwargs['retry_connect'] = retry_connect
        lazy_connect_args = (hosts, kwargs)
        return

    cluster = Cluster(hosts, **kwargs)
    try:
        session = cluster.connect()
    except NoHostAvailable:
        if retry_connect:
            kwargs['default_keyspace'] = default_keyspace
            kwargs['consistency'] = consistency
            kwargs['lazy_connect'] = False
            kwargs['retry_connect'] = retry_connect
            lazy_connect_args = (hosts, kwargs)
        raise
    session.row_factory = dict_factory

def execute(query, params=None, consistency_level=None):

    handle_lazy_connect()

    if not session:
        raise CQLEngineException("It is required to setup() cqlengine before executing queries")

    if consistency_level is None:
        consistency_level = default_consistency_level

    if isinstance(query, Statement):
        pass

    elif isinstance(query, BaseCQLStatement):
        params = query.get_context()
        query = str(query)
        query = SimpleStatement(query, consistency_level=consistency_level)

    elif isinstance(query, six.string_types):
        query = SimpleStatement(query, consistency_level=consistency_level)

    LOG.info(query.query_string)

    params = params or {}
    result = session.execute(query, params)

    return result

def get_session():
    handle_lazy_connect()
    return session

def get_cluster():
    handle_lazy_connect()
    return cluster

def handle_lazy_connect():
    global lazy_connect_args
    if lazy_connect_args:
        hosts, kwargs = lazy_connect_args
        lazy_connect_args = None
        setup(hosts, **kwargs)
