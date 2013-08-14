#http://pypi.python.org/pypi/cql/1.0.4
#http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2 /
#http://cassandra.apache.org/doc/cql/CQL.html

from collections import namedtuple
import Queue
import random

import cql
import logging

from copy import copy
from cqlengine.exceptions import CQLEngineException

from contextlib import contextmanager

from thrift.transport.TTransport import TTransportException

LOG = logging.getLogger('cqlengine.cql')

class CQLConnectionError(CQLEngineException): pass

Host = namedtuple('Host', ['name', 'port'])

_max_connections = 10

# global connection pool
connection_pool = None


class CQLConnectionError(CQLEngineException): pass


class RowResult(tuple):
    pass

QueryResult = namedtuple('RowResult', ('columns', 'results'))


def _column_tuple_factory(colnames, values):
    return tuple(colnames), [RowResult(v) for v in values]


def setup(hosts, username=None, password=None, max_connections=10, default_keyspace=None, consistency='ONE'):
    """
    Records the hosts and connects to one of them

    :param hosts: list of hosts, strings in the <hostname>:<port>, or just <hostname>
    """
    global _max_connections
    global connection_pool
    _max_connections = max_connections

    if default_keyspace:
        from cqlengine import models
        models.DEFAULT_KEYSPACE = default_keyspace

    _hosts = []
    for host in hosts:
        host = host.strip()
        host = host.split(':')
        if len(host) == 1:
            _hosts.append(Host(host[0], 9160))
        elif len(host) == 2:
            _hosts.append(Host(*host))
        else:
            raise CQLConnectionError("Can't parse {}".format(''.join(host)))

    if not _hosts:
        raise CQLConnectionError("At least one host required")

    connection_pool = ConnectionPool(_hosts, username, password, consistency)


class ConnectionPool(object):
    """Handles pooling of database connections."""

    def __init__(self, hosts, username=None, password=None, consistency=None):
        self._hosts = hosts
        self._username = username
        self._password = password
        self._consistency = consistency

        self._queue = Queue.Queue(maxsize=_max_connections)

    def clear(self):
        """
        Force the connection pool to be cleared. Will close all internal
        connections.
        """
        try:
            while not self._queue.empty():
                self._queue.get().close()
        except:
            pass

    def get(self):
        """
        Returns a usable database connection. Uses the internal queue to
        determine whether to return an existing connection or to create
        a new one.
        """
        try:
            if self._queue.empty():
                return self._create_connection()
            return self._queue.get()
        except CQLConnectionError as cqle:
            raise cqle

    def put(self, conn):
        """
        Returns a connection to the queue freeing it up for other queries to
        use.

        :param conn: The connection to be released
        :type conn: connection
        """

        if self._queue.full():
            conn.close()
        else:
            self._queue.put(conn)

    def _create_connection(self):
        """
        Creates a new connection for the connection pool.

        should only return a valid connection that it's actually connected to
        """
        if not self._hosts:
            raise CQLConnectionError("At least one host required")

        hosts = copy(self._hosts)
        random.shuffle(hosts)

        for host in hosts:
            try:
                new_conn = cql.connect(
                    host.name,
                    host.port,
                    user=self._username,
                    password=self._password,
                    consistency_level=self._consistency
                )
                new_conn.set_cql_version('3.0.0')
                return new_conn
            except Exception as e:
                logging.debug("Could not establish connection to {}:{}".format(host.name, host.port))
                pass

        raise CQLConnectionError("Could not connect to any server in cluster")

    def execute(self, query, params):
        while True:
            try:
                con = self.get()
                if not con:
                    raise CQLEngineException("Error calling execute without calling setup.")
                cur = con.cursor()
                cur.execute(query, params)
                columns = [i[0] for i in cur.description or []]
                results = [RowResult(r) for r in cur.fetchall()]
                LOG.debug('{} {}'.format(query, repr(params)))
                self.put(con)
                return QueryResult(columns, results)
            except CQLConnectionError as ex:
                raise CQLEngineException("Could not execute query against the cluster")
            except cql.ProgrammingError as ex:
                raise CQLEngineException(unicode(ex))
            except TTransportException:
                raise CQLEngineException("Could not execute query against the cluster")


def execute(query, params=None):
    params = params or {}
    return connection_pool.execute(query, params)

@contextmanager
def connection_manager():
    """ :rtype: ConnectionPool """
    global connection_pool
    # tmp = connection_pool.get()
    yield connection_pool
    # connection_pool.put(tmp)
