#http://pypi.python.org/pypi/cql/1.0.4
#http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2 /
#http://cassandra.apache.org/doc/cql/CQL.html

from collections import namedtuple
try:
    import Queue as queue
except ImportError:
    # python 3
    import queue
import random

import cql
import logging

from copy import copy
from cqlengine.exceptions import CQLEngineException

from cql import OperationalError

from contextlib import contextmanager

from thrift.transport.TTransport import TTransportException
from cqlengine.statements import BaseCQLStatement

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
            port = 9160
        elif len(host) == 2:
            try:
                port = int(host[1])
            except ValueError:
                raise CQLConnectionError("Can't parse port as int {}".format(':'.join(host)))
        else:
            raise CQLConnectionError("Can't parse host string {}".format(':'.join(host)))

        _hosts.append(Host(host[0], port))

    if not _hosts:
        raise CQLConnectionError("At least one host required")

    connection_pool = ConnectionPool(_hosts, username, password, consistency, timeout)


class ConnectionPool(object):
    """Handles pooling of database connections."""

    def __init__(
            self,
            hosts,
            username=None,
            password=None,
            consistency=None,
            timeout=None):
        self._hosts = hosts
        self._username = username
        self._password = password
        self._consistency = consistency
        self._timeout = timeout

        self._queue = queue.Queue(maxsize=_max_connections)

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
            # get with block=False returns an item if one
            # is immediately available, else raises the Empty exception
            return self._queue.get(block=False)
        except queue.Empty:
            return self._create_connection()

    def put(self, conn):
        """
        Returns a connection to the queue freeing it up for other queries to
        use.

        :param conn: The connection to be released
        :type conn: connection
        """

        try:
            self._queue.put(conn, block=False)
        except queue.Full:
            conn.close()

    def _create_transport(self, host):
        """
        Create a new Thrift transport for the given host.

        :param host: The host object
        :type host: Host

        :rtype: thrift.TTransport.*

        """
        from thrift.transport import TSocket, TTransport

        thrift_socket = TSocket.TSocket(host.name, host.port)

        if self._timeout is not None:
            thrift_socket.setTimeout(self._timeout)

        return TTransport.TFramedTransport(thrift_socket)

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
                transport = self._create_transport(host)
                new_conn = cql.connect(
                    host.name,
                    host.port,
                    user=self._username,
                    password=self._password,
                    consistency_level=self._consistency,
                    transport=transport
                )
                new_conn.set_cql_version('3.0.0')
                return new_conn
            except Exception as exc:
                logging.debug("Could not establish connection to"
                              " {}:{} ({!r})".format(host.name, host.port, exc))

        raise CQLConnectionError("Could not connect to any server in cluster")

    def execute(self, query, params, consistency_level=None):
        if not consistency_level:
            consistency_level = self._consistency

        while True:
            try:
                con = self.get()
                if not con:
                    raise CQLEngineException("Error calling execute without calling setup.")
                LOG.debug('{} {}'.format(query, repr(params)))
                cur = con.cursor()
                cur.execute(query, params, consistency_level=consistency_level)
                columns = [i[0] for i in cur.description or []]
                results = [RowResult(r) for r in cur.fetchall()]
                self.put(con)
                return QueryResult(columns, results)
            except CQLConnectionError as ex:
                raise CQLEngineException("Could not execute query against the cluster")
            except cql.ProgrammingError as ex:
                raise CQLEngineException(unicode(ex))
            except TTransportException:
                pass
            except OperationalError as ex:
                LOG.exception("Operational Error %s on %s:%s", ex, con.host, con.port)
                raise ex


def execute(query, params=None, consistency_level=None):
    if isinstance(query, BaseCQLStatement):
        params = query.get_context()
        query = str(query)
    params = params or {}
    if consistency_level is None:
        consistency_level = connection_pool._consistency
    return connection_pool.execute(query, params, consistency_level)

@contextmanager
def connection_manager():
    """ :rtype: ConnectionPool """
    global connection_pool
    # tmp = connection_pool.get()
    yield connection_pool
    # connection_pool.put(tmp)
