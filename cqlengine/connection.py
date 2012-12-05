#http://pypi.python.org/pypi/cql/1.0.4
#http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2 /
#http://cassandra.apache.org/doc/cql/CQL.html

from collections import namedtuple
import Queue
import random

import cql

from cqlengine.exceptions import CQLEngineException

from thrift.transport.TTransport import TTransportException


class CQLConnectionError(CQLEngineException): pass

Host = namedtuple('Host', ['name', 'port'])
_hosts = []
_host_idx = 0
_conn= None
_username = None
_password = None
_max_connections = 10

def setup(hosts, username=None, password=None, max_connections=10):
    """
    Records the hosts and connects to one of them

    :param hosts: list of hosts, strings in the <hostname>:<port>, or just <hostname>
    """
    global _hosts
    global _username
    global _password
    global _max_connections
    _username = username
    _password = password
    _max_connections = max_connections
    
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

    random.shuffle(_hosts)
    
    con = ConnectionPool.get()
    ConnectionPool.put(con)


class ConnectionPool(object):
    """Handles pooling of database connections."""

    # Connection pool queue
    _queue = None

    @classmethod
    def clear(cls):
        """
        Force the connection pool to be cleared. Will close all internal
        connections.
        """
        try:
            while not cls._queue.empty():
                cls._queue.get().close()
        except:
            pass
    
    @classmethod
    def get(cls):
        """
        Returns a usable database connection. Uses the internal queue to
        determine whether to return an existing connection or to create
        a new one.
        """
        try:
            if cls._queue.empty():
                return cls._create_connection()
            return cls._queue.get()
        except CQLConnectionError as cqle:
            raise cqle
        except:
            if not cls._queue:
                cls._queue = Queue.Queue(maxsize=_max_connections)
            return cls._create_connection()

    @classmethod
    def put(cls, conn):
        """
        Returns a connection to the queue freeing it up for other queries to
        use.

        :param conn: The connection to be released
        :type conn: connection
        """
        try:
            if cls._queue.full():
                conn.close()
            else:
                cls._queue.put(conn)
        except:
            if not cls._queue:
                cls._queue = Queue.Queue(maxsize=_max_connections)
            cls._queue.put(conn)

    @classmethod
    def _create_connection(cls):
        """
        Creates a new connection for the connection pool.
        """
        global _hosts
        global _username
        global _password
        
        if not _hosts:
            raise CQLConnectionError("At least one host required")

        host = _hosts[_host_idx]

        new_conn = cql.connect(host.name, host.port, user=_username, password=_password)
        new_conn.set_cql_version('3.0.0')
        return new_conn
        

class connection_manager(object):
    """
    Connection failure tolerant connection manager. Written to be used in a 'with' block for connection pooling
    """
    def __init__(self):
        if not _hosts:
            raise CQLConnectionError("No connections have been configured, call cqlengine.connection.setup")
        self.keyspace = None
        self.con = ConnectionPool.get()
        self.cur = None
        
    def close(self):
        if self.cur: self.cur.close()
        ConnectionPool.put(self.con)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def execute(self, query, params={}):
        """
        Gets a connection from the pool and executes the given query, returns the cursor

        if there's a connection problem, this will silently create a new connection pool
        from the available hosts, and remove the problematic host from the host list
        """
        global _host_idx

        for i in range(len(_hosts)):
            try:
                self.cur = self.con.cursor()
                self.cur.execute(query, params)
                return self.cur
            except TTransportException:
                #TODO: check for other errors raised in the event of a connection / server problem
                #move to the next connection and set the connection pool
                self.con = None
                _host_idx += 1
                _host_idx %= len(_hosts)
                self.con.close()
                self.con = ConnectionPool._create_connection()

        raise CQLConnectionError("couldn't reach a Cassandra server")

