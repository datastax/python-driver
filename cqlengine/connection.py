#http://pypi.python.org/pypi/cql/1.0.4
#http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2 /
#http://cassandra.apache.org/doc/cql/CQL.html

from collections import namedtuple
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

def _set_conn(host):
    """
    """
    global _conn
    _conn = cql.connect(host.name, host.port, user=_username, password=_password)
    _conn.set_cql_version('3.0.0')

def setup(hosts, username=None, password=None):
    """
    Records the hosts and connects to one of them

    :param hosts: list of hosts, strings in the <hostname>:<port>, or just <hostname>
    """
    global _hosts
    global _username
    global _password

    _username = username
    _password = password

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
    host = _hosts[_host_idx]
    _set_conn(host)
    

class connection_manager(object):
    """
    Connection failure tolerant connection manager. Written to be used in a 'with' block for connection pooling
    """
    def __init__(self):
        if not _hosts:
            raise CQLConnectionError("No connections have been configured, call cqlengine.connection.setup")
        self.keyspace = None
        self.con = _conn

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def execute(self, query, params={}):
        """
        Gets a connection from the pool and executes the given query, returns the cursor

        if there's a connection problem, this will silently create a new connection pool
        from the available hosts, and remove the problematic host from the host list
        """
        global _host_idx

        for i in range(len(_hosts)):
            try:
                cur = self.con.cursor()
                cur.execute(query, params)
                return cur
            except TTransportException:
                #TODO: check for other errors raised in the event of a connection / server problem
                #move to the next connection and set the connection pool
                self.con_pool.return_connection(self.con)
                self.con = None
                _host_idx += 1
                _host_idx %= len(_hosts)
                host = _hosts[_host_idx]
                _set_conn(host)
                self.con = _conn

        raise CQLConnectionError("couldn't reach a Cassandra server")

