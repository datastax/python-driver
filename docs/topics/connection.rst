==============
Connection
==============

.. module:: cqlengine.connection

The setup function in `cqlengine.connection` records the Cassandra servers to connect to.
If there is a problem with one of the servers, cqlengine will try to connect to each of the other connections before failing.

.. function:: setup(hosts [, username=None, password=None])

    :param hosts: list of hosts, strings in the <hostname>:<port>, or just <hostname>
    :type hosts: list

    Records the hosts and connects to one of them

See the example at :ref:`getting-started`


