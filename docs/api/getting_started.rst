Getting Started
===============

Before we can start executing any queries against Cassandra we need to setup our cluster. Setting up our cluster
allows us to set specific options like what port CQL native transport is listening to for connections, SSL options,
the loadbalancing policy to use for handling queries and more which can be found here :doc:`/api/cassandra/cluster` ::

  from cassandra.cluster import Cluster

  options = {
      'contact_points': ['10.1.1.3', '10.1.1.4', '10.1.1.5'],
      'port': 9042
  }

  cluster = Cluster(**options)
  session = cluster.connect(keyspace='users')

Instantiating a cluster does not actually connect us to any nodes. To begin executing queries we need a session, which is created by calling cluster.connect(). connect takes an optional 'keyspace' argument allowing all queries in that session to be operated on that keyspace. Alternatively, you can set the keyspace after the session is created. Sessions should NOT be instantiated outside the use of a Cluster. The Cluster handles the
creation and disposal of sessions. ::

  session.set_keyspace('users')

Now that we have a session we can begin to execute queries. If you are using Cassandra in a way that allows you to not block calls you can execute queries asynchronously. More about the execute and execute_async functions can be found here :doc:`/api/cassandra/cluster` (this should link to execute and execute_async) ::

  # without async (blocking)
  result = session.execute('SELECT * FROM users')
  for row in results:
      print row

  # with async
  def handle_success(results):
      for row in results:
          print row

  def handle_error(exception_error):
      print exception_error

  future = session.execute_async('SELECT * FROM users')
  future.add_callbacks(handle_success, handle_error)

When executing queries from a session, the driver picks a Cassandra node to act as the coordinator. As shown earlier in our Cluster example with the options we passed several ip addresses which the driver can communicate with. By default the driver will touch those nodes in the list and grab the ip addresses of any nodes that those nodes have found via gossip. To change this behavior you can use different Load Balancing policies that are available here :doc:`/api/cassandra/policies` ::

  from cassandra.cluster import Cluster
  from cassandra.policies import DCAwareRoundRobinPolicy

  options = {
      'contact_points': ['10.1.1.3', '10.1.1.4', '10.1.1.5'],
      'port': 9042,
      'load_balancing_policy': DCAwareRoundRobinPolicy(local_dc='datacenter1')
  }

  cluster = Cluster(**options)