``cassandra.cluster`` - Clusters and Sessions
=============================================

.. module:: cassandra.cluster

.. autoclass:: Cluster ([contact_points=('127.0.0.1',)][, port=9042][, executor_threads=2], **attr_kwargs)
   :members:
   :exclude-members: on_up, on_down, add_host, remove_host, connection_factory

.. autoclass:: Session ()
   :members:
   :exclude-members: on_up, on_down, on_add, on_remove, add_host, prepare_on_all_hosts, submit

.. autoclass:: ResponseFuture ()
   :members:
   :exclude-members: send_request

.. autoexception:: NoHostAvailable ()
   :members:
