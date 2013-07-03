``cassandra.policies`` - Load balancing and Failure Handling Policies
=====================================================================

.. module:: cassandra.policies

Load Balancing
--------------

.. autoclass:: HostDistance
    :members:

.. autoclass:: LoadBalancingPolicy
   :members:

.. autoclass:: RoundRobinPolicy
   :members:

.. autoclass:: DCAwareRoundRobinPolicy
   :members:

.. autoclass:: TokenAwarePolicy
   :members:

Marking Hosts Up or Down
------------------------

.. autoclass:: ConvictionPolicy
   :members:

.. autoclass:: SimpleConvictionPolicy
   :members:

Reconnecting to Dead Hosts
--------------------------

.. autoclass:: ReconnectionPolicy
   :members:

.. autoclass:: ConstantReconnectionPolicy
   :members:

.. autoclass:: ExponentialReconnectionPolicy
   :members:

Retrying Failed Operations
--------------------------

.. autoclass:: WriteType
   :members:

.. autoclass:: RetryPolicy
   :members:

.. autoclass:: FallthroughRetryPolicy
   :members:

.. autoclass:: DowngradingConsistencyRetryPolicy
   :members:
