``cassandra.metadata`` - Schema and Ring Topology
=================================================

.. module:: cassandra.metadata

.. autoclass:: Metadata ()
   :members:
   :exclude-members: rebuild_schema, rebuild_token_map, add_host, remove_host, get_host

Schemas
-------

.. autoclass:: KeyspaceMetadata ()
   :members:

.. autoclass:: TableMetadata ()
   :members:

.. autoclass:: ColumnMetadata ()
   :members:

.. autoclass:: IndexMetadata ()
   :members:

Tokens and Ring Topology
------------------------

.. autoclass:: TokenMap ()
   :members:

.. autoclass:: Token ()
   :members:

.. autoclass:: Murmur3Token
   :members:

.. autoclass:: MD5Token
   :members:

.. autoclass:: BytesToken
   :members:

.. autoclass:: ReplicationStrategy
   :members:

.. autoclass:: SimpleStrategy
   :members:

.. autoclass:: NetworkTopologyStrategy
   :members:

.. autoclass:: LocalStrategy
   :members:
