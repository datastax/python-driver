``cassandra.metadata`` - Schema and Ring Topology
=================================================

.. module:: cassandra.metadata

.. autodata:: cql_keywords
   :annotation:

.. autodata:: cql_keywords_unreserved
   :annotation:

.. autodata:: cql_keywords_reserved
   :annotation:

.. autoclass:: Metadata ()
   :members:
   :exclude-members: rebuild_schema, rebuild_token_map, add_host, remove_host, get_host

Schemas
-------

.. autoclass:: KeyspaceMetadata ()
   :members:

.. autoclass:: UserType ()
   :members:

.. autoclass:: Function ()
   :members:

.. autoclass:: Aggregate ()
   :members:

.. autoclass:: TableMetadata ()
   :members:

.. autoclass:: ColumnMetadata ()
   :members:

.. autoclass:: IndexMetadata ()
   :members:

.. autoclass:: MaterializedViewMetadata ()
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
