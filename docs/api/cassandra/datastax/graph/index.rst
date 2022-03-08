``cassandra.datastax.graph`` - Graph Statements, Options, and Row Factories
===========================================================================

.. _api-datastax-graph:

.. module:: cassandra.datastax.graph

.. autofunction:: single_object_row_factory

.. autofunction:: graph_result_row_factory

.. autofunction:: graph_object_row_factory

.. autofunction:: graph_graphson2_row_factory

.. autofunction:: graph_graphson3_row_factory

.. function:: to_int(value)

    Wraps a value to be explicitly serialized as a graphson Int.

.. function:: to_bigint(value)

    Wraps a value to be explicitly serialized as a graphson Bigint.

.. function:: to_smallint(value)

    Wraps a value to be explicitly serialized as a graphson Smallint.

.. function:: to_float(value)

    Wraps a value to be explicitly serialized as a graphson Float.

.. function:: to_double(value)

   Wraps a value to be explicitly serialized as a graphson Double.

.. autoclass:: GraphProtocol
   :members:
   :noindex:

.. autoclass:: GraphOptions
   :noindex:

   .. autoattribute:: graph_name

   .. autoattribute:: graph_source

   .. autoattribute:: graph_language

   .. autoattribute:: graph_read_consistency_level

   .. autoattribute:: graph_write_consistency_level

   .. autoattribute:: is_default_source

   .. autoattribute:: is_analytics_source

   .. autoattribute:: is_graph_source

   .. automethod:: set_source_default

   .. automethod:: set_source_analytics

   .. automethod:: set_source_graph


.. autoclass:: SimpleGraphStatement
   :members:
   :noindex:

.. autoclass:: Result
   :members:
   :noindex:

.. autoclass:: Vertex
   :members:
   :noindex:

.. autoclass:: VertexProperty
   :members:
   :noindex:

.. autoclass:: Edge
   :members:
   :noindex:

.. autoclass:: Path
   :members:
   :noindex:

.. autoclass:: T
   :members:
   :noindex:

.. autoclass:: GraphSON1Serializer
   :members:
   :noindex:

.. autoclass:: GraphSON1Deserializer
   :noindex:

   .. automethod:: deserialize_date

   .. automethod:: deserialize_timestamp

   .. automethod:: deserialize_time

   .. automethod:: deserialize_duration

   .. automethod:: deserialize_int

   .. automethod:: deserialize_bigint

   .. automethod:: deserialize_double

   .. automethod:: deserialize_float

   .. automethod:: deserialize_uuid

   .. automethod:: deserialize_blob

   .. automethod:: deserialize_decimal

   .. automethod:: deserialize_point

   .. automethod:: deserialize_linestring

   .. automethod:: deserialize_polygon

.. autoclass:: GraphSON2Reader
   :members:
   :noindex:
