``cassandra.encoder`` - Encoders for non-prepared Statements
============================================================

.. module:: cassandra.encoder

.. autoclass:: Encoder ()

   .. autoattribute:: cassandra.encoder.Encoder.mapping

   .. automethod:: cassandra.encoder.Encoder.cql_encode_none ()

   .. automethod:: cassandra.encoder.Encoder.cql_encode_object ()

   .. automethod:: cassandra.encoder.Encoder.cql_encode_all_types ()

   .. automethod:: cassandra.encoder.Encoder.cql_encode_sequence ()

   .. automethod:: cassandra.encoder.Encoder.cql_encode_str ()

   .. automethod:: cassandra.encoder.Encoder.cql_encode_unicode ()

   .. automethod:: cassandra.encoder.Encoder.cql_encode_bytes ()

      Converts strings, buffers, and bytearrays into CQL blob literals.

   .. automethod:: cassandra.encoder.Encoder.cql_encode_datetime ()

   .. automethod:: cassandra.encoder.Encoder.cql_encode_date ()

   .. automethod:: cassandra.encoder.Encoder.cql_encode_map_collection ()

   .. automethod:: cassandra.encoder.Encoder.cql_encode_list_collection ()

   .. automethod:: cassandra.encoder.Encoder.cql_encode_set_collection ()

   .. automethod:: cql_encode_tuple ()
