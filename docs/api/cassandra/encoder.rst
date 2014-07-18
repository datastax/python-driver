``cassandra.encoder`` - Encoders for non-prepared Statements
============================================================

.. module:: cassandra.encoder

.. data:: cql_encoders

    A map of python types to encoder functions.

.. autofunction:: cql_encode_none ()

.. autofunction:: cql_encode_object ()

.. autofunction:: cql_encode_all_types ()

.. autofunction:: cql_encode_sequence ()

String Types
------------

.. autofunction:: cql_encode_str ()

.. autofunction:: cql_encode_unicode ()

.. autofunction:: cql_encode_bytes ()

Date Types
----------

.. autofunction:: cql_encode_datetime ()

.. autofunction:: cql_encode_date ()

Collection Types
----------------

.. autofunction:: cql_encode_map_collection ()

.. autofunction:: cql_encode_list_collection ()

.. autofunction:: cql_encode_set_collection ()

.. autofunction:: cql_encode_tuple ()
