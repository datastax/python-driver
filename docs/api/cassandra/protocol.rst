``cassandra.protocol`` - Protocol Features
=====================================================================

.. module:: cassandra.protocol

.. _custom_payload:

Custom Payloads
---------------
Native protocol version 4+ allows for a custom payload to be sent between clients
and custom query handlers. The payload is specified as a string:binary_type dict
holding custom key/value pairs.

By default these are ignored by the server. They can be useful for servers implementing
a custom QueryHandler.

See :meth:`.Session.execute`, ::meth:`.Session.execute_async`, :attr:`.ResponseFuture.custom_payload`.

.. autoclass:: _ProtocolHandler

    .. autoattribute:: message_types_by_opcode
        :annotation: = {default mapping}

    .. automethod:: encode_message

    .. automethod:: decode_message

.. _faster_deser:

Faster Deserialization
----------------------
When python-driver is compiled with Cython, it uses a Cython-based deserialization path
to deserialize messages. By default, the driver will use a Cython-based parser that returns
lists of rows similar to the pure-Python version. In addition, there are two additional
ProtocolHandler classes that can be used to deserialize response messages: ``LazyProtocolHandler``
and ``NumpyProtocolHandler``. They can be used as follows:

.. code:: python

    from cassandra.protocol import NumpyProtocolHandler, LazyProtocolHandler
    from cassandra.query import tuple_factory
    s.client_protocol_handler = LazyProtocolHandler   # for a result iterator
    s.row_factory = tuple_factory  #required for Numpy results
    s.client_protocol_handler = NumpyProtocolHandler  # for a dict of NumPy arrays as result

These protocol handlers comprise different parsers, and return results as described below:

    - ProtocolHandler: this default implementation is a drop-in replacement for the pure-Python version.
        The rows are all parsed upfront, before results are returned.

    - LazyProtocolHandler: near drop-in replacement for the above, except that it returns an iterator over rows,
        lazily decoded into the default row format (this is more efficient since all decoded results are not materialized at once)

    - NumpyProtocolHander: deserializes results directly into NumPy arrays. This facilitates efficient integration with
        analysis toolkits such as Pandas.
