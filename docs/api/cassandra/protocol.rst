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

.. autoclass:: ProtocolHandler

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
    s.client_protocol_handler = LazyProtocolHandler   # for a result iterator
    s.client_protocol_handler = NumpyProtocolHandler  # for a dict of NumPy arrays as result

These protocol handlers comprise different column parsers (with corresponding names), and return
results as described below:

.. autofunction:: cython_protocol_handler
