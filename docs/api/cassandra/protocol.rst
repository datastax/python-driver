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
