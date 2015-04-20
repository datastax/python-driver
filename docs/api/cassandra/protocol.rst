.. _custom_payload:

Custom Payload
==============
Native protocol version 4+ allows for a custom payload to be sent between clients
and custom query handlers. The payload is specified as a string:binary_type dict
holding custom key/value pairs.

By default these are ignored by the server. They can be useful for servers implementing
a custom QueryHandler.

