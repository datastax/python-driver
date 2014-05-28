``cassandra.query`` - Prepared Statements, Batch Statements, Tracing, and Row Factories
=======================================================================================

.. module:: cassandra.query

.. autofunction:: tuple_factory

.. autofunction:: named_tuple_factory

.. autofunction:: dict_factory

.. autofunction:: ordered_dict_factory

.. autoclass:: Statement
   :members:

.. autoclass:: SimpleStatement
   :members:

.. autoclass:: PreparedStatement ()
   :members:

.. autoclass:: BoundStatement
   :members:

.. autoclass:: BatchStatement (batch_type=BatchType.LOGGED, retry_policy=None, consistency_level=None)
   :members:

.. autoclass:: BatchType ()

    .. autoattribute:: LOGGED

    .. autoattribute:: UNLOGGED

    .. autoattribute:: COUNTER

.. autoclass:: ValueSequence
   :members:

.. autoclass:: QueryTrace ()
   :members:

.. autoclass:: TraceEvent ()
   :members:

.. autoexception:: TraceUnavailable
