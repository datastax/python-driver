``cassandra.query`` - Prepared Statements, Batch Statements, Tracing, and Row Factories
=======================================================================================

.. module:: cassandra.query

.. autofunction:: tuple_factory

.. autofunction:: named_tuple_factory

.. autofunction:: dict_factory

.. autofunction:: ordered_dict_factory

.. autoclass:: SimpleStatement
   :members:

.. autoclass:: PreparedStatement ()
   :members:

.. autoclass:: BoundStatement
   :members:

.. autoclass:: Statement ()
   :members:

.. autodata:: UNSET_VALUE
   :annotation:

.. autoclass:: BatchStatement (batch_type=BatchType.LOGGED, retry_policy=None, consistency_level=None)
   :members:

.. autoclass:: BatchType ()

    .. autoattribute:: LOGGED

    .. autoattribute:: UNLOGGED

    .. autoattribute:: COUNTER

.. autoclass:: cassandra.query.ValueSequence

    A wrapper class that is used to specify that a sequence of values should
    be treated as a CQL list of values instead of a single column collection when used
    as part of the `parameters` argument for :meth:`.Session.execute()`. 

    This is typically needed when supplying a list of keys to select.
    For example::

        >>> my_user_ids = ('alice', 'bob', 'charles')
        >>> query = "SELECT * FROM users WHERE user_id IN %s"
        >>> session.execute(query, parameters=[ValueSequence(my_user_ids)])

.. autoclass:: QueryTrace ()
   :members:

.. autoclass:: TraceEvent ()
   :members:

.. autoexception:: TraceUnavailable
