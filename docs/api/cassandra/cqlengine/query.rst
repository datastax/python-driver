``cassandra.cqlengine.query`` - Query and filter model objects
=================================================================

.. module:: cassandra.cqlengine.query

QuerySet
--------
QuerySet objects are typically obtained by calling :meth:`~.cassandra.cqlengine.models.Model.objects` on a model class.
The methods here are used to filter, order, and constrain results.

.. autoclass:: ModelQuerySet

    .. automethod:: all

    .. automethod:: batch

    .. automethod:: consistency

    .. automethod:: count

    .. method:: len(queryset)

       Returns the number of rows matched by this query. This function uses :meth:`~.cassandra.cqlengine.query.ModelQuerySet.count` internally.

       *Note: This function executes a SELECT COUNT() and has a performance cost on large datasets*

    .. automethod:: distinct

    .. automethod:: filter

    .. automethod:: get

    .. automethod:: limit

    .. automethod:: fetch_size

    .. automethod:: if_not_exists

    .. automethod:: if_exists

    .. automethod:: order_by

    .. automethod:: allow_filtering

    .. automethod:: only

    .. automethod:: defer

    .. automethod:: timestamp

    .. automethod:: ttl

    .. automethod:: using

    .. _blind_updates:

    .. automethod:: update

.. autoclass:: BatchQuery
   :members:

   .. automethod:: add_query
   .. automethod:: execute

.. autoclass:: ContextQuery

.. autoclass:: DoesNotExist

.. autoclass:: MultipleObjectsReturned

.. autoclass:: LWTException
