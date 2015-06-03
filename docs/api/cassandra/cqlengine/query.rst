``cassandra.cqlengine.query`` - Query and filter model objects
=================================================================

.. module:: cassandra.cqlengine.query

QuerySet
--------
QuerySet objects are typically obtained by calling :meth:`~.cassandra.cqlengine.models.Model.objects` on a model class.
The mehtods here are used to filter, order, and constrain results.

.. autoclass:: ModelQuerySet

    .. automethod:: all

    .. automethod:: batch

    .. automethod:: consistency

    .. automethod:: count

    .. automethod:: filter

    .. automethod:: get

    .. automethod:: limit

    .. automethod:: order_by

    .. automethod:: allow_filtering

    .. automethod:: timestamp

    .. automethod:: ttl

    .. _blind_updates:

    .. automethod:: update

.. autoclass:: DoesNotExist

.. autoclass:: MultipleObjectsReturned

.. autoclass:: LWTException
