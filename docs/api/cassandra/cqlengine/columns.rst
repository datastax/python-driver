``cassandra.cqlengine.columns`` - Column types for object mapping models
========================================================================

.. module:: cassandra.cqlengine.columns

Columns
-------

    Columns in your models map to columns in your CQL table. You define CQL columns by defining column attributes on your model classes.
    For a model to be valid it needs at least one primary key column and one non-primary key column.

    Just as in CQL, the order you define your columns in is important, and is the same order they are defined in on a model's corresponding table.

    Each column on your model definitions needs to be an instance of a Column class.

.. autoclass:: Column(**kwargs)

    .. autoattribute:: primary_key

    .. autoattribute:: partition_key

    .. autoattribute:: index

    .. autoattribute:: db_field

    .. autoattribute:: default

    .. autoattribute:: required

    .. autoattribute:: clustering_order

    .. autoattribute:: discriminator_column

    .. autoattribute:: static

Column Types
------------

Columns of all types are initialized by passing :class:`.Column` attributes to the constructor by keyword.
    
.. autoclass:: Ascii(**kwargs)

.. autoclass:: BigInt(**kwargs)

.. autoclass:: Blob(**kwargs)

.. autoclass:: Bytes(**kwargs)

.. autoclass:: Boolean(**kwargs)

.. autoclass:: Counter

.. autoclass:: Date(**kwargs)

.. autoclass:: DateTime(**kwargs)

    .. autoattribute:: truncate_microseconds

.. autoclass:: Decimal(**kwargs)

.. autoclass:: Double(**kwargs)

.. autoclass:: Float

.. autoclass:: Integer(**kwargs)

.. autoclass:: List

.. autoclass:: Map

.. autoclass:: Set

.. autoclass:: SmallInt(**kwargs)

.. autoclass:: Text

.. autoclass:: Time(**kwargs)

.. autoclass:: TimeUUID(**kwargs)

.. autoclass:: TinyInt(**kwargs)

.. autoclass:: UserDefinedType

.. autoclass:: UUID(**kwargs)

.. autoclass:: VarInt(**kwargs)
