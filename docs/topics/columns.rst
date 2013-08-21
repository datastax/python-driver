=======
Columns
=======

**Users of versions < 0.4, please read this post before upgrading:** `Breaking Changes`_

.. _Breaking Changes: https://groups.google.com/forum/?fromgroups#!topic/cqlengine-users/erkSNe1JwuU

.. module:: cqlengine.columns

.. class:: Bytes()

    Stores arbitrary bytes (no validation), expressed as hexadecimal ::

        columns.Bytes()


.. class:: Ascii()

    Stores a US-ASCII character string ::

        columns.Ascii()


.. class:: Text()

    Stores a UTF-8 encoded string ::

       columns.Text()

    **options**

        :attr:`~columns.Text.min_length`
            Sets the minimum length of this string. If this field is not set , and the column is not a required field, it defaults to 0, otherwise 1.

        :attr:`~columns.Text.max_length`
            Sets the maximum length of this string. Defaults to None

.. class:: Integer()

    Stores an integer value ::

        columns.Integer()

.. class:: DateTime()

    Stores a datetime value.

        columns.DateTime()

.. class:: UUID()

    Stores a type 1 or type 4 UUID.

        columns.UUID()

.. class:: TimeUUID()

    Stores a UUID value as the cql type 'timeuuid' ::

        columns.TimeUUID()

    .. classmethod:: from_datetime(dt)

        generates a TimeUUID for the given datetime

        :param dt: the datetime to create a time uuid from
        :type dt: datetime.datetime

        :returns: a time uuid created from the given datetime
        :rtype: uuid1

.. class:: Boolean()

    Stores a boolean True or False value ::

        columns.Boolean()

.. class:: Float()

    Stores a floating point value ::

        columns.Float()

    **options**

        :attr:`~columns.Float.double_precision`
            If True, stores a double precision float value, otherwise single precision. Defaults to True.

.. class:: Decimal()

    Stores a variable precision decimal value ::

        columns.Decimal()

.. class:: Counter()

    Counters can be incremented and decremented ::

        columns.Counter()


Collection Type Columns
----------------------------

    CQLEngine also supports container column types. Each container column requires a column class argument to specify what type of objects it will hold. The Map column requires 2, one for the key, and the other for the value

    *Example*

    .. code-block:: python

        class Person(Model):
            id          = columns.UUID(primary_key=True, default=uuid.uuid4)
            first_name  = columns.Text()
            last_name   = columns.Text()

            friends     = columns.Set(columns.Text)
            enemies     = columns.Set(columns.Text)
            todo_list   = columns.List(columns.Text)
            birthdays   = columns.Map(columns.Text, columns.DateTime)



.. class:: Set()

    Stores a set of unordered, unique values. Available only with Cassandra 1.2 and above ::

        columns.Set(value_type)

    **options**

        :attr:`~columns.Set.value_type`
            The type of objects the set will contain

        :attr:`~columns.Set.strict`
            If True, adding this column will raise an exception during save if the value is not a python `set` instance. If False, it will attempt to coerce the value to a set. Defaults to True.

.. class:: List()

    Stores a list of ordered values. Available only with Cassandra 1.2 and above ::

        columns.List(value_type)

    **options**

        :attr:`~columns.List.value_type`
            The type of objects the set will contain

.. class:: Map()

    Stores a map (dictionary) collection, available only with Cassandra 1.2 and above ::

        columns.Map(key_type, value_type)

    **options**

        :attr:`~columns.Map.key_type`
            The type of the map keys

        :attr:`~columns.Map.value_type`
            The type of the map values

Column Options
==============

    Each column can be defined with optional arguments to modify the way they behave. While some column types may define additional column options, these are the options that are available on all columns:

    .. attribute:: BaseColumn.primary_key

        If True, this column is created as a primary key field. A model can have multiple primary keys. Defaults to False.

        *In CQL, there are 2 types of primary keys: partition keys and clustering keys. As with CQL, the first primary key is the partition key, and all others are clustering keys, unless partition keys are specified manually using* :attr:`BaseColumn.partition_key`

    .. attribute:: BaseColumn.partition_key

        If True, this column is created as partition primary key. There may be many partition keys defined, forming a *composite partition key*

    .. attribute:: BaseColumn.index

        If True, an index will be created for this column. Defaults to False.

        *Note: Indexes can only be created on models with one primary key*

    .. attribute:: BaseColumn.db_field

        Explicitly sets the name of the column in the database table. If this is left blank, the column name will be the same as the name of the column attribute. Defaults to None.

    .. attribute:: BaseColumn.default

        The default value for this column. If a model instance is saved without a value for this column having been defined, the default value will be used. This can be either a value or a callable object (ie: datetime.now is a valid default argument).

    .. attribute:: BaseColumn.required

        If True, this model cannot be saved without a value defined for this column. Defaults to True. Primary key fields cannot have their required fields set to False.

    .. attribute:: BaseColumn.clustering_order

        Defines CLUSTERING ORDER for this column (valid choices are "asc" (default) or "desc"). It may be specified only for clustering primary keys - more: http://www.datastax.com/docs/1.2/cql_cli/cql/CREATE_TABLE#using-clustering-order
