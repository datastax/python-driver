=======
Columns
=======

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

.. class:: Integer()

    Stores an integer value ::

        columns.Integer()

.. class:: DateTime()

    Stores a datetime value.

    Python's datetime.now callable is set as the default value for this column ::

        columns.DateTime()

.. class:: UUID()

    Stores a type 1 or type 4 UUID.

    Python's uuid.uuid4 callable is set as the default value for this column. ::

        columns.UUID()

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

Column Options
==============

    Each column can be defined with optional arguments to modify the way they behave. While some column types may define additional column options, these are the options that are available on all columns:

    .. attribute:: BaseColumn.primary_key

        If True, this column is created as a primary key field. A model can have multiple primary keys. Defaults to False.

        *In CQL, there are 2 types of primary keys: partition keys and clustering keys. As with CQL, the first primary key is the partition key, and all others are clustering keys.*

    .. attribute:: BaseColumn.index

        If True, an index will be created for this column. Defaults to False.
        
        *Note: Indexes can only be created on models with one primary key*

    .. attribute:: BaseColumn.db_field

        Explicitly sets the name of the column in the database table. If this is left blank, the column name will be the same as the name of the column attribute. Defaults to None.

    .. attribute:: BaseColumn.default

        The default value for this column. If a model instance is saved without a value for this column having been defined, the default value will be used. This can be either a value or a callable object (ie: datetime.now is a valid default argument).

    .. attribute:: BaseColumn.required

        If True, this model cannot be saved without a value defined for this column. Defaults to True. Primary key fields cannot have their required fields set to False.

