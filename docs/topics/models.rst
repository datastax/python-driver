======
Models
======

.. module:: cqlengine.models

A model is a python class representing a CQL table.

Example
=======

This example defines a Person table, with the columns ``first_name`` and ``last_name``

.. code-block:: python

    from cqlengine import columns
    from cqlengine.models import Model
    
    class Person(Model):
        first_name  = columns.Text()
        last_name = columns.Text()
     

The Person model would create this CQL table:

.. code-block:: sql

   CREATE TABLE cqlengine.person (
       id uuid,
       first_name text,
       last_name text,
       PRIMARY KEY (id)
   )

Columns
=======

    Columns in your models map to columns in your CQL table. You define CQL columns by defining column attributes on your model classes. For a model to be valid it needs at least one primary key column (defined automatically if you don't define one) and one non-primary key column.

    Just as in CQL, the order you define your columns in is important, and is the same order they are defined in on a model's corresponding table.

Column Types
============

    Each column on your model definitions needs to an instance of a Column class. The column types that are included with cqlengine as of this writing are:

    * :class:`~cqlengine.columns.Bytes`
    * :class:`~cqlengine.columns.Ascii`
    * :class:`~cqlengine.columns.Text`
    * :class:`~cqlengine.columns.Integer`
    * :class:`~cqlengine.columns.DateTime`
    * :class:`~cqlengine.columns.UUID`
    * :class:`~cqlengine.columns.TimeUUID`
    * :class:`~cqlengine.columns.Boolean`
    * :class:`~cqlengine.columns.Float`
    * :class:`~cqlengine.columns.Decimal`
    * :class:`~cqlengine.columns.Set`
    * :class:`~cqlengine.columns.List`
    * :class:`~cqlengine.columns.Map`

Column Options
--------------

    Each column can be defined with optional arguments to modify the way they behave. While some column types may define additional column options, these are the options that are available on all columns:

    :attr:`~cqlengine.columns.BaseColumn.primary_key`
        If True, this column is created as a primary key field. A model can have multiple primary keys. Defaults to False.

        *In CQL, there are 2 types of primary keys: partition keys and clustering keys. As with CQL, the first primary key is the partition key, and all others are clustering keys, unless partition keys are specified manually using* :attr:`~cqlengine.columns.BaseColumn.partition_key`

    :attr:`~cqlengine.columns.BaseColumn.partition_key`
        If True, this column is created as partition primary key. There may be many partition keys defined, forming *composite partition key*

    :attr:`~cqlengine.columns.BaseColumn.index`
        If True, an index will be created for this column. Defaults to False.
        
        *Note: Indexes can only be created on models with one primary key*

    :attr:`~cqlengine.columns.BaseColumn.db_field`
        Explicitly sets the name of the column in the database table. If this is left blank, the column name will be the same as the name of the column attribute. Defaults to None.

    :attr:`~cqlengine.columns.BaseColumn.default`
        The default value for this column. If a model instance is saved without a value for this column having been defined, the default value will be used. This can be either a value or a callable object (ie: datetime.now is a valid default argument).

    :attr:`~cqlengine.columns.BaseColumn.required`
        If True, this model cannot be saved without a value defined for this column. Defaults to True. Primary key fields cannot have their required fields set to False.

Model Methods
=============
    Below are the methods that can be called on model instances.

.. class:: Model(\*\*values)

    Creates an instance of the model. Pass in keyword arguments for columns you've defined on the model.

    *Example*

    .. code-block:: python
        
        #using the person model from earlier:
        class Person(Model):
            first_name  = columns.Text()
            last_name = columns.Text()

        person = Person(first_name='Blake', last_name='Eggleston')
        person.first_name  #returns 'Blake'
        person.last_name  #returns 'Eggleston'
        

    .. method:: save()

        Saves an object to the database

        *Example*

        .. code-block:: python

            #create a person instance
            person = Person(first_name='Kimberly', last_name='Eggleston')
            #saves it to Cassandra
            person.save()


    .. method:: delete()
    
        Deletes the object from the database.

Model Attributes
================

    .. attribute:: Model.table_name

        *Optional.* Sets the name of the CQL table for this model. If left blank, the table name will be the name of the model, with it's module name as it's prefix. Manually defined table names are not inherited.

    .. attribute:: Model.keyspace

        *Optional.* Sets the name of the keyspace used by this model. Defaulst to cqlengine

Automatic Primary Keys
======================
    CQL requires that all tables define at least one primary key. If a model definition does not include a primary key column, cqlengine will automatically add a uuid primary key column named ``id``.
