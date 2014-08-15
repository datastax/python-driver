======
Models
======

**Users of versions < 0.4, please read this post before upgrading:** `Breaking Changes`_

.. _Breaking Changes: https://groups.google.com/forum/?fromgroups#!topic/cqlengine-users/erkSNe1JwuU

.. module:: cqlengine.connection

.. module:: cqlengine.models

A model is a python class representing a CQL table.

Examples
========

This example defines a Person table, with the columns ``first_name`` and ``last_name``

.. code-block:: python

    from cqlengine import columns
    from cqlengine.models import Model

    class Person(Model):
        id = columns.UUID(primary_key=True)
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

Here's an example of a comment table created with clustering keys, in descending order:

.. code-block:: python

    from cqlengine import columns
    from cqlengine.models import Model

    class Comment(Model):
        photo_id = columns.UUID(primary_key=True)
        comment_id = columns.TimeUUID(primary_key=True, clustering_order="DESC")
        comment = columns.Text()

The Comment model's ``create table`` would look like the following:

.. code-block:: sql

    CREATE TABLE comment (
      photo_id uuid,
      comment_id timeuuid,
      comment text,
      PRIMARY KEY (photo_id, comment_id)
    ) WITH CLUSTERING ORDER BY (comment_id DESC)

To sync the models to the database, you may do the following:

.. code-block:: python

    from cqlengine.management import sync_table
    sync_table(Person)
    sync_table(Comment)


Columns
=======

    Columns in your models map to columns in your CQL table. You define CQL columns by defining column attributes on your model classes. For a model to be valid it needs at least one primary key column and one non-primary key column.

    Just as in CQL, the order you define your columns in is important, and is the same order they are defined in on a model's corresponding table.

Column Types
============

    Each column on your model definitions needs to an instance of a Column class. The column types that are included with cqlengine as of this writing are:

    * :class:`~cqlengine.columns.Bytes`
    * :class:`~cqlengine.columns.Ascii`
    * :class:`~cqlengine.columns.Text`
    * :class:`~cqlengine.columns.Integer`
    * :class:`~cqlengine.columns.BigInt`
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

    Each column can be defined with optional arguments to modify the way they behave. While some column types may
    define additional column options, these are the options that are available on all columns:

    :attr:`~cqlengine.columns.BaseColumn.primary_key`
        If True, this column is created as a primary key field. A model can have multiple primary keys. Defaults to False.

        *In CQL, there are 2 types of primary keys: partition keys and clustering keys. As with CQL, the first
        primary key is the partition key, and all others are clustering keys, unless partition keys are specified
        manually using* :attr:`~cqlengine.columns.BaseColumn.partition_key`

    :attr:`~cqlengine.columns.BaseColumn.partition_key`
        If True, this column is created as partition primary key. There may be many partition keys defined,
        forming a *composite partition key*

    :attr:`~cqlengine.columns.BaseColumn.clustering_order`
        ``ASC`` or ``DESC``, determines the clustering order of a clustering key.

    :attr:`~cqlengine.columns.BaseColumn.index`
        If True, an index will be created for this column. Defaults to False.

    :attr:`~cqlengine.columns.BaseColumn.db_field`
        Explicitly sets the name of the column in the database table. If this is left blank, the column name will be
        the same as the name of the column attribute. Defaults to None.

    :attr:`~cqlengine.columns.BaseColumn.default`
        The default value for this column. If a model instance is saved without a value for this column having been
        defined, the default value will be used. This can be either a value or a callable object (ie: datetime.now is a valid default argument).
        Callable defaults will be called each time a default is assigned to a None value

    :attr:`~cqlengine.columns.BaseColumn.required`
        If True, this model cannot be saved without a value defined for this column. Defaults to False. Primary key fields always require values.

    :attr:`~cqlengine.columns.BaseColumn.static`
        Defined a column as static.  Static columns are shared by all rows in a partition.

Model Methods
=============
    Below are the methods that can be called on model instances.

.. class:: Model(\*\*values)

    Creates an instance of the model. Pass in keyword arguments for columns you've defined on the model.

    *Example*

    .. code-block:: python

        #using the person model from earlier:
        class Person(Model):
            id = columns.UUID(primary_key=True)
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

    .. method:: batch(batch_object)

        Sets the batch object to run instance updates and inserts queries with.

    .. method:: timestamp(timedelta_or_datetime)

        Sets the timestamp for the query

    .. method:: ttl(ttl_in_sec)

        Sets the ttl values to run instance updates and inserts queries with.

    .. method:: update(**values)

        Performs an update on the model instance. You can pass in values to set on the model
        for updating, or you can call without values to execute an update against any modified
        fields. If no fields on the model have been modified since loading, no query will be
        performed. Model validation is performed normally.

        It is possible to do a blind update, that is, to update a field without having first selected the object out of the database.  See :ref:`Blind Updates <blind_updates>`

    .. method:: get_changed_columns()

        Returns a list of column names that have changed since the model was instantiated or saved

Model Attributes
================

    .. attribute:: Model.__abstract__

        *Optional.* Indicates that this model is only intended to be used as a base class for other models. You can't create tables for abstract models, but checks around schema validity are skipped during class construction.

    .. attribute:: Model.__table_name__

        *Optional.* Sets the name of the CQL table for this model. If left blank, the table name will be the name of the model, with it's module name as it's prefix. Manually defined table names are not inherited.

    .. _keyspace-change:
    .. attribute:: Model.__keyspace__

        Sets the name of the keyspace used by this model.

        **Prior to cqlengine 0.16, this setting defaulted
        to 'cqlengine'. As of 0.16, this field needs to be set on all non-abstract models, or their base classes.**

    .. _ttl-change:
    .. attribute:: Model.__default_ttl__

        Sets the default ttl used by this model.  This can be overridden by using the ``ttl(ttl_in_sec)`` method.


Table Polymorphism
==================

    As of cqlengine 0.8, it is possible to save and load different model classes using a single CQL table.
    This is useful in situations where you have different object types that you want to store in a single cassandra row.

    For instance, suppose you want a table that stores rows of pets owned by an owner:

    .. code-block:: python

        class Pet(Model):
            __table_name__ = 'pet'
            owner_id = UUID(primary_key=True)
            pet_id = UUID(primary_key=True)
            pet_type = Text(polymorphic_key=True)
            name = Text()

            def eat(self, food):
                pass

            def sleep(self, time):
                pass

        class Cat(Pet):
            __polymorphic_key__ = 'cat'
            cuteness = Float()

            def tear_up_couch(self):
                pass

        class Dog(Pet):
            __polymorphic_key__ = 'dog'
            fierceness = Float()

            def bark_all_night(self):
                pass

    After calling ``sync_table`` on each of these tables, the columns defined in each model will be added to the
    ``pet`` table. Additionally, saving ``Cat`` and ``Dog`` models will save the meta data needed to identify each row
    as either a cat or dog.

    To setup a polymorphic model structure, follow these steps

    1.  Create a base model with a column set as the polymorphic_key (set ``polymorphic_key=True`` in the column definition)
    2.  Create subclass models, and define a unique ``__polymorphic_key__`` value on each
    3.  Run ``sync_table`` on each of the sub tables

    **About the polymorphic key**

    The polymorphic key is what cqlengine uses under the covers to map logical cql rows to the appropriate model type. The
    base model maintains a map of polymorphic keys to subclasses. When a polymorphic model is saved, this value is automatically
    saved into the polymorphic key column. You can set the polymorphic key column to any column type that you like, with
    the exception of container and counter columns, although ``Integer`` columns make the most sense. Additionally, if you
    set ``index=True`` on your polymorphic key column, you can execute queries against polymorphic subclasses, and a
    ``WHERE`` clause will be automatically added to your query, returning only rows of that type. Note that you must
    define a unique ``__polymorphic_key__`` value to each subclass, and that you can only assign a single polymorphic
    key column per model


Extending Model Validation
==========================

    Each time you save a model instance in cqlengine, the data in the model is validated against the schema you've defined
    for your model. Most of the validation is fairly straightforward, it basically checks that you're not trying to do
    something like save text into an integer column, and it enforces the ``required`` flag set on column definitions.
    It also performs any transformations needed to save the data properly.

    However, there are often additional constraints or transformations you want to impose on your data, beyond simply
    making sure that Cassandra won't complain when you try to insert it. To define additional validation on a model,
    extend the model's validation method:

    .. code-block:: python

        class Member(Model):
            person_id = UUID(primary_key=True)
            name = Text(required=True)

            def validate(self):
                super(Member, self).validate()
                if self.name == 'jon':
                    raise ValidationError('no jon\'s allowed')

    *Note*: while not required, the convention is to raise a ``ValidationError`` (``from cqlengine import ValidationError``)
    if validation fails


Table Properties
================

    Each table can have its own set of configuration options.
    These can be specified on a model with the following attributes:

    .. attribute:: Model.__bloom_filter_fp_chance

    .. attribute:: Model.__caching__

    .. attribute:: Model.__comment__

    .. attribute:: Model.__dclocal_read_repair_chance__

    .. attribute:: Model.__default_time_to_live__

    .. attribute:: Model.__gc_grace_seconds__

    .. attribute:: Model.__index_interval__

    .. attribute:: Model.__memtable_flush_period_in_ms__

    .. attribute:: Model.__populate_io_cache_on_flush__

    .. attribute:: Model.__read_repair_chance__

    .. attribute:: Model.__replicate_on_write__

    Example:

    .. code-block:: python

        from cqlengine import CACHING_ROWS_ONLY, columns
        from cqlengine.models import Model

        class User(Model):
            __caching__ = CACHING_ROWS_ONLY  # cache only rows instead of keys only by default
            __gc_grace_seconds__ = 86400  # 1 day instead of the default 10 days

            user_id = columns.UUID(primary_key=True)
            name = columns.Text()

    Will produce the following CQL statement:

    .. code-block:: sql

        CREATE TABLE cqlengine.user (
            user_id uuid,
            name text,
            PRIMARY KEY (user_id)
        ) WITH caching = 'rows_only'
           AND gc_grace_seconds = 86400;

    See the `list of supported table properties for more information
    <http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/tabProp.html>`_.


Compaction Options
==================

    As of cqlengine 0.7 we've added support for specifying compaction options.  cqlengine will only use your compaction options if you have a strategy set.  When a table is synced, it will be altered to match the compaction options set on your table.  This means that if you are changing settings manually they will be changed back on resync.  Do not use the compaction settings of cqlengine if you want to manage your compaction settings manually.

    cqlengine supports all compaction options as of Cassandra 1.2.8.

    Available Options:

    .. attribute:: Model.__compaction_bucket_high__

    .. attribute:: Model.__compaction_bucket_low__

    .. attribute:: Model.__compaction_max_compaction_threshold__

    .. attribute:: Model.__compaction_min_compaction_threshold__

    .. attribute:: Model.__compaction_min_sstable_size__

    .. attribute:: Model.__compaction_sstable_size_in_mb__

    .. attribute:: Model.__compaction_tombstone_compaction_interval__

    .. attribute:: Model.__compaction_tombstone_threshold__

    For example:

    .. code-block:: python

        class User(Model):
            __compaction__ = cqlengine.LeveledCompactionStrategy
            __compaction_sstable_size_in_mb__ = 64
            __compaction_tombstone_threshold__ = .2

            user_id = columns.UUID(primary_key=True)
            name = columns.Text()

    or for SizeTieredCompaction:

    .. code-block:: python

        class TimeData(Model):
            __compaction__ = SizeTieredCompactionStrategy
            __compaction_bucket_low__ = .3
            __compaction_bucket_high__ = 2
            __compaction_min_threshold__ = 2
            __compaction_max_threshold__ = 64
            __compaction_tombstone_compaction_interval__ = 86400

    Tables may use `LeveledCompactionStrategy` or `SizeTieredCompactionStrategy`.  Both options are available in the top level cqlengine module.  To reiterate, you will need to set your `__compaction__` option explicitly in order for cqlengine to handle any of your settings.


Manipulating model instances as dictionaries
============================================

    As of cqlengine 0.12, we've added support for treating model instances like dictionaries. See below for examples.

    .. code-block:: python

        class Person(Model):
            first_name  = columns.Text()
            last_name = columns.Text()

        kevin = Person.create(first_name="Kevin", last_name="Deldycke")
        dict(kevin)  # returns {'first_name': 'Kevin', 'last_name': 'Deldycke'}
        kevin['first_name']  # returns 'Kevin'
        kevin.keys()  # returns ['first_name', 'last_name']
        kevin.values()  # returns ['Kevin', 'Deldycke']
        kevin.items()  # returns [('first_name', 'Kevin'), ('last_name', 'Deldycke')]

        kevin['first_name'] = 'KEVIN5000'  # changes the models first name

