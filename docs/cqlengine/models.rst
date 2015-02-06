======
Models
======

.. module:: cqlengine.models

A model is a python class representing a CQL table. Models derive from :class:`Model`, and
define basic table properties and columns for a table.

Columns in your models map to columns in your CQL table. You define CQL columns by defining column attributes on your model classes.
For a model to be valid it needs at least one primary key column and one non-primary key column. Just as in CQL, the order you define
your columns in is important, and is the same order they are defined in on a model's corresponding table.

Some basic examples defining models are shown below. Consult the :doc:`Model API docs </api/cassandra/cqlengine/models>` and :doc:`Column API docs </api/cassandra/cqlengine/columns>` for complete details.

Example Definitions
===================

This example defines a ``Person`` table, with the columns ``first_name`` and ``last_name``

.. code-block:: python

    from cassandra.cqlengine import columns
    from cassandra.cqlengine.models import Model

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
   );

Here's an example of a comment table created with clustering keys, in descending order:

.. code-block:: python

    from cassandra.cqlengine import columns
    from cassandra.cqlengine.models import Model

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
    ) WITH CLUSTERING ORDER BY (comment_id DESC);

To sync the models to the database, you may do the following*:

.. code-block:: python

    from cassandra.cqlengine.management import sync_table
    sync_table(Person)
    sync_table(Comment)

\*Note: synchronizing models causes schema changes, and should be done with caution.
Please see the discussion in :doc:`manage_schemas` for considerations.

For examples on manipulating data and creating queries, see :doc:`queryset`

Manipulating model instances as dictionaries
============================================

Model instances can be accessed like dictionaries.

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
if validation fails.

.. _table_polymorphism:

Table Polymorphism
==================
It is possible to save and load different model classes using a single CQL table.
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
