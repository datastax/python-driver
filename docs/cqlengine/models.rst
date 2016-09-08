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
Please see the discussion in :doc:`/api/cassandra/cqlengine/management` for considerations.

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

*Note*: while not required, the convention is to raise a ``ValidationError`` (``from cassandra.cqlengine import ValidationError``)
if validation fails.

.. _model_inheritance:

Model Inheritance
=================
It is possible to save and load different model classes using a single CQL table.
This is useful in situations where you have different object types that you want to store in a single cassandra row.

For instance, suppose you want a table that stores rows of pets owned by an owner:

.. code-block:: python

    class Pet(Model):
        __table_name__ = 'pet'
        owner_id = UUID(primary_key=True)
        pet_id = UUID(primary_key=True)
        pet_type = Text(discriminator_column=True)
        name = Text()

        def eat(self, food):
            pass

        def sleep(self, time):
            pass

    class Cat(Pet):
        __discriminator_value__ = 'cat'
        cuteness = Float()

        def tear_up_couch(self):
            pass

    class Dog(Pet):
        __discriminator_value__ = 'dog'
        fierceness = Float()

        def bark_all_night(self):
            pass

After calling ``sync_table`` on each of these tables, the columns defined in each model will be added to the
``pet`` table. Additionally, saving ``Cat`` and ``Dog`` models will save the meta data needed to identify each row
as either a cat or dog.

To setup a model structure with inheritance, follow these steps

1.  Create a base model with a column set as the distriminator (``distriminator_column=True`` in the column definition)
2.  Create subclass models, and define a unique ``__discriminator_value__`` value on each
3.  Run ``sync_table`` on each of the sub tables

**About the discriminator value**

The discriminator value is what cqlengine uses under the covers to map logical cql rows to the appropriate model type. The
base model maintains a map of discriminator values to subclasses. When a specialized model is saved, its discriminator value is
automatically saved into the discriminator column. The discriminator column may be any column type except counter and container types.
Additionally, if you set ``index=True`` on your discriminator column, you can execute queries against specialized subclasses, and a
``WHERE`` clause will be automatically added to your query, returning only rows of that type. Note that you must
define a unique ``__discriminator_value__`` to each subclass, and that you can only assign a single discriminator column per model.

.. _user_types:

User Defined Types
==================
cqlengine models User Defined Types (UDTs) much like tables, with fields defined by column type attributes. However, UDT instances
are only created, presisted, and queried via table Models. A short example to introduce the pattern::

    from cassandra.cqlengine.columns import *
    from cassandra.cqlengine.models import Model
    from cassandra.cqlengine.usertype import UserType

    class address(UserType):
        street = Text()
        zipcode = Integer()

    class users(Model):
        __keyspace__ = 'account'
        name = Text(primary_key=True)
        addr = UserDefinedType(address)

    users.create(name="Joe", addr=address(street="Easy St.", zipcode=99999))
    user = users.objects(name="Joe")[0]
    print user.name, user.addr
    # Joe address(street=u'Easy St.', zipcode=99999)

UDTs are modeled by inheriting :class:`~.usertype.UserType`, and setting column type attributes. Types are then used in defining
models by declaring a column of type :class:`~.columns.UserDefinedType`, with the ``UserType`` class as a parameter.

``sync_table`` will implicitly
synchronize any types contained in the table. Alternatively :func:`~.management.sync_type` can be used to create/alter types
explicitly.

Upon declaration, types are automatically registered with the driver, so query results return instances of your ``UserType``
class*.

***Note**: UDTs were not added to the native protocol until v3. When setting up the cqlengine connection, be sure to specify
``protocol_version=3``. If using an earlier version, UDT queries will still work, but the returned type will be a namedtuple.
