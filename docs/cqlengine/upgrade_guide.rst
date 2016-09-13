========================
Upgrade Guide
========================

This is an overview of things that changed as the cqlengine project was merged into
cassandra-driver. While efforts were taken to preserve the API and most functionality exactly,
conversion to this package will still require certain minimal updates (namely, imports).

**THERE IS ONE FUNCTIONAL CHANGE**, described in the first section below.

Functional Changes
==================
List Prepend Reversing
----------------------
Legacy cqlengine included a workaround for a Cassandra bug in which prepended list segments were
reversed (`CASSANDRA-8733 <https://issues.apache.org/jira/browse/CASSANDRA-8733>`_). As of
this integration, this workaround is removed. The first released integrated version emits
a warning when prepend is used. Subsequent versions will have this warning removed.

Date Column Type
----------------
The Date column type in legacy cqlengine used a ``timestamp`` CQL type and truncated the time.
Going forward, the :class:`~.columns.Date` type represents a ``date`` for Cassandra 2.2+
(`PYTHON-245 <https://datastax-oss.atlassian.net/browse/PYTHON-245>`_).
Users of the legacy functionality should convert models to use :class:`~.columns.DateTime` (which
uses ``timestamp`` internally), and use the build-in ``datetime.date`` for input values.

Remove cqlengine
================
To avoid confusion or mistakes using the legacy package in your application, it
is prudent to remove the cqlengine package when upgrading to the integrated version.

The driver setup script will warn if the legacy package is detected during install,
but it will not prevent side-by-side installation.

Organization
============
Imports
-------
cqlengine is now integrated as a sub-package of the driver base package 'cassandra'.
Upgrading will require adjusting imports to cqlengine. For example::

    from cassandra.cqlengine import columns

is now::

    from cassandra.cqlengine import columns

Package-Level Aliases
---------------------
Legacy cqlengine defined a number of aliases at the package level, which became redundant
when the package was integrated for a driver. These have been removed in favor of absolute
imports, and referring to cannonical definitions. For example, ``cqlengine.ONE`` was an alias
of ``cassandra.ConsistencyLevel.ONE``. In the integrated package, only the
:class:`cassandra.ConsistencyLevel` remains.

Additionally, submodule aliases are removed from cqlengine in favor of absolute imports.

These aliases are removed, and not deprecated because they should be straightforward to iron out
at module load time.

Exceptions
----------
The legacy cqlengine.exceptions module had a number of Exception classes that were variously
common to the package, or only used in specific modules. Common exceptions were relocated to
cqlengine, and specialized exceptions were placed in the module that raises them. Below is a
listing of updated locations:

============================  ==========
Exception class               New module
============================  ==========
CQLEngineException            cassandra.cqlengine
ModelException                cassandra.cqlengine.models
ValidationError               cassandra.cqlengine
UndefinedKeyspaceException    cassandra.cqlengine.connection
LWTException                  cassandra.cqlengine.query
IfNotExistsWithCounterColumn  cassandra.cqlengine.query
============================  ==========

UnicodeMixin Consolidation
--------------------------
``class UnicodeMixin`` was defined in several cqlengine modules. This has been consolidated
to a single definition in the cqlengine package init file. This is not technically part of
the API, but noted here for completeness.

API Deprecations
================
This upgrade served as a good juncture to deprecate certain API features and invite users to upgrade
to new ones. The first released version does not change functionality -- only introduces deprecation
warnings. Future releases will remove these features in favor of the alternatives.

Float/Double Overload
---------------------
Previously there was no ``Double`` column type. Doubles were modeled by specifying ``Float(double_precision=True)``.
This inititializer parameter is now deprecated. Applications should use :class:`~.columns.Double` for CQL ``double``, and :class:`~.columns.Float`
for CQL ``float``.

Schema Management
-----------------
``cassandra.cqlengine.management.create_keyspace`` is deprecated. Instead, use the new replication-strategy-specific
functions that accept explicit options for known strategies:

- :func:`~.create_keyspace_simple`
- :func:`~.create_keyspace_network_topology`

``cassandra.cqlengine.management.delete_keyspace`` is deprecated in favor of a new function, :func:`~.drop_keyspace`. The
intent is simply to make the function match the CQL verb it invokes.

Model Inheritance
-----------------
The names for class attributes controlling model inheritance are changing. Changes are as follows:

- Replace 'polymorphic_key' in the base class Column definition with :attr:`~.discriminator_column`
- Replace the '__polymorphic_key__' class attribute the derived classes with :attr:`~.__discriminator_value__`

The functionality is unchanged -- the intent here is to make the names and language around these attributes more precise.
For now, the old names are just deprecated, and the mapper will emit warnings if they are used. The old names
will be removed in a future version.

The example below shows a simple translation:

Before::

    class Pet(Model):
        __table_name__ = 'pet'
        owner_id = UUID(primary_key=True)
        pet_id = UUID(primary_key=True)
        pet_type = Text(polymorphic_key=True)
        name = Text()

    class Cat(Pet):
        __polymorphic_key__ = 'cat'

    class Dog(Pet):
        __polymorphic_key__ = 'dog'

After::

    class Pet(models.Model):
        __table_name__ = 'pet'
        owner_id = UUID(primary_key=True)
        pet_id = UUID(primary_key=True)
        pet_type = Text(discriminator_column=True)
        name = Text()

    class Cat(Pet):
        __discriminator_value__ = 'cat'

    class Dog(Pet):
        __discriminator_value__ = 'dog'


TimeUUID.from_datetime
----------------------
This function is deprecated in favor of the core utility function :func:`~.uuid_from_time`.
