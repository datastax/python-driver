================
Managing Schemas
================

**Users of versions < 0.4, please read this post before upgrading:** `Breaking Changes`_

.. _Breaking Changes: https://groups.google.com/forum/?fromgroups#!topic/cqlengine-users/erkSNe1JwuU

.. module:: cqlengine.connection

.. module:: cqlengine.management

Once a connection has been made to Cassandra, you can use the functions in ``cqlengine.management`` to create and delete keyspaces, as well as create and delete tables for defined models

.. function:: create_keyspace(name)

    :param name: the keyspace name to create
    :type name: string

    creates a keyspace with the given name

.. function:: delete_keyspace(name)

    :param name: the keyspace name to delete
    :type name: string

    deletes the keyspace with the given name

.. function:: create_table(model [, create_missing_keyspace=True])
    
    :param model: the :class:`~cqlengine.model.Model` class to make a table with
    :type model: :class:`~cqlengine.model.Model`
    :param create_missing_keyspace: *Optional* If True, the model's keyspace will be created if it does not already exist. Defaults to ``True``
    :type create_missing_keyspace: bool

    creates a CQL table for the given model

.. function:: delete_table(model)

    :param model: the :class:`~cqlengine.model.Model` class to delete a column family for
    :type model: :class:`~cqlengine.model.Model`

    deletes the CQL table for the given model

    

See the example at :ref:`getting-started`


