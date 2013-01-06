
.. _index:

=======================
cqlengine documentation
=======================

cqlengine is a Cassandra CQL 3 ORM for Python with an interface similar to the Django orm and mongoengine

:ref:`getting-started`

Contents:

.. toctree::
    :maxdepth: 2
    
    topics/models
    topics/queryset
    topics/columns
    topics/connection
    topics/manage_schemas

.. _getting-started:

Getting Started
===============

    .. code-block:: python

        #first, define a model
        from cqlengine import columns
        from cqlengine import Model

        class ExampleModel(Model):
            example_id      = columns.UUID(primary_key=True)  
            example_type    = columns.Integer(index=True)
            created_at      = columns.DateTime()
            description     = columns.Text(required=False)

        #next, setup the connection to your cassandra server(s)...
        >>> from cqlengine import connection
        >>> connection.setup(['127.0.0.1:9160'])

        #...and create your CQL table
        >>> from cqlengine.management import create_table
        >>> create_table(ExampleModel)

        #now we can create some rows:
        >>> em1 = ExampleModel.create(example_type=0, description="example1")
        >>> em2 = ExampleModel.create(example_type=0, description="example2")
        >>> em3 = ExampleModel.create(example_type=0, description="example3")
        >>> em4 = ExampleModel.create(example_type=0, description="example4")
        >>> em5 = ExampleModel.create(example_type=1, description="example5")
        >>> em6 = ExampleModel.create(example_type=1, description="example6")
        >>> em7 = ExampleModel.create(example_type=1, description="example7")
        >>> em8 = ExampleModel.create(example_type=1, description="example8")
        # Note: the UUID and DateTime columns will create uuid4 and datetime.now
        # values automatically if we don't specify them when creating new rows

        #and now we can run some queries against our table
        >>> ExampleModel.objects.count()
        8
        >>> q = ExampleModel.objects(example_type=1)
        >>> q.count()
        4
        >>> for instance in q:
        >>>     print q.description
        example5
        example6
        example7
        example8

        #here we are applying additional filtering to an existing query
        #query objects are immutable, so calling filter returns a new
        #query object
        >>> q2 = q.filter(example_id=em5.example_id)

        >>> q2.count()
        1
        >>> for instance in q2:
        >>>     print q.description
        example5


`Report a Bug <https://github.com/bdeggleston/cqlengine/issues>`_

`Users Mailing List <https://groups.google.com/forum/?fromgroups#!forum/cqlengine-users>`_

`Dev Mailing List <https://groups.google.com/forum/?fromgroups#!forum/cqlengine-dev>`_

**NOTE: cqlengine is in alpha and under development, some features may change. Make sure to check the changelog and test your app before upgrading**


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

