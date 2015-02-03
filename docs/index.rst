Python Cassandra Driver
=======================
A Python client driver for `Apache Cassandra <http://cassandra.apache.org>`_.
This driver works exclusively with the Cassandra Query Language v3 (CQL3)
and Cassandra's native protocol.  Cassandra 1.2+ is supported.

The driver supports Python 2.6, 2.7, 3.3, and 3.4.

This driver is open source under the
`Apache v2 License <http://www.apache.org/licenses/LICENSE-2.0.html>`_.
The source code for this driver can be found on `GitHub <http://github.com/datastax/python-driver>`_.

Contents
--------
:doc:`installation`
    How to install the driver.

:doc:`getting_started`
    A guide through the first steps of connecting to Cassandra and executing queries.

:doc:`api/index`
    The API documentation.

:doc:`upgrading`
    A guide to upgrading versions of the driver.

:doc:`performance`
    Tips for getting good performance.

:doc:`query_paging`
    Notes on paging large query results.

:doc:`user_defined_types`
    Working with Cassandra 2.1's user-defined types.

:doc:`security`
    An overview of the security features of the driver.

Getting Help
------------
Please send questions to the `mailing list <https://groups.google.com/a/lists.datastax.com/forum/#!forum/python-driver-user>`_.

Alternatively, you can use IRC.  Connect to the #datastax-drivers channel on irc.freenode.net.
If you don't have an IRC client, you can use `freenode's web-based client <http://webchat.freenode.net/?channels=#datastax-drivers>`_.

Reporting Issues
----------------
Please report any bugs and make any feature requests on the
`JIRA <https://datastax-oss.atlassian.net/browse/PYTHON>`_ issue tracker.

If you would like to contribute, please feel free to open a pull request.


.. toctree::
   :hidden:

   api/index
   installation
   getting_started
   upgrading
   performance
   query_paging
   security
   user_defined_types

Indices and Tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

..
    cqlengine documentation
    =======================
    
    **Users of versions < 0.16, the default keyspace 'cqlengine' has been removed. Please read this before upgrading:** :ref:`Breaking Changes <keyspace-change>`
    
    cqlengine is a Cassandra CQL 3 Object Mapper for Python
    
    :ref:`getting-started`
    
    Download
    ========
    
    `Github <https://github.com/cqlengine/cqlengine>`_
    
    `PyPi <https://pypi.python.org/pypi/cqlengine>`_
    
    Contents:
    =========
    
    .. toctree::
        :maxdepth: 2
    
        topics/models
        topics/queryset
        topics/columns
        topics/connection
        topics/manage_schemas
        topics/external_resources
        topics/related_projects
        topics/third_party
        topics/development
        topics/faq
    
    .. _getting-started:
    
    Getting Started
    ===============
    
        .. code-block:: python
    
            #first, define a model
            from cqlengine import columns
            from cqlengine import Model
    
            class ExampleModel(Model):
                example_id      = columns.UUID(primary_key=True, default=uuid.uuid4)
                example_type    = columns.Integer(index=True)
                created_at      = columns.DateTime()
                description     = columns.Text(required=False)
    
            #next, setup the connection to your cassandra server(s)...
            >>> from cqlengine import connection
    
            # see http://datastax.github.io/python-driver/api/cassandra/cluster.html for options
            # the list of hosts will be passed to create a Cluster() instance
            >>> connection.setup(['127.0.0.1'], "cqlengine")
    
            # if you're connecting to a 1.2 cluster
            >>> connection.setup(['127.0.0.1'], "cqlengine", protocol_version=1)
    
            #...and create your CQL table
            >>> from cqlengine.management import sync_table
            >>> sync_table(ExampleModel)
    
            #now we can create some rows:
            >>> em1 = ExampleModel.create(example_type=0, description="example1", created_at=datetime.now())
            >>> em2 = ExampleModel.create(example_type=0, description="example2", created_at=datetime.now())
            >>> em3 = ExampleModel.create(example_type=0, description="example3", created_at=datetime.now())
            >>> em4 = ExampleModel.create(example_type=0, description="example4", created_at=datetime.now())
            >>> em5 = ExampleModel.create(example_type=1, description="example5", created_at=datetime.now())
            >>> em6 = ExampleModel.create(example_type=1, description="example6", created_at=datetime.now())
            >>> em7 = ExampleModel.create(example_type=1, description="example7", created_at=datetime.now())
            >>> em8 = ExampleModel.create(example_type=1, description="example8", created_at=datetime.now())
    
            #and now we can run some queries against our table
            >>> ExampleModel.objects.count()
            8
            >>> q = ExampleModel.objects(example_type=1)
            >>> q.count()
            4
            >>> for instance in q:
            >>>     print instance.description
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
            >>>     print instance.description
            example5
    
    
    `Report a Bug <https://github.com/cqlengine/cqlengine/issues>`_
    
    `Users Mailing List <https://groups.google.com/forum/?fromgroups#!forum/cqlengine-users>`_
    
    
    Indices and tables
    >>>>>>> cqlengine/master
    ==================
    
    * :ref:`genindex`
    * :ref:`modindex`
    * :ref:`search`
