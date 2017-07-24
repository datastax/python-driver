``cassandra.cqlengine.models`` - Table models for object mapping
================================================================

.. module:: cassandra.cqlengine.models

Model
-----
.. autoclass:: Model(\*\*kwargs)

    The initializer creates an instance of the model. Pass in keyword arguments for columns you've defined on the model.

    .. code-block:: python

        class Person(Model):
            id = columns.UUID(primary_key=True)
            first_name  = columns.Text()
            last_name = columns.Text()

        person = Person(first_name='Blake', last_name='Eggleston')
        person.first_name  #returns 'Blake'
        person.last_name  #returns 'Eggleston'

    Model attributes define how the model maps to tables in the database. These are class variables that should be set
    when defining Model deriviatives.

    .. autoattribute:: __abstract__
        :annotation:  = False

    .. autoattribute:: __table_name__

    .. autoattribute:: __table_name_case_sensitive__

    .. autoattribute:: __keyspace__

    .. autoattribute:: __connection__

    .. attribute:: __default_ttl__
        :annotation:  = None

        Will be deprecated in release 4.0. You can set the default ttl by configuring the table ``__options__``. See :ref:`ttl-change` for more details.

    .. autoattribute:: __discriminator_value__

        See :ref:`model_inheritance` for usage examples.

    Each table can have its own set of configuration options, including compaction. Unspecified, these default to sensible values in
    the server. To override defaults, set options using the model ``__options__`` attribute, which allows options specified a dict.

    When a table is synced, it will be altered to match the options set on your table.
    This means that if you are changing settings manually they will be changed back on resync.

    Do not use the options settings of cqlengine if you want to manage your compaction settings manually.

    See the `list of supported table properties for more information
    <http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/tabProp.html>`_.

    .. attribute:: __options__

        For example:

        .. code-block:: python

            class User(Model):
                __options__ = {'compaction': {'class': 'LeveledCompactionStrategy',
                                              'sstable_size_in_mb': '64',
                                              'tombstone_threshold': '.2'},
                               'read_repair_chance': '0.5',
                               'comment': 'User data stored here'}

                user_id = columns.UUID(primary_key=True)
                name = columns.Text()

        or :

        .. code-block:: python

            class TimeData(Model):
                __options__ = {'compaction': {'class': 'SizeTieredCompactionStrategy',
                                              'bucket_low': '.3',
                                              'bucket_high': '2',
                                              'min_threshold': '2',
                                              'max_threshold': '64',
                                              'tombstone_compaction_interval': '86400'},
                               'gc_grace_seconds': '0'}

    .. autoattribute:: __compute_routing_key__


    The base methods allow creating, storing, and querying modeled objects.

    .. automethod:: create

    .. method:: if_not_exists()

        Check the existence of an object before insertion. The existence of an
        object is determined by its primary key(s). And please note using this flag
        would incur performance cost.

        If the insertion isn't applied, a :class:`~cassandra.cqlengine.query.LWTException` is raised.

        .. code-block:: python

            try:
                TestIfNotExistsModel.if_not_exists().create(id=id, count=9, text='111111111111')
            except LWTException as e:
                # handle failure case
                print e.existing  # dict containing LWT result fields

        This method is supported on Cassandra 2.0 or later.

    .. method:: if_exists()

        Check the existence of an object before an update or delete. The existence of an
        object is determined by its primary key(s). And please note using this flag
        would incur performance cost.

        If the update or delete isn't applied, a :class:`~cassandra.cqlengine.query.LWTException` is raised.

        .. code-block:: python

            try:
                TestIfExistsModel.objects(id=id).if_exists().update(count=9, text='111111111111')
            except LWTException as e:
                # handle failure case
                pass

        This method is supported on Cassandra 2.0 or later.

    .. automethod:: save

    .. automethod:: update

    .. method:: iff(**values)

        Checks to ensure that the values specified are correct on the Cassandra cluster.
        Simply specify the column(s) and the expected value(s).  As with if_not_exists,
        this incurs a performance cost.

        If the insertion isn't applied, a :class:`~cassandra.cqlengine.query.LWTException` is raised.

        .. code-block:: python

            t = TestTransactionModel(text='some text', count=5)
            try:
                 t.iff(count=5).update('other text')
            except LWTException as e:
                # handle failure case
                print e.existing # existing object

    .. automethod:: get

    .. automethod:: filter

    .. automethod:: all

    .. automethod:: delete

    .. method:: batch(batch_object)

        Sets the batch object to run instance updates and inserts queries with.

        See :doc:`/cqlengine/batches` for usage examples

    .. automethod:: timeout

    .. method:: timestamp(timedelta_or_datetime)

        Sets the timestamp for the query

    .. method:: ttl(ttl_in_sec)

       Sets the ttl values to run instance updates and inserts queries with.

    .. method:: using(connection=None)

        Change the context on the fly of the model instance (keyspace, connection)

    .. automethod:: column_family_name

    Models also support dict-like access:

    .. method:: len(m)

        Returns the number of columns defined in the model

    .. method:: m[col_name]

        Returns the value of column ``col_name``

    .. method:: m[col_name] = value

        Set ``m[col_name]`` to value

    .. automethod:: keys

    .. automethod:: values

    .. automethod:: items
