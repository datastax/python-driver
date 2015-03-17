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

    *Model attributes define how the model maps to tables in the database. These are class variables that should be set
    when defining Model deriviatives.*

    .. autoattribute:: __abstract__
        :annotation:  = False

    .. autoattribute:: __table_name__

    .. autoattribute:: __keyspace__

    .. _ttl-change:
    .. autoattribute:: __default_ttl__

    .. autoattribute:: __polymorphic_key__

    .. autoattribute:: __discriminator_value__

        See :ref:`model_inheritance` for usage examples.

    *Each table can have its own set of configuration options.
    These can be specified on a model with the following attributes:*

    See the `list of supported table properties for more information
    <http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/tabProp.html>`_.

    .. attribute:: __bloom_filter_fp_chance

    .. attribute:: __caching__

    .. attribute:: __comment__

    .. attribute:: __dclocal_read_repair_chance__

    .. attribute:: __default_time_to_live__

    .. attribute:: __gc_grace_seconds__

    .. attribute:: __index_interval__

    .. attribute:: __memtable_flush_period_in_ms__

    .. attribute:: __populate_io_cache_on_flush__

    .. attribute:: __read_repair_chance__

    .. attribute:: __replicate_on_write__


    *Model presently supports specifying compaction options via class attributes. 
    cqlengine will only use your compaction options if you have a strategy set.
    When a table is synced, it will be altered to match the compaction options set on your table.
    This means that if you are changing settings manually they will be changed back on resync.*

    *Do not use the compaction settings of cqlengine if you want to manage your compaction settings manually.*

    *cqlengine supports all compaction options as of Cassandra 1.2.8.*

    **These attibutes will likely be replaced by a single options string in a future version, and are therefore deprecated.**

    .. attribute:: __compaction_bucket_high__
        :annotation: Deprecated

    .. attribute:: __compaction_bucket_low__
        :annotation: Deprecated

    .. attribute:: __compaction_max_compaction_threshold__
        :annotation: Deprecated

    .. attribute:: __compaction_min_compaction_threshold__
        :annotation: Deprecated

    .. attribute:: __compaction_min_sstable_size__
        :annotation: Deprecated

    .. attribute:: __compaction_sstable_size_in_mb__
        :annotation: Deprecated

    .. attribute:: __compaction_tombstone_compaction_interval__
        :annotation: Deprecated

    .. attribute:: __compaction_tombstone_threshold__
        :annotation: Deprecated

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

    *The base methods allow creating, storing, and querying modeled objects.*

    .. automethod:: create

    .. method:: if_not_exists()

        Check the existence of an object before insertion. The existence of an
        object is determined by its primary key(s). And please note using this flag
        would incur performance cost.

        if the insertion didn't applied, a LWTException exception would be raised.

        .. code-block:: python

            try:
                TestIfNotExistsModel.if_not_exists().create(id=id, count=9, text='111111111111')
            except LWTException as e:
                # handle failure case
                print e.existing # existing object

        This method is supported on Cassandra 2.0 or later.

    .. automethod:: save

    .. automethod:: update

    .. method:: iff(**values)

        Checks to ensure that the values specified are correct on the Cassandra cluster.
        Simply specify the column(s) and the expected value(s).  As with if_not_exists,
        this incurs a performance cost.

        If the insertion isn't applied, a LWTException is raised

        .. code-block:: python

            t = TestTransactionModel(text='some text', count=5)
            try:
                 t.iff(count=5).update('other text')
            except LWTException as e:
                # handle failure

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
