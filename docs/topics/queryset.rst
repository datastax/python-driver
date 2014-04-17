==============
Making Queries
==============

**Users of versions < 0.4, please read this post before upgrading:** `Breaking Changes`_

.. _Breaking Changes: https://groups.google.com/forum/?fromgroups#!topic/cqlengine-users/erkSNe1JwuU

.. module:: cqlengine.connection

.. module:: cqlengine.query

Retrieving objects
==================
    Once you've populated Cassandra with data, you'll probably want to retrieve some of it. This is accomplished with QuerySet objects. This section will describe how to use QuerySet objects to retrieve the data you're looking for.

Retrieving all objects
----------------------
    The simplest query you can make is to return all objects from a table.

    This is accomplished with the ``.all()`` method, which returns a QuerySet of all objects in a table

    Using the Person example model, we would get all Person objects like this:

    .. code-block:: python

        all_objects = Person.objects.all()

.. _retrieving-objects-with-filters:

Retrieving objects with filters
-------------------------------
    Typically, you'll want to query only a subset of the records in your database.

    That can be accomplished with the QuerySet's ``.filter(\*\*)`` method.

    For example, given the model definition:

    .. code-block:: python

        class Automobile(Model):
            manufacturer = columns.Text(primary_key=True)
            year = columns.Integer(primary_key=True)
            model = columns.Text()
            price = columns.Decimal()

    ...and assuming the Automobile table contains a record of every car model manufactured in the last 20 years or so, we can retrieve only the cars made by a single manufacturer like this:


    .. code-block:: python

        q = Automobile.objects.filter(manufacturer='Tesla')

    You can also use the more convenient syntax:

    .. code-block:: python

        q = Automobile.objects(Automobile.manufacturer == 'Tesla')

    We can then further filter our query with another call to **.filter**

    .. code-block:: python

        q = q.filter(year=2012)

    *Note: all queries involving any filtering MUST define either an '=' or an 'in' relation to either a primary key column, or an indexed column.*

Accessing objects in a QuerySet
===============================

    There are several methods for getting objects out of a queryset

    * iterating over the queryset
        .. code-block:: python

            for car in Automobile.objects.all():
                #...do something to the car instance
                pass

    * list index
        .. code-block:: python

            q = Automobile.objects.all()
            q[0] #returns the first result
            q[1] #returns the second result


    * list slicing
        .. code-block:: python

            q = Automobile.objects.all()
            q[1:] #returns all results except the first
            q[1:9] #returns a slice of the results

        *Note: CQL does not support specifying a start position in it's queries. Therefore, accessing elements using array indexing / slicing will load every result up to the index value requested*

    * calling :attr:`get() <query.QuerySet.get>` on the queryset
        .. code-block:: python

            q = Automobile.objects.filter(manufacturer='Tesla')
            q = q.filter(year=2012)
            car = q.get()

        this returns the object matching the queryset

    * calling :attr:`first() <query.QuerySet.first>` on the queryset
        .. code-block:: python

            q = Automobile.objects.filter(manufacturer='Tesla')
            q = q.filter(year=2012)
            car = q.first()

        this returns the first value in the queryset

.. _query-filtering-operators:

Filtering Operators
===================

    :attr:`Equal To <query.QueryOperator.EqualsOperator>`

        The default filtering operator.

        .. code-block:: python

            q = Automobile.objects.filter(manufacturer='Tesla')
            q = q.filter(year=2012)  #year == 2012

    In addition to simple equal to queries, cqlengine also supports querying with other operators by appending a ``__<op>`` to the field name on the filtering call

    :attr:`in (__in) <query.QueryOperator.InOperator>`

        .. code-block:: python

            q = Automobile.objects.filter(manufacturer='Tesla')
            q = q.filter(year__in=[2011, 2012])


    :attr:`> (__gt) <query.QueryOperator.GreaterThanOperator>`

        .. code-block:: python

            q = Automobile.objects.filter(manufacturer='Tesla')
            q = q.filter(year__gt=2010)  # year > 2010

            # or the nicer syntax

            q.filter(Automobile.year > 2010)

    :attr:`>= (__gte) <query.QueryOperator.GreaterThanOrEqualOperator>`

        .. code-block:: python

            q = Automobile.objects.filter(manufacturer='Tesla')
            q = q.filter(year__gte=2010)  # year >= 2010

            # or the nicer syntax

            q.filter(Automobile.year >= 2010)

    :attr:`< (__lt) <query.QueryOperator.LessThanOperator>`

        .. code-block:: python

            q = Automobile.objects.filter(manufacturer='Tesla')
            q = q.filter(year__lt=2012)  # year < 2012

            # or...

            q.filter(Automobile.year < 2012)

    :attr:`<= (__lte) <query.QueryOperator.LessThanOrEqualOperator>`

        .. code-block:: python

            q = Automobile.objects.filter(manufacturer='Tesla')
            q = q.filter(year__lte=2012)  # year <= 2012

            q.filter(Automobile.year <= 2012)


TimeUUID Functions
==================

    In addition to querying using regular values, there are two functions you can pass in when querying TimeUUID columns to help make filtering by them easier. Note that these functions don't actually return a value, but instruct the cql interpreter to use the functions in it's query.

    .. class:: MinTimeUUID(datetime)

        returns the minimum time uuid value possible for the given datetime

    .. class:: MaxTimeUUID(datetime)

        returns the maximum time uuid value possible for the given datetime

    *Example*

    .. code-block:: python

        class DataStream(Model):
            time    = cqlengine.TimeUUID(primary_key=True)
            data    = cqlengine.Bytes()

        min_time = datetime(1982, 1, 1)
        max_time = datetime(1982, 3, 9)

        DataStream.filter(time__gt=cqlengine.MinTimeUUID(min_time), time__lt=cqlengine.MaxTimeUUID(max_time))

Token Function
==============

    Token functon may be used only on special, virtual column pk__token, representing token of partition key (it also works for composite partition keys).
    Cassandra orders returned items by value of partition key token, so using cqlengine.Token we can easy paginate through all table rows.

    See http://cassandra.apache.org/doc/cql3/CQL.html#tokenFun

    *Example*

    .. code-block:: python

        class Items(Model):
            id      = cqlengine.Text(primary_key=True)
            data    = cqlengine.Bytes()

        query = Items.objects.all().limit(10)

        first_page = list(query);
        last = first_page[-1]
        next_page = list(query.filter(pk__token__gt=cqlengine.Token(last.pk)))

QuerySets are imutable
======================

    When calling any method that changes a queryset, the method does not actually change the queryset object it's called on, but returns a new queryset object with the attributes of the original queryset, plus the attributes added in the method call.

    *Example*

    .. code-block:: python

        #this produces 3 different querysets
        #q does not change after it's initial definition
        q = Automobiles.objects.filter(year=2012)
        tesla2012 = q.filter(manufacturer='Tesla')
        honda2012 = q.filter(manufacturer='Honda')

Ordering QuerySets
==================

    Since Cassandra is essentially a distributed hash table on steroids, the order you get records back in will not be particularly predictable.

    However, you can set a column to order on with the ``.order_by(column_name)`` method.

    *Example*

    .. code-block:: python

        #sort ascending
        q = Automobiles.objects.all().order_by('year')
        #sort descending
        q = Automobiles.objects.all().order_by('-year')

    *Note: Cassandra only supports ordering on a clustering key. In other words, to support ordering results, your model must have more than one primary key, and you must order on a primary key, excluding the first one.*

    *For instance, given our Automobile model, year is the only column we can order on.*

Values Lists
============

    There is a special QuerySet's method ``.values_list()`` - when called, QuerySet returns lists of values instead of model instances. It may significantly speedup things with lower memory footprint for large responses.
    Each tuple contains the value from the respective field passed into the ``values_list()`` call — so the first item is the first field, etc. For example:


Batch Queries
=============

    cqlengine now supports batch queries using the BatchQuery class. Batch queries can be started and stopped manually, or within a context manager. To add queries to the batch object, you just need to precede the create/save/delete call with a call to batch, and pass in the batch object.

Batch Query General Use Pattern
-------------------------------

    You can only create, update, and delete rows with a batch query, attempting to read rows out of the database with a batch query will fail.

    .. code-block:: python

        from cqlengine import BatchQuery

        #using a context manager
        with BatchQuery() as b:
            now = datetime.now()
            em1 = ExampleModel.batch(b).create(example_type=0, description="1", created_at=now)
            em2 = ExampleModel.batch(b).create(example_type=0, description="2", created_at=now)
            em3 = ExampleModel.batch(b).create(example_type=0, description="3", created_at=now)

        # -- or --

        #manually
        b = BatchQuery()
        now = datetime.now()
        em1 = ExampleModel.batch(b).create(example_type=0, description="1", created_at=now)
        em2 = ExampleModel.batch(b).create(example_type=0, description="2", created_at=now)
        em3 = ExampleModel.batch(b).create(example_type=0, description="3", created_at=now)
        b.execute()

        # updating in a batch

        b = BatchQuery()
        em1.description = "new description"
        em1.batch(b).save()
        em2.description = "another new description"
        em2.batch(b).save()
        b.execute()

        # deleting in a batch
        b = BatchQuery()
        ExampleModel.objects(id=some_id).batch(b).delete()
        ExampleModel.objects(id=some_id2).batch(b).delete()
        b.execute()


    Typically you will not want the block to execute if an exception occurs inside the `with` block.  However, in the case that this is desirable, it's achievable by using the following syntax:

    .. code-block:: python

        with BatchQuery(execute_on_exception=True) as b:
            LogEntry.batch(b).create(k=1, v=1)
            mystery_function() # exception thrown in here
            LogEntry.batch(b).create(k=1, v=2) # this code is never reached due to the exception, but anything leading up to here will execute in the batch.

    If an exception is thrown somewhere in the block, any statements that have been added to the batch will still be executed.  This is useful for some logging situations.

Batch Query Execution Callbacks
-------------------------------

    In order to allow secondary tasks to be chained to the end of batch, BatchQuery instances allow callbacks to be
    registered with the batch, to be executed immediately after the batch executes.

    Multiple callbacks can be attached to same BatchQuery instance, they are executed in the same order that they
    are added to the batch.

    The callbacks attached to a given batch instance are executed only if the batch executes. If the batch is used as a
    context manager and an exception is raised, the queued up callbacks will not be run.

    .. code-block:: python

        def my_callback(*args, **kwargs):
            pass

        batch = BatchQuery()

        batch.add_callback(my_callback)
        batch.add_callback(my_callback, 'positional arg', named_arg='named arg value')

        # if you need reference to the batch within the callback,
        # just trap it in the arguments to be passed to the callback:
        batch.add_callback(my_callback, cqlengine_batch=batch)

        # once the batch executes...
        batch.execute()

        # the effect of the above scheduled callbacks will be similar to
        my_callback()
        my_callback('positional arg', named_arg='named arg value')
        my_callback(cqlengine_batch=batch)

    Failure in any of the callbacks does not affect the batch's execution, as the callbacks are started after the execution
    of the batch is complete.



QuerySet method reference
=========================

.. class:: QuerySet

    .. method:: all()

        Returns a queryset matching all rows

    .. method:: batch(batch_object)

        Sets the batch object to run the query on. Note that running a select query with a batch object will raise an exception

    .. method:: consistency(consistency_setting)

        Sets the consistency level for the operation.  Options may be imported from the top level :attr:`cqlengine` package.


    .. method:: count()

        Returns the number of matching rows in your QuerySet

    .. method:: filter(\*\*values)

        :param values: See :ref:`retrieving-objects-with-filters`

        Returns a QuerySet filtered on the keyword arguments

    .. method:: get(\*\*values)

        :param values: See :ref:`retrieving-objects-with-filters`

        Returns a single object matching the QuerySet. If no objects are matched, a :attr:`~models.Model.DoesNotExist` exception is raised. If more than one object is found, a :attr:`~models.Model.MultipleObjectsReturned` exception is raised.

    .. method:: limit(num)

        Limits the number of results returned by Cassandra.

        *Note that CQL's default limit is 10,000, so all queries without a limit set explicitly will have an implicit limit of 10,000*

    .. method:: order_by(field_name)

        :param field_name: the name of the field to order on. *Note: the field_name must be a clustering key*
        :type field_name: string

        Sets the field to order on.

    .. method:: allow_filtering()

        Enables the (usually) unwise practive of querying on a clustering key without also defining a partition key

    .. method:: timestamp(timestamp_or_long_or_datetime)

        Allows for custom timestamps to be saved with the record.

    .. method:: ttl(ttl_in_seconds)

        :param ttl_in_seconds: time in seconds in which the saved values should expire
        :type ttl_in_seconds: int

        Sets the ttl to run the query query with. Note that running a select query with a ttl value will raise an exception

    .. method:: update(**values)

        Performs an update on the row selected by the queryset. Include values to update in the
        update like so:

        .. code-block:: python
            Model.objects(key=n).update(value='x')

        Passing in updates for columns which are not part of the model will raise a ValidationError.
        Per column validation will be performed, but instance level validation will not
        (`Model.validate` is not called).

        The queryset update method also supports blindly adding and removing elements from container columns, without
        loading a model instance from Cassandra.

        Using the syntax `.update(column_name={x, y, z})` will overwrite the contents of the container, like updating a
        non container column. However, adding `__<operation>` to the end of the keyword arg, makes the update call add
        or remove items from the collection, without overwriting then entire column.


        Given the model below, here are the operations that can be performed on the different container columns:

        .. code-block:: python

            class Row(Model):
                row_id      = columns.Integer(primary_key=True)
                set_column  = columns.Set(Integer)
                list_column = columns.Set(Integer)
                map_column  = columns.Set(Integer, Integer)

        :class:`~cqlengine.columns.Set`

        - `add`: adds the elements of the given set to the column
        - `remove`: removes the elements of the given set to the column


        .. code-block:: python

            # add elements to a set
            Row.objects(row_id=5).update(set_column__add={6})

            # remove elements to a set
            Row.objects(row_id=5).update(set_column__remove={4})

        :class:`~cqlengine.columns.List`

        - `append`: appends the elements of the given list to the end of the column
        - `prepend`: prepends the elements of the given list to the beginning of the column

        .. code-block:: python

            # append items to a list
            Row.objects(row_id=5).update(list_column__append=[6, 7])

            # prepend items to a list
            Row.objects(row_id=5).update(list_column__prepend=[1, 2])


        :class:`~cqlengine.columns.Map`

        - `update`: adds the given keys/values to the columns, creating new entries if they didn't exist, and overwriting old ones if they did

        .. code-block:: python

            # add items to a map
            Row.objects(row_id=5).update(map_column__update={1: 2, 3: 4})
