==============
Making Queries
==============

.. module:: cqlengine.queryset

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
            options = columns.Set(columns.Text)

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

        .. note::

            * CQL does not support specifying a start position in it's queries. Therefore, accessing elements using array indexing will load every result up to the index value requested
            * Using negative indices requires a "SELECT COUNT()" to be executed. This has a performance cost on large datasets.

    * list slicing
        .. code-block:: python

            q = Automobile.objects.all()
            q[1:] #returns all results except the first
            q[1:9] #returns a slice of the results

        .. note::

            * CQL does not support specifying a start position in it's queries. Therefore, accessing elements using array slicing will load every result up to the index value requested
            * Using negative indices requires a "SELECT COUNT()" to be executed. This has a performance cost on large datasets.

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

    :attr:`CONTAINS (__contains) <query.QueryOperator.ContainsOperator>`

        The CONTAINS operator is available for all collection types (List, Set, Map).

        .. code-block:: python

            q = Automobile.objects.filter(manufacturer='Tesla')
            q.filter(options__contains='backup camera').allow_filtering()

        Note that we need to use allow_filtering() since the *options* column has no secondary index.

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
            id      = columns.UUID(partition_key=True)
            time    = columns.TimeUUID(primary_key=True)
            data    = columns.Bytes()

        min_time = datetime(1982, 1, 1)
        max_time = datetime(1982, 3, 9)

        DataStream.filter(time__gt=functions.MinTimeUUID(min_time), time__lt=functions.MaxTimeUUID(max_time))

Token Function
==============

    Token functon may be used only on special, virtual column pk__token, representing token of partition key (it also works for composite partition keys).
    Cassandra orders returned items by value of partition key token, so using cqlengine.Token we can easy paginate through all table rows.

    See http://cassandra.apache.org/doc/cql3/CQL-3.0.html#tokenFun

    *Example*

    .. code-block:: python

        class Items(Model):
            id      = columns.Text(primary_key=True)
            data    = columns.Bytes()

        query = Items.objects.all().limit(10)

        first_page = list(query);
        last = first_page[-1]
        next_page = list(query.filter(pk__token__gt=cqlengine.Token(last.pk)))

QuerySets are immutable
=======================

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

    .. code-block:: python

        items = list(range(20))
        random.shuffle(items)
        for i in items:
            TestModel.create(id=1, clustering_key=i)

        values = list(TestModel.objects.values_list('clustering_key', flat=True))
        # [19L, 18L, 17L, 16L, 15L, 14L, 13L, 12L, 11L, 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L, 0L]

Per Query Timeouts
===================

By default all queries are executed with the timeout defined in `~cqlengine.connection.setup()`
The examples below show how to specify a per-query timeout.
A timeout is specified in seconds and can be an int, float or None.
None means no timeout.


    .. code-block:: python

        class Row(Model):
            id = columns.Integer(primary_key=True)
            name = columns.Text()


    Fetch all objects with a timeout of 5 seconds

    .. code-block:: python

        Row.objects().timeout(5).all()

    Create a single row with a 50ms timeout

    .. code-block:: python

        Row(id=1, name='Jon').timeout(0.05).create()

    Delete a single row with no timeout

    .. code-block:: python

        Row(id=1).timeout(None).delete()

    Update a single row with no timeout

    .. code-block:: python

        Row(id=1).timeout(None).update(name='Blake')

    Batch query timeouts

    .. code-block:: python

        with BatchQuery(timeout=10) as b:
            Row(id=1, name='Jon').create()


    NOTE: You cannot set both timeout and batch at the same time, batch will use the timeout defined in it's constructor.
    Setting the timeout on the model is meaningless and will raise an AssertionError.


.. _ttl-change:

Default TTL and Per Query TTL
=============================

Model default TTL now relies on the *default_time_to_live* feature, introduced in Cassandra 2.0. It is not handled anymore in the CQLEngine Model (cassandra-driver >=3.6). You can set the default TTL of a table like this:

    Example:

    .. code-block:: python

        class User(Model):
            __options__ = {'default_time_to_live': 20}

            user_id = columns.UUID(primary_key=True)
            ...

You can set TTL per-query if needed. Here are a some examples:

    Example:

    .. code-block:: python

        class User(Model):
            __options__ = {'default_time_to_live': 20}

            user_id = columns.UUID(primary_key=True)
            ...

        user = User.objects.create(user_id=1)  # Default TTL 20 will be set automatically on the server

        user.ttl(30).update(age=21)            # Update the TTL to 30
        User.objects.ttl(10).create(user_id=1)  # TTL 10
        User(user_id=1, age=21).ttl(10).save()  # TTL 10


Named Tables
===================

Named tables are a way of querying a table without creating an class.  They're useful for querying system tables or exploring an unfamiliar database.


    .. code-block:: python

        from cassandra.cqlengine.connection import setup
        setup("127.0.0.1", "cqlengine_test")

        from cassandra.cqlengine.named import NamedTable
        user = NamedTable("cqlengine_test", "user")
        user.objects()
        user.objects()[0]

        # {u'pk': 1, u't': datetime.datetime(2014, 6, 26, 17, 10, 31, 774000)}
