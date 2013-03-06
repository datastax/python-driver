==============
Making Queries
==============

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
----------------------------------------
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

    :attr:`>= (__gte) <query.QueryOperator.GreaterThanOrEqualOperator>`

        .. code-block:: python

            q = Automobile.objects.filter(manufacturer='Tesla')
            q = q.filter(year__gte=2010)  # year >= 2010

    :attr:`< (__lt) <query.QueryOperator.LessThanOperator>`

        .. code-block:: python

            q = Automobile.objects.filter(manufacturer='Tesla')
            q = q.filter(year__lt=2012)  # year < 2012

    :attr:`<= (__lte) <query.QueryOperator.LessThanOrEqualOperator>`

        .. code-block:: python

            q = Automobile.objects.filter(manufacturer='Tesla')
            q = q.filter(year__lte=2012)  # year <= 2012


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

QuerySet method reference
=========================

.. class:: QuerySet

    .. method:: all()

        Returns a queryset matching all rows

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
