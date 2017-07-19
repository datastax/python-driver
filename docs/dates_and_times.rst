Working with Dates and Times
============================

This document is meant to provide on overview of the assumptions and limitations of the driver time handling, the
reasoning behind it, and describe approaches to working with these types.

timestamps (Cassandra DateType)
-------------------------------

Timestamps in Cassandra are timezone-naive timestamps encoded as millseconds since UNIX epoch. Clients working with
timestamps in this database usually find it easiest to reason about them if they are always assumed to be UTC. To quote the
pytz documentation, "The preferred way of dealing with times is to always work in UTC, converting to localtime only when
generating output to be read by humans." The driver adheres to this tenant, and assumes UTC is always in the database. The
driver attempts to make this correct on the way in, and assumes no timezone on the way out.

Write Path
~~~~~~~~~~
When inserting timestamps, the driver handles serialization for the write path as follows:

If the input is a ``datetime.datetime``, the serialization is normalized by starting with the ``utctimetuple()`` of the
value.

- If the ``datetime`` object is timezone-aware, the timestamp is shifted, and represents the UTC timestamp equivalent.
- If the ``datetime`` object is timezone-naive, this results in no shift -- any ``datetime`` with no timezone information is assumed to be UTC

Note the second point above applies even to "local" times created using ``now()``::

    >>> d = datetime.now()

    >>> print(d.tzinfo)
    None


These do not contain timezone information intrinsically, so they will be assumed to be UTC and not shifted. When generating
timestamps in the application, it is clearer to use ``datetime.utcnow()`` to be explicit about it.

If the input for a timestamp is numeric, it is assumed to be a epoch-relative millisecond timestamp, as specified in the
CQL spec -- no scaling or conversion is done.

Read Path
~~~~~~~~~
The driver always assumes persisted timestamps are UTC and makes no attempt to localize them. Returned values are
timezone-naive ``datetime.datetime``. We follow this approach because the datetime API has deficiencies around daylight
saving time, and the defacto package for handling this is a third-party package (we try to minimize external dependencies
and not make decisions for the integrator).

The decision for how to handle timezones is left to the application. For the most part it is straightforward to apply
localization to the ``datetime``\s returned by queries. One prevalent method is to use pytz for localization::

    import pytz
    user_tz = pytz.timezone('US/Central')
    timestamp_naive = row.ts
    timestamp_utc = pytz.utc.localize(timestamp_naive)
    timestamp_presented = timestamp_utc.astimezone(user_tz)

This is the most robust approach (likely refactored into a function). If it is deemed too cumbersome to apply for all call
sites in the application, it is possible to patch the driver with custom deserialization for this type. However, doing
this depends depends some on internal APIs and what extensions are present, so we will only mention the possibility, and
not spell it out here.

date, time (Cassandra DateType)
-------------------------------
Date and time in Cassandra are idealized markers, much like ``datetime.date`` and ``datetime.time`` in the Python standard
library. Unlike these Python implementations, the Cassandra encoding supports much wider ranges. To accommodate these
ranges without overflow, this driver returns these data in custom types: :class:`.util.Date` and :class:`.util.Time`.

Write Path
~~~~~~~~~~
For simple (not prepared) statements, the input values for each of these can be either a string literal or an encoded
integer. See `Working with dates <https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#working-with-dates>`_
or `Working with time <https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#working-with-time>`_ for details
on the encoding or string formats.

For prepared statements, the driver accepts anything that can be used to construct the :class:`.util.Date` or
:class:`.util.Time` classes. See the linked API docs for details.

Read Path
~~~~~~~~~
The driver always returns custom types for ``date`` and ``time``.

The driver returns :class:`.util.Date` for ``date`` in order to accommodate the wider range of values without overflow.
For applications working within the supported range of [``datetime.MINYEAR``, ``datetime.MAXYEAR``], these are easily
converted to standard ``datetime.date`` insances using :meth:`.Date.date`.

The driver returns :class:`.util.Time` for ``time`` in order to retain nanosecond precision stored in the database.
For applications not concerned with this level of precision, these are easily converted to standard ``datetime.time``
insances using :meth:`.Time.time`.
