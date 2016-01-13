Lightweight Transactions (Compare-and-set)
==========================================

Lightweight Transactions (LWTs) are mostly pass-through CQL for the driver. However,
the server returns some specialized results indicating the outcome and optional state
preceding the transaction.

For pertinent execution parameters, see :attr:`.Statement.serial_consistency_level`.

This section discusses working with specialized result sets returned by the server for LWTs,
and how to work with them using the driver.


Specialized Results
-------------------
The result returned from a LWT request is always a single row result. It will always have
prepended a special column named ``[applied]``. How this value appears in your results depends
on the row factory in use. See below for examples.

The value of this ``[applied]`` column is boolean value indicating whether or not the transaction was applied.
If ``True``, it is the only column in the result. If ``False``, the additional columns depend on the LWT operation being
executed:

- When using a ``UPDATE ... IF "col" = ...`` clause, the result will contain the ``[applied]`` column, plus the existing columns
  and values for any columns in the ``IF`` clause (and thus the value that caused the transaction to fail).

- When using ``INSERT ... IF NOT EXISTS``, the result will contain the ``[applied]`` column, plus all columns and values
  of the existing row that rejected the transaction.

- ``UPDATE .. IF EXISTS`` never has additional columns, regardless of ``[applied]`` status.

How the ``[applied]`` column manifests depends on the row factory in use. Considering the following (initially empty) table::

    CREATE TABLE test.t (
        k int PRIMARY KEY,
        v int,
        x int
    )

... the following sections show the expected result for a number of example statements, using the three base row factories.

named_tuple_factory (default)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The name ``[applied]`` is not a valid Python identifier, so the square brackets are actually removed
from the attribute for the resulting ``namedtuple``. The row always has a boolean column ``applied`` in position 0::

    >>> session.execute("INSERT INTO t (k,v) VALUES (0,0) IF NOT EXISTS")
    Row(applied=True)

    >>> session.execute("INSERT INTO t (k,v) VALUES (0,0) IF NOT EXISTS")
    Row(applied=False, k=0, v=0, x=None)

    >>> session.execute("UPDATE t SET v = 1, x = 2 WHERE k = 0 IF v =0")
    Row(applied=True)

    >>> session.execute("UPDATE t SET v = 1, x = 2 WHERE k = 0 IF v =0 AND x = 1")
    Row(applied=False, v=1, x=2)

tuple_factory
~~~~~~~~~~~~~
This return type does not refer to names, but the boolean value ``applied`` is always present in position 0::

    >>> session.execute("INSERT INTO t (k,v) VALUES (0,0) IF NOT EXISTS")
    (True,)

    >>> session.execute("INSERT INTO t (k,v) VALUES (0,0) IF NOT EXISTS")
    (False, 0, 0, None)

    >>> session.execute("UPDATE t SET v = 1, x = 2 WHERE k = 0 IF v =0")
    (True,)

    >>> session.execute("UPDATE t SET v = 1, x = 2 WHERE k = 0 IF v =0 AND x = 1")
    (False, 1, 2)

dict_factory
~~~~~~~~~~~~
The retuned ``dict`` contains the ``[applied]`` key::

    >>> session.execute("INSERT INTO t (k,v) VALUES (0,0) IF NOT EXISTS")
    {u'[applied]': True}

    >>> session.execute("INSERT INTO t (k,v) VALUES (0,0) IF NOT EXISTS")
    {u'x': 2, u'[applied]': False, u'v': 1}

    >>> session.execute("UPDATE t SET v = 1, x = 2 WHERE k = 0 IF v =0")
    {u'x': None, u'[applied]': False, u'k': 0, u'v': 0}

    >>> session.execute("UPDATE t SET v = 1, x = 2 WHERE k = 0 IF v =0 AND x = 1")
    {u'[applied]': True}


