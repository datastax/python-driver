==========================
Frequently Asked Questions
==========================

Why don't updates work correctly on models instantiated as Model(field=value, field2=value2)?
------------------------------------------------------------------------------------------------

The recommended way to create new rows is with the models .create method. The values passed into a model's init method are interpreted by the model as the values as they were read from a row. This allows the model to "know" which rows have changed since the row was read out of cassandra, and create suitable update statements.

How to preserve ordering in batch query?
-------------------------------------------

Statement Ordering is not supported by CQL3 batches. Therefore,
once cassandra needs resolving conflict(Updating the same column in one batch),
The algorithm below would be used.

 * If timestamps are different, pick the column with the largest timestamp (the value being a regular column or a tombstone)
 * If timestamps are the same, and one of the columns in a tombstone ('null') - pick the tombstone
 * If timestamps are the same, and none of the columns are tombstones, pick the column with the largest value

Below is an example to show this scenario.

.. code-block:: python

    class MyMode(Model):
        id    = columns.Integer(primary_key=True)
        count = columns.Integer()
        text  = columns.Text()

    with BatchQuery() as b:
       MyModel.batch(b).create(id=1, count=2, text='123') 
       MyModel.batch(b).create(id=1, count=3, text='111')

    assert MyModel.objects(id=1).first().count == 3
    assert MyModel.objects(id=1).first().text  == '123'

The largest value of count is 3, and the largest value of text would be '123'.

The workaround is applying timestamp to each statement, then Cassandra would
resolve to the statement with the lastest timestamp.

.. code-block:: python

    with BatchQuery() as b:
        MyModel.timestamp(datetime.now()).batch(b).create(id=1, count=2, text='123')
        MyModel.timestamp(datetime.now()).batch(b).create(id=1, count=3, text='111')

    assert MyModel.objects(id=1).first().count == 3
    assert MyModel.objects(id=1).first().text  == '111'

How can I delete individual values from a row?
-------------------------------------------------

When inserting with CQLEngine, ``None`` is equivalent to CQL ``NULL`` or to
issuing a ``DELETE`` on that column. For example:

.. code-block:: python

    class MyModel(Model):
        id    = columns.Integer(primary_key=True)
        text  = columns.Text()

    m = MyModel.create(id=1, text='We can delete this with None')
    assert MyModel.objects(id=1).first().text is not None

    m.update(text=None)
    assert MyModel.objects(id=1).first().text is None
