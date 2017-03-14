=============
Batch Queries
=============

cqlengine supports batch queries using the BatchQuery class. Batch queries can be started and stopped manually, or within a context manager. To add queries to the batch object, you just need to precede the create/save/delete call with a call to batch, and pass in the batch object.


Batch Query General Use Pattern
===============================

    You can only create, update, and delete rows with a batch query, attempting to read rows out of the database with a batch query will fail.

    .. code-block:: python

        from cassandra.cqlengine.query import BatchQuery

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
===============================

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

Logged vs Unlogged Batches
---------------------------
    By default, queries in cqlengine are LOGGED, which carries additional overhead from UNLOGGED.  To explicitly state which batch type to use, simply:


    .. code-block:: python

        from cassandra.cqlengine.query import BatchType
        with BatchQuery(batch_type=BatchType.Unlogged) as b:
            LogEntry.batch(b).create(k=1, v=1)
            LogEntry.batch(b).create(k=1, v=2)
