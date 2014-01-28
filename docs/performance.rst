Performance Notes
=================
The python driver for Cassandra offers several methods for executing queries.
You can synchronously block for queries to complete using
:meth:`.Session.execute()`, you can use a future-like interface through
:meth:`.Session.execute_async()`, or you can attach a callback to the future
with :meth:`.ResponseFuture.add_callback()`.  Each of these methods has
different performance characteristics and behaves differently when
multiple threads are used.

Benchmark Notes
---------------
All benchmarks were executed using the
`benchmark scripts <https://github.com/datastax/python-driver/tree/master/benchmarks>`_
in the driver repository.  They were executed on a laptop with 16 GiB of RAM, an SSD,
and a 2 GHz, four core CPU with hyperthreading.  The Cassandra cluster was a three
node `ccm <https://github.com/pcmanus/ccm>`_ cluster running on the same laptop
with version 1.2.13 of Cassandra. I suggest testing these benchmarks against your
own cluster when tuning the driver for optimal throughput or latency.

The 1.0.0 version of the driver was used with all default settings.  For these
benchmarks, the driver was configured to use the ``libev`` reactor.  You can also run
the benchmarks using the ``asyncore`` event loop (:class:`~.AsyncoreConnection`) 
by using the ``--asyncore-only`` command line option. 

Each benchmark completes 100,000 small inserts. The replication factor for the
keyspace was three, so all nodes were replicas for the inserted rows.

Synchronous Execution (`sync.py <https://github.com/datastax/python-driver/blob/master/benchmarks/sync.py>`_)
-------------------------------------------------------------------------------------------------------------
Although this is the simplest way to make queries, it has low throughput
in single threaded environments.  This is basically what the benchmark
is doing:

.. code-block:: python

    from cassandra.cluster import Cluster

    cluster = Cluster([127.0.0.1, 127.0.0.2, 127.0.0.3])
    session = cluster.connect()

    for i in range(100000):
        session.execute("INSERT INTO mykeyspace.mytable (key, b, c) VALUES (a, 'b', 'c')")

.. code-block:: bash

    ~/python-driver $ python benchmarks/sync.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=1
    Average throughput: 434.08/sec


This technique does scale reasonably well as we add more threads:

.. code-block:: bash

    ~/python-driver $ python benchmarks/sync.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=2
    Average throughput: 830.49/sec
    ~/python-driver $ python benchmarks/sync.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=4
    Average throughput: 1078.27/sec
    ~/python-driver $ python benchmarks/sync.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=8
    Average throughput: 1275.20/sec
    ~/python-driver $ python benchmarks/sync.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=16
    Average throughput: 1345.56/sec


In my environment, throughput is maximized at about 20 threads.


Batched Futures (`future_batches.py <https://github.com/datastax/python-driver/blob/master/benchmarks/future_batches.py>`_)
---------------------------------------------------------------------------------------------------------------------------
This is a simple way to work with futures for higher throughput.  Essentially,
we start 120 queries asynchronously at the same time and then wait for them
all to complete. We then repeat this process until all 100,000 operations
have completed:

.. code-block:: python

    futures = Queue.Queue(maxsize=121)
    for i in range(100000):
        if i % 120 == 0:
            # clear the existing queue
            while True:
                try:
                    futures.get_nowait().result()
                except Queue.Empty:
                    break

        future = session.execute_async(query)
        futures.put_nowait(future)

As expected, this improves throughput in a single-threaded environment:

.. code-block:: bash

    ~/python-driver $ python benchmarks/future_batches.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=1
    Average throughput: 3477.56/sec

However, adding more threads may actually harm throughput:

.. code-block:: bash

    ~/python-driver $ python benchmarks/future_batches.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=2
    Average throughput: 2360.52/sec
    ~/python-driver $ python benchmarks/future_batches.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=4
    Average throughput: 2293.21/sec
    ~/python-driver $ python benchmarks/future_batches.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=8
    Average throughput: 2244.85/sec


Queued Futures (`future_full_pipeline.py <https://github.com/datastax/python-driver/blob/master/benchmarks/future_full_pipeline.py>`_)
--------------------------------------------------------------------------------------------------------------------------------------
This pattern is similar to batched futures.  The main difference is that
every time we put a future on the queue, we pull the oldest future out
and wait for it to complete:

.. code-block:: python

        futures = Queue.Queue(maxsize=121)
        for i in range(100000):
            if i >= 120:
                old_future = futures.get_nowait()
                old_future.result()

            future = session.execute_async(query)
            futures.put_nowait(future)

This gets slightly better throughput than the Batched Futures pattern:

.. code-block:: bash

    ~/python-driver $ python benchmarks/future_full_pipeline.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=1
    Average throughput: 3635.76/sec

But this has the same throughput issues when multiple threads are used:

.. code-block:: bash

    ~/python-driver $ python benchmarks/future_full_pipeline.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=2
    Average throughput: 2213.62/sec
    ~/python-driver $ python benchmarks/future_full_pipeline.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=4
    Average throughput: 2707.62/sec
    ~/python-driver $ python benchmarks/future_full_pipeline.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=8
    Average throughput: 2462.42/sec

Unthrottled Futures (`future_full_throttle.py <https://github.com/datastax/python-driver/blob/master/benchmarks/future_full_throttle.py>`_)
-------------------------------------------------------------------------------------------------------------------------------------------
What happens if we don't throttle our async requests at all?

.. code-block:: python

    futures = []
    for i in range(100000):
        future = session.execute_async(query)
        futures.append(future)

    for future in futures:
        future.result()

Throughput is about the same as the previous pattern, but a lot of memory will
be consumed by the list of Futures:

.. code-block:: bash

    ~/python-driver $ python benchmarks/future_full_throttle.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=1
    Average throughput: 3474.11/sec
    ~/python-driver $ python benchmarks/future_full_throttle.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=2
    Average throughput: 2389.61/sec
    ~/python-driver $ python benchmarks/future_full_throttle.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=4
    Average throughput: 2371.75/sec
    ~/python-driver $ python benchmarks/future_full_throttle.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=8
    Average throughput: 2165.29/sec

Callback Chaining (`callbacks_full_pipeline.py <https://github.com/datastax/python-driver/blob/master/benchmarks/callbacks_full_pipeline.py>`_)
-----------------------------------------------------------------------------------------------------------------------------------------------
This pattern is very different from the previous patterns.  Here we're taking
advantage of the :meth:`.ResponseFuture.add_callback()` function to start
another request as soon as one finishes.  Futhermore, we're starting 120
of these callback chains, so we've always got about 120 operations in
flight at any time:

.. code-block:: python

    from itertools import count
    from threading import Event

    num_started = count()
    num_finished = count()
    initial = object()
    finished_event = Event()

    def handle_error(exc):
        log.error("Error on insert: %r", exc)

    def insert_next(previous_result):
        current_num = num_started.next()

        if previous_result is not initial:
            num = next(num_finished)
            if num >= 100000:
                finished_event.set()

        if current_num <= 100000:
            future = session.execute_async(query)
            future.add_callbacks(insert_next, handle_error)

    for i in range(120):
        insert_next(initial)

    finished_event.wait()

This is a more complex pattern, but the throughput is excellent:

.. code-block:: bash

    ~/python-driver $ python benchmarks/callback_full_pipeline.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=1
    Average throughput: 7647.30/sec

Part of the reason why performance is so good is that everything is running on
single thread: the internal event loop thread that powers the driver.  The
downside to this is that adding more threads doesn't improve anything:

.. code-block:: bash

    ~/python-driver $ python benchmarks/callback_full_pipeline.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=2
    Average throughput: 7704.58/sec


What happens if we have more than 120 callback chains running?

With 250 chains:

.. code-block:: bash

    ~/python-driver $ python benchmarks/callback_full_pipeline.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=1
    Average throughput: 7794.22/sec

Things look pretty good with 250 chains.  If we try 500 chains, we start to max out
all of the connections in the connection pools.  The problem is that the current
version of the driver isn't very good at throttling these callback chains, so
a lot of time gets spent waiting for new connections and performance drops
dramatically:

.. code-block:: bash

    ~/python-driver $ python benchmarks/callback_full_pipeline.py -n 100000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --libev-only --threads=1
    Average throughput: 679.61/sec

Until this is improved, you should limit the number of callback chains you run.

PyPy
----
Almost all of these patterns become CPU-bound pretty quickly with CPython, the
normal implementation of python. `PyPy <http://pypy.org>`_ is an alternative
implementation of Python (written in Python) which uses a JIT compiler to
reduce CPU consumption.  This leads to a huge improvement in the driver
performance:

.. code-block:: bash

    ~/python-driver $ pypy benchmarks/callback_full_pipeline.py -n 500000 --hosts=127.0.0.1,127.0.0.2,127.0.0.3 --asyncore-only --threads=1
    Average throughput: 18782.00/sec

Eventually the driver may add C extensions to reduce CPU consumption, which
would probably narrow the gap between the performance of CPython and PyPy.

multiprocessing
---------------
All of the patterns here may be used over multiple processes using the
`multiprocessing <http://docs.python.org/2/library/multiprocessing.html>`_
module.  Multiple processes will scale significantly better than multiple
threads will, so if high throughput is your goal, consider this option.

Just be sure to **never share any** :class:`~.Cluster`, :class:`~.Session`,
**or** :class:`~.ResponseFuture` **objects across multiple processes**. These
objects should all be created after forking the process, not before.
