> **NOTE:** The following benchmarks are not indicative of actual Cassandra
> performance and are instead to be used as relative results to compare the
> different request methods.
>
> All tests were done on a single node within AWS running Cassandra 3.11.3 on
> an `m5.4xlarge` instance.

# Write Throughputs

## Callback Full Pipeline

This request method fires off all asynchronous requests at the same time.

When the `ResponseFuture`'s callback is executed, the callback code ensures all
pending requests are completed before exiting the code block. This is done by
setting a Python `Event` in the callback while waiting for the `Event` in the
main thread.

Assuming the Cassandra cluster is not overloaded by the large number of
parallel requests and `OperationTimedOut` exceptions are not raised, this is
the fastest the driver can perform.

```
+ python benchmarks/callback_full_pipeline.py --num-ops 150000 -H 172.30.0.56
2018-08-08 19:17:16,303 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-08 19:17:16,435 [INFO] root: ==== AsyncoreConnection ====
2018-08-08 19:17:33,679 [INFO] root: Total time: 14.27s
2018-08-08 19:17:33,679 [INFO] root: Average throughput: 10511.13/sec
2018-08-08 19:17:33,679 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-08 19:17:33,911 [INFO] root: ==== LibevConnection ====
2018-08-08 19:17:50,300 [INFO] root: Total time: 13.32s
2018-08-08 19:17:50,300 [INFO] root: Average throughput: 11261.17/sec
2018-08-08 19:17:50,300 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-08 19:17:50,432 [INFO] root: ==== TwistedConnection ====
2018-08-08 19:18:10,613 [INFO] root: Total time: 17.17s
2018-08-08 19:18:10,613 [INFO] root: Average throughput: 8733.76/sec
```

## WritePipeline Object

The `WritePipeline` object is a blend of the "Callback Full Pipeline" and
"Future Batches" request methods.

To the developer, the `WritePipeline` is initialized with a Cassandra `session`
and `WritePipeline.execute()` is called for each asynchronous request.
`WritePipeline.confirm()` is used to ensure all requests were processed
correctly before exiting the code block.

Internally, the `WritePipeline` creates a request `Queue` which is executed
and managed via `ResponseFuture` callbacks. If the number of pending futures
is too large, all futures are first consumed internally by calling
`WritePipeline.confirm()` before allowing new requests to be created.
`WritePipeline.confirm()` uses a Python `Event` to ensure all in-flight
requests have returned.

```
+ python benchmarks/pipeline.py --num-ops 150000 -H 172.30.0.56
2018-08-08 19:18:20,317 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-08 19:18:20,449 [INFO] root: ==== AsyncoreConnection ====
2018-08-08 19:18:46,428 [INFO] root: Total time: 23.41s
2018-08-08 19:18:46,429 [INFO] root: Average throughput: 6407.10/sec
2018-08-08 19:18:46,429 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-08 19:18:46,662 [INFO] root: ==== LibevConnection ====
2018-08-08 19:19:13,132 [INFO] root: Total time: 23.70s
2018-08-08 19:19:13,132 [INFO] root: Average throughput: 6328.08/sec
2018-08-08 19:19:13,132 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-08 19:19:13,364 [INFO] root: ==== TwistedConnection ====
2018-08-08 19:19:51,967 [INFO] root: Total time: 36.05s
2018-08-08 19:19:51,967 [INFO] root: Average throughput: 4161.26/sec
```

## Future Batches

This request method fires off a set number of asynchronous requests and adds
these `ResponseFuture` objects to the future `Queue`.

Each time the future `Queue` reaches its maximum capacity, the future `Queue`
is read from in its *entirety* before creating another asynchronous request
that is added back to the future `Queue`.

Before exiting, all futures within the future `Queue` are consumed to ensure
all pending requests have been processed.

```
+ python benchmarks/future_batches.py --num-ops 150000 -H 172.30.0.56
2018-08-07 04:26:39,062 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:26:39,194 [INFO] root: ==== AsyncoreConnection ====
2018-08-07 04:27:05,877 [INFO] root: Total time: 23.78s
2018-08-07 04:27:05,877 [INFO] root: Average throughput: 6308.53/sec
2018-08-07 04:27:05,877 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:27:06,108 [INFO] root: ==== LibevConnection ====
2018-08-07 04:27:31,350 [INFO] root: Total time: 22.29s
2018-08-07 04:27:31,350 [INFO] root: Average throughput: 6730.50/sec
2018-08-07 04:27:31,350 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:27:31,526 [INFO] root: ==== TwistedConnection ====
2018-08-07 04:28:09,978 [INFO] root: Total time: 35.64s
2018-08-07 04:28:09,978 [INFO] root: Average throughput: 4209.29/sec
```

## Future Full Pipeline

This request method fires off a set number of asynchronous requests and adds
these `ResponseFuture` objects to the future `Queue`.

Once the the future `Queue` reaches its maximum capacity, the future `Queue`
consumes a *single* future before creating a *single* asynchronous request
that is added back to the future `Queue`.

Before exiting, all futures within the future `Queue` are consumed to ensure
all pending requests have been processed.

```
+ python benchmarks/future_full_pipeline.py --num-ops 150000 -H 172.30.0.56
2018-08-07 04:28:12,587 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:28:12,722 [INFO] root: ==== AsyncoreConnection ====
2018-08-07 04:28:42,443 [INFO] root: Total time: 26.82s
2018-08-07 04:28:42,443 [INFO] root: Average throughput: 5592.19/sec
2018-08-07 04:28:42,443 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:28:42,674 [INFO] root: ==== LibevConnection ====
2018-08-07 04:29:10,494 [INFO] root: Total time: 24.94s
2018-08-07 04:29:10,494 [INFO] root: Average throughput: 6014.01/sec
2018-08-07 04:29:10,494 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:29:10,626 [INFO] root: ==== TwistedConnection ====
2018-08-07 04:29:52,830 [INFO] root: Total time: 39.28s
2018-08-07 04:29:52,830 [INFO] root: Average throughput: 3819.16/sec
```

## Future Full Throttle

This request method fires off all asynchronous requests at the same time.

Before exiting, all futures within the future `List` are consumed to ensure
all pending requests have been processed.

Assuming the Cassandra cluster is not overloaded by the large number of
parallel requests and `OperationTimedOut` exceptions are not raised, this
request method then synchronously consumes each future in a FIFO ordering even
though the requests were made asynchronously.

While FIFO verification of write requests may not be required, often
times FIFO consumption of read requests are required since the resulting
data may be merged with known data.

```
+ python benchmarks/future_full_throttle.py --num-ops 150000 -H 172.30.0.56
2018-08-07 04:29:55,634 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:29:55,766 [INFO] root: ==== AsyncoreConnection ====
2018-08-07 04:30:28,940 [INFO] root: Total time: 30.34s
2018-08-07 04:30:28,940 [INFO] root: Average throughput: 4944.40/sec
2018-08-07 04:30:28,940 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:30:29,174 [INFO] root: ==== LibevConnection ====
2018-08-07 04:31:03,236 [INFO] root: Total time: 31.15s
2018-08-07 04:31:03,236 [INFO] root: Average throughput: 4816.09/sec
2018-08-07 04:31:03,236 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:31:03,367 [INFO] root: ==== TwistedConnection ====
2018-08-07 04:31:40,248 [INFO] root: Total time: 34.04s
2018-08-07 04:31:40,248 [INFO] root: Average throughput: 4406.98/sec
```

## Synchronous Requests

This request method fires off each request synchronously.

Since only *one* request is in-flight at a given time, this will be the slowest
request method.

```
+ python benchmarks/sync.py --num-ops 150000 -H 172.30.0.56
2018-08-07 04:31:44,558 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:31:44,690 [INFO] root: ==== AsyncoreConnection ====
2018-08-07 04:32:37,343 [INFO] root: Total time: 50.14s
2018-08-07 04:32:37,343 [INFO] root: Average throughput: 2991.66/sec
2018-08-07 04:32:37,343 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:32:37,575 [INFO] root: ==== LibevConnection ====
2018-08-07 04:33:22,182 [INFO] root: Total time: 42.08s
2018-08-07 04:33:22,183 [INFO] root: Average throughput: 3565.04/sec
2018-08-07 04:33:22,183 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:33:22,314 [INFO] root: ==== TwistedConnection ====
2018-08-07 04:34:21,940 [INFO] root: Total time: 57.17s
2018-08-07 04:34:21,940 [INFO] root: Average throughput: 2623.56/sec
```

# Read Throughputs

## Callback Full Pipeline

This request method fires off all asynchronous requests at the same time.

When the `ResponseFuture`'s callback is executed, the callback code ensures all
pending requests are completed before exiting the code block. This is done by
setting a Python `Event` in the callback while waiting for the `Event` in the
main thread.

Assuming the Cassandra cluster is not overloaded by the large number of
parallel requests and `OperationTimedOut` exceptions are not raised, this is
the fastest the driver can perform.

```
+ python benchmarks/callback_full_pipeline.py --num-ops 150000 -H 172.30.0.56 --read
2018-08-08 19:20:18,369 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-08 19:20:18,503 [INFO] root: ==== AsyncoreConnection ====
2018-08-08 19:21:52,157 [INFO] root: Total time: 91.14s
2018-08-08 19:21:52,157 [INFO] root: Average throughput: 1645.89/sec
2018-08-08 19:21:52,157 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-08 19:21:52,389 [INFO] root: ==== LibevConnection ====
2018-08-08 19:23:23,840 [INFO] root: Total time: 88.85s
2018-08-08 19:23:23,840 [INFO] root: Average throughput: 1688.30/sec
2018-08-08 19:23:23,840 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-08 19:23:23,975 [INFO] root: ==== TwistedConnection ====
2018-08-08 19:24:58,355 [INFO] root: Total time: 91.94s
2018-08-08 19:24:58,355 [INFO] root: Average throughput: 1631.56/sec
```

## ReadPipeline Object

The `ReadPipeline` object is a blend of the "Callback Full Pipeline" and
"Future Full Throttle" request methods.

To the developer, the `ReadPipeline` is initialized with a Cassandra `session`
and `ReadPipeline.execute()` is called for each asynchronous request.
`ReadPipeline.results()` is used to consume all requests in FIFO ordering.

Internally, the `ReadPipeline` creates a request `Queue` which is executed
and managed via `ResponseFuture` callbacks. If the number of unconsumed results
is too large, no new requests are created.
`ReadPipeline.results()` iterates over a `Queue` of `ResponseFuture` objects
and allows pending requests to be executed asynchronously each time an
additional result is consumed.

```
+ python benchmarks/pipeline.py --num-ops 150000 -H 172.30.0.56 --read
2018-08-08 19:25:59,768 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-08 19:25:59,900 [INFO] root: ==== AsyncoreConnection ====
2018-08-08 19:27:46,331 [INFO] root: Total time: 103.91s
2018-08-08 19:27:46,331 [INFO] root: Average throughput: 1443.52/sec
2018-08-08 19:27:46,331 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-08 19:27:46,563 [INFO] root: ==== LibevConnection ====
2018-08-08 19:29:31,621 [INFO] root: Total time: 102.48s
2018-08-08 19:29:31,622 [INFO] root: Average throughput: 1463.72/sec
2018-08-08 19:29:31,622 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-08 19:29:31,751 [INFO] root: ==== TwistedConnection ====
2018-08-08 19:31:27,978 [INFO] root: Total time: 113.79s
2018-08-08 19:31:27,978 [INFO] root: Average throughput: 1318.23/sec
```

## Future Batches

This request method fires off a set number of asynchronous requests and adds
these `ResponseFuture` objects to the future `Queue`.

Each time the future `Queue` reaches its maximum capacity, the future `Queue`
is read from in its *entirety* before creating another asynchronous request
that is added back to the future `Queue`.

Before exiting, all futures within the future `Queue` are consumed to ensure
all pending requests have been processed.

```
+ python benchmarks/future_batches.py --num-ops 150000 -H 172.30.0.56 --read
2018-08-07 04:46:51,271 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:46:51,403 [INFO] root: ==== AsyncoreConnection ====
2018-08-07 04:48:24,745 [INFO] root: Total time: 90.75s
2018-08-07 04:48:24,745 [INFO] root: Average throughput: 1652.84/sec
2018-08-07 04:48:24,745 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:48:24,976 [INFO] root: ==== LibevConnection ====
2018-08-07 04:49:55,937 [INFO] root: Total time: 88.45s
2018-08-07 04:49:55,937 [INFO] root: Average throughput: 1695.87/sec
2018-08-07 04:49:55,937 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:49:56,068 [INFO] root: ==== TwistedConnection ====
2018-08-07 04:51:46,465 [INFO] root: Total time: 107.96s
2018-08-07 04:51:46,465 [INFO] root: Average throughput: 1389.43/sec
```

## Future Full Pipeline

This request method fires off a set number of asynchronous requests and adds
these `ResponseFuture` objects to the future `Queue`.

Once the the future `Queue` reaches its maximum capacity, the future `Queue`
consumes a *single* future before creating a *single* asynchronous request
that is added back to the future `Queue`.

Before exiting, all futures within the future `Queue` are consumed to ensure
all pending requests have been processed.

```
+ python benchmarks/future_full_pipeline.py --num-ops 150000 -H 172.30.0.56 --read
2018-08-07 04:51:48,991 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:51:49,123 [INFO] root: ==== AsyncoreConnection ====
2018-08-07 04:53:27,992 [INFO] root: Total time: 96.27s
2018-08-07 04:53:27,993 [INFO] root: Average throughput: 1558.16/sec
2018-08-07 04:53:27,993 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:53:28,222 [INFO] root: ==== LibevConnection ====
2018-08-07 04:55:04,824 [INFO] root: Total time: 94.00s
2018-08-07 04:55:04,824 [INFO] root: Average throughput: 1595.78/sec
2018-08-07 04:55:04,824 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 04:55:04,955 [INFO] root: ==== TwistedConnection ====
2018-08-07 04:56:58,690 [INFO] root: Total time: 111.22s
2018-08-07 04:56:58,690 [INFO] root: Average throughput: 1348.73/sec
```

## Future Full Throttle

This request method fires off all asynchronous requests at the same time.

Before exiting, all futures within the future `List` are consumed to ensure
all pending requests have been processed.

Assuming the Cassandra cluster is not overloaded by the large number of
parallel requests and `OperationTimedOut` exceptions are not raised, this
request method then synchronously consumes each future in a FIFO ordering even
though the requests were made asynchronously.

While FIFO verification of write requests may not be required, often
times FIFO consumption of read requests are required since the resulting
data may be merged with known data.

**Note:** The following execution could not complete successfully due to the
large number of parallel requests. The number of operations was divided by 10
in order to gain a basic idea of throughput.

```
+ python benchmarks/future_full_throttle.py --num-ops 15000 -H 172.30.0.56 --read
2018-08-07 05:19:45,419 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 05:19:45,651 [INFO] root: ==== AsyncoreConnection ====
2018-08-07 05:19:59,715 [INFO] root: Total time: 11.45s
2018-08-07 05:19:59,716 [INFO] root: Average throughput: 1309.58/sec
2018-08-07 05:19:59,716 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 05:19:59,946 [INFO] root: ==== LibevConnection ====
2018-08-07 05:20:12,663 [INFO] root: Total time: 10.24s
2018-08-07 05:20:12,663 [INFO] root: Average throughput: 1464.50/sec
2018-08-07 05:20:12,663 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 05:20:12,893 [INFO] root: ==== TwistedConnection ====
2018-08-07 05:20:25,810 [INFO] root: Total time: 10.52s
2018-08-07 05:20:25,810 [INFO] root: Average throughput: 1425.74/sec
```

## Synchronous Requests

This request method fires off each request synchronously.

Since only *one* request is in-flight at a given time, this will be the slowest
request method.

```
+ python benchmarks/sync.py --num-ops 150000 -H 172.30.0.56 --read
2018-08-07 05:08:30,524 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 05:08:30,757 [INFO] root: ==== AsyncoreConnection ====
2018-08-07 05:10:32,994 [INFO] root: Total time: 119.74s
2018-08-07 05:10:32,994 [INFO] root: Average throughput: 1252.74/sec
2018-08-07 05:10:32,994 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 05:10:33,125 [INFO] root: ==== LibevConnection ====
2018-08-07 05:12:27,412 [INFO] root: Total time: 111.74s
2018-08-07 05:12:27,413 [INFO] root: Average throughput: 1342.39/sec
2018-08-07 05:12:27,413 [INFO] root: Using 'cassandra' package from ['/usr/local/lib/python2.7/dist-packages/cassandra_driver-3.14.0-py2.7-linux-x86_64.egg/cassandra']
2018-08-07 05:12:27,543 [INFO] root: ==== TwistedConnection ====
2018-08-07 05:14:41,783 [INFO] root: Total time: 131.86s
2018-08-07 05:14:41,783 [INFO] root: Average throughput: 1137.60/sec
```

# Pipeline Tuning

In order to easily tune `WritePipeline` and `ReadPipeline` parameters against
different sized clusters running in different network environments on different
hardware, `./benchmarks/pipeline_tuning.py` has been created to iteratively
run the `Pipeline` benchmark using increasing parameter values.

At the end of the tuning benchmark, the fastest runs are used to create an
average of the tunable parameter values. These values are good starting points
for future performance tuning with that specific Cassandra cluster.
