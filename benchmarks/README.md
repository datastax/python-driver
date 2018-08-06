> **NOTE:** The following benchmarks are not indicative of actual Cassandra
> performance and are instead to be used as relative results to compare the
> different request methods.
>
> All tests were done on a local laptop running Cassandra 3.11.2 on OS X.

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
+ python benchmarks/callback_full_pipeline.py --num-ops 50000
2018-08-03 18:05:09,744 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:05:09,744 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:05:09,978 [INFO] root: ==== AsyncoreConnection ====
2018-08-03 18:05:21,523 [INFO] root: Total time: 9.06s
2018-08-03 18:05:21,523 [INFO] root: Average throughput: 5516.36/sec
2018-08-03 18:05:21,523 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:05:21,652 [INFO] root: ==== TwistedConnection ====
2018-08-03 18:05:33,723 [INFO] root: Total time: 9.60s
2018-08-03 18:05:33,723 [INFO] root: Average throughput: 5208.19/sec
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
+ python benchmarks/pipeline.py --num-ops 50000
2018-08-03 18:05:33,947 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:05:33,947 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:05:34,072 [INFO] root: ==== AsyncoreConnection ====
2018-08-03 18:05:36,137 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:05:50,133 [INFO] root: Total time: 13.52s
2018-08-03 18:05:50,134 [INFO] root: Average throughput: 3698.07/sec
2018-08-03 18:05:50,134 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:05:50,258 [INFO] root: ==== TwistedConnection ====
2018-08-03 18:05:52,504 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:06:16,780 [INFO] root: Total time: 23.94s
2018-08-03 18:06:16,780 [INFO] root: Average throughput: 2088.48/sec
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
+ python benchmarks/future_batches.py --num-ops 50000
2018-08-03 18:06:17,364 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:06:17,364 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:06:17,491 [INFO] root: ==== AsyncoreConnection ====
2018-08-03 18:06:34,970 [INFO] root: Total time: 15.02s
2018-08-03 18:06:34,971 [INFO] root: Average throughput: 3329.23/sec
2018-08-03 18:06:34,971 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:06:35,100 [INFO] root: ==== TwistedConnection ====
2018-08-03 18:06:58,580 [INFO] root: Total time: 21.02s
2018-08-03 18:06:58,580 [INFO] root: Average throughput: 2378.37/sec
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
+ python benchmarks/future_full_pipeline.py --num-ops 50000
2018-08-03 18:06:59,129 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:06:59,129 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:06:59,256 [INFO] root: ==== AsyncoreConnection ====
2018-08-03 18:07:18,166 [INFO] root: Total time: 16.41s
2018-08-03 18:07:18,166 [INFO] root: Average throughput: 3046.46/sec
2018-08-03 18:07:18,166 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:07:18,297 [INFO] root: ==== TwistedConnection ====
2018-08-03 18:07:45,822 [INFO] root: Total time: 25.03s
2018-08-03 18:07:45,822 [INFO] root: Average throughput: 1997.40/sec
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
+ python benchmarks/future_full_throttle.py --num-ops 50000
2018-08-03 18:07:46,160 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:07:46,161 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:07:46,286 [INFO] root: ==== AsyncoreConnection ====
2018-08-03 18:08:05,541 [INFO] root: Total time: 16.57s
2018-08-03 18:08:05,541 [INFO] root: Average throughput: 3018.07/sec
2018-08-03 18:08:05,541 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:08:05,663 [INFO] root: ==== TwistedConnection ====
2018-08-03 18:08:34,282 [INFO] root: Total time: 26.11s
2018-08-03 18:08:34,282 [INFO] root: Average throughput: 1914.96/sec
```

## Synchronous Requests

This request method fires off each request synchronously.

Since only *one* request is in-flight at a given time, this will be the slowest
request method.

```
+ python benchmarks/sync.py --num-ops 50000
2018-08-03 18:08:35,390 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:08:35,390 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:08:35,521 [INFO] root: ==== AsyncoreConnection ====
2018-08-03 18:09:11,932 [INFO] root: Total time: 34.01s
2018-08-03 18:09:11,932 [INFO] root: Average throughput: 1470.28/sec
2018-08-03 18:09:11,932 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:09:12,059 [INFO] root: ==== TwistedConnection ====
2018-08-03 18:09:53,698 [INFO] root: Total time: 39.22s
2018-08-03 18:09:53,698 [INFO] root: Average throughput: 1274.94/sec
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
+ python benchmarks/callback_full_pipeline.py --num-ops 50000 --read
2018-08-03 18:09:54,088 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:09:54,089 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:09:54,319 [INFO] root: ==== AsyncoreConnection ====
2018-08-03 18:11:02,393 [INFO] root: Total time: 65.60s
2018-08-03 18:11:02,393 [INFO] root: Average throughput: 762.24/sec
2018-08-03 18:11:02,393 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:11:02,526 [INFO] root: ==== TwistedConnection ====
2018-08-03 18:12:10,430 [INFO] root: Total time: 65.51s
2018-08-03 18:12:10,431 [INFO] root: Average throughput: 763.20/sec
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
+ python benchmarks/pipeline.py --num-ops 50000 --read
2018-08-03 18:12:11,132 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:12:11,132 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:12:11,259 [INFO] root: ==== AsyncoreConnection ====
2018-08-03 18:12:13,333 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:13:14,372 [INFO] root: Total time: 60.69s
2018-08-03 18:13:14,372 [INFO] root: Average throughput: 823.80/sec
2018-08-03 18:13:14,372 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:13:14,504 [INFO] root: ==== TwistedConnection ====
2018-08-03 18:13:16,636 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:14:49,721 [INFO] root: Total time: 92.69s
2018-08-03 18:14:49,721 [INFO] root: Average throughput: 539.44/sec
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
+ python benchmarks/future_batches.py --num-ops 50000 --read
2018-08-03 18:14:50,149 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:14:50,149 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:14:50,387 [INFO] root: ==== AsyncoreConnection ====
2018-08-03 18:15:55,079 [INFO] root: Total time: 62.27s
2018-08-03 18:15:55,080 [INFO] root: Average throughput: 802.95/sec
2018-08-03 18:15:55,080 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:15:55,209 [INFO] root: ==== TwistedConnection ====
2018-08-03 18:17:51,799 [INFO] root: Total time: 114.16s
2018-08-03 18:17:51,799 [INFO] root: Average throughput: 437.98/sec
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
+ python benchmarks/future_full_pipeline.py --num-ops 50000 --read
2018-08-03 18:17:52,305 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:17:52,306 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:17:52,442 [INFO] root: ==== AsyncoreConnection ====
2018-08-03 18:19:05,990 [INFO] root: Total time: 71.09s
2018-08-03 18:19:05,990 [INFO] root: Average throughput: 703.34/sec
2018-08-03 18:19:05,990 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:19:06,122 [INFO] root: ==== TwistedConnection ====
2018-08-03 18:21:01,888 [INFO] root: Total time: 113.35s
2018-08-03 18:21:01,889 [INFO] root: Average throughput: 441.13/sec
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

**Note:** The following execution could not complete successfully on my local
machine due to the large number of parallel requests. The faulty throughput is
omitted below.

```
+ python benchmarks/future_full_throttle.py --num-ops 50000 --read
2018-08-03 18:21:02,576 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:21:02,576 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:21:02,814 [INFO] root: ==== AsyncoreConnection ====
Exception in thread Thread-4:
Traceback (most recent call last):
  File "/usr/local/Cellar/python/2.7.13_1/Frameworks/Python.framework/Versions/2.7/lib/python2.7/threading.py", line 801, in __bootstrap_inner
    self.run()
  File "benchmarks/future_full_throttle.py", line 34, in run
    future.result()
  File "/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra/cluster.py", line 4033, in result
    raise self._final_exception
OperationTimedOut: errors={'127.0.0.1': 'Client request timeout. See Session.execute[_async](timeout)'}, last_host=127.0.0.1

2018-08-03 18:21:35,466 [INFO] root: Total time: --.--s
2018-08-03 18:21:35,467 [INFO] root: Average throughput: --/sec
2018-08-03 18:21:35,472 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:21:35,764 [INFO] root: ==== TwistedConnection ====
2018-08-03 18:23:06,036 [INFO] root: Total time: 87.72s
2018-08-03 18:23:06,037 [INFO] root: Average throughput: 569.97/sec
```

## Synchronous Requests

This request method fires off each request synchronously.

Since only *one* request is in-flight at a given time, this will be the slowest
request method.

```
+ python benchmarks/sync.py --num-ops 50000 --read
2018-08-03 18:23:07,726 [WARNING] root: Not benchmarking libev reactor because libev is not available
2018-08-03 18:23:07,726 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:23:07,852 [INFO] root: ==== AsyncoreConnection ====
2018-08-03 18:24:50,007 [INFO] root: Total time: 99.68s
2018-08-03 18:24:50,007 [INFO] root: Average throughput: 501.60/sec
2018-08-03 18:24:50,007 [INFO] root: Using 'cassandra' package from ['/Users/joaquin/repos/tlp/python-driver/benchmarks/../cassandra']
2018-08-03 18:24:50,244 [INFO] root: ==== TwistedConnection ====
2018-08-03 18:26:23,052 [INFO] root: Total time: 90.39s
```

# Pipeline Tuning

In order to easily tune `WritePipeline` and `ReadPipeline` parameters against
different sized clusters running in different network environments on different
hardware, `./benchmarks/pipeline_tuning.py` has been created to iteratively
run the `Pipeline` benchmark using increasing parameter values.

At the end of the tuning benchmark, the fastest runs are used to create an
average of the tunable parameter values. These values are good starting points
for future performance tuning with that specific Cassandra cluster.
