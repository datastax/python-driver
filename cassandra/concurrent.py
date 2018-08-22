# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from collections import namedtuple
from heapq import heappush, heappop
from itertools import cycle
import six
from six.moves import xrange, zip
from six.moves.queue import Queue, Empty
from threading import Condition, Event, Lock
import sys

from cassandra.cluster import ResultSet
from cassandra.query import PreparedStatement

import logging
log = logging.getLogger(__name__)


ExecutionResult = namedtuple('ExecutionResult', ['success', 'result_or_exc'])

def execute_concurrent(session, statements_and_parameters, concurrency=100, raise_on_first_error=True, results_generator=False):
    """
    Executes a sequence of (statement, parameters) tuples concurrently.  Each
    ``parameters`` item must be a sequence or :const:`None`.

    The `concurrency` parameter controls how many statements will be executed
    concurrently.  When :attr:`.Cluster.protocol_version` is set to 1 or 2,
    it is recommended that this be kept below 100 times the number of
    core connections per host times the number of connected hosts (see
    :meth:`.Cluster.set_core_connections_per_host`).  If that amount is exceeded,
    the event loop thread may attempt to block on new connection creation,
    substantially impacting throughput.  If :attr:`~.Cluster.protocol_version`
    is 3 or higher, you can safely experiment with higher levels of concurrency.

    If `raise_on_first_error` is left as :const:`True`, execution will stop
    after the first failed statement and the corresponding exception will be
    raised.

    `results_generator` controls how the results are returned.

    * If :const:`False`, the results are returned only after all requests have completed.
    * If :const:`True`, a generator expression is returned. Using a generator results in a constrained
      memory footprint when the results set will be large -- results are yielded
      as they return instead of materializing the entire list at once. The trade for lower memory
      footprint is marginal CPU overhead (more thread coordination and sorting out-of-order results
      on-the-fly).

    A sequence of ``ExecutionResult(success, result_or_exc)`` namedtuples is returned
    in the same order that the statements were passed in.  If ``success`` is :const:`False`,
    there was an error executing the statement, and ``result_or_exc`` will be
    an :class:`Exception`.  If ``success`` is :const:`True`, ``result_or_exc``
    will be the query result.

    Example usage::

        select_statement = session.prepare("SELECT * FROM users WHERE id=?")

        statements_and_params = []
        for user_id in user_ids:
            params = (user_id, )
            statements_and_params.append((select_statement, params))

        results = execute_concurrent(
            session, statements_and_params, raise_on_first_error=False)

        for (success, result) in results:
            if not success:
                handle_error(result)  # result will be an Exception
            else:
                process_user(result[0])  # result will be a list of rows

    Note: in the case that `generators` are used, it is important to ensure the consumers do not
    block or attempt further synchronous requests, because no further IO will be processed until
    the consumer returns. This may also produce a deadlock in the IO event thread.
    """
    if concurrency <= 0:
        raise ValueError("concurrency must be greater than 0")

    if not statements_and_parameters:
        return []

    executor = ConcurrentExecutorGenResults(session, statements_and_parameters) if results_generator else ConcurrentExecutorListResults(session, statements_and_parameters)
    return executor.execute(concurrency, raise_on_first_error)


class _ConcurrentExecutor(object):

    max_error_recursion = 100

    def __init__(self, session, statements_and_params):
        self.session = session
        self._enum_statements = enumerate(iter(statements_and_params))
        self._condition = Condition()
        self._fail_fast = False
        self._results_queue = []
        self._current = 0
        self._exec_count = 0
        self._exec_depth = 0

    def execute(self, concurrency, fail_fast):
        self._fail_fast = fail_fast
        self._results_queue = []
        self._current = 0
        self._exec_count = 0
        with self._condition:
            for n in xrange(concurrency):
                if not self._execute_next():
                    break
        return self._results()

    def _execute_next(self):
        # lock must be held
        try:
            (idx, (statement, params)) = next(self._enum_statements)
            self._exec_count += 1
            self._execute(idx, statement, params)
            return True
        except StopIteration:
            pass

    def _execute(self, idx, statement, params):
        self._exec_depth += 1
        try:
            future = self.session.execute_async(statement, params, timeout=None)
            args = (future, idx)
            future.add_callbacks(
                callback=self._on_success, callback_args=args,
                errback=self._on_error, errback_args=args)
        except Exception as exc:
            # exc_info with fail_fast to preserve stack trace info when raising on the client thread
            # (matches previous behavior -- not sure why we wouldn't want stack trace in the other case)
            e = sys.exc_info() if self._fail_fast and six.PY2 else exc

            # If we're not failing fast and all executions are raising, there is a chance of recursing
            # here as subsequent requests are attempted. If we hit this threshold, schedule this result/retry
            # and let the event loop thread return.
            if self._exec_depth < self.max_error_recursion:
                self._put_result(e, idx, False)
            else:
                self.session.submit(self._put_result, e, idx, False)
        self._exec_depth -= 1

    def _on_success(self, result, future, idx):
        future.clear_callbacks()
        self._put_result(ResultSet(future, result), idx, True)

    def _on_error(self, result, future, idx):
        self._put_result(result, idx, False)

    @staticmethod
    def _raise(exc):
        if six.PY2 and isinstance(exc, tuple):
            (exc_type, value, traceback) = exc
            six.reraise(exc_type, value, traceback)
        else:
            raise exc


class ConcurrentExecutorGenResults(_ConcurrentExecutor):

    def _put_result(self, result, idx, success):
        with self._condition:
            heappush(self._results_queue, (idx, ExecutionResult(success, result)))
            self._execute_next()
            self._condition.notify()

    def _results(self):
        with self._condition:
            while self._current < self._exec_count:
                while not self._results_queue or self._results_queue[0][0] != self._current:
                    self._condition.wait()
                while self._results_queue and self._results_queue[0][0] == self._current:
                    _, res = heappop(self._results_queue)
                    try:
                        self._condition.release()
                        if self._fail_fast and not res[0]:
                            self._raise(res[1])
                        yield res
                    finally:
                        self._condition.acquire()
                    self._current += 1


class ConcurrentExecutorListResults(_ConcurrentExecutor):

    _exception = None

    def execute(self, concurrency, fail_fast):
        self._exception = None
        return super(ConcurrentExecutorListResults, self).execute(concurrency, fail_fast)

    def _put_result(self, result, idx, success):
        self._results_queue.append((idx, ExecutionResult(success, result)))
        with self._condition:
            self._current += 1
            if not success and self._fail_fast:
                if not self._exception:
                    self._exception = result
                self._condition.notify()
            elif not self._execute_next() and self._current == self._exec_count:
                self._condition.notify()

    def _results(self):
        with self._condition:
            while self._current < self._exec_count:
                self._condition.wait()
                if self._exception and self._fail_fast:
                    self._raise(self._exception)
        if self._exception and self._fail_fast:  # raise the exception even if there was no wait
            self._raise(self._exception)
        return [r[1] for r in sorted(self._results_queue)]



def execute_concurrent_with_args(session, statement, parameters, *args, **kwargs):
    """
    Like :meth:`~cassandra.concurrent.execute_concurrent()`, but takes a single
    statement and a sequence of parameters.  Each item in ``parameters``
    should be a sequence or :const:`None`.

    Example usage::

        statement = session.prepare("INSERT INTO mytable (a, b) VALUES (1, ?)")
        parameters = [(x,) for x in range(1000)]
        execute_concurrent_with_args(session, statement, parameters, concurrency=50)
    """
    return execute_concurrent(session, zip(cycle((statement,)), parameters), *args, **kwargs)


# a sentinel to detect if a kwarg was using the default value or was passed
# a real result
RESULT_SENTINEL = object()


class Pipeline(object):
    """
    This ``Pipeline`` object is not meant to be used by itself and instead
    should be used via the :class:`cassandra.concurrent.WritePipeline` or
    :class:`cassandra.concurrent.ReadPipeline` objects.

    This ``Pipeline`` object is missing fundamental functionality to ensure
    write requests are processed or read requests are able to be read.
    """

    session = None
    """
    A :class:`cassandra.cluster.Session` object used by the ``Pipeline``
    to send Cassandra requests.
    """

    max_in_flight_requests = 100
    """
    The maximum number of in-flight requests that have yet to return responses.

    A default of 100 in-flight requests for the ``WritePipeline`` and 100 for
    the ``ReadPipeline`` are ideal for a small single node cluster. Performance
    tuning would be ideal to see the sort of throughput each individual cluster
    can achieve. Keep in mind that less in-flight requests are ideal for older
    versions of Cassandra.

    As :attr:`cassandra.concurrent.Pipeline.max_in_flight_requests` is reached,
    statements are queued up for execution as soon as a
    :class:`cassandra.cluster.ResponseFuture` is returned.
    """

    max_unsent_write_requests = 40000
    """
    This value is specifically for a ``WritePipeline`` to set the maximum
    number of queued write requests that have yet to be delivered to Cassandra.

    This can be set to `None`, `0`, or `False` to avoid any limitation on the
    number of queued write requests that have yet to be processed, but the
    user should be aware of the possibility of running out of memory.

    As :attr:`cassandra.concurrent.Pipeline.max_unsent_write_requests` is
    reached, :meth:`cassandra.concurrent.Pipeline.confirm()` is called to
    ensure all unsent write requests are sent and return without error in an
    effort to alleviate memory pressures from the queued unsent requests.
    """

    max_unconsumed_read_responses = False
    """
    This value is specifically for a ``ReadPipeline`` to alleviate memory
    pressure. If this limit is reached and there are already plenty of
    :class:`cassandra.cluster.ResponseFuture` results that have yet to be read,
    no further requests will be sent to Cassandra until
    :meth:`cassandra.concurrent.ReadPipeline.results()` is called.

    Once :meth:`cassandra.concurrent.ReadPipeline.results()` is called and a
    single future is consumed, another request will be immediately sent to
    Cassandra. We will continue to process new requests until the number of
    in-flight requests is saturated or this limit is again reached.

    To avoid out of memory exceptions, be sure to periodically read from the
    ``ReadPipeline`` by calling
    :meth:`cassandra.concurrent.ReadPipeline.results()` especially when dealing
    with large read results since these will be stored in memory until
    consumption.
    """

    error_handler = None
    """
    Allows for custom error handlers to be implemented instead of simply
    raising the first encountered Cassandra request exception.

    Other implementations may call for a log of failed requests or immediate
    retries of the failed request.
    """

    allow_non_performant_queries = False
    """
    Only :class:`cassandra.query.PreparedStatement` requests are allowed when
    using the ``Pipeline``, by default. If other Statement types must be used,
    set :attr:`cassandra.concurrent.Pipeline.allow_non_performant_queries` to
    `True` with the understanding that there may some performance hit since
    :class:`cassandra.query.SimpleStatement` queries will require server-side
    processing, :class:`cassandra.query.BoundStatement` requests can be
    generated on the fly by using :class:`cassandra.query.PreparedStatement`
    requests and bind values, and :class:`cassandra.query.BatchStatement`
    requests should only contain mutations targeting the same partition to
    avoid a Cassandra anti-pattern.
    """

    def __init__(self, session,
                 max_in_flight_requests=100,
                 max_unsent_write_requests=40000,
                 max_unconsumed_read_responses=False,
                 error_handler=None,
                 allow_non_performant_queries=False):
        # the Cassandra session object
        self.session = session

        # set max_in_flight_requests
        self.max_in_flight_requests = max_in_flight_requests

        # set the maximum number of unsent write requests before the Pipeline
        # blocks to ensure that all in-flight write requests have been
        # processed and confirmed to not have thrown any exceptions.
        # ignore the maximum size of pending statements if set to None/0/False
        self.max_unsent_write_requests = max_unsent_write_requests

        # set the maximum number of unconsumed futures to hold onto
        # before continuing to process more pending read requests
        self.max_unconsumed_read_responses = max_unconsumed_read_responses

        # use a custom error_handler function upon future.result() errors
        self.error_handler = error_handler

        # allow for Statements, BoundStatements, and BatchStatements to be
        # processed. By default, only PreparedStatements are processed.
        self.allow_non_performant_queries = allow_non_performant_queries

        # hold futures for the ReadPipeline superclass
        self.futures = Queue()

        # store all pending PreparedStatements along with matching args/kwargs
        self.statements = Queue()

        # track when all pending statements and futures have returned
        self.completed_requests = Event()

        # track the number of in-flight futures and completed statements
        # always to be used with an in_flight_counter_lock
        self.in_flight_counter = 0

        # ensure that self.completed_requests will never be set() between:
        # 1. emptying the self.statements
        # 2. creating the last future
        self.in_flight_counter_lock = Lock()

        # ensure that we are not using settings for the ReadPipeline within
        # the WritePipeline, or vice versa
        if self.max_unsent_write_requests \
                and self.max_unconsumed_read_responses:
            raise ValueError('The pipeline can either be a Read or Write'
                             ' Pipeline, not both. As such,'
                             ' max_unsent_write_requests and'
                             ' max_unconsumed_read_responses'
                             ' cannot both be non-zero.')

    def __future_callback(self, previous_result=RESULT_SENTINEL):
        """
        This is handled by the
        :meth:`cassandra.cluster.ResponseFuture.add_callbacks()` logic.
        Once a request has been completed, this method is called to ensure
        no errors have occurred and to keep tally of the pending in-flight
        requests.

        If an error has occurred, the exception will immediately be raised
        or handled by :attr:`Pipeline.error_handler`.

        In all cases, calling this method will try to maximize the number
        of in-flight requests.
        """
        # check to see if we're processing a future.result()
        if previous_result is not RESULT_SENTINEL:
            # handle the case where future.result() is an exception
            if isinstance(previous_result, BaseException):
                log.error('Error on statement: %r', previous_result)

                # if an self.error_handler has been defined,
                # handle appropriately
                if self.error_handler:
                    self.error_handler(previous_result)
                else:
                    # else, raise the seen future.result() exception
                    raise previous_result

            # ensure that the last statement within self.statements is not
            # being processed
            with self.in_flight_counter_lock:
                # decrement the number of in-flight requests
                self.in_flight_counter -= 1

                # if there are no more pending statements and all in-flight
                # futures have returned set self.completed_requests to True
                if self.in_flight_counter < 1:
                    if self.statements.empty():
                        if self.in_flight_counter < 0:
                            raise RuntimeError('The in_flight_counter should'
                                               ' never have been less than 0.'
                                               ' The lock mechanism is not'
                                               ' working as expected!')
                        self.completed_requests.set()

        # attempt to process the another request
        self._maximize_in_flight_requests()

    def _maximize_in_flight_requests(self):
        """
        This code is called multiple times within the Pipeline infrastructure
        to ensure we're keeping as many in-flight requests processing as
        possible.

        We're cautious here and default to a no-op in cases where:

        * There are already too many in-flight requests.
        * We already have too many :class:`cassandra.cluster.ResponseFuture`
          objects taking up memory.
        * There are no more statements that need to be processed.

        In all other cases, we call
        :meth:`cassandra.cluster.Session.execute_async()` with the queued
        statement's _args_, _kwargs_, and a callback to
        :meth:`cassandra.concurrent.Pipeline.__future_callback()`.

        In cases where we're using a ReadPipeline, we will store the future
        for later consumption of the :class:`cassandra.cluster.ResponseFuture`
        objects by way of :meth:`cassandra.concurrent.ReadPipeline.results()`.
        """
        # convert pending statements to in-flight futures if we haven't hit our
        # threshold
        if self.in_flight_counter > self.max_in_flight_requests:
            return

        # convert pending statements to in-flight futures if there aren't too
        # many futures that have not been processed.
        # if there are too many futures, wait until ReadPipeline.results()
        # has been called to start consuming futures and processing new
        # statements in parallel with potentially costly business logic
        if self.max_unconsumed_read_responses \
                and self.futures.qsize() > self.max_unconsumed_read_responses:
            return

        # grab the next statement, if still available
        try:
            # keep track of the number of in-flight requests
            with self.in_flight_counter_lock:
                args, kwargs = self.statements.get_nowait()
                self.in_flight_counter += 1
            self.completed_requests.clear()
        except Empty:
            # exit early if there are no more statements to process
            return


        # send the statement to Cassandra and await for the future's callback
        future = self.session.execute_async(*args, **kwargs)
        future.add_callbacks(self.__future_callback,
                             self.__future_callback)

        # if we're processing read requests,
        # hold onto the future for later processing
        if self.max_unconsumed_read_responses:
            self.futures.put(future)

    def execute(self, *args, **kwargs):
        """
        This method passes all _args_ and
        _kwargs_ to :meth:`cassandra.cluster.Session.execute_async()`
        internally to increase familiarity with standard python-driver usage.
        By default, :class:`cassandra.query.PreparedStatement` queries will be
        processed as expected.

        If :class:`cassandra.query.SimpleStatement`,
        :class:`cassandra.query.BoundStatement`,
        and :class:`cassandra.query.BatchStatement` statements need to be
        processed,
        :attr:`cassandra.concurrent.Pipeline.allow_non_performant_queries` will
        need to be set to `True`. :meth:`cassandra.query.BatchStatements`
        should only be used if all statements will modify the same partition to
        avoid Cassandra anti-patterns.

        If using the :class:`cassandra.concurrent.WritePipeline`:

        * When :attr:`Pipeline.max_unsent_write_requests` is reached,
        :meth:`Pipeline.confirm()` is called to ensure all unsent write
        requests are sent and return without error in an effort to alleviate
        memory pressures from the queued unsent requests.

        * Once :meth:`Pipeline.confirm()` is returned, or if
        :attr:`Pipeline.max_unsent_write_requests` is not reached, continue.

        The _args_ and _kwargs_ are placed into the `self.statements`
        Queue and
        :meth:`cassandra.concurrent.Pipeline._maximize_in_flight_requests()`
        is called to process any pending requests in the `self.statements`
        Queue.

        This method returns immediately and continues to process pending
        statements asynchronously without any further interaction.

        If using the :class:`cassandra.concurrent.WritePipeline`:

        * To ensure all pending statements are processed, call
        :meth:`Pipeline.confirm()` manually if using the
        :class:`cassandra.concurrent.WritePipeline`.

        If using the :class:`cassandra.concurrent.ReadPipeline`:

        * All requests can be read by consuming the
        :meth:`cassandra.concurrent.ReadPipeline.results()` iterator.

        :param args: Any _args_ for
                     :meth:`cassandra.cluster.Session.execute_async()`.
        :param kwargs: Any _kwargs_ for
                       :meth:`cassandra.cluster.Session.execute_async()`.
        """
        # to ensure maximum throughput, only interact with PreparedStatements
        # as is the best practice
        if not self.allow_non_performant_queries \
                and not isinstance(args[0], PreparedStatement):
            raise TypeError('Only PreparedStatements are allowed when'
                            ' using the Pipeline. If other Statement types'
                            ' must be used, set'
                            ' Pipeline.self.allow_non_performant_queries to'
                            ' `True` with the understanding that there may'
                            ' some performance hit since SimpleStatements will'
                            ' require server-side processing, BoundStatements'
                            ' can be generated on the fly by using'
                            ' PreparedStatements and bind values, and'
                            ' BatchStatements should only contain mutations'
                            ' targeting the same partition to avoid a'
                            ' Cassandra anti-pattern.')

        # if the soft maximum size of pending statements has been exceeded,
        # wait until all pending statements and in-flight futures have returned
        # ignore the maximum size of pending statements if set to None/0/False
        if self.max_unsent_write_requests \
                and self.statements.qsize() > self.max_unsent_write_requests:
            self.confirm()

        # reset the self.completed_requests Event and block on self.confirm()
        # until the new statement has been processed
        self.completed_requests.clear()

        # add the new statement to the pending statements Queue
        self.statements.put((args, kwargs))

        # attempt to process the newest statement
        self._maximize_in_flight_requests()


class WritePipeline(Pipeline):
    """
    The ``WritePipeline`` is a helper object meant to add high-performance
    write request pipelining to any application with minimal code changes.

    A :class:`cassandra.cluster.Session` object is first created and used
    to initiate the ``WritePipeline``. The ``WritePipeline`` can then be
    used to execute multiple queries asynchronously using
    :meth:`cassandra.concurrent.Pipeline.execute()`.

    As :attr:`cassandra.concurrent.Pipeline.max_in_flight_requests` is reached,
    statements are queued up for execution as soon as a
    :class:`cassandra.cluster.ResponseFuture` is returned.

    As :attr:`cassandra.concurrent.Pipeline.max_unsent_write_requests` is
    reached, :meth:`cassandra.concurrent.Pipeline.confirm()` is called to
    ensure all unsent write requests are sent and return without error in an
    effort to alleviate memory pressures from the queued unsent requests.

    Once all requests have been sent into the ``WritePipeline`` and the
    business logic requires confirmation on the write requests or the
    program is about to exit, :meth:`cassandra.concurrent.Pipeline.confirm()`
    should be called to block until all sent requests have returned and
    confirmed to have been ingested without error.

    :meth:`cassandra.concurrent.Pipeline.execute()` passes all _args_ and
    _kwargs_ to :meth:`cassandra.cluster.Session.execute_async()`
    internally to increase familiarity with standard python-driver usage.
    By default, :class:`cassandra.query.PreparedStatement` queries will be
    processed as expected.

    If :class:`cassandra.query.SimpleStatement`,
    :class:`cassandra.query.BoundStatement`,
    and :class:`cassandra.query.BatchStatement` statements need to be
    processed,
    :attr:`cassandra.concurrent.Pipeline.allow_non_performant_queries` will
    need to be set to `True`. :meth:`cassandra.query.BatchStatements`
    should only be used if all statements will modify the same partition to
    avoid Cassandra anti-patterns.

    Example usage::

    >>> from cassandra.cluster import Cluster
    >>> from cassandra.concurrent import WritePipeline
    >>> cluster = Cluster(['192.168.1.1', '192.168.1.2'])
    >>> session = cluster.connect()
    >>> write_pipeline = WritePipeline(session)
    >>> prepared_statement = session.prepare("INSERT INTO mykeyspace.users (name, age) VALUES (?, ?)")
    >>> write_pipeline.execute(prepared_statement, ('Jorge', 28))
    >>> write_pipeline.execute(prepared_statement, ('Jose', 28))
    >>> write_pipeline.execute(prepared_statement, ('Sara', 25))
    >>> ...
    >>> write_pipeline.confirm()
    >>> ...
    >>> cluster.shutdown()

    The ``WritePipeline`` also functions as a context manager allowing for
    with-block usage. By using the ``with`` keyword there is no longer a
    need to call on :meth:`cassandra.concurrent.Pipeline.confirm()` since the
    context manager will handle that logic upon leaving the scope of the
    with-block.

    Example usage::

    >>> from cassandra.cluster import Cluster
    >>> cluster = Cluster(['192.168.1.1', '192.168.1.2'])
    >>> session = cluster.connect()
    >>> prepared_statement = session.prepare("INSERT INTO mykeyspace.users (name, age) VALUES (?, ?)")
    >>> with WritePipeline(session) as write_pipeline:
    ...    write_pipeline.execute(prepared_statement, ('Jorge', 28))
    ...    write_pipeline.execute(prepared_statement, ('Jose', 28))
    ...    write_pipeline.execute(prepared_statement, ('Sara', 25))
    >>> ...
    >>> cluster.shutdown()
    """

    session = None
    """
    A :class:`cassandra.cluster.Session` object used by the ``Pipeline``
    to send Cassandra requests.
    """

    max_in_flight_requests = 100
    """
    The maximum number of in-flight requests that have yet to return responses.

    A default of 100 in-flight requests for the ``WritePipeline`` and 100 for
    the ``ReadPipeline`` are ideal for a small single node cluster. Performance
    tuning would be ideal to see the sort of throughput each individual cluster
    can achieve. Keep in mind that less in-flight requests are ideal for older
    versions of Cassandra.

    As :attr:`cassandra.concurrent.Pipeline.max_in_flight_requests` is reached,
    statements are queued up for execution as soon as a
    :class:`cassandra.cluster.ResponseFuture` is returned.
    """

    max_unsent_write_requests = 40000
    """
    This value is specifically for a ``WritePipeline`` to set the maximum
    number of queued write requests that have yet to be delivered to Cassandra.

    This can be set to `None`, `0`, or `False` to avoid any limitation on the
    number of queued write requests that have yet to be processed, but the
    user should be aware of the possibility of running out of memory.

    As :attr:`cassandra.concurrent.Pipeline.max_unsent_write_requests` is
    reached, :meth:`cassandra.concurrent.Pipeline.confirm()` is called to
    ensure all unsent write requests are sent and return without error in an
    effort to alleviate memory pressures from the queued unsent requests.
    """

    error_handler = None
    """
    Allows for custom error handlers to be implemented instead of simply
    raising the first encountered Cassandra request exception.

    Other implementations may call for a log of failed requests or immediate
    retries of the failed request.
    """

    allow_non_performant_queries = False
    """
    Only :class:`cassandra.query.PreparedStatement` requests are allowed when
    using the ``Pipeline``, by default. If other Statement types must be used,
    set :attr:`cassandra.concurrent.Pipeline.allow_non_performant_queries` to
    `True` with the understanding that there may some performance hit since
    :class:`cassandra.query.SimpleStatement` queries will require server-side
    processing, :class:`cassandra.query.BoundStatement` requests can be
    generated on the fly by using :class:`cassandra.query.PreparedStatement`
    requests and bind values, and :class:`cassandra.query.BatchStatement`
    requests should only contain mutations targeting the same partition to
    avoid a Cassandra anti-pattern.
    """

    def __init__(self, session,
                 max_in_flight_requests=100,
                 max_unsent_write_requests=40000,
                 error_handler=None,
                 allow_non_performant_queries=False):
        super(WritePipeline, self).__init__(session=session,
                                            max_in_flight_requests=max_in_flight_requests,
                                            max_unsent_write_requests=max_unsent_write_requests,
                                            max_unconsumed_read_responses=False,
                                            error_handler=error_handler,
                                            allow_non_performant_queries=allow_non_performant_queries)

    def __enter__(self):
        """
        Adds with-block support.

        :return: A ``WritePipeline`` object.
        :rtype: cassandra.concurrent.WritePipeline
        """
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """
        Ensures all writes are confirmed when exiting with-blocks.

        Failure to confirm all pending requests may cause some requests not to
        be delivered to Cassandra.
        """
        if exception_type:
            log.error(
                'Exception type seen within codeblock: %s' % exception_type)
            log.error(
                'Exception value seen within codeblock: %s' % exception_value)
            log.error(
                'Exception traceback seen within codeblock: %s' % traceback)
            log.info('Attempting to confirm() all other pending writes.')

        self.confirm()

    def confirm(self):
        """
        Blocks until all pending statements and in-flight requests have
        returned.
        """
        # this Event() is set in __future_callback() if all sent requests have
        # returned and cleared each time execute() is called since another
        # statement is added to the `self.statements` Queue
        self.completed_requests.wait()


class ReadPipeline(Pipeline):
    """
    The ``ReadPipeline`` is a helper object meant to add high-performance
    read request pipelining to any application with minimal code changes.

    A :class:`cassandra.cluster.Session` object is first created and used
    to initiate the ``ReadPipeline``. The ``ReadPipeline`` can then be
    used to execute multiple queries asynchronously using
    :meth:`cassandra.concurrent.Pipeline.execute()`.

    As :attr:`cassandra.concurrent.Pipeline.max_in_flight_requests` is reached,
    statements are queued up for execution as soon as a
    :class:`cassandra.cluster.ResponseFuture` is returned.

    Once all requests have been sent into the ``ReadPipeline`` and the
    business logic requires consuming those read requests,
    :meth:`cassandra.concurrent.ReadPipeline.results()` will return an iterator
    of :class:`cassandra.cluster.ResultSet` objects which are generated when
    calling :meth:`cassandra.cluster.ResponseFuture.result()` internally within
    the :class:`cassandra.concurrent.Pipeline`.

    The result from :meth:`cassandra.concurrent.ReadPipeline.results()` is an
    iterator of :class:`cassandra.cluster.ResultSet` objects which in turn
    another iterator over the result's rows and the same type of object
    returned when calling :meth:`cassandra.cluster.Session.execute()`.

    The results from :meth:`cassandra.concurrent.ReadPipeline.results()` follow
    the same ordering as statements that went into
    :meth:`cassandra.concurrent.Pipeline.execute()` without a top-level
    indication of the keyspace, table, nor query that was called that is not
    already accessible within the :class:`cassandra.cluster.ResultSet`.

    It's recommended to keep a dedicated read_pipeline for each query unless
    schemas are identical or business logic will handle the different types of
    returned Cassandra rows.

    :meth:`cassandra.concurrent.Pipeline.execute()` passes all _args_ and
    _kwargs_ to :meth:`cassandra.cluster.Session.execute_async()`
    internally to increase familiarity with standard python-driver usage.
    By default, :class:`cassandra.query.PreparedStatement` queries will be
    processed as expected.

    If :class:`cassandra.query.SimpleStatement`,
    :class:`cassandra.query.BoundStatement`,
    and :class:`cassandra.query.BatchStatement` statements need to be
    processed,
    :attr:`cassandra.concurrent.Pipeline.allow_non_performant_queries` will
    need to be set to `True`. :meth:`cassandra.query.BatchStatements`
    should only be used if all statements will modify the same partition to
    avoid Cassandra anti-patterns.

    Example usage::

    >>> from cassandra.cluster import Cluster
    >>> from cassandra.concurrent import ReadPipeline
    >>> cluster = Cluster(['192.168.1.1', '192.168.1.2'])
    >>> session = cluster.connect()
    >>> read_pipeline = ReadPipeline(session)
    >>> prepared_statement = session.prepare('SELECT * FROM mykeyspace.users WHERE name = ?')
    >>> read_pipeline.execute(prepared_statement, ('Jorge'))
    >>> read_pipeline.execute(prepared_statement, ('Jose'))
    >>> read_pipeline.execute(prepared_statement, ('Sara'))
    >>> prepared_statement = session.prepare('SELECT * FROM old_keyspace.old_users WHERE name = ?')
    >>> read_pipeline.execute(prepared_statement, ('Jorge'))
    >>> read_pipeline.execute(prepared_statement, ('Jose'))
    >>> read_pipeline.execute(prepared_statement, ('Sara'))
    >>> for result in read_pipeline.results():
    ...     for row in result:
    ...         print row.name, row.age
    >>> ...
    >>> cluster.shutdown()
    """

    session = None
    """
    A :class:`cassandra.cluster.Session` object used by the ``Pipeline``
    to send Cassandra requests.
    """

    max_in_flight_requests = 100
    """
    The maximum number of in-flight requests that have yet to return responses.

    A default of 100 in-flight requests for the ``WritePipeline`` and 100 for
    the ``ReadPipeline`` are ideal for a small single node cluster. Performance
    tuning would be ideal to see the sort of throughput each individual cluster
    can achieve. Keep in mind that less in-flight requests are ideal for older
    versions of Cassandra.

    As :attr:`Pipeline.max_in_flight_requests` is reached, statements
    are queued up for execution as soon as a
    :class:`cassandra.cluster.ResponseFuture` is returned.
    """

    max_unconsumed_read_responses = False
    """
    This value is specifically for a ``ReadPipeline`` to alleviate memory
    pressure. If this limit is reached and there are already plenty of
    :class:`cassandra.cluster.ResponseFuture` results that have yet to be read,
    no further requests will be sent to Cassandra until
    :meth:`cassandra.concurrent.ReadPipeline.results()` is called.

    Once :meth:`cassandra.concurrent.ReadPipeline.results()` is called and a
    single future is consumed, another request will be immediately sent to
    Cassandra. We will continue to process new requests until the number of
    in-flight requests is saturated or this limit is again reached.

    To avoid out of memory exceptions, be sure to periodically read from the
    ``ReadPipeline`` by calling
    :meth:`cassandra.concurrent.ReadPipeline.results()` especially when dealing
    with large read results since these will be stored in memory until
    consumption.
    """

    error_handler = None
    """
    Allows for custom error handlers to be implemented instead of simply
    raising the first encountered Cassandra request exception.

    Other implementations may call for a log of failed requests or immediate
    retries of the failed request.
    """

    allow_non_performant_queries = False
    """
    Only :class:`cassandra.query.PreparedStatement` requests are allowed when
    using the ``Pipeline``, by default. If other Statement types must be used,
    set :attr:`cassandra.concurrent.Pipeline.allow_non_performant_queries` to
    `True` with the understanding that there may some performance hit since
    :class:`cassandra.query.SimpleStatement` queries will require server-side
    processing, :class:`cassandra.query.BoundStatement` requests can be
    generated on the fly by using :class:`cassandra.query.PreparedStatement`
    requests and bind values, and :class:`cassandra.query.BatchStatement`
    requests should only contain mutations targeting the same partition to
    avoid a Cassandra anti-pattern.
    """

    def __init__(self, session,
                 max_in_flight_requests=100,
                 max_unconsumed_read_responses=400,
                 error_handler=None,
                 allow_non_performant_queries=False):
        super(ReadPipeline, self).__init__(session=session,
                                           max_in_flight_requests=max_in_flight_requests,
                                           max_unsent_write_requests=False,
                                           max_unconsumed_read_responses=max_unconsumed_read_responses,
                                           error_handler=error_handler,
                                           allow_non_performant_queries=allow_non_performant_queries)

    def results(self):
        """
        Iterate over and return all read request `future.results()`.

        :return: An iterator of Cassandra `ResultSets`, which in turn are
                 iterators over the rows from a query result.
        :rtype: iter(cassandra.cluster.ResultSet)
        """
        while not self.completed_requests.is_set():
            future = self.futures.get()

            # always ensure that at least one future has been queued.
            # useful for cases where `max_unconsumed_read_responses == 0`
            self._maximize_in_flight_requests()

            # yield the ResultSet which in turn is another iterator over the
            # rows within the query's result
            yield future.result()
