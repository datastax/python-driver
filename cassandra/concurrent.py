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
from itertools import count, cycle
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

# a sentinel to detect if a kwarg was using the default value or was passed
# a user-defined parameter
PROTOCOL_DEFAULT = object()


class WritePipeline(object):
    def __init__(self, session,
                 max_in_flight_requests=PROTOCOL_DEFAULT,
                 max_unconfirmed_write_requests=5000,
                 max_unconsumed_read_responses=False,
                 error_handler=None):
        # the Cassandra session object
        self.session = session

        # set max_in_flight_requests if provided
        if max_in_flight_requests is not PROTOCOL_DEFAULT:
            self.max_in_flight_requests = max_in_flight_requests
        else:
            # else, use the recommended defaults for newer protocol versions
            if self.session._protocol_version >= 3:
                self.max_in_flight_requests = 1000
            else:
                self.max_in_flight_requests = 100

        # set the maximum number of unsent write requests before the Pipeline
        # blocks to ensure that all in-flight write requests have been
        # processed and confirmed to not have thrown any exceptions.
        # ignore the maximum size of pending statements if set to None/0/False
        self.max_unconfirmed_write_requests = max_unconfirmed_write_requests

        # set the maximum number of unconsumed futures to hold onto
        # before continuing to process more pending read requests
        self.max_unconsumed_read_responses = max_unconsumed_read_responses

        # use a custom error_handler function upon future.result() errors
        self.error_handler = error_handler

        # hold futures for the ReadPipeline superclass
        self.futures = Queue()

        # store all pending PreparedStatements along with matching args/kwargs
        self.statements = Queue()

        # track the number of in-flight futures and completed statements
        self.num_started = count()
        self.num_finished = count()

        # track when all pending statements and futures have returned
        self.completed_futures = Event()

        # ensure that self.completed_futures will never be set() between:
        # 1. emptying the self.statements
        # 2. creating the last future
        self.executing_lock = Lock()

        # ensure that we are not using settings for the ReadPipeline within
        # the WritePipeline, or vice versa
        if self.max_unconfirmed_write_requests \
                and self.max_unconsumed_read_responses:
            raise ValueError('The pipeline can either be a Read or Write'
                             ' Pipeline, not both. As such,'
                             ' max_unconfirmed_write_requests and'
                             ' max_unconsumed_read_responses'
                             ' cannot both be non-zero.')

    def __enter__(self):
        # add with-block support
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # ensure all writes are confirmed when exiting the with-block
        self.confirm()

    def __future_callback(self, previous_result=RESULT_SENTINEL):
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
            with self.executing_lock:
                # if there are no more pending statements and all in-flight
                # futures have returned set self.completed_futures to True
                if self.statements.empty() \
                        and next(self.num_finished) >= self.num_started:
                    self.completed_futures.set()

        # attempt to process the another request
        self.__maximize_in_flight_requests()

    def __maximize_in_flight_requests(self):
        # convert pending statements to in-flight futures if we haven't hit our
        # soft concurrency threshold
        if self.num_started - self.num_finished > self.max_in_flight_requests:
            return

        # convert pending statements to in-flight futures if there aren't too
        # many futures that have not been processed.
        # if there are too many futures, wait until ReadPipeline.results()
        # has been called to start consuming futures and processing new
        # statements in parallel with potentially costly business logic
        if self.max_unconsumed_read_responses \
                and self.futures.qsize() > self.max_unconsumed_read_responses:
            return

        # ensure self.completed_futures is never set to True when we
        # are processing the very last pending statement
        with self.executing_lock:
            # grab the next statement, if still available
            try:
                args, kwargs = self.statements.get_nowait()
            except Empty:
                # exit early if there are no more statements to process
                return

            # keep track of the number of in-flight requests
            next(self.num_started)

        # send the statement to Cassandra and await for the future's callback
        future = self.session.execute_async(*args, **kwargs)
        future.add_callbacks(self.__future_callback,
                             self.__future_callback)

        # if we're processing read requests,
        # hold onto the future for later processing
        if self.max_unconsumed_read_responses:
            self.futures.put(future)

    def execute(self, *args, **kwargs):
        # to ensure maximum throughput, only interact with PreparedStatements
        # as is the best practice
        if not isinstance(args[0], PreparedStatement):
            raise TypeError('Only PreparedStatements are allowed when when'
                            ' using the WritePipeline.')

        # if the soft maximum size of pending statements has been exceeded,
        # wait until all pending statements and in-flight futures have returned
        # ignore the maximum size of pending statements if set to None/0/False
        if self.max_unconfirmed_write_requests \
                and self.statements.qsize() > self.max_unconfirmed_write_requests:
            self.confirm()

        # reset the self.completed_futures Event and block on self.confirm()
        # until the new statement has been processed
        self.completed_futures.clear()

        # add the new statement to the pending statements Queue
        self.statements.put((args, kwargs))

        # attempt to process the newest statement
        self.__maximize_in_flight_requests()

    def confirm(self):
        # block until all pending statements and in-flight futures
        # have returned
        self.completed_futures.wait()


class ReadPipeline(WritePipeline):
    def __init__(self, *args, **kwargs):
        if 'max_unconfirmed_write_requests' in kwargs:
            raise ValueError('A custom max_unconfirmed_write_requests value is'
                             ' not supported for the ReadPipeline.')

        # set max_unconfirmed_write_requests to None to avoid mid-loop confirms()
        # since we need the user to call on results() in order to catch all
        # pending future.results()
        kwargs['max_unconfirmed_write_requests'] = None

        # set a default value for the max_unconsumed_read_responses to store
        # in memory before the user must consume ReadPipeline.results()
        if 'max_unconsumed_read_responses' not in kwargs:
            kwargs['max_unconsumed_read_responses'] = 2000

        super(ReadPipeline, self).__init__(*args, **kwargs)

    def __enter__(self):
        # do not implement a with-block since reads should always be returned
        # to the user
        raise NotImplementedError

    def __exit__(self, *args, **kwargs):
        # not needed since we do not implement a with-block
        raise NotImplementedError

    def confirm(self):
        # reads should always be returned to the user, not simply checked for
        # communication exceptions
        raise NotImplementedError

    def results(self):
        """
        Iterate over and return all read request `future.results()`.

        :return: An iterator of Cassandra `ResultSets`, which in turn are
                 iterators over the rows from a query result.
        :rtype: iter(cassandra.cluster.ResultSet)
        """
        for future in self.futures:
            # always ensure that at least one future has been queued.
            # useful for cases where `max_unconsumed_read_responses == 0`
            self.__maximize_in_flight_requests()

            # yield the ResultSet which in turn is another iterator over the
            # rows within the query's result
            yield future.results()
