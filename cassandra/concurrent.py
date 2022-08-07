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
from threading import Condition
import sys

from cassandra.cluster import ResultSet, EXEC_PROFILE_DEFAULT

import logging
log = logging.getLogger(__name__)


ExecutionResult = namedtuple('ExecutionResult', ['success', 'result_or_exc'])

def execute_concurrent(session, statements_and_parameters, concurrency=100, raise_on_first_error=True, results_generator=False, execution_profile=EXEC_PROFILE_DEFAULT):
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

    `execution_profile` argument is the execution profile to use for this
    request, it is passed directly to :meth:`Session.execute_async`.

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

    executor = ConcurrentExecutorGenResults(session, statements_and_parameters, execution_profile) \
        if results_generator else ConcurrentExecutorListResults(session, statements_and_parameters, execution_profile)
    return executor.execute(concurrency, raise_on_first_error)


class _ConcurrentExecutor(object):

    max_error_recursion = 100

    def __init__(self, session, statements_and_params, execution_profile):
        self.session = session
        self._enum_statements = enumerate(iter(statements_and_params))
        self._execution_profile = execution_profile
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
            future = self.session.execute_async(statement, params, timeout=None, execution_profile=self._execution_profile)
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
