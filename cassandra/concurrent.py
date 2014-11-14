# Copyright 2013-2014 DataStax, Inc.
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

import six
import sys

from itertools import count, cycle
import logging
from six.moves import xrange
from threading import Event

from cassandra.cluster import PagedResult

log = logging.getLogger(__name__)


def execute_concurrent(session, statements_and_parameters, concurrency=100, raise_on_first_error=True):
    """
    Executes a sequence of (statement, parameters) tuples concurrently.  Each
    ``parameters`` item must be a sequence or :const:`None`.

    A sequence of ``(success, result_or_exc)`` tuples is returned in the same
    order that the statements were passed in.  If ``success`` is :const:`False`,
    there was an error executing the statement, and ``result_or_exc`` will be
    an :class:`Exception`.  If ``success`` is :const:`True`, ``result_or_exc``
    will be the query result.

    If `raise_on_first_error` is left as :const:`True`, execution will stop
    after the first failed statement and the corresponding exception will be
    raised.

    The `concurrency` parameter controls how many statements will be executed
    concurrently.  It is recommended that this be kept below the number of
    core connections per host times the number of connected hosts (see
    :meth:`.Cluster.set_core_connections_per_host`).  If that amount is exceeded,
    the event loop thread may attempt to block on new connection creation,
    substantially impacting throughput.

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

    """
    if concurrency <= 0:
        raise ValueError("concurrency must be greater than 0")

    if not statements_and_parameters:
        return []

    # TODO handle iterators and generators naturally without converting the
    # whole thing to a list.  This would require not building a result
    # list of Nones up front (we don't know how many results there will be),
    # so a dict keyed by index should be used instead.  The tricky part is
    # knowing when you're the final statement to finish.
    statements_and_parameters = list(statements_and_parameters)

    event = Event()
    first_error = [] if raise_on_first_error else None
    to_execute = len(statements_and_parameters)
    results = [None] * to_execute
    num_finished = count(1)
    statements = enumerate(iter(statements_and_parameters))
    for i in xrange(min(concurrency, len(statements_and_parameters))):
        _execute_next(_sentinel, i, event, session, statements, results, None, num_finished, to_execute, first_error)

    event.wait()
    if first_error:
        exc = first_error[0]
        if six.PY2 and isinstance(exc, tuple):
            (exc_type, value, traceback) = exc
            six.reraise(exc_type, value, traceback)
        else:
            raise exc
    else:
        return results


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
    return execute_concurrent(session, list(zip(cycle((statement,)), parameters)), *args, **kwargs)


_sentinel = object()


def _handle_error(error, result_index, event, session, statements, results,
                  future, num_finished, to_execute, first_error):
    if first_error is not None:
        first_error.append(error)
        event.set()
        return
    else:
        results[result_index] = (False, error)
        if next(num_finished) >= to_execute:
            event.set()
            return

    try:
        (next_index, (statement, params)) = next(statements)
    except StopIteration:
        return

    try:
        future = session.execute_async(statement, params)
        args = (next_index, event, session, statements, results, future, num_finished, to_execute, first_error)
        future.add_callbacks(
            callback=_execute_next, callback_args=args,
            errback=_handle_error, errback_args=args)
    except Exception as exc:
        if first_error is not None:
            if six.PY2:
                first_error.append(sys.exc_info())
            else:
                first_error.append(exc)
            event.set()
            return
        else:
            results[next_index] = (False, exc)
            if next(num_finished) >= to_execute:
                event.set()
                return


def _execute_next(result, result_index, event, session, statements, results,
                  future, num_finished, to_execute, first_error):
    if result is not _sentinel:
        if future.has_more_pages:
            result = PagedResult(future, result)
            future.clear_callbacks()
        results[result_index] = (True, result)
        finished = next(num_finished)
        if finished >= to_execute:
            event.set()
            return

    try:
        (next_index, (statement, params)) = next(statements)
    except StopIteration:
        return

    try:
        future = session.execute_async(statement, params)
        args = (next_index, event, session, statements, results, future, num_finished, to_execute, first_error)
        future.add_callbacks(
            callback=_execute_next, callback_args=args,
            errback=_handle_error, errback_args=args)
    except Exception as exc:
        if first_error is not None:
            if six.PY2:
                first_error.append(sys.exc_info())
            else:
                first_error.append(exc)
            event.set()
            return
        else:
            results[next_index] = (False, exc)
            if next(num_finished) >= to_execute:
                event.set()
                return
