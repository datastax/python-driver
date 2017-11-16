# Copyright 2013-2017 DataStax, Inc.
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

from __future__ import absolute_import
from concurrent.futures import Future

__all__ = ['CQLEngineFuture', 'CQLEngineFutureWaiter']


class CQLEngineFuture(Future):
    """
    CQLEngineFuture provides a specialized `concurrent.futures.Future` that can wrap a `CQLEngineFuture` or a
    `ResponseFuture` to work with the internal of CQLEngine.
    """

    _future = None
    _post_processing = None
    _result = None

    def __init__(self, future=None, post_processing=None, result=None):
        super(CQLEngineFuture, self).__init__()
        self._future = future

        if self._future is None:
            self.set_result(result)
        else:
            if post_processing:
                self._post_processing = post_processing

            self._set_up_completion_callbacks()

    def _set_up_completion_callbacks(self):
        if isinstance(self._future, (CQLEngineFuture, CQLEngineFutureWaiter)):
            self._future.add_done_callback(self._future_callback)
        else:  # ResponseFuture
            self._future.add_callbacks(self._response_future_callback, self._response_future_errback)

    def _execute_post_processing(self, results):
        """CQLEngine post-processing execution"""
        # Execute all post_processing functions
        if self._post_processing:
            results = self._post_processing(results)
        return results

    def _future_callback(self, _):
        """Callback handler for `Future` types (e.g CQLEngineFuture). This is mostly used internally
        when no request to server is required and/or we need to wrap another CQLEngineFuture"""
        try:
            results = _.result()  # raise if error
            results = self._execute_post_processing(results)
            self.set_result(results)
        except Exception as e:
            self.set_exception(e)

    def _response_future_callback(self, _):
        """Callback handler for `ResponseFuture` type"""
        try:
            if self._future.result().has_more_pages:
                # When more pages have to be fetched for materialization,
                # we cannot execute this in the main RF callback
                self._future.session.cluster.executor.submit(self._handle_response_future)
            else:
                self._handle_response_future()
        except Exception as e:
            self.set_exception(e)

    def _response_future_errback(self, exc):
        """Errback handler for `ResponseFuture` type"""
        self.set_exception(exc)

    def _handle_response_future(self):
        """
        Handle the result of a `ResponseFuture`:

        1- Fetch and materialize all rows if the response future has_more_pages
        2- Execute internal cqlengine post_processing functions
        """
        try:
            result_set = self._future.result()
            self._future.clear_callbacks()

            # Ensure all rows are fetched and materialized.. important with fetch_size()
            results = list(result_set)

            results = self._execute_post_processing(results)
            self.set_result(results)
        except Exception as e:
            self.set_exception(e)


class CQLEngineFutureWaiter(Future):
    """Wrap and wait that all futures are done."""

    _count = 0
    _futures = None

    def __init__(self, futures):
        super(CQLEngineFutureWaiter, self).__init__()
        self._futures = futures
        self._count = len(self._futures)

        if self._futures:
            for future in self._futures:
                future.add_done_callback(self._set_if_done)
        else:
            self.set_result(None)

    def _set_if_done(self, _):
        self._count -= 1
        if self._count == 0:
            for future in self._futures:
                if future.exception() is not None:
                    self.set_exception(future.exception())
                    break
            self.set_result(None)  # No result, it's just a waiter

