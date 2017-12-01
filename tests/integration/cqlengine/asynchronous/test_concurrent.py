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

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from cassandra.cqlengine.connection import get_connection
from cassandra.cluster import OperationTimedOut
from cassandra import InvalidRequest

from cassandra.cqlengine.query import BatchQuery

from tests import notwindows
from tests.integration.cqlengine.base import BaseCassEngTestCaseWithTable, TestMultiKeyModel


from concurrent.futures import wait
from itertools import count


class ConcurrentTests(BaseCassEngTestCaseWithTable):
    text_to_write = 'write this text'

    def test_create_async_from_model(self):
        """
        The test writes using the asynchronous execution from the class
        instead of from the instance to C*. Reads in the same way verifying
        the rows were properly written.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result the appropriate table is populated and the rows
        are retrieved correctly.

        @test_category object_mapper
        """
        self._write_sample_from_model()
        self._verify_reads_from_model()

    def test_create_async_from_instance(self):
        """
        The test writes using the asynchronous execution from the instance
        instead of from the class to C*. Reads in the same way verifying
        the rows were properly written.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result the appropriate table is populated and the rows
        are retrieved correctly.

        @test_category object_mapper
        """
        self._write_sample_from_instance()
        self._verify_reads_from_instance()

    def test_save_async(self):
        """
        The test populates the model :class:`~.TestMultiKeyModel`
        using :meth:`cassandra.cqlengine.models.Model.save_async`. Then verifies
        the rows where written correctly.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result the appropriate C* table is populated and the rows are
        read correctly.

        @test_category object_mapper
        """
        m = TestMultiKeyModel.create_async().result()

        write_futures = []
        for i in range(10):
            m.partition = i
            m.cluster = i
            m.count = 5
            m.text = self.text_to_write
            write_futures.append(m.save_async())

        self._verify_reads_from_model()

    def test_update_async(self):
        """
        The test populates the model :class:`~.TestMultiKeyModel`
        using :meth:`cassandra.cqlengine.models.Model.update_async`. Then verifies
        the rows where written correctly.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result the appropriate C* table is populated and the rows are
        read correctly.

        @test_category object_mapper
        """
        m = TestMultiKeyModel.create_async(partition=0, cluster=0, count=5, text=self.text_to_write).result()

        write_futures = []
        for i in range(10):
            m.partition = i
            m.cluster = i
            m.count = 5
            m.text = self.text_to_write
            write_futures.append(m.update_async())

        wait(write_futures)

        self._verify_reads_from_model()

    def test_update_async_modelqueryset(self):
        """
        The test populates the model :class:`~.TestMultiKeyModel`
        using :meth:`cassandra.cqlengine.query.DMLQuery.update_async`. The difference
        with the previous test is that :meth:`cassandra.cqlengine.models.Model.update_async`
        isn't applied to a particular instance of :class:`~.TestMultiKeyModel` but to the
        result of filtering all the rows in the table. Then verifies the rows where written
        correctly.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result the appropriate C* table is populated and the rows are
        read correctly.

        @test_category object_mapper
        """
        self._write_sample_from_model()
        write_futures = []
        for i in range(10):
            write_futures.append(TestMultiKeyModel.objects.filter(partition=i, cluster=i).
                                 update_async(count=None, text="other_text"))
        wait(write_futures)
        for i in range(10):
            result = TestMultiKeyModel.get_async(partition=i, cluster=i).result()
            self.assertEqual((result.partition, result.cluster, result.count, result.text), (i, i, None, "other_text"))

    def test_delete_async_from_instance(self):
        """
        The test populates the model :class:`~.TestMultiKeyModel`
        using :meth:`cassandra.cqlengine.models.Model.create_async`. Then deletes
        all the populated rows using :meth:`cassandra.cqlengine.models.Model.delete_async`.
        Then verifies the table is empty.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result the table should be empty since all the rows have been deleted.

        @test_category object_mapper
        """
        models = []
        for i in range(10):
            models.append(TestMultiKeyModel.create_async(partition=i, cluster=i, count=5,
                                                         text=self.text_to_write).result())
        self._verify_reads_from_model()

        # None values are treated in a slightly different way
        models.append(TestMultiKeyModel.create_async(partition=11, cluster=11, count=None,
                                                     text=None).result())

        delete_futures = []
        for m in models:
            delete_futures.append(m.delete_async())

        wait(delete_futures)

        self._verify_table_is_empty()

    def test_delete_async(self):
        """
        The test populates the model :class:`~.TestMultiKeyModel`
        using :meth:`cassandra.cqlengine.models.Model.create_async`. Then deletes
        all the populated rows using :meth:`cassandra.cqlengine.query.DMLQuery.delete_async`.
        Then verifies the table is empty. The difference with the previous test is that
        :meth:`cassandra.cqlengine.models.Model.delete_async` isn't applied to a
        particular instance of :class:`~.TestMultiKeyModel` but to the result of
        filtering all the rows in the table.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result the table should be empty since all the rows have been deleted.

        @test_category object_mapper
        """
        self._write_sample_from_model()

        delete_futures = []
        for i in range(10):
            delete_futures.append(TestMultiKeyModel.objects(partition=i, cluster=i).delete_async())

        wait(delete_futures)

        read_futures = []
        for i in range(10):
            read_futures.append(TestMultiKeyModel.get_async(partition=i, cluster=i))

        for i, future in enumerate(read_futures):
            with self.assertRaises(TestMultiKeyModel.DoesNotExist):
                future.result()

    def test_get_all(self):
        """
        The test populates the model :class:`~.TestMultiKeyModel`
        using :meth:`cassandra.cqlengine.models.Model.create_async`. Then verifies the
        rows have been correctly written using :meth:`cassandra.cqlengine.models.Model.get_async`
        and :meth:`cassandra.cqlengine.models.Model.get_all`

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result the appropriate table is populated and the rows are retrieved correctly.

        @test_category object_mapper
        """
        self._write_sample_from_model()
        read_futures = self._verify_reads_from_model()

        all_the_rows = TestMultiKeyModel.get_all_async().result()
        self.assertEqual(len(all_the_rows), 10)

        for i, future in enumerate(read_futures):
            result = future.result()
            self.assertIn(result, all_the_rows)

        all_the_rows_sync = TestMultiKeyModel.get_all()
        self.assertEqual(all_the_rows, all_the_rows_sync)

    def test_read_with_small_fetch_size(self):
        """
        The test populates the model :class:`~.TestMultiKeyModel`
        using :meth:`cassandra.cqlengine.models.Model.create_async`. Then verifies the
        rows have been correctly written using :meth:`cassandra.cqlengine.models.Model.get_async`
        and :meth:`cassandra.cqlengine.models.Model.get_all`. The test sets a small fetch size
        which will make cqlengine spin threads to handle the remaining rows.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result the appropriate table is populated and the rows are retrieved correctly

        @test_category object_mapper
        """
        self._write_sample_from_model(1000)
        session = get_connection().session
        original_fetch_size = session.default_fetch_size
        self.addCleanup(lambda : setattr(get_connection().session, "default_fetch_size", original_fetch_size))
        session.default_fetch_size = 4

        read_futures = self._verify_reads_from_model(1000)

        all_the_rows = TestMultiKeyModel.get_all_async().result()
        self.assertEqual(len(all_the_rows), 1000)

        for i, future in enumerate(read_futures):
            result = future.result()
            self.assertIn(result, all_the_rows)

        all_the_rows_sync = TestMultiKeyModel.get_all()
        self.assertEqual(all_the_rows, all_the_rows_sync)

    def test_count_async(self):
        """
        The test populates the model :class:`~.TestMultiKeyModel`
        using :meth:`cassandra.cqlengine.models.Model.create_async`. Then verifies the
        rows can be counted correctly using :meth:`cassandra.cqlengine.models.Model.count_async`.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result the appropriate table is populated and
        :meth:`cassandra.cqlengine.models.Model.count_async` returns the expected number

        @test_category object_mapper
        """
        self._write_sample_from_model()

        count_futures = []
        for i in range(10):
            count_futures.append(TestMultiKeyModel.objects(partition=i, cluster=i).count_async())

        count_all = TestMultiKeyModel.objects().count_async()

        for count_future in count_futures:
            self.assertEqual(count_future.result(), 1)
        self.assertEqual(count_all.result(), 10)

    def test_add_done_callback(self):
        """
        The test verifies :meth:`cassandra.cqlengine.concurrent.CQLEngineFuture.add_done_callback`
        sets the callbacks to called as expected under a mixed load of asynchronous queries.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result The callback is called the appropriate number of times.

        @test_category object_mapper
        """
        futures = []
        counter = count()

        def check_called(response):
            self.assertIsNotNone(response)
            next(counter)

        n_iters = 10000
        for i in range(n_iters):
            futures.append(TestMultiKeyModel.create_async(partition=i, cluster=i,
                                                            count=5, text=self.text_to_write))
            futures.append(TestMultiKeyModel.get_async(partition=i, cluster=i))
            futures.append(TestMultiKeyModel.objects(partition=i, cluster=i).delete_async())

        for future in futures:
            future.add_done_callback(check_called)

        wait(futures)
        self.assertEqual(next(counter), 3 * n_iters)

    @notwindows
    def test_exception(self):
        """
        The test verifies that the exceptions are properly surfaced in
        :class:`cassandra.cqlengine.concurrent.CQLEngineFuture`.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result The exceptions are properly surfaced.

        @test_category object_mapper
        """
        # Test DoesNotExist
        read_future = TestMultiKeyModel(partition=0, cluster=0).get_async()
        with self.assertRaises(TestMultiKeyModel.DoesNotExist):
            read_future.result()

        # Test OperationTimedOut
        m = TestMultiKeyModel(partition=0, cluster=0, count=5, text=self.text_to_write)
        save_future = m.timeout(0).save_async()
        with self.assertRaises(OperationTimedOut):
            save_future.result()

        # Test server exception
        m = TestMultiKeyModel(partition=1, count=5, text=self.text_to_write)
        save_future = m.save_async() # Invalid consistency level
        with self.assertRaises(InvalidRequest):
            save_future.result()

    def test_batch(self):
        """
        The test verifies batches work as expected using asynchronous execution.
        First populates C* using batches and then deletes all the table entries
        using batches as well. The table should be empty at the end of the test.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result The table should be empty after all the batches have finished.

        @test_category object_mapper
        """
        batch_futures = []
        for i in range(10):
            b = BatchQuery()
            TestMultiKeyModel.batch(b).create(partition=i, cluster=i, count=5, text=self.text_to_write)
            batch_futures.append(b.execute_async())

        wait(batch_futures)
        self._verify_reads_from_model()

        batch_futures = []
        for i in range(10):
            b = BatchQuery()
            TestMultiKeyModel.batch(b).filter(partition=i, cluster=i).delete()
            batch_futures.append(b.execute_async())

        wait(batch_futures)
        self._verify_table_is_empty()

    def test_batch_context(self):
        """
        The test verifies batches work as expected using asynchronous execution.
        First populates C* using batches and then deletes all the table entries
        using batches as well. The table should be empty at the end of the test.
        The table is different from the previous one in that it uses the context
        of :class:`cassandra.cqlengine.query.BatchQuery`.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result The table should be empty after all the batches have finished.

        @test_category object_mapper
        """
        batch_futures = []
        for i in range(10):
            with BatchQuery() as b:
                TestMultiKeyModel.batch(b).create(partition=i, cluster=i, count=5, text=self.text_to_write)
                batch_futures.append(b.execute_async())

        wait(batch_futures)
        self._verify_reads_from_model()

        batch_futures = []
        for i in range(10):
            with BatchQuery() as b:
                TestMultiKeyModel.batch(b).filter(partition=i, cluster=i).delete()
                batch_futures.append(b.execute_async())

        wait(batch_futures)
        self._verify_table_is_empty()

    def test_batch_exception(self):
        """
        The test verifies that the exceptions are properly surfaced in
        :class:`cassandra.cqlengine.concurrent.CQLEngineFuture` when using batches.
        One of the batch queries is missing a partition key and therefore none of the
        other queries should be written. The test verifies the table is therefore empty.

        @since 3.13
        @jira_ticket PYTHON-605
        @expected_result The exceptions are properly surfaced and the corresponding table
        is empty.

        @test_category object_mapper
        """
        b = BatchQuery()
        for i in range(10):
            TestMultiKeyModel.batch(b).create(partition=i, cluster=i, count=5, text=self.text_to_write)
        TestMultiKeyModel.batch(b).create(partition=11, count=5, text=self.text_to_write)
        batch_future = b.execute_async()

        with self.assertRaises(InvalidRequest):
            batch_future.result()

        self._verify_table_is_empty()

    def _write_sample_from_instance(self, n=10):
        write_futures = []
        for i in range(n):
            write_futures.append(TestMultiKeyModel(partition=i, cluster=i,
                                                                count=5, text=self.text_to_write).save_async())
        wait(write_futures)

    def _write_sample_from_model(self, n=10):
        write_futures = []
        for i in range(n):
            write_futures.append(TestMultiKeyModel.create_async(partition=i, cluster=i,
                                                                count=5, text=self.text_to_write))
        wait(write_futures)
        for future in write_futures:
            self.assertTrue(future.done)

    def _verify_reads_from_instance(self, n=10):
        read_futures = []
        for i in range(n):
            read_futures.append(TestMultiKeyModel.objects.filter(partition=i, cluster=i).get_async())

        for i, future in enumerate(read_futures):
            result = future.result()
            self.assertEqual((result.partition, result.cluster, result.count, result.text), (i, i, 5,
                                                                                             self.text_to_write))
        return read_futures

    def _verify_reads_from_model(self, n=10):
        read_futures = []
        for i in range(n):
            read_futures.append(TestMultiKeyModel.get_async(partition=i, cluster=i))

        for i, future in enumerate(read_futures):
            result = future.result()
            self.assertEqual((result.partition, result.cluster, result.count, result.text), (i, i, 5,
                                                                                             self.text_to_write))
        return read_futures

    def _verify_table_is_empty(self):
        all_the_rows = TestMultiKeyModel.get_all()
        self.assertEqual(len(all_the_rows), 0)
