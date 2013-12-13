import cassandra

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
from tests.integration.long.utils import reset_coordinators, force_stop, \
    create_schema, init, query, assert_queried, wait_for_down, wait_for_up, \
    start

try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa


class ConsistencyTests(unittest.TestCase):
    def _cl_failure(self, consistency_level, e):
        self.fail('%s seen for CL.%s with message: %s' % (
            type(e), ConsistencyLevel.value_to_name[consistency_level],
            e.message))

    def _cl_expected_failure(self, cl):
        self.fail('Test passed at ConsistencyLevel.%s' %
                  ConsistencyLevel.value_to_name[cl])


    def test_rfone_tokenaware(self):
        keyspace = 'test_rfone_tokenaware'
        cluster = Cluster(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
        session = cluster.connect()

        create_schema(session, keyspace, replication_factor=1)
        init(session, keyspace, 12)

        reset_coordinators()
        query(session, keyspace, 12)
        assert_queried(1, 0)
        assert_queried(2, 12)
        assert_queried(3, 0)

        try:
            force_stop(2)
            wait_for_down(cluster, 2)

            accepted_list = [ConsistencyLevel.ANY]

            fail_list = [
                ConsistencyLevel.ONE,
                ConsistencyLevel.TWO,
                ConsistencyLevel.QUORUM,
                ConsistencyLevel.THREE,
                ConsistencyLevel.ALL,
                ConsistencyLevel.LOCAL_QUORUM,
                ConsistencyLevel.EACH_QUORUM
            ]

            # Test writes that expected to complete successfully
            # BUG: CL.ANY should work
            # for cl in accepted_list:
            #     try:
            #         init(session, keyspace, 12, consistency_level=cl)
            #     except Exception as e:
            #         self._cl_failure(cl, e)

            # Test reads that expected to complete successfully
            for cl in accepted_list:
                try:
                    query(session, keyspace, 12, consistency_level=cl)
                    self._cl_expected_failure(cl)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.ANY]:
                        self._cl_failure(cl, e)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test writes that expected to fail
            for cl in fail_list:
                try:
                    init(session, keyspace, 12, consistency_level=cl)
                    self._cl_expected_failure(cl)
                except cassandra.Unavailable as e:
                    if not cl in [ConsistencyLevel.ONE,
                                  ConsistencyLevel.TWO,
                                  ConsistencyLevel.QUORUM,
                                  ConsistencyLevel.THREE,
                                  ConsistencyLevel.ALL]:
                        self._cl_failure(cl, e)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.LOCAL_QUORUM,
                                  ConsistencyLevel.EACH_QUORUM]:
                        self._cl_failure(cl, e)

            # Test reads that expected to fail
            for cl in fail_list:
                try:
                    query(session, keyspace, 12, consistency_level=cl)
                    self._cl_expected_failure(cl)
                except cassandra.Unavailable as e:
                    if not cl in [ConsistencyLevel.ONE,
                                  ConsistencyLevel.TWO,
                                  ConsistencyLevel.QUORUM,
                                  ConsistencyLevel.THREE,
                                  ConsistencyLevel.ALL]:
                        self._cl_failure(cl, e)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.LOCAL_QUORUM,
                                  ConsistencyLevel.EACH_QUORUM]:
                        self._cl_failure(cl, e)
        finally:
            start(2)
            wait_for_up(cluster, 2)


    def test_rftwo_tokenaware(self):
        keyspace = 'test_rftwo_tokenaware'
        cluster = Cluster(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
        session = cluster.connect()

        create_schema(session, keyspace, replication_factor=2)
        init(session, keyspace, 12)

        reset_coordinators()
        query(session, keyspace, 12)
        assert_queried(1, 0)
        assert_queried(2, 12)
        assert_queried(3, 0)

        try:
            force_stop(2)
            wait_for_down(cluster, 2)

            accepted_list = [
                ConsistencyLevel.ANY,
                ConsistencyLevel.ONE
            ]

            fail_list = [
                ConsistencyLevel.TWO,
                ConsistencyLevel.QUORUM,
                ConsistencyLevel.THREE,
                ConsistencyLevel.ALL,
                ConsistencyLevel.LOCAL_QUORUM,
                ConsistencyLevel.EACH_QUORUM
            ]

            # Test writes that expected to complete successfully
            for cl in accepted_list:
                try:
                    init(session, keyspace, 12, consistency_level=cl)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test reads that expected to complete successfully
            for cl in accepted_list:
                try:
                    reset_coordinators()
                    query(session, keyspace, 12, consistency_level=cl)
                    # Bug: I believe the Java-Driver does this differently
                    # and RoundRobins after the ideal token is not available.
                    # I like the Python Driver's approach, but we should
                    # probably make all policies act the same way, whichever
                    # way gets chosen?
                    assert_queried(1, 0)
                    assert_queried(2, 0)
                    assert_queried(3, 12)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.ANY]:
                        self._cl_failure(cl, e)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test writes that expected to fail
            for cl in fail_list:
                try:
                    init(session, keyspace, 12, consistency_level=cl)
                    self._cl_expected_failure(cl)
                except cassandra.Unavailable as e:
                    if not cl in [ConsistencyLevel.ONE,
                                  ConsistencyLevel.TWO,
                                  ConsistencyLevel.QUORUM,
                                  ConsistencyLevel.THREE,
                                  ConsistencyLevel.ALL]:
                        self._cl_failure(cl, e)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.LOCAL_QUORUM,
                                  ConsistencyLevel.EACH_QUORUM]:
                        self._cl_failure(cl, e)

            # Test reads that expected to fail
            for cl in fail_list:
                try:
                    query(session, keyspace, 12, consistency_level=cl)
                    self._cl_expected_failure(cl)
                except cassandra.Unavailable as e:
                    if not cl in [ConsistencyLevel.ONE,
                                  ConsistencyLevel.TWO,
                                  ConsistencyLevel.QUORUM,
                                  ConsistencyLevel.THREE,
                                  ConsistencyLevel.ALL]:
                        self._cl_failure(cl, e)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.LOCAL_QUORUM,
                                  ConsistencyLevel.EACH_QUORUM]:
                        self._cl_failure(cl, e)
        finally:
            start(2)
            wait_for_up(cluster, 2)

    def test_rfthree_tokenaware(self):
        keyspace = 'test_rfthree_tokenaware'
        cluster = Cluster(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
        session = cluster.connect()

        create_schema(session, keyspace, replication_factor=3)
        init(session, keyspace, 12)

        reset_coordinators()
        query(session, keyspace, 12)
        assert_queried(1, 0)
        assert_queried(2, 12)
        assert_queried(3, 0)

        try:
            force_stop(2)
            wait_for_down(cluster, 2)

            accepted_list = [
                ConsistencyLevel.ANY,
                ConsistencyLevel.ONE,
                ConsistencyLevel.TWO,
                ConsistencyLevel.QUORUM
            ]

            fail_list = [
                ConsistencyLevel.THREE,
                ConsistencyLevel.ALL,
                ConsistencyLevel.LOCAL_QUORUM,
                ConsistencyLevel.EACH_QUORUM
            ]

            # Test writes that expected to complete successfully
            for cl in accepted_list:
                try:
                    init(session, keyspace, 12, consistency_level=cl)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test reads that expected to complete successfully
            for cl in accepted_list:
                try:
                    reset_coordinators()
                    query(session, keyspace, 12, consistency_level=cl)
                    # Bug: I believe the Java-Driver does this differently
                    assert_queried(1, 12)
                    assert_queried(2, 0)
                    assert_queried(3, 0)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.ANY]:
                        self._cl_failure(cl, e)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test writes that expected to fail
            for cl in fail_list:
                try:
                    init(session, keyspace, 12, consistency_level=cl)
                    self._cl_expected_failure(cl)
                except cassandra.Unavailable as e:
                    if not cl in [ConsistencyLevel.ONE,
                                  ConsistencyLevel.TWO,
                                  ConsistencyLevel.QUORUM,
                                  ConsistencyLevel.THREE,
                                  ConsistencyLevel.ALL]:
                        self._cl_failure(cl, e)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.LOCAL_QUORUM,
                                  ConsistencyLevel.EACH_QUORUM]:
                        self._cl_failure(cl, e)

            # Test reads that expected to fail
            for cl in fail_list:
                try:
                    query(session, keyspace, 12, consistency_level=cl)
                    self._cl_expected_failure(cl)
                except cassandra.Unavailable as e:
                    if not cl in [ConsistencyLevel.ONE,
                                  ConsistencyLevel.TWO,
                                  ConsistencyLevel.QUORUM,
                                  ConsistencyLevel.THREE,
                                  ConsistencyLevel.ALL]:
                        self._cl_failure(cl, e)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.LOCAL_QUORUM,
                                  ConsistencyLevel.EACH_QUORUM]:
                        self._cl_failure(cl, e)
        finally:
            start(2)
            wait_for_up(cluster, 2)


    def test_rfthree_tokenaware_none_down(self):
        keyspace = 'test_rfthree_tokenaware_none_down'
        cluster = Cluster(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
        session = cluster.connect()

        create_schema(session, keyspace, replication_factor=3)
        init(session, keyspace, 12)

        reset_coordinators()
        query(session, keyspace, 12)
        assert_queried(1, 0)
        assert_queried(2, 12)
        assert_queried(3, 0)

        accepted_list = [
            ConsistencyLevel.ANY,
            ConsistencyLevel.ONE,
            ConsistencyLevel.TWO,
            ConsistencyLevel.QUORUM,
            ConsistencyLevel.THREE,
            ConsistencyLevel.ALL,
        ]

        fail_list = [
            ConsistencyLevel.LOCAL_QUORUM,
            ConsistencyLevel.EACH_QUORUM
        ]

        # Test writes that expected to complete successfully
        for cl in accepted_list:
            try:
                init(session, keyspace, 12, consistency_level=cl)
            except Exception as e:
                self._cl_failure(cl, e)

        # Test reads that expected to complete successfully
        for cl in accepted_list:
            try:
                reset_coordinators()
                query(session, keyspace, 12, consistency_level=cl)
                assert_queried(1, 0)
                assert_queried(2, 12)
                assert_queried(3, 0)
            except cassandra.InvalidRequest as e:
                if not cl in [ConsistencyLevel.ANY]:
                    self._cl_failure(cl, e)
            except Exception as e:
                self._cl_failure(cl, e)

        # Test writes that expected to fail
        for cl in fail_list:
            try:
                init(session, keyspace, 12, consistency_level=cl)
                self._cl_expected_failure(cl)
            except cassandra.Unavailable as e:
                if not cl in [ConsistencyLevel.ONE,
                              ConsistencyLevel.TWO,
                              ConsistencyLevel.QUORUM,
                              ConsistencyLevel.THREE,
                              ConsistencyLevel.ALL]:
                    self._cl_failure(cl, e)
            except cassandra.InvalidRequest as e:
                if not cl in [ConsistencyLevel.LOCAL_QUORUM,
                              ConsistencyLevel.EACH_QUORUM]:
                    self._cl_failure(cl, e)

        # Test reads that expected to fail
        for cl in fail_list:
            try:
                query(session, keyspace, 12, consistency_level=cl)
                self._cl_expected_failure(cl)
            except cassandra.Unavailable as e:
                if not cl in [ConsistencyLevel.ONE,
                              ConsistencyLevel.TWO,
                              ConsistencyLevel.QUORUM,
                              ConsistencyLevel.THREE,
                              ConsistencyLevel.ALL]:
                    self._cl_failure(cl, e)
            except cassandra.InvalidRequest as e:
                if not cl in [ConsistencyLevel.LOCAL_QUORUM,
                              ConsistencyLevel.EACH_QUORUM]:
                    self._cl_failure(cl, e)
