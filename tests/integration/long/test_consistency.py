import traceback
import cassandra

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy, \
    DowngradingConsistencyRetryPolicy
from tests.integration.long.utils import force_stop, create_schema, \
    wait_for_down, wait_for_up, start, CoordinatorStats

try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa


class ConsistencyTests(unittest.TestCase):

    def setUp(self):
        self.cs = CoordinatorStats()

    def _cl_failure(self, consistency_level, e):
        self.fail('%s seen for CL.%s:\n\n%s' % (
            type(e), ConsistencyLevel.value_to_name[consistency_level],
            traceback.format_exc()))

    def _cl_expected_failure(self, cl):
        self.fail('Test passed at ConsistencyLevel.%s:\n\n%s' % (
                  ConsistencyLevel.value_to_name[cl], traceback.format_exc()))


    def test_rfone_tokenaware(self):
        keyspace = 'test_rfone_tokenaware'
        cluster = Cluster(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
        session = cluster.connect()
        wait_for_up(cluster, 1)
        wait_for_up(cluster, 2)

        create_schema(session, keyspace, replication_factor=1)
        self.cs.init(session, keyspace, 12)
        self.cs.query(session, keyspace, 12)

        self.cs.assert_queried(self, 1, 0)
        self.cs.assert_queried(self, 2, 12)
        self.cs.assert_queried(self, 3, 0)

        try:
            self.cs.reset_coordinators()
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
            for cl in accepted_list:
                try:
                    self.cs.init(session, keyspace, 12, consistency_level=cl)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test reads that expected to complete successfully
            for cl in accepted_list:
                try:
                    self.cs.query(session, keyspace, 12, consistency_level=cl)
                    self._cl_expected_failure(cl)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.ANY]:
                        self._cl_failure(cl, e)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test writes that expected to fail
            for cl in fail_list:
                try:
                    self.cs.init(session, keyspace, 12, consistency_level=cl)
                    self._cl_expected_failure(cl)
                except (cassandra.Unavailable, cassandra.WriteTimeout) as e:
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
                    self.cs.query(session, keyspace, 12, consistency_level=cl)
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
        wait_for_up(cluster, 1)
        wait_for_up(cluster, 2)

        create_schema(session, keyspace, replication_factor=2)
        self.cs.init(session, keyspace, 12)
        self.cs.query(session, keyspace, 12)

        self.cs.assert_queried(self, 1, 0)
        self.cs.assert_queried(self, 2, 12)
        self.cs.assert_queried(self, 3, 0)

        try:
            self.cs.reset_coordinators()
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
                    self.cs.init(session, keyspace, 12, consistency_level=cl)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test reads that expected to complete successfully
            for cl in accepted_list:
                try:
                    self.cs.reset_coordinators()
                    self.cs.query(session, keyspace, 12, consistency_level=cl)
                    self.cs.assert_queried(self, 1, 0)
                    self.cs.assert_queried(self, 2, 0)
                    self.cs.assert_queried(self, 3, 12)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.ANY]:
                        self._cl_failure(cl, e)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test writes that expected to fail
            for cl in fail_list:
                try:
                    self.cs.init(session, keyspace, 12, consistency_level=cl)
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
                    self.cs.query(session, keyspace, 12, consistency_level=cl)
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
        self.cs.init(session, keyspace, 12)
        self.cs.query(session, keyspace, 12)

        self.cs.assert_queried(self, 1, 0)
        self.cs.assert_queried(self, 2, 12)
        self.cs.assert_queried(self, 3, 0)

        try:
            self.cs.reset_coordinators()
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
                    self.cs.init(session, keyspace, 12, consistency_level=cl)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test reads that expected to complete successfully
            for cl in accepted_list:
                try:
                    self.cs.reset_coordinators()
                    self.cs.query(session, keyspace, 12, consistency_level=cl)
                    self.cs.assert_queried(self, 1, 0)
                    self.cs.assert_queried(self, 2, 0)
                    self.cs.assert_queried(self, 3, 12)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.ANY]:
                        self._cl_failure(cl, e)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test writes that expected to fail
            for cl in fail_list:
                try:
                    self.cs.init(session, keyspace, 12, consistency_level=cl)
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
                    self.cs.query(session, keyspace, 12, consistency_level=cl)
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
        wait_for_up(cluster, 1)
        wait_for_up(cluster, 2)

        create_schema(session, keyspace, replication_factor=3)
        self.cs.init(session, keyspace, 12)
        self.cs.query(session, keyspace, 12)

        self.cs.assert_queried(self, 1, 0)
        self.cs.assert_queried(self, 2, 12)
        self.cs.assert_queried(self, 3, 0)

        self.cs.reset_coordinators()

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
                self.cs.init(session, keyspace, 12, consistency_level=cl)
            except Exception as e:
                self._cl_failure(cl, e)

        # Test reads that expected to complete successfully
        for cl in accepted_list:
            try:
                self.cs.reset_coordinators()
                self.cs.query(session, keyspace, 12, consistency_level=cl)
                self.cs.assert_queried(self, 1, 0)
                self.cs.assert_queried(self, 2, 12)
                self.cs.assert_queried(self, 3, 0)
            except cassandra.InvalidRequest as e:
                if not cl in [ConsistencyLevel.ANY]:
                    self._cl_failure(cl, e)
            except Exception as e:
                self._cl_failure(cl, e)

        # Test writes that expected to fail
        for cl in fail_list:
            try:
                self.cs.init(session, keyspace, 12, consistency_level=cl)
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
                self.cs.query(session, keyspace, 12, consistency_level=cl)
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


    def test_rfone_downgradingcl(self):
        keyspace = 'test_rfone_downgradingcl'
        cluster = Cluster(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
            default_retry_policy=DowngradingConsistencyRetryPolicy())
        session = cluster.connect()

        create_schema(session, keyspace, replication_factor=1)
        self.cs.init(session, keyspace, 12)
        self.cs.query(session, keyspace, 12)

        self.cs.assert_queried(self, 1, 0)
        self.cs.assert_queried(self, 2, 12)
        self.cs.assert_queried(self, 3, 0)

        try:
            self.cs.reset_coordinators()
            force_stop(2)
            wait_for_down(cluster, 2)

            accepted_list = [
                ConsistencyLevel.ANY
            ]

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
            for cl in accepted_list:
                try:
                    self.cs.init(session, keyspace, 12, consistency_level=cl)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test reads that expected to complete successfully
            for cl in accepted_list:
                try:
                    self.cs.reset_coordinators()
                    self.cs.query(session, keyspace, 12, consistency_level=cl)
                    self.cs.assert_queried(self, 1, 0)
                    self.cs.assert_queried(self, 2, 0)
                    self.cs.assert_queried(self, 3, 12)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.ANY]:
                        self._cl_failure(cl, e)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test writes that expected to fail
            for cl in fail_list:
                try:
                    self.cs.init(session, keyspace, 12, consistency_level=cl)
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
                    self.cs.query(session, keyspace, 12, consistency_level=cl)
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


    def test_rftwo_downgradingcl(self):
        keyspace = 'test_rftwo_downgradingcl'
        cluster = Cluster(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
            default_retry_policy=DowngradingConsistencyRetryPolicy())
        session = cluster.connect()

        create_schema(session, keyspace, replication_factor=2)
        self.cs.init(session, keyspace, 12)
        self.cs.query(session, keyspace, 12)

        self.cs.assert_queried(self, 1, 0)
        self.cs.assert_queried(self, 2, 12)
        self.cs.assert_queried(self, 3, 0)

        try:
            self.cs.reset_coordinators()
            force_stop(2)
            wait_for_down(cluster, 2)

            accepted_list = [
                ConsistencyLevel.ANY,
                ConsistencyLevel.ONE,
                ConsistencyLevel.TWO,
                ConsistencyLevel.QUORUM,
                ConsistencyLevel.THREE,
                ConsistencyLevel.ALL
            ]

            fail_list = [
                ConsistencyLevel.LOCAL_QUORUM,
                ConsistencyLevel.EACH_QUORUM
            ]

            # Test writes that expected to complete successfully
            for cl in accepted_list:
                try:
                    self.cs.init(session, keyspace, 12, consistency_level=cl)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test reads that expected to complete successfully
            for cl in accepted_list:
                try:
                    self.cs.reset_coordinators()
                    self.cs.query(session, keyspace, 12, consistency_level=cl)
                    self.cs.assert_queried(self, 1, 0)
                    self.cs.assert_queried(self, 2, 0)
                    self.cs.assert_queried(self, 3, 12)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.ANY]:
                        self._cl_failure(cl, e)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test writes that expected to fail
            for cl in fail_list:
                try:
                    self.cs.init(session, keyspace, 12, consistency_level=cl)
                    self._cl_expected_failure(cl)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.LOCAL_QUORUM,
                                  ConsistencyLevel.EACH_QUORUM]:
                        self._cl_failure(cl, e)

            # Test reads that expected to fail
            for cl in fail_list:
                try:
                    self.cs.query(session, keyspace, 12, consistency_level=cl)
                    self._cl_expected_failure(cl)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.LOCAL_QUORUM,
                                  ConsistencyLevel.EACH_QUORUM]:
                        self._cl_failure(cl, e)
        finally:
            start(2)
            wait_for_up(cluster, 2)


    def test_rfthree_roundrobin_downgradingcl(self):
        keyspace = 'test_rfthree_roundrobin_downgradingcl'
        cluster = Cluster(
            load_balancing_policy=RoundRobinPolicy(),
            default_retry_policy=DowngradingConsistencyRetryPolicy())
        self.rfthree_downgradingcl(cluster, keyspace, True)

    def test_rfthree_tokenaware_downgradingcl(self):
        keyspace = 'test_rfthree_tokenaware_downgradingcl'
        cluster = Cluster(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
            default_retry_policy=DowngradingConsistencyRetryPolicy())
        self.rfthree_downgradingcl(cluster, keyspace, False)

    def rfthree_downgradingcl(self, cluster, keyspace, roundrobin):
        session = cluster.connect()

        create_schema(session, keyspace, replication_factor=2)
        self.cs.init(session, keyspace, 12)
        self.cs.query(session, keyspace, 12)

        if roundrobin:
            self.cs.assert_queried(self, 1, 4)
            self.cs.assert_queried(self, 2, 4)
            self.cs.assert_queried(self, 3, 4)
        else:
            self.cs.assert_queried(self, 1, 0)
            self.cs.assert_queried(self, 2, 12)
            self.cs.assert_queried(self, 3, 0)

        try:
            self.cs.reset_coordinators()
            force_stop(2)
            wait_for_down(cluster, 2)

            accepted_list = [
                ConsistencyLevel.ANY,
                ConsistencyLevel.ONE,
                ConsistencyLevel.TWO,
                ConsistencyLevel.QUORUM,
                ConsistencyLevel.THREE,
                ConsistencyLevel.ALL
            ]

            fail_list = [
                ConsistencyLevel.LOCAL_QUORUM,
                ConsistencyLevel.EACH_QUORUM
            ]

            # Test writes that expected to complete successfully
            for cl in accepted_list:
                try:
                    self.cs.init(session, keyspace, 12, consistency_level=cl)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test reads that expected to complete successfully
            for cl in accepted_list:
                try:
                    self.cs.reset_coordinators()
                    self.cs.query(session, keyspace, 12, consistency_level=cl)
                    if roundrobin:
                        self.cs.assert_queried(self, 1, 6)
                        self.cs.assert_queried(self, 2, 0)
                        self.cs.assert_queried(self, 3, 6)
                    else:
                        self.cs.assert_queried(self, 1, 0)
                        self.cs.assert_queried(self, 2, 0)
                        self.cs.assert_queried(self, 3, 12)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.ANY]:
                        self._cl_failure(cl, e)
                except Exception as e:
                    self._cl_failure(cl, e)

            # Test writes that expected to fail
            for cl in fail_list:
                try:
                    self.cs.init(session, keyspace, 12, consistency_level=cl)
                    self._cl_expected_failure(cl)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.LOCAL_QUORUM,
                                  ConsistencyLevel.EACH_QUORUM]:
                        self._cl_failure(cl, e)

            # Test reads that expected to fail
            for cl in fail_list:
                try:
                    self.cs.query(session, keyspace, 12, consistency_level=cl)
                    self._cl_expected_failure(cl)
                except cassandra.InvalidRequest as e:
                    if not cl in [ConsistencyLevel.LOCAL_QUORUM,
                                  ConsistencyLevel.EACH_QUORUM]:
                        self._cl_failure(cl, e)
        finally:
            start(2)
            wait_for_up(cluster, 2)

    # TODO: can't be done in this class since we reuse the ccm cluster
    #       instead we should create these elsewhere
    # def test_rfthree_downgradingcl_twodcs(self):
    # def test_rfthree_downgradingcl_twodcs_dcaware(self):
