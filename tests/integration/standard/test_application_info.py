# Copyright 2025 ScyllaDB, Inc.
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

import unittest

from cassandra.application_info import ApplicationInfo
from tests.integration import TestCluster, use_single_node, remove_cluster, xfail_scylla


def setup_module():
    use_single_node()


def teardown_module():
    remove_cluster()


@xfail_scylla("#scylladb/scylla-enterprise#5467 - not released yet")
class ApplicationInfoTest(unittest.TestCase):
    attribute_to_startup_key = {
        'application_name': 'APPLICATION_NAME',
        'application_version': 'APPLICATION_VERSION',
        'client_id': 'CLIENT_ID',
    }

    def test_create_session_and_check_system_views_clients(self):
        """
        Test to ensure that ApplicationInfo user provides endup in `client_options` of `system_views.clients` table
        """

        for application_info_args in [
            {
                'application_name': None,
                'application_version': None,
                'client_id': None,
            },
            {
                'application_name': 'some-application-name',
                'application_version': 'some-application-version',
                'client_id': 'some-client-id',
            },
            {
                'application_name': 'some-application-name',
                'application_version': None,
                'client_id': None,
            },
            {
                'application_name': None,
                'application_version': 'some-application-version',
                'client_id': None,
            },
            {
                'application_name': None,
                'application_version': None,
                'client_id': 'some-client-id',
            },
        ]:
            with self.subTest(**application_info_args):
                try:
                    cluster = TestCluster(
                        application_info=ApplicationInfo(
                            **application_info_args
                        ))

                    found = False
                    for row in cluster.connect().execute("select client_options from system_views.clients"):
                        if not row[0]:
                            continue
                        for attribute_key, startup_key in self.attribute_to_startup_key.items():
                            expected_value = application_info_args.get(attribute_key)
                            if expected_value:
                                if row[0].get(startup_key) != expected_value:
                                    break
                            else:
                                # Check that it is absent
                                if row[0].get(startup_key, None) is not None:
                                    break
                        else:
                            found = True
                    assert found
                finally:
                    cluster.shutdown()
