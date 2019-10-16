# Copyright DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import os

from cassandra.datastax import cloud

from mock import patch


class CloudTests(unittest.TestCase):

    current_path = os.path.dirname(os.path.abspath(__file__))
    config_zip = {
        'secure_connect_bundle': os.path.join(current_path, './creds.zip')
    }
    metadata_json = """
        {"region":"local",
         "contact_info": {
             "type":"sni_proxy",
             "local_dc":"dc1",
             "contact_points":[
                 "b13ae7b4-e711-4660-8dd1-bec57d37aa64",
                 "d4330144-5fb3-425a-86a1-431b3e4d0671",
                 "86537b87-91a9-4c59-b715-716486e72c42"
             ],
             "sni_proxy_address":"localhost:30002"
         }
        }"""

    @staticmethod
    def _read_metadata_info_side_effect(config, _):
        return config

    def _check_config(self, config):
        self.assertEqual(config.username, 'cassandra')
        self.assertEqual(config.password, 'cassandra')
        self.assertEqual(config.host, 'localhost')
        self.assertEqual(config.port, 30443)
        self.assertEqual(config.keyspace, 'system')
        self.assertEqual(config.local_dc, None)
        self.assertIsNotNone(config.ssl_context)
        self.assertIsNone(config.sni_host)
        self.assertIsNone(config.sni_port)
        self.assertIsNone(config.host_ids)

    def test_read_cloud_config_from_zip(self):

        with patch('cassandra.datastax.cloud.read_metadata_info', side_effect=self._read_metadata_info_side_effect):
            config = cloud.get_cloud_config(self.config_zip)

        self._check_config(config)

    def test_parse_metadata_info(self):
        config = cloud.CloudConfig()
        cloud.parse_metadata_info(config, self.metadata_json)
        self.assertEqual(config.sni_host, 'localhost')
        self.assertEqual(config.sni_port, 30002)
        self.assertEqual(config.local_dc, 'dc1')

        host_ids = [
            "b13ae7b4-e711-4660-8dd1-bec57d37aa64",
            "d4330144-5fb3-425a-86a1-431b3e4d0671",
            "86537b87-91a9-4c59-b715-716486e72c42"
        ]
        for host_id in host_ids:
            self.assertIn(host_id, config.host_ids)
