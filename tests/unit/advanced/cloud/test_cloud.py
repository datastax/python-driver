# Copyright DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms
import tempfile
import os
import shutil
import six

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from cassandra import DriverException
from cassandra.datastax import cloud

from mock import patch

from tests import notwindows

class CloudTests(unittest.TestCase):

    current_path = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(current_path, './creds.zip')
    config_zip = {
        'secure_connect_bundle': creds_path
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

    @notwindows
    def test_use_default_tempdir(self):
        tmpdir = tempfile.mkdtemp()

        def clean_tmp_dir():
            os.chmod(tmpdir, 0o777)
            shutil.rmtree(tmpdir)
        self.addCleanup(clean_tmp_dir)

        tmp_creds_path = os.path.join(tmpdir, 'creds.zip')
        shutil.copyfile(self.creds_path, tmp_creds_path)
        os.chmod(tmpdir, 0o544)
        config = {
            'secure_connect_bundle': tmp_creds_path
        }

        # The directory is not writtable.. we expect a permission error
        exc = PermissionError if six.PY3 else OSError
        with self.assertRaises(exc):
            cloud.get_cloud_config(config)

        # With use_default_tempdir, we expect an connection refused
        # since the cluster doesn't exist
        with self.assertRaises(DriverException):
            config = {
                'secure_connect_bundle': tmp_creds_path,
                'use_default_tempdir': True
            }
            cloud.get_cloud_config(config)
