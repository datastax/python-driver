# Copyright ScyllaDB, Inc.
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

import sys
import ssl
import tempfile
import base64
from ssl import SSLContext
from contextlib import contextmanager
from itertools import islice

import yaml

from cassandra.connection import SniEndPointFactory
from cassandra.auth import AuthProvider, PlainTextAuthProvider


@contextmanager
def file_or_memory(path=None, data=None):
    # since we can't read keys/cert from memory yet
    # see https://github.com/python/cpython/pull/2449 which isn't accepted and PEP-543 that was withdrawn
    # so we use temporary file to load the key
    if data:
        with tempfile.NamedTemporaryFile(mode="wb") as f:
            d = base64.b64decode(data)
            f.write(d)
            if not d.endswith(b"\n"):
                f.write(b"\n")

            f.flush()
            yield f.name

    if path:
        yield path


def nth(iterable, n, default=None):
    "Returns the nth item or a default value"
    return next(islice(iterable, n, None), default)


class CloudConfiguration:
    endpoint_factory: SniEndPointFactory
    contact_points: list
    auth_provider: AuthProvider = None
    ssl_options: dict
    ssl_context: SSLContext
    skip_tls_verify: bool

    def __init__(self, configuration_file, pyopenssl=False, endpoint_factory=None):
        cloud_config = yaml.safe_load(open(configuration_file))

        self.current_context = cloud_config['contexts'][cloud_config['currentContext']]
        self.data_centers = cloud_config['datacenters']
        self.current_data_center = self.data_centers[self.current_context['datacenterName']]
        self.auth_info = cloud_config['authInfos'][self.current_context['authInfoName']]
        self.ssl_options = {}
        self.skip_tls_verify = self.current_data_center.get('insecureSkipTlsVerify', False)
        self.ssl_context = self.create_pyopenssl_context() if pyopenssl else self.create_ssl_context()

        proxy_address, port, node_domain = self.get_server(self.current_data_center)

        if not endpoint_factory:
            endpoint_factory = SniEndPointFactory(proxy_address, port=int(port), node_domain=node_domain)
        else:
            assert isinstance(endpoint_factory, SniEndPointFactory)
        self.endpoint_factory = endpoint_factory

        username, password = self.auth_info.get('username'), self.auth_info.get('password')
        if username and password:
            self.auth_provider = PlainTextAuthProvider(username, password)

    @property
    def contact_points(self):
        _contact_points = []
        for data_center in self.data_centers.values():
            _, _, node_domain = self.get_server(data_center)
            _contact_points.append(self.endpoint_factory.create_from_sni(node_domain))
        return _contact_points

    def get_server(self, data_center):
        address = data_center.get('server')
        address = address.split(":")
        port = nth(address, 1, default=9142)
        address = nth(address, 0)
        node_domain = data_center.get('nodeDomain')
        assert address and port and node_domain, "server or nodeDomain are missing"
        return address, port, node_domain

    def create_ssl_context(self):
        ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.verify_mode = ssl.CERT_NONE if self.skip_tls_verify else ssl.CERT_REQUIRED
        for data_center in self.data_centers.values():
            with file_or_memory(path=data_center.get('certificateAuthorityPath'),
                                data=data_center.get('certificateAuthorityData')) as cafile:
                ssl_context.load_verify_locations(cadata=open(cafile).read())
        with file_or_memory(path=self.auth_info.get('clientCertificatePath'),
                            data=self.auth_info.get('clientCertificateData')) as certfile, \
                file_or_memory(path=self.auth_info.get('clientKeyPath'), data=self.auth_info.get('clientKeyData')) as keyfile:
            ssl_context.load_cert_chain(keyfile=keyfile,
                                        certfile=certfile)

        return ssl_context

    def create_pyopenssl_context(self):
        try:
            from OpenSSL import SSL
        except ImportError as e:
            raise ImportError(
                "PyOpenSSL must be installed to connect to scylla-cloud with the Eventlet or Twisted event loops") \
                .with_traceback(e.__traceback__)
        ssl_context = SSL.Context(SSL.TLS_METHOD)
        ssl_context.set_verify(SSL.VERIFY_PEER, callback=lambda _1, _2, _3, _4, ok: True if self.skip_tls_verify else ok)
        for data_center in self.data_centers.values():
            with file_or_memory(path=data_center.get('certificateAuthorityPath'),
                                data=data_center.get('certificateAuthorityData')) as cafile:
                ssl_context.load_verify_locations(cafile)
        with file_or_memory(path=self.auth_info.get('clientCertificatePath'),
                            data=self.auth_info.get('clientCertificateData')) as certfile, \
                file_or_memory(path=self.auth_info.get('clientKeyPath'), data=self.auth_info.get('clientKeyData')) as keyfile:
            ssl_context.use_privatekey_file(keyfile)
            ssl_context.use_certificate_file(certfile)

        return ssl_context

    @classmethod
    def create(cls, configuration_file, pyopenssl=False, endpoint_factory=None):
        return cls(configuration_file, pyopenssl=pyopenssl, endpoint_factory=endpoint_factory)
