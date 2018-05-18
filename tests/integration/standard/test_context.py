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

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from cassandra.registry import MessageCodecRegistry
from cassandra.cluster import Cluster
from cassandra.context import DriverContext
from cassandra.protocol import ProtocolHandler, ResultMessage
from cassandra.protocol import NumpyProtocolHandler, LazyProtocolHandler, HAVE_CYTHON, HAVE_NUMPY

from tests.integration import get_supported_protocol_versions
from tests import if_cython, if_numpy

from nose import SkipTest

class MyResultMessageCodec(ResultMessage.Codec):
    @classmethod
    def decode(cls, f, protocol_version, user_type_map, result_metadata, *args):
        result_message = ResultMessage.Codec.decode(f, protocol_version, user_type_map, result_metadata, *args)
        colnames, parsed_rows = result_message.results
        # We are only going to modify it for the query requesting this columns
        if colnames == ["key", "host_id", "partitioner"]:
            result_message.results = (colnames, (("madeup_key", "madeup_id", "madeup_partitioner"), ))
        return result_message


class MyDriverContext(DriverContext):
    def __init__(self):
        self.codec_registry = MessageCodecRegistry.factory()
        self.handler = ProtocolHandler(self.codec_registry.encoders,
                                                 self.codec_registry.decoders)

    def add_decoder(self, protocol_version, opcode, decode):
        self.codec_registry.add_decoder(protocol_version,
                                                 opcode,
                                                 decode)
        self.handler = ProtocolHandler(self.codec_registry.encoders,
                                                 self.codec_registry.decoders)

    @property
    def protocol_handler(self):
        return self.handler

    @property
    def message_codec_registry(self):
        return self.codec_registry


if HAVE_CYTHON:
    class LazyDriverContext(MyDriverContext):
        def __init__(self):
            self.codec_registry = MessageCodecRegistry.factory()
            self.handler = LazyProtocolHandler(self.codec_registry.encoders,
                                                    self.codec_registry.decoders)

if HAVE_NUMPY:
    class NumpyDriverContext(MyDriverContext):
        def __init__(self):
            self.codec_registry = MessageCodecRegistry.factory()
            self.handler = LazyProtocolHandler(self.codec_registry.encoders,
                                                    self.codec_registry.decoders)


class ContextTests(unittest.TestCase):
    def test_context_can_be_passed(self):
        context = DriverContext()
        cluster = Cluster(context=context)
        session = cluster.connect()
        self.assertIsNotNone(session.execute("SELECT key from system.local"))
        self.addCleanup(cluster.shutdown)

    def test_customized_context(self):
        self._customized_context_with_protocol(MyDriverContext(), ProtocolHandler)

    @if_cython
    def test_customized_context_lazy(self):
        self._customized_context_with_protocol(LazyDriverContext(), LazyProtocolHandler)

    @if_numpy
    def test_customized_context_numpy(self):
        self._customized_context_with_protocol(NumpyDriverContext(), NumpyProtocolHandler)

    def _customized_context_with_protocol(self, context, protocol_class):
        for protocol_version in get_supported_protocol_versions():
            context = context
            context.add_decoder(protocol_version, MyResultMessageCodec.opcode, MyResultMessageCodec.decode)

            with Cluster(protocol_version=protocol_version, context=context) as cluster:
                session = cluster.connect()
                session.protocol_handler_class = protocol_class
                results = session.execute("SELECT key, host_id, partitioner from system.local")
                self.assertIsNotNone(results)
                self.assertEqual(results.one(), ("madeup_key", "madeup_id", "madeup_partitioner"))
                self.addCleanup(cluster.shutdown)
