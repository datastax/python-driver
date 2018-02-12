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

# Entirely for test purpose
import six

from cassandra import CoreProtocolVersion
from cassandra.marshal import int32_pack
from cassandra.protocol import AuthResponseMessage

class MessageCodec(object):

    opcode = None

    @staticmethod
    def encode(f, *args):
        raise NotImplementedError()

    @staticmethod
    def decode(f, *args):
        raise NotImplementedError()


class ProtocolV4Codecs(object):

    @staticmethod
    def register(registry):
        registry.add_encoder(AuthResponseMessage,CoreProtocolVersion.V4, AuthResponseMessageCodec)


class AuthResponseMessageCodec(MessageCodec):

    @staticmethod
    def encode(f, *args):
        response = args[0]
        write_longstring(f, response)


def write_int(f, i):
    f.write(int32_pack(i))

def write_longstring(f, s):
    if isinstance(s, six.text_type):
        s = s.encode('utf8')
    write_int(f, len(s))
    f.write(s)

