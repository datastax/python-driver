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

from collections import defaultdict

from cassandra import ProtocolVersion
from cassandra.protocol import *


class MessageCodecRegistry(object):
    encoders = None
    decoders = None

    def __init__(self):
        self.encoders = defaultdict(dict)
        self.decoders = defaultdict(dict)

    @staticmethod
    def _add(registry, protocol_version, opcode, func):
        registry[protocol_version][opcode] = func

    @staticmethod
    def _get(registry, protocol_version, opcode):
        try:
            return registry[protocol_version][opcode]
        except KeyError:
            raise ValueError(
                "No codec registered for message '{0:02X}' and "
                "protocol version '{1}'".format(opcode, protocol_version))

    def add_encoder(self, protocol_version, opcode, encoder):
        return self._add(self.encoders, protocol_version, opcode, encoder)

    def add_decoder(self, protocol_version, opcode, decoder):
        return self._add(self.decoders, protocol_version, opcode, decoder)

    def get_encoder(self, protocol_version, opcode):
        return self._get(self.encoders, protocol_version, opcode)

    def get_decoder(self, protocol_version, opcode):
        return self._get(self.decoders, protocol_version, opcode)

    @classmethod
    def factory(cls):
        """Factory to construct the default message codec registry"""

        registry = cls()
        # TODO will be get from the DriverContext protocol version registry later
        protocol_versions = (ProtocolVersion.V3, ProtocolVersion.V4, ProtocolVersion.V5)
        for v in protocol_versions:
            for message in [
                StartupMessage,
                RegisterMessage,
                BatchMessage,
                QueryMessage,
                ExecuteMessage,
                PrepareMessage,
                OptionsMessage,
                AuthResponseMessage,
            ]:
                registry.add_encoder(v, message.opcode, message.encode)

            error_decoders = [(e.error_code, e.decode) for e in [
                UnavailableErrorMessage,
                ReadTimeoutErrorMessage,
                WriteTimeoutErrorMessage,
                IsBootstrappingErrorMessage,
                OverloadedErrorMessage,
                UnauthorizedErrorMessage,
                ServerError,
                ProtocolException,
                BadCredentials,
                TruncateError,
                ReadFailureMessage,
                FunctionFailureMessage,
                WriteFailureMessage,
                CDCWriteException,
                SyntaxException,
                InvalidRequestException,
                ConfigurationException,
                PreparedQueryNotFound,
                AlreadyExistsException
            ]]

            for codec in [
                ReadyMessage,
                EventMessage.Codec,
                ResultMessage.Codec,
                AuthenticateMessage,
                AuthSuccessMessage,
                AuthChallengeMessage,
                SupportedMessage,
                ErrorMessage.Codec(error_decoders)

            ]:
                registry.add_decoder(v, codec.opcode, codec.decode)

        return registry
