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
from cassandra.cqltypes import (
    CustomType, AsciiType, BytesType, BooleanType, CounterColumnType,
    DateType, DecimalType, DoubleType, FloatType, Int32Type, InetAddressType,
    IntegerType, ListType, LongType, MapType, SetType, TimeUUIDType,
    UTF8Type, VarcharType, UUIDType, UserType, TupleType, SimpleDateType,
    TimeType, ByteType, ShortType, DurationType, CompositeType,
    DynamicCompositeType, ColumnToCollectionType, ReversedType, FrozenType,
    lookup_casstype, apache_cassandra_type_prefix)


class CqlTypeRegistry(object):
    """Default implementation of the CqlTypeRegistry"""

    lookup_casstype = staticmethod(lookup_casstype)

    type_codes = None

    _casstypes = None
    _cqltypes = None

    def __init__(self, types):
        self._casstypes = {}
        self._cqltypes = {}
        self.type_codes = {}
        for t in types:
            if hasattr(t, 'type_code') and t.type_code not in self.type_codes:
                self.type_codes[t.type_code] = t
            self.add_type(t)

    def add_type(self, type_class):
        self._casstypes[type_class.__name__] = type_class
        try:
            # some types don't have a typename
            if not type_class.typename.startswith(apache_cassandra_type_prefix):
                self._cqltypes[type_class.typename] = type_class
        except AttributeError:
            pass

    def lookup(self, casstype):
        return self.lookup_casstype(casstype, registry=self)

    def lookup_by_typename(self, typename):
        return self._cqltypes[typename]

    def __getitem__(self, item):
        return self._casstypes[item]

    @classmethod
    def factory(cls, types=None):
        """"Factory to construct the default cql type registry

        :param types: All data types to register.
        """
        if types is None:
            types = [
                CustomType, AsciiType, BytesType, BooleanType,
                CounterColumnType, DateType, DecimalType,
                DoubleType, FloatType, Int32Type,
                InetAddressType, IntegerType, ListType,
                LongType, MapType, SetType, TimeUUIDType,
                UTF8Type, VarcharType, UUIDType, UserType,
                TupleType, SimpleDateType, TimeType, ByteType,
                ShortType, DurationType, CompositeType, DynamicCompositeType,
                FrozenType, ColumnToCollectionType, ReversedType
            ]
        return cls(types)


class ProtocolVersionRegistry(object):
    """Default implementation of the ProtocolVersionRegistry"""

    # default versions and support definition
    protocol_version = ProtocolVersion

    supported_versions = None
    """A tuple of supported protocol versions"""

    beta_versions = None
    """A tuple of registered beta protocol versions"""

    def __init__(self, protocol_versions, beta_versions=None):
        self.supported_versions = sorted(protocol_versions, reverse=True)
        self.beta_versions = tuple(beta_versions or [])

    @property
    def min_supported(self):
        """
        Return the minimum protocol version supported by this driver.
        """
        return min(self.supported_versions)

    @property
    def max_supported(self):
        """
        Return the maximum protocol version supported by this driver.
        """
        return max(self.supported_versions)

    def get_lower_supported(self, previous_version):
        """
        Return the lower supported protocol version. Beta versions are omitted.
        """
        try:
            version = next(v for v in sorted(self.supported_versions, reverse=True) if
                           v not in self.beta_versions and v < previous_version)
        except StopIteration:
            version = None

        return version

    @property
    def max_non_beta_supported(self):
        return max(v for v in self.supported_versions if v not in self.beta_versions)

    @classmethod
    def factory(cls, protocol_versions=None, beta_versions=None):
        """"Factory to construct the default protocol version registry

        :param protocol_versions: All protocol versions to register, including beta ones.
        :param beta_versions: The list of beta versions.
        """
        return cls(protocol_versions or cls.protocol_version.SUPPORTED_VERSIONS,
                   beta_versions or cls.protocol_version.BETA_VERSIONS)


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
    def factory(cls, context):
        """Factory to construct the default message codec registry"""

        registry = cls()
        for v in context.protocol_version_registry.supported_versions:
            for message in [
                StartupMessage,
                RegisterMessage,
                OptionsMessage,
                AuthResponseMessage,
            ]:
                registry.add_encoder(v, message.opcode, message.Codec())

            for message in [
                BatchMessage,
                QueryMessage,
                ExecuteMessage,
                PrepareMessage
            ]:
                registry.add_encoder(v, message.opcode, message.Codec(context))

            error_decoders = [(e.error_code, e.Codec()) for e in [
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
                FunctionFailureMessage,
                CDCWriteException,
                SyntaxException,
                InvalidRequestException,
                ConfigurationException,
                PreparedQueryNotFound,
                AlreadyExistsException
            ]]

            error_decoders += [(e.error_code, e.Codec(context)) for e in [
                ReadFailureMessage,
                WriteFailureMessage
            ]]

            for message in [
                ReadyMessage,
                EventMessage,
                AuthenticateMessage,
                AuthSuccessMessage,
                AuthChallengeMessage,
                SupportedMessage
            ]:
                registry.add_decoder(v, message.opcode, message.Codec())

            registry.add_decoder(v, ResultMessage.opcode, ResultMessage.Codec(context))
            registry.add_decoder(v, ErrorMessage.opcode, ErrorMessage.Codec(error_decoders))

        return registry
