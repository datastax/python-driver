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

import six

from cassandra import CoreProtocolVersion


class ProtocolVersionRegistry(object):
    """Default implementation of the ProtocolVersionRegistry"""

    _protocol_versions = None
    """A map of registered protocol versions by code"""

    def __init__(self, protocol_versions=None):
        versions = protocol_versions if protocol_versions else CoreProtocolVersion.VERSIONS
        self._protocol_versions = {v.code: v for v in versions}

    # TODO no need to re-compute those each time... just set properties when the registry is modified
    def supported_versions(self):
        """
        Return a tuple of all supported protocol versions.
        """
        return sorted(six.itervalues(self._protocol_versions), reverse=True)

    def beta_versions(self):
        """
        Return a tuple of all beta protocol versions.
        """
        return (v for v in self.supported_versions() if v.is_beta)

    def min_supported(self):
        """
        Return the minimum protocol version supported by this driver.
        """
        return min(self.supported_versions())

    def max_supported(self):
        """
        Return the maximum protocol versioni supported by this driver.
        """
        return max(self.supported_versions())

    def get_lower_supported(self, previous_version):
        """
        Return the lower supported protocol version. Beta versions are omitted.
        """
        try:
            version = next(v for v in sorted(self.supported_versions(), reverse=True) if
                           not v.is_beta and v < previous_version)
        except StopIteration:
            version = None

        return version

    def max_non_beta_supported(self):
        return max(v for v in self.supported_versions() if not v.is_beta)

    # will disappear anyway with codecs
    def uses_int_query_flags(self, version):
        try:
            return version.code >= 5
        except:
            return version >= 5

    def uses_prepare_flags(self, version):
        try:
            return version.code >= 5
        except:
            return version >= 5

    def uses_prepared_metadata(self, version):
        try:
            return version.code >= 5
        except:
            return version >= 5

    def uses_error_code_map(self, version):
        try:
            return version.code >= 5
        except:
            return version >= 5

    def uses_keyspace_flag(self, version):
        try:
            return version.code >= 5
        except:
            return version >= 5


# test purpose
class MessageCodecRegistry(object):
    """Default implementation of the MessageCodecRegistry"""

    _message_codecs = None

    def __init__(self):
        # we should probably have  an ordered data structure here... so we could fall back
        # to the codec of the previous version if nothing has changed
        # Otherwise, we register all codecs for all versions explicitly (might be clearer)
        self._message_codecs = {}

    def add_encoder(self, message, protocol_version, codec):
        k = (message.opcode, protocol_version.code)
        self._message_codecs[k] = codec

    # add_decoder

    def get_encoder(self, message, protocol_version):
        k = (message.opcode, protocol_version)
        try:
            return self._message_codecs[k].encode
        except KeyError:
            raise ValueError("No codec registered for message '{0:02X}' and protocol version '{1}'".format(message.opcode, protocol_version))

    # get decoder