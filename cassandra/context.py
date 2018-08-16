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

__all__ = ['DriverContext', 'DefaultDriverContext']


class SingletonProvider(object):
    """
    Providers are strategies of accessing objects. The SingletonProvider
    returns the same object instance on each call. The instance is also
    lazy-initialized.

    :param provider: a callable that is used to create the object instance.
    :param *args: the provider callable args
    :param *kwargs: the provider callable kwargs
    """
    _obj = None
    _provider = None
    _args = None
    _kwargs = None

    def __init__(self, provider, *args, **kwargs):
        self._provider = provider
        self._args = args
        self._kwargs = kwargs

    def __call__(self):
        if self._obj is None:
            self._obj = self._provider(*self._args, **self._kwargs)
        return self._obj


class DriverContext(object):

    _protocol_version_registry = None
    _type_registry = None
    _message_codec_registry = None
    # the default protocol handler
    _protocol_handler = None

    def __init__(self):
        # This file requires delayed imports because the DefaultDriverContext
        # class is also imported from various components in the code.
        from cassandra.protocol import ProtocolHandler
        from cassandra import registry
        self._protocol_version_registry = SingletonProvider(
            registry.ProtocolVersionRegistry.factory)
        self._type_registry = SingletonProvider(registry.CqlTypeRegistry.factory)
        self._message_codec_registry = SingletonProvider(
            registry.MessageCodecRegistry.factory, self)
        self._protocol_handler = SingletonProvider(ProtocolHandler, self)

    @property
    def type_registry(self):
        return self._type_registry()

    @property
    def message_codec_registry(self):
        return self._message_codec_registry()

    @property
    def protocol_handler(self):
        return self._protocol_handler()

    @property
    def protocol_version_registry(self):
        return self._protocol_version_registry()

    @staticmethod
    def factory():
        return DriverContext()


DefaultDriverContext = SingletonProvider(DriverContext.factory)
"""Default DriverContext provider"""