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
try:
    from puresasl.client import SASLClient
except ImportError:
    SASLClient = None

class AuthProvider(object):
    """
    An abstract class that defines the interface that will be used for
    creating :class:`~.Authenticator` instances when opening new
    connections to Cassandra.

    .. versionadded:: 2.0.0
    """

    def new_authenticator(self, host):
        """
        Implementations of this class should return a new instance
        of :class:`~.Authenticator` or one of its subclasses.
        """
        raise NotImplementedError()


class Authenticator(object):
    """
    An abstract class that handles SASL authentication with Cassandra servers.

    Each time a new connection is created and the server requires authentication,
    a new instance of this class will be created by the corresponding
    :class:`~.AuthProvider` to handler that authentication. The lifecycle of the
    new :class:`~.Authenticator` will the be:

    1) The :meth:`~.initial_response()` method will be called. The return
    value will be sent to the server to initiate the handshake.

    2) The server will respond to each client response by either issuing a
    challenge or indicating that the authentication is complete (successful or not).
    If a new challenge is issued, :meth:`~.evaluate_challenge()`
    will be called to produce a response that will be sent to the
    server. This challenge/response negotiation will continue until the server
    responds that authentication is successful (or an :exc:`~.AuthenticationFailed`
    is raised).

    3) When the server indicates that authentication is successful,
    :meth:`~.on_authentication_success` will be called a token string that
    that the server may optionally have sent.

    The exact nature of the negotiation between the client and server is specific
    to the authentication mechanism configured server-side.

    .. versionadded:: 2.0.0
    """

    server_authenticator_class = None
    """ Set during the connection AUTHENTICATE phase """

    def initial_response(self):
        """
        Returns an message to send to the server to initiate the SASL handshake.
        :const:`None` may be returned to send an empty message.
        """
        return None

    def evaluate_challenge(self, challenge):
        """
        Called when the server sends a challenge message.  Generally, this method
        should return :const:`None` when authentication is complete from a
        client perspective.  Otherwise, a string should be returned.
        """
        raise NotImplementedError()

    def on_authentication_success(self, token):
        """
        Called when the server indicates that authentication was successful.
        Depending on the authentication mechanism, `token` may be :const:`None`
        or a string.
        """
        pass


class PlainTextAuthProvider(AuthProvider):
    """
    An :class:`~.AuthProvider` that works with Cassandra's PasswordAuthenticator.

    Example usage::

        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider

        auth_provider = PlainTextAuthProvider(
                username='cassandra', password='cassandra')
        cluster = Cluster(auth_provider=auth_provider)

    .. versionadded:: 2.0.0
    """

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def new_authenticator(self, host):
        return PlainTextAuthenticator(self.username, self.password)


class PlainTextAuthenticator(Authenticator):
    """
    An :class:`~.Authenticator` that works with Cassandra's PasswordAuthenticator.

    .. versionadded:: 2.0.0
    """

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def initial_response(self):
        return "\x00%s\x00%s" % (self.username, self.password)

    def evaluate_challenge(self, challenge):
        return None


class SaslAuthProvider(AuthProvider):
    """
    An :class:`~.AuthProvider` supporting general SASL auth mechanisms

    Suitable for GSSAPI or other SASL mechanisms

    Example usage::

        from cassandra.cluster import Cluster
        from cassandra.auth import SaslAuthProvider

        sasl_kwargs = {'service': 'something',
                       'mechanism': 'GSSAPI',
                       'qops': 'auth'.split(',')}
        auth_provider = SaslAuthProvider(**sasl_kwargs)
        cluster = Cluster(auth_provider=auth_provider)

    .. versionadded:: 2.1.4
    """

    def __init__(self, **sasl_kwargs):
        if SASLClient is None:
            raise ImportError('The puresasl library has not been installed')
        if 'host' in sasl_kwargs:
            raise ValueError("kwargs should not contain 'host' since it is passed dynamically to new_authenticator")
        self.sasl_kwargs = sasl_kwargs

    def new_authenticator(self, host):
        return SaslAuthenticator(host, **self.sasl_kwargs)


class SaslAuthenticator(Authenticator):
    """
    A pass-through :class:`~.Authenticator` using the third party package
    'pure-sasl' for authentication

    .. versionadded:: 2.1.4
    """

    def __init__(self, host, service, mechanism='GSSAPI', **sasl_kwargs):
        if SASLClient is None:
            raise ImportError('The puresasl library has not been installed')
        self.sasl = SASLClient(host, service, mechanism, **sasl_kwargs)

    def initial_response(self):
        return self.sasl.process()

    def evaluate_challenge(self, challenge):
        return self.sasl.process(challenge)
