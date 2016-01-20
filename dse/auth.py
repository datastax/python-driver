from cassandra.auth import AuthProvider, Authenticator

try:
    import kerberos
    _have_kerberos = True
except ImportError:
    _have_kerberos = False

try:
    from puresasl.client import SASLClient
    _have_puresasl = True
except ImportError:
    _have_puresasl = False


class DSEPlainTextAuthProvider(AuthProvider):
    def __init__(self, username=None, password=None):
        self.username = username
        self.password = password

    def new_authenticator(self, host):
        return PlainTextAuthenticator(self.username, self.password)


class DSEGSSAPIAuthProvider(AuthProvider):
    def __init__(self, service=None, qops=None):
        if not _have_puresasl:
            raise ImportError('The puresasl library has not been installed')
        if not _have_kerberos:
            raise ImportError('The kerberos library has not been installed')
        self.service = service
        self.qops = qops

    def new_authenticator(self, host):
        return GSSAPIAuthenticator(host, self.service, self.qops)


class BaseDSEAuthenticator(Authenticator):
    def get_mechanism(self):
        raise NotImplementedError("get_mechanism not implemented")

    def get_initial_challenge(self):
        raise NotImplementedError("get_initial_challenge not implemented")

    def initial_response(self):
        if self.server_authenticator_class == "com.datastax.bdp.cassandra.auth.DseAuthenticator":
            return self.get_mechanism()
        else:
            return self.evaluate_challenge(self.get_initial_challenge())


class PlainTextAuthenticator(BaseDSEAuthenticator):
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def get_mechanism(self):
        return "PLAIN"

    def get_initial_challenge(self):
        return "PLAIN-START"

    def evaluate_challenge(self, challenge):
        if challenge == 'PLAIN-START':
            return "\x00%s\x00%s" % (self.username, self.password)
        raise Exception('Did not receive a valid challenge response from server')


class GSSAPIAuthenticator(BaseDSEAuthenticator):
    def __init__(self, host, service, qops):
        self.sasl = SASLClient(host, service, 'GSSAPI', authorization_id=None, callback=None, qops=qops)

    def get_mechanism(self):
        return "GSSAPI"

    def get_initial_challenge(self):
        return "GSSAPI-START"

    def evaluate_challenge(self, challenge):
        if challenge == 'GSSAPI-START':
            return self.sasl.process()
        else:
            return self.sasl.process(challenge)
