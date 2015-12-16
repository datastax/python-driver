from cassandra.auth import AuthProvider, Authenticator

try:
    from puresasl.client import SASLClient
except ImportError:
    SASLClient = None

class DseAuthProvider(AuthProvider):
    def __init__(self, username=None, password=None, service=None, qops=None):
        self.username = username
        self.password = password
        if username is None:
            if SASLClient is None:
                raise ImportError('The puresasl library has not been installed')
            self.service = service
            self.qops = qops

    def new_authenticator(self, host):
        if self.username:
            return PlainTextAuthenticator(self.username, self.password)
        return GSSAPIAuthenticator(host, self.service, self.qops)

class BaseDseAuthenticator(Authenticator):
    def get_mechanism(self):
        raise NotImplementedError("get_mechanism not implemented")

    def get_initial_challenge(self):
        raise NotImplementedError("get_initial_challenge not implemented")

    def initial_response(self):
        if (self.authenticator_class == "com.datastax.bdp.cassandra.auth.DseAuthenticator"):
            return self.get_mechanism()
        else:
            return self.evaluate_challenge(self.get_initial_challenge())

class PlainTextAuthenticator(BaseDseAuthenticator):
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

class GSSAPIAuthenticator(BaseDseAuthenticator):
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
