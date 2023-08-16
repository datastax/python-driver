import logging

log = logging.getLogger(__name__)


RATE_LIMIT_ERROR_EXTENSION = "SCYLLA_RATE_LIMIT_ERROR"

class ProtocolFeatures(object):
    rate_limit_error = None

    def __init__(self, rate_limit_error=None):
        self.rate_limit_error = rate_limit_error

    @staticmethod
    def parse_from_supported(supported):
        return ProtocolFeatures(rate_limit_error = ProtocolFeatures.maybe_parse_rate_limit_error(supported))

    @staticmethod
    def maybe_parse_rate_limit_error(supported):
        vals = supported.get(RATE_LIMIT_ERROR_EXTENSION)
        if vals is not None:
            code_str = ProtocolFeatures.get_cql_extension_field(vals, "ERROR_CODE")
            return int(code_str)

    #  Looks up a field which starts with `key=` and returns the rest
    @staticmethod
    def get_cql_extension_field(vals, key):
        for v in vals:
            stripped_v = v.strip()
            if stripped_v.startswith(key) and stripped_v[len(key)] == '=':
                result = stripped_v[len(key) + 1:]
                return result
        return None

    def add_startup_options(self, options):
        if self.rate_limit_error is not None:
            options[RATE_LIMIT_ERROR_EXTENSION] = ""

