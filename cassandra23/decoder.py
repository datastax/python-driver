import struct
import six
from six.moves import range
import uuid


# Low level byte pack and unpack methods.
def _make_pack_unpack_field(format):
    s = struct.Struct(format)
    return (
        s.pack,
        lambda b: s.unpack(b)[0]
    )

_header_struct = struct.Struct('!BBBBL')
pack_cql_header = _header_struct.pack
unpack_cql_header = _header_struct.unpack
pack_cql_byte, unpack_cql_byte = _make_pack_unpack_field('!B')
pack_cql_int, unpack_cql_int = _make_pack_unpack_field('!i')
pack_cql_short, unpack_cql_short = _make_pack_unpack_field('!H')


# Maximum values for these data types.
MAX_INT = 0x7FFFFFFF
MAX_SHORT = 0xFFFF

    
def read_header(f):
    """
    Read a CQL protocol frame header.

    A frame header consists of 4 bytes for the fields version, flags, stream and opcode. This is followed by a 4
    byte length field, reading a total of 8 bytes.

    :returns: tuple consisting of the version, flags, stream, opcode and length fields.

    """
    return unpack_cql_header(f.read(8))


def write_header(f, version, flags, stream_id, opcode, length):
    """
    Write a CQL protocol frame header.
    """
    f.write(pack_cql_header(version, flags, stream_id, opcode, length))


def read_byte(f):
    return f.read()


def write_byte(f, v):
    f.write(pack_cql_byte(v))


def read_int(f):
    return unpack_cql_int(f.read(4))


def write_int(f, v):
    f.write(pack_cql_int(v))


def read_short(f):
    return unpack_cql_short(f.read(2))


def write_short(f, v):
    f.write(pack_cql_short(v))


def read_string(f):
    """
    :returns: Python 3 returns a str; Python 2 returns a unicode string.
    """
    n = f.read_short()
    return f.read(n).decode('UTF8')


def write_string(f, v):
    # TODO: Should really check that a short string isn't longer than a 2^2.
    if isinstance(v, six.text_type):
        b = v.encode('UTF8')
        write_short(f, len(b))
        f.write(b)
    elif isinstance(v, str):
        # This assumes that str will be caught by the previous if statement with Python 3.
        write_short(f, len(v))
        f.write(v)
    else:
        write_string(f, str(v))


def read_long_string(f):
    """
    :returns: Python 3 returns a str; Python 2 returns a unicode string.
    """
    n = read_int(f)
    return f.read(n).decode('UTF8')


def write_long_string(f, v):
    # TODO: Should really check that a long string isn't longer than a 2^4 / 2.
    if isinstance(v, six.text_type):
        b = v.encode('UTF8')
        write_int(f, len(b))
        f.write(b)
    elif isinstance(v, str):
        # This assumes that str will be caught by the previous if statement with Python 3.
        write_int(f, len(v))
        f.write(v)
    else:
        write_long_string(f, str(v))


def read_uuid(f):
    return uuid.UUID(bytes=f.read(16))


def write_uuid(f, v):
    assert isinstance(v, uuid.UUID)
    f.write(v.bytes)


def read_string_list(f):
    n = read_short(f)
    return [read_string(f) for _ in range(n)]


def write_string_list(f, v):
    n = len(v)
    for idx in range(n):
        write_string(f, v[idx])


def read_bytes(f):
    n = read_int(f)
    return None if n < 0 else f.read(n)


def write_bytes(f, v):
    if v is None:
        write_int(f, -1)
        f.write(v)
    else:
        write_int(f, len(v))
        f.write(v)


def read_short_bytes(f):
    n = read_short(f)
    return f.read(n)


def write_short_bytes(f, v):
    if v is None:
        write_short(f, 0)
    else:
        n = len(v)
        assert n <= MAX_SHORT
        write_short(f, n)
        f.write(v)


def read_inet(f):
    n = f.read(1)
    values = f.read(n)
    raise NotImplementedError


def write_inet(f, v):
    raise NotImplementedError

read_consistency = read_short
write_consistency = write_short


def read_string_map(f):
    n = read_short(f)
    return dict((read_string(f), read_string(f)) for _ in range(n))


def write_string_map(f, v):
    write_short(f, len(v))
    for key, value in six.iteritems(v):
        write_string(f, key)
        write_string(f, value)


def read_string_multimap(f):
    n = read_short(f)
    return dict((read_string(f), read_string_list(f)) for _ in range(n))


def write_string_multimap(f, v):
    write_short(f, len(v))
    for key, value in six.iteritems(v):
        write_string(f, key)
        write_string_list(f, value)


## Define messages ##############################

HEADER_DIRECTION_FROM_CLIENT = 0x00
HEADER_DIRECTION_TO_CLIENT = 0x80
HEADER_DIRECTION_MASK = 0x80

COMPRESSED_FLAG = 0x01
TRACING_FLAG = 0x02

_message_types_by_name = {}
_message_types_by_opcode = {}
_error_classes = {}


class _RegisterMessageType(type):
    def __init__(cls, what, *args, **kwargs):
        if what not in ('_MessageType', 'NewBase'):
            _message_types_by_name[cls.name] = cls
            _message_types_by_opcode[cls.opcode] = cls


def _get_params(message_obj):
    base_attrs = dir(_MessageType)
    return (
        (n, a) for n, a in message_obj.__dict__.items()
        if n not in base_attrs and not n.startswith('_') and not callable(a)
    )


class _MessageType(six.with_metaclass(_RegisterMessageType, object)):
    opcode = None
    name = None
    tracing = False

    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__, ', '.join('%s=%r' % i for i in _get_params(self)))

    def send_body(self, buf, protocol_version):
        """
        Encode the body of this message for sending.

        :param buf: An instance of `ByteBuffer`.
        :param protocol_version: Version of the protocol currently being used.

        """
        pass

    def to_binary(self, stream_id, protocol_version, compression=None):
        """
        Pack this message into it's binary format.
        """
        body = six.BytesIO()
        self.send_body(body, protocol_version)
        body = body.getvalue()

        flags = 0
        if compression and len(body) > 0:
            body = compression(body)
            flags |= COMPRESSED_FLAG
        if self.tracing:
            flags |= TRACING_FLAG

        msg = six.BytesIO()
        write_header(
            msg,
            protocol_version | HEADER_DIRECTION_FROM_CLIENT,
            flags, stream_id, self.opcode, len(body)
        )
        msg.write(body)

        return msg.getvalue()


def decode_response(stream_id, flags, opcode, body, decompressor=None):
    """
    Build msg class.
    """
    if flags & COMPRESSED_FLAG:
        if callable(decompressor):
            body = decompressor(body)
            flags ^= COMPRESSED_FLAG
        else:
            raise TypeError("De-compressor not available for compressed frame!")

    body = six.BytesIO(body)
    if flags & TRACING_FLAG:
        trace_id = read_uuid(body)
        flags ^= TRACING_FLAG
    else:
        trace_id = None

    if flags:
        # TODO: log.warn("Unknown protocol flags set: %02x. May cause problems.", flags)
        pass

    msg_class = _message_types_by_opcode[opcode]
    msg = msg_class.recv_body(body)
    msg.stream_id = stream_id
    msg.trace_id = trace_id
    return msg


class StartupMessage(_MessageType):
    opcode = 0x01
    name = 'STARTUP'

    KNOWN_OPTION_KEYS = set(('CQL_VERSION', 'COMPRESSION',))

    def __init__(self, cqlversion, options):
        self.cqlversion = cqlversion
        self.options = options

    def send_body(self, f, protocol_version):
        opt_map = self.options.copy()
        opt_map['CQL_VERSION'] = self.cqlversion
        write_string_map(f, opt_map)


class ReadyMessage(_MessageType):
    opcode = 0x02
    name = 'READY'

    @classmethod
    def recv_body(cls, f):
        return cls()


class AuthenticateMessage(_MessageType):
    opcode = 0x03
    name = 'AUTHENTICATE'

    def __init__(self, authenticator):
        self.authenticator = authenticator

    @classmethod
    def recv_body(cls, f):
        authenticator = read_string(f)
        return cls(authenticator)


class CredentialsMessage(_MessageType):
    opcode = 0x04
    name = 'CREDENTIALS'

    def __init__(self, credentials):
        self.credentials = credentials

    def send_body(self, f, protocol_version):
        write_string_map(f, self.credentials)


class OptionsMessage(_MessageType):
    opcode = 0x05
    name = 'OPTIONS'


class SupportedMessage(_MessageType):
    opcode = 0x06
    name = 'SUPPORTED'

    def __init__(self, cql_versions, options):
        self.cql_versions = cql_versions
        self.options = options

    @classmethod
    def recv_body(cls, f):
        options = read_string_multimap(f)
        cql_versions = options.pop('CQL_VERSION')
        return cls(cql_versions, options)


class QueryMessage(_MessageType):
    opcode = 0x07
    name = 'QUERY'

    def __init__(self, query, consistency_level):
        self.query = query
        self.consistency_level = consistency_level

    def send_body(self, f, protocol_version):
        write_long_string(f, self.query)
        write_consistency(f, self.consistency_level)

    @classmethod
    def recv_body(cls, f):
        query = read_long_string(f)
        consistency_level = read_consistency(f)
        return cls(query, consistency_level)


class ResultMessage(_MessageType):
    opcode = 0x08
    name = 'RESULT'

    def __init__(self, kind, results):
        self.kind = kind
        self.results = results


class PrepareMessage(_MessageType):
    opcode = 0x09
    name = 'PREPARE'

    def __init__(self, query):
        self.query = query

    def send_body(self, f, protocol_version):
        write_long_string(f, self.query)
