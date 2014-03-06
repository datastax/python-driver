import logging
import socket
from uuid import UUID

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO  # ignore flake8 warning: # NOQA

from cassandra import (Unavailable, WriteTimeout, ReadTimeout,
                       AlreadyExists, InvalidRequest, Unauthorized)
from cassandra.marshal import (int32_pack, int32_unpack, uint16_pack, uint16_unpack,
                               int8_pack, int8_unpack)
from cassandra.cqltypes import (AsciiType, BytesType, BooleanType,
                                CounterColumnType, DateType, DecimalType,
                                DoubleType, FloatType, Int32Type,
                                InetAddressType, IntegerType, ListType,
                                LongType, MapType, SetType, TimeUUIDType,
                                UTF8Type, UUIDType, lookup_casstype)

log = logging.getLogger(__name__)


class NotSupportedError(Exception):
    pass


class InternalError(Exception):
    pass


HEADER_DIRECTION_FROM_CLIENT = 0x00
HEADER_DIRECTION_TO_CLIENT = 0x80
HEADER_DIRECTION_MASK = 0x80


_message_types_by_name = {}
_message_types_by_opcode = {}


class _register_msg_type(type):
    def __init__(cls, name, bases, dct):
        if not name.startswith('_'):
            _message_types_by_name[cls.name] = cls
            _message_types_by_opcode[cls.opcode] = cls


class _MessageType(object):
    __metaclass__ = _register_msg_type

    tracing = False

    def to_string(self, stream_id, protocol_version, compression=None):
        body = StringIO()
        self.send_body(body, protocol_version)
        body = body.getvalue()
        version = protocol_version | HEADER_DIRECTION_FROM_CLIENT
        flags = 0
        if compression is not None and len(body) > 0:
            body = compression(body)
            flags |= 0x01
        if self.tracing:
            flags |= 0x02
        msglen = int32_pack(len(body))
        msg_parts = map(int8_pack, (version, flags, stream_id, self.opcode)) + [msglen, body]
        return ''.join(msg_parts)

    def __str__(self):
        paramstrs = ['%s=%r' % (pname, getattr(self, pname)) for pname in _get_params(self)]
        return '<%s(%s)>' % (self.__class__.__name__, ', '.join(paramstrs))
    __repr__ = __str__


def _get_params(message_obj):
    base_attrs = dir(_MessageType)
    return [a for a in dir(message_obj)
            if a not in base_attrs and not a.startswith('_') and not callable(getattr(message_obj, a))]


def decode_response(stream_id, flags, opcode, body, decompressor=None):
    if flags & 0x01:
        if decompressor is None:
            raise Exception("No decompressor available for compressed frame!")
        body = decompressor(body)
        flags ^= 0x01

    body = StringIO(body)
    if flags & 0x02:
        trace_id = UUID(bytes=body.read(16))
        flags ^= 0x02
    else:
        trace_id = None

    if flags:
        log.warn("Unknown protocol flags set: %02x. May cause problems.", flags)

    msg_class = _message_types_by_opcode[opcode]
    msg = msg_class.recv_body(body)
    msg.stream_id = stream_id
    msg.trace_id = trace_id
    return msg


error_classes = {}


class ErrorMessage(_MessageType, Exception):
    opcode = 0x00
    name = 'ERROR'
    summary = 'Unknown'

    def __init__(self, code, message, info):
        self.code = code
        self.message = message
        self.info = info

    @classmethod
    def recv_body(cls, f):
        code = read_int(f)
        msg = read_string(f)
        subcls = error_classes.get(code, cls)
        extra_info = subcls.recv_error_info(f)
        return subcls(code=code, message=msg, info=extra_info)

    def summary_msg(self):
        msg = 'code=%04x [%s] message="%s"' \
              % (self.code, self.summary, self.message)
        if self.info is not None:
            msg += (' info=' + repr(self.info))
        return msg

    def __str__(self):
        return '<ErrorMessage %s>' % self.summary_msg()
    __repr__ = __str__

    @staticmethod
    def recv_error_info(f):
        pass

    def to_exception(self):
        return self


class ErrorMessageSubclass(_register_msg_type):
    def __init__(cls, name, bases, dct):
        if cls.error_code is not None:
            error_classes[cls.error_code] = cls


class ErrorMessageSub(ErrorMessage):
    __metaclass__ = ErrorMessageSubclass
    error_code = None


class RequestExecutionException(ErrorMessageSub):
    pass


class RequestValidationException(ErrorMessageSub):
    pass


class ServerError(ErrorMessageSub):
    summary = 'Server error'
    error_code = 0x0000


class ProtocolException(ErrorMessageSub):
    summary = 'Protocol error'
    error_code = 0x000A


class UnavailableErrorMessage(RequestExecutionException):
    summary = 'Unavailable exception'
    error_code = 0x1000

    @staticmethod
    def recv_error_info(f):
        return {
            'consistency': read_consistency_level(f),
            'required_replicas': read_int(f),
            'alive_replicas': read_int(f),
        }

    def to_exception(self):
        return Unavailable(self.summary_msg(), **self.info)


class OverloadedErrorMessage(RequestExecutionException):
    summary = 'Coordinator node overloaded'
    error_code = 0x1001


class IsBootstrappingErrorMessage(RequestExecutionException):
    summary = 'Coordinator node is bootstrapping'
    error_code = 0x1002


class TruncateError(RequestExecutionException):
    summary = 'Error during truncate'
    error_code = 0x1003


class WriteTimeoutErrorMessage(RequestExecutionException):
    summary = 'Timeout during write request'
    error_code = 0x1100

    @staticmethod
    def recv_error_info(f):
        return {
            'consistency': read_consistency_level(f),
            'received_responses': read_int(f),
            'required_responses': read_int(f),
            'write_type': read_string(f),
        }

    def to_exception(self):
        return WriteTimeout(self.summary_msg(), **self.info)


class ReadTimeoutErrorMessage(RequestExecutionException):
    summary = 'Timeout during read request'
    error_code = 0x1200

    @staticmethod
    def recv_error_info(f):
        return {
            'consistency': read_consistency_level(f),
            'received_responses': read_int(f),
            'required_responses': read_int(f),
            'data_retrieved': bool(read_byte(f)),
        }

    def to_exception(self):
        return ReadTimeout(self.summary_msg(), **self.info)


class SyntaxException(RequestValidationException):
    summary = 'Syntax error in CQL query'
    error_code = 0x2000


class UnauthorizedErrorMessage(RequestValidationException):
    summary = 'Unauthorized'
    error_code = 0x2100

    def to_exception(self):
        return Unauthorized(self.summary_msg())


class InvalidRequestException(RequestValidationException):
    summary = 'Invalid query'
    error_code = 0x2200

    def to_exception(self):
        return InvalidRequest(self.summary_msg())


class ConfigurationException(RequestValidationException):
    summary = 'Query invalid because of configuration issue'
    error_code = 0x2300


class PreparedQueryNotFound(RequestValidationException):
    summary = 'Matching prepared statement not found on this node'
    error_code = 0x2500

    @staticmethod
    def recv_error_info(f):
        # return the query ID
        return read_binary_string(f)


class AlreadyExistsException(ConfigurationException):
    summary = 'Item already exists'
    error_code = 0x2400

    @staticmethod
    def recv_error_info(f):
        return {
            'keyspace': read_string(f),
            'table': read_string(f),
        }

    def to_exception(self):
        return AlreadyExists(**self.info)


class StartupMessage(_MessageType):
    opcode = 0x01
    name = 'STARTUP'

    KNOWN_OPTION_KEYS = set((
        'CQL_VERSION',
        'COMPRESSION',
    ))

    def __init__(self, cqlversion, options):
        self.cqlversion = cqlversion
        self.options = options

    def send_body(self, f, protocol_version):
        optmap = self.options.copy()
        optmap['CQL_VERSION'] = self.cqlversion
        write_stringmap(f, optmap)


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
        authname = read_string(f)
        return cls(authenticator=authname)


class CredentialsMessage(_MessageType):
    opcode = 0x04
    name = 'CREDENTIALS'

    def __init__(self, creds):
        self.creds = creds

    def send_body(self, f, protocol_version):
        write_short(f, len(self.creds))
        for credkey, credval in self.creds.items():
            write_string(f, credkey)
            write_string(f, credval)


class OptionsMessage(_MessageType):
    opcode = 0x05
    name = 'OPTIONS'

    def send_body(self, f, protocol_version):
        pass


class SupportedMessage(_MessageType):
    opcode = 0x06
    name = 'SUPPORTED'

    def __init__(self, cql_versions, options):
        self.cql_versions = cql_versions
        self.options = options

    @classmethod
    def recv_body(cls, f):
        options = read_stringmultimap(f)
        cql_versions = options.pop('CQL_VERSION')
        return cls(cql_versions=cql_versions, options=options)


class QueryMessage(_MessageType):
    opcode = 0x07
    name = 'QUERY'

    _VALUES_FLAG = 0x01
    _SKIP_METADATA_FLAG = 0x01
    _PAGE_SIZE_FLAG = 0x04
    _WITH_PAGING_STATE_SIZE_FLAG = 0x08
    _WITH_SERIAL_CONSISTENCY_FLAG = 0x10

    def __init__(self, query, consistency_level, values=None):
        self.query = query
        self.consistency_level = consistency_level
        self.values = values

    def send_body(self, f, protocol_version):
        write_longstring(f, self.query)
        write_consistency_level(f, self.consistency_level)
        if protocol_version < 2:
            assert not self.values, "QueryMessage got unbound values with protocol v1"
            return

        flags = 0x00
        if self.values:
            flags |= self._VALUES_FLAG

        write_byte(f, flags)

        if self.values:
            write_short(f, len(self.values))
            for value in self.values:
                write_value(f, value)


CUSTOM_TYPE = object()

RESULT_KIND_VOID = 0x0001
RESULT_KIND_ROWS = 0x0002
RESULT_KIND_SET_KEYSPACE = 0x0003
RESULT_KIND_PREPARED = 0x0004
RESULT_KIND_SCHEMA_CHANGE = 0x0005


class ResultMessage(_MessageType):
    opcode = 0x08
    name = 'RESULT'

    _type_codes = {
        0x0000: CUSTOM_TYPE,
        0x0001: AsciiType,
        0x0002: LongType,
        0x0003: BytesType,
        0x0004: BooleanType,
        0x0005: CounterColumnType,
        0x0006: DecimalType,
        0x0007: DoubleType,
        0x0008: FloatType,
        0x0009: Int32Type,
        0x000A: UTF8Type,
        0x000B: DateType,
        0x000C: UUIDType,
        0x000D: UTF8Type,
        0x000E: IntegerType,
        0x000F: TimeUUIDType,
        0x0010: InetAddressType,
        0x0020: ListType,
        0x0021: MapType,
        0x0022: SetType,
    }

    _FLAGS_GLOBAL_TABLES_SPEC = 0x0001

    def __init__(self, kind, results):
        self.kind = kind
        self.results = results

    @classmethod
    def recv_body(cls, f):
        kind = read_int(f)
        if kind == RESULT_KIND_VOID:
            results = None
        elif kind == RESULT_KIND_ROWS:
            results = cls.recv_results_rows(f)
        elif kind == RESULT_KIND_SET_KEYSPACE:
            ksname = read_string(f)
            results = ksname
        elif kind == RESULT_KIND_PREPARED:
            results = cls.recv_results_prepared(f)
        elif kind == RESULT_KIND_SCHEMA_CHANGE:
            results = cls.recv_results_schema_change(f)
        return cls(kind=kind, results=results)

    @classmethod
    def recv_results_rows(cls, f):
        column_metadata = cls.recv_results_metadata(f)
        rowcount = read_int(f)
        rows = [cls.recv_row(f, len(column_metadata)) for x in xrange(rowcount)]
        colnames = [c[2] for c in column_metadata]
        coltypes = [c[3] for c in column_metadata]
        return (colnames, [tuple(ctype.from_binary(val) for ctype, val in zip(coltypes, row))
                           for row in rows])

    @classmethod
    def recv_results_prepared(cls, f):
        query_id = read_binary_string(f)
        column_metadata = cls.recv_results_metadata(f)
        return (query_id, column_metadata)

    @classmethod
    def recv_results_metadata(cls, f):
        flags = read_int(f)
        glob_tblspec = bool(flags & cls._FLAGS_GLOBAL_TABLES_SPEC)
        colcount = read_int(f)
        if glob_tblspec:
            ksname = read_string(f)
            cfname = read_string(f)
        column_metadata = []
        for x in xrange(colcount):
            if glob_tblspec:
                colksname = ksname
                colcfname = cfname
            else:
                colksname = read_string(f)
                colcfname = read_string(f)
            colname = read_string(f)
            coltype = cls.read_type(f)
            column_metadata.append((colksname, colcfname, colname, coltype))
        return column_metadata

    @classmethod
    def recv_results_schema_change(cls, f):
        change_type = read_string(f)
        keyspace = read_string(f)
        table = read_string(f)
        return dict(change_type=change_type, keyspace=keyspace, table=table)

    @classmethod
    def read_type(cls, f):
        optid = read_short(f)
        try:
            typeclass = cls._type_codes[optid]
        except KeyError:
            raise NotSupportedError("Unknown data type code 0x%04x. Have to skip"
                                    " entire result set." % (optid,))
        if typeclass in (ListType, SetType):
            subtype = cls.read_type(f)
            typeclass = typeclass.apply_parameters(subtype)
        elif typeclass == MapType:
            keysubtype = cls.read_type(f)
            valsubtype = cls.read_type(f)
            typeclass = typeclass.apply_parameters(keysubtype, valsubtype)
        elif typeclass == CUSTOM_TYPE:
            classname = read_string(f)
            typeclass = lookup_casstype(classname)

        return typeclass

    @staticmethod
    def recv_row(f, colcount):
        return [read_value(f) for x in xrange(colcount)]


class PrepareMessage(_MessageType):
    opcode = 0x09
    name = 'PREPARE'

    def __init__(self, query):
        self.query = query

    def send_body(self, f, protocol_version):
        write_longstring(f, self.query)


class ExecuteMessage(_MessageType):
    opcode = 0x0A
    name = 'EXECUTE'

    def __init__(self, query_id, query_params, consistency_level):
        self.query_id = query_id
        self.query_params = query_params
        self.consistency_level = consistency_level

    def send_body(self, f, protocol_version):
        write_string(f, self.query_id)
        write_short(f, len(self.query_params))
        for param in self.query_params:
            write_value(f, param)
        write_consistency_level(f, self.consistency_level)


class BatchMessage(_MessageType):
    opcode = 0x0D
    name = 'BATCH'

    def __init__(self, batch_type, queries, consistency_level):
        self.batch_type = batch_type
        self.queries = queries
        self.consistency_level = consistency_level

    def send_body(self, f, protocol_version):
        write_byte(f, self.batch_type.value)
        write_short(f, len(self.queries))
        for string_or_query_id, params in self.queries:
            if isinstance(string_or_query_id, basestring):
                write_byte(f, 0)
                write_longstring(f, string_or_query_id)
            else:
                write_byte(f, 1)
                write_short(f, string_or_query_id)
            write_short(f, len(params))
            for param in params:
                write_value(f, param)

        write_consistency_level(f, self.consistency_level)


known_event_types = frozenset((
    'TOPOLOGY_CHANGE',
    'STATUS_CHANGE',
    'SCHEMA_CHANGE'
))


class RegisterMessage(_MessageType):
    opcode = 0x0B
    name = 'REGISTER'

    def __init__(self, event_list):
        self.event_list = event_list

    def send_body(self, f, protocol_version):
        write_stringlist(f, self.event_list)


class EventMessage(_MessageType):
    opcode = 0x0C
    name = 'EVENT'

    def __init__(self, event_type, event_args):
        self.event_type = event_type
        self.event_args = event_args

    @classmethod
    def recv_body(cls, f):
        event_type = read_string(f).upper()
        if event_type in known_event_types:
            read_method = getattr(cls, 'recv_' + event_type.lower())
            return cls(event_type=event_type, event_args=read_method(f))
        raise NotSupportedError('Unknown event type %r' % event_type)

    @classmethod
    def recv_topology_change(cls, f):
        # "NEW_NODE" or "REMOVED_NODE"
        change_type = read_string(f)
        address = read_inet(f)
        return dict(change_type=change_type, address=address)

    @classmethod
    def recv_status_change(cls, f):
        # "UP" or "DOWN"
        change_type = read_string(f)
        address = read_inet(f)
        return dict(change_type=change_type, address=address)

    @classmethod
    def recv_schema_change(cls, f):
        # "CREATED", "DROPPED", or "UPDATED"
        change_type = read_string(f)
        keyspace = read_string(f)
        table = read_string(f)
        return dict(change_type=change_type, keyspace=keyspace, table=table)


def read_byte(f):
    return int8_unpack(f.read(1))


def write_byte(f, b):
    f.write(int8_pack(b))


def read_int(f):
    return int32_unpack(f.read(4))


def write_int(f, i):
    f.write(int32_pack(i))


def read_short(f):
    return uint16_unpack(f.read(2))


def write_short(f, s):
    f.write(uint16_pack(s))


def read_consistency_level(f):
    return read_short(f)


def write_consistency_level(f, cl):
    write_short(f, cl)


def read_string(f):
    size = read_short(f)
    contents = f.read(size)
    return contents.decode('utf8')


def read_binary_string(f):
    size = read_short(f)
    contents = f.read(size)
    return contents


def write_string(f, s):
    if isinstance(s, unicode):
        s = s.encode('utf8')
    write_short(f, len(s))
    f.write(s)


def read_longstring(f):
    size = read_int(f)
    contents = f.read(size)
    return contents.decode('utf8')


def write_longstring(f, s):
    if isinstance(s, unicode):
        s = s.encode('utf8')
    write_int(f, len(s))
    f.write(s)


def read_stringlist(f):
    numstrs = read_short(f)
    return [read_string(f) for x in xrange(numstrs)]


def write_stringlist(f, stringlist):
    write_short(f, len(stringlist))
    for s in stringlist:
        write_string(f, s)


def read_stringmap(f):
    numpairs = read_short(f)
    strmap = {}
    for x in xrange(numpairs):
        k = read_string(f)
        strmap[k] = read_string(f)
    return strmap


def write_stringmap(f, strmap):
    write_short(f, len(strmap))
    for k, v in strmap.items():
        write_string(f, k)
        write_string(f, v)


def read_stringmultimap(f):
    numkeys = read_short(f)
    strmmap = {}
    for x in xrange(numkeys):
        k = read_string(f)
        strmmap[k] = read_stringlist(f)
    return strmmap


def write_stringmultimap(f, strmmap):
    write_short(f, len(strmmap))
    for k, v in strmmap.items():
        write_string(f, k)
        write_stringlist(f, v)


def read_value(f):
    size = read_int(f)
    if size < 0:
        return None
    return f.read(size)


def write_value(f, v):
    if v is None:
        write_int(f, -1)
    else:
        write_int(f, len(v))
        f.write(v)


def read_inet(f):
    size = read_byte(f)
    addrbytes = f.read(size)
    port = read_int(f)
    if size == 4:
        addrfam = socket.AF_INET
    elif size == 16:
        addrfam = socket.AF_INET6
    else:
        raise InternalError("bad inet address: %r" % (addrbytes,))
    return (socket.inet_ntop(addrfam, addrbytes), port)


def write_inet(f, addrtuple):
    addr, port = addrtuple
    if ':' in addr:
        addrfam = socket.AF_INET6
    else:
        addrfam = socket.AF_INET
    addrbytes = socket.inet_pton(addrfam, addr)
    write_byte(f, len(addrbytes))
    f.write(addrbytes)
    write_int(f, port)
