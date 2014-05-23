# Copyright 2013-2014 DataStax, Inc.
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

import logging
import socket
from uuid import UUID

import six
from six.moves import range

from cassandra import (Unavailable, WriteTimeout, ReadTimeout,
                       AlreadyExists, InvalidRequest, Unauthorized,
                       UnsupportedOperation)
from cassandra.marshal import (int32_pack, int32_unpack, uint16_pack, uint16_unpack,
                               int8_pack, int8_unpack, header_pack)
from cassandra.cqltypes import (AsciiType, BytesType, BooleanType,
                                CounterColumnType, DateType, DecimalType,
                                DoubleType, FloatType, Int32Type,
                                InetAddressType, IntegerType, ListType,
                                LongType, MapType, SetType, TimeUUIDType,
                                UTF8Type, UUIDType, lookup_casstype)
from cassandra.policies import WriteType

log = logging.getLogger(__name__)


class NotSupportedError(Exception):
    pass


class InternalError(Exception):
    pass


HEADER_DIRECTION_FROM_CLIENT = 0x00
HEADER_DIRECTION_TO_CLIENT = 0x80
HEADER_DIRECTION_MASK = 0x80

COMPRESSED_FLAG = 0x01
TRACING_FLAG = 0x02

_message_types_by_name = {}
_message_types_by_opcode = {}


class _RegisterMessageType(type):
    def __init__(cls, name, bases, dct):
        if not name.startswith('_'):
            _message_types_by_name[cls.name] = cls
            _message_types_by_opcode[cls.opcode] = cls


@six.add_metaclass(_RegisterMessageType)
class _MessageType(object):

    tracing = False

    def to_binary(self, stream_id, protocol_version, compression=None):
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

    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__, ', '.join('%s=%r' % i for i in _get_params(self)))


def _get_params(message_obj):
    base_attrs = dir(_MessageType)
    return (
        (n, a) for n, a in message_obj.__dict__.items()
        if n not in base_attrs and not n.startswith('_') and not callable(a)
    )


def decode_response(stream_id, flags, opcode, body, decompressor=None):
    if flags & COMPRESSED_FLAG:
        if decompressor is None:
            raise Exception("No de-compressor available for compressed frame!")
        body = decompressor(body)
        flags ^= COMPRESSED_FLAG

    body = six.BytesIO(body)
    if flags & TRACING_FLAG:
        trace_id = UUID(bytes=body.read(16))
        flags ^= TRACING_FLAG
    else:
        trace_id = None

    if flags:
        log.warning("Unknown protocol flags set: %02x. May cause problems.", flags)

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


class ErrorMessageSubclass(_RegisterMessageType):
    def __init__(cls, name, bases, dct):
        if cls.error_code is not None:  # Server has an error code of 0.
            error_classes[cls.error_code] = cls


@six.add_metaclass(ErrorMessageSubclass)
class ErrorMessageSub(ErrorMessage):
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


class BadCredentials(ErrorMessageSub):
    summary = 'Bad credentials'
    error_code = 0x0100


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
            'write_type': WriteType.name_to_value[read_string(f)],
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
        if protocol_version > 1:
            raise UnsupportedOperation(
                "Credentials-based authentication is not supported with "
                "protocol version 2 or higher.  Use the SASL authentication "
                "mechanism instead.")
        write_short(f, len(self.creds))
        for credkey, credval in self.creds.items():
            write_string(f, credkey)
            write_string(f, credval)


class AuthChallengeMessage(_MessageType):
    opcode = 0x0E
    name = 'AUTH_CHALLENGE'

    def __init__(self, challenge):
        self.challenge = challenge

    @classmethod
    def recv_body(cls, f):
        return cls(read_longstring(f))


class AuthResponseMessage(_MessageType):
    opcode = 0x0F
    name = 'AUTH_RESPONSE'

    def __init__(self, response):
        self.response = response

    def send_body(self, f, protocol_version):
        write_longstring(f, self.response)


class AuthSuccessMessage(_MessageType):
    opcode = 0x10
    name = 'AUTH_SUCCESS'

    def __init__(self, token):
        self.token = token

    @classmethod
    def recv_body(cls, f):
        return cls(read_longstring(f))


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


# used for QueryMessage and ExecuteMessage
_VALUES_FLAG = 0x01
_SKIP_METADATA_FLAG = 0x01
_PAGE_SIZE_FLAG = 0x04
_WITH_PAGING_STATE_FLAG = 0x08
_WITH_SERIAL_CONSISTENCY_FLAG = 0x10


class QueryMessage(_MessageType):
    opcode = 0x07
    name = 'QUERY'

    def __init__(self, query, consistency_level, serial_consistency_level=None,
                 fetch_size=None, paging_state=None):
        self.query = query
        self.consistency_level = consistency_level
        self.serial_consistency_level = serial_consistency_level
        self.fetch_size = fetch_size
        self.paging_state = paging_state

    def send_body(self, f, protocol_version):
        write_longstring(f, self.query)
        write_consistency_level(f, self.consistency_level)
        flags = 0x00
        if self.serial_consistency_level:
            if protocol_version >= 2:
                flags |= _WITH_SERIAL_CONSISTENCY_FLAG
            else:
                raise UnsupportedOperation(
                    "Serial consistency levels require the use of protocol version "
                    "2 or higher. Consider setting Cluster.protocol_version to 2 "
                    "to support serial consistency levels.")

        if self.fetch_size:
            if protocol_version >= 2:
                flags |= _PAGE_SIZE_FLAG
            else:
                raise UnsupportedOperation(
                    "Automatic query paging may only be used with protocol version "
                    "2 or higher. Consider setting Cluster.protocol_version to 2.")

        if self.paging_state:
            if protocol_version >= 2:
                flags |= _WITH_PAGING_STATE_FLAG
            else:
                raise UnsupportedOperation(
                    "Automatic query paging may only be used with protocol version "
                    "2 or higher. Consider setting Cluster.protocol_version to 2.")

        write_byte(f, flags)
        if self.fetch_size:
            write_int(f, self.fetch_size)
        if self.paging_state:
            write_longstring(f, self.paging_state)
        if self.serial_consistency_level:
            write_consistency_level(f, self.serial_consistency_level)

CUSTOM_TYPE = object()

RESULT_KIND_VOID = 0x0001
RESULT_KIND_ROWS = 0x0002
RESULT_KIND_SET_KEYSPACE = 0x0003
RESULT_KIND_PREPARED = 0x0004
RESULT_KIND_SCHEMA_CHANGE = 0x0005


class ResultMessage(_MessageType):
    opcode = 0x08
    name = 'RESULT'

    kind = None
    results = None
    paging_state = None

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
    _HAS_MORE_PAGES_FLAG = 0x0002
    _NO_METADATA_FLAG = 0x0004

    def __init__(self, kind, results, paging_state=None):
        self.kind = kind
        self.results = results
        self.paging_state = paging_state

    @classmethod
    def recv_body(cls, f):
        kind = read_int(f)
        paging_state = None
        if kind == RESULT_KIND_VOID:
            results = None
        elif kind == RESULT_KIND_ROWS:
            paging_state, results = cls.recv_results_rows(f)
        elif kind == RESULT_KIND_SET_KEYSPACE:
            ksname = read_string(f)
            results = ksname
        elif kind == RESULT_KIND_PREPARED:
            results = cls.recv_results_prepared(f)
        elif kind == RESULT_KIND_SCHEMA_CHANGE:
            results = cls.recv_results_schema_change(f)
        return cls(kind, results, paging_state)

    @classmethod
    def recv_results_rows(cls, f):
        paging_state, column_metadata = cls.recv_results_metadata(f)
        rowcount = read_int(f)
        rows = [cls.recv_row(f, len(column_metadata)) for _ in range(rowcount)]
        colnames = [c[2] for c in column_metadata]
        coltypes = [c[3] for c in column_metadata]
        return (
            paging_state,
            (colnames, [tuple(ctype.from_binary(val) for ctype, val in zip(coltypes, row))
                        for row in rows]))

    @classmethod
    def recv_results_prepared(cls, f):
        query_id = read_binary_string(f)
        _, column_metadata = cls.recv_results_metadata(f)
        return (query_id, column_metadata)

    @classmethod
    def recv_results_metadata(cls, f):
        flags = read_int(f)
        glob_tblspec = bool(flags & cls._FLAGS_GLOBAL_TABLES_SPEC)
        colcount = read_int(f)
        if flags & cls._HAS_MORE_PAGES_FLAG:
            paging_state = read_binary_longstring(f)
        else:
            paging_state = None
        if glob_tblspec:
            ksname = read_string(f)
            cfname = read_string(f)
        column_metadata = []
        for _ in range(colcount):
            if glob_tblspec:
                colksname = ksname
                colcfname = cfname
            else:
                colksname = read_string(f)
                colcfname = read_string(f)
            colname = read_string(f)
            coltype = cls.read_type(f)
            column_metadata.append((colksname, colcfname, colname, coltype))
        return paging_state, column_metadata

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
            typeclass = typeclass.apply_parameters((subtype,))
        elif typeclass == MapType:
            keysubtype = cls.read_type(f)
            valsubtype = cls.read_type(f)
            typeclass = typeclass.apply_parameters((keysubtype, valsubtype))
        elif typeclass == CUSTOM_TYPE:
            classname = read_string(f)
            typeclass = lookup_casstype(classname)

        return typeclass

    @staticmethod
    def recv_row(f, colcount):
        return [read_value(f) for _ in range(colcount)]


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

    def __init__(self, query_id, query_params, consistency_level,
                 serial_consistency_level=None, fetch_size=None,
                 paging_state=None):
        self.query_id = query_id
        self.query_params = query_params
        self.consistency_level = consistency_level
        self.serial_consistency_level = serial_consistency_level
        self.fetch_size = fetch_size
        self.paging_state = paging_state

    def send_body(self, f, protocol_version):
        write_string(f, self.query_id)
        if protocol_version == 1:
            if self.serial_consistency_level:
                raise UnsupportedOperation(
                    "Serial consistency levels require the use of protocol version "
                    "2 or higher. Consider setting Cluster.protocol_version to 2 "
                    "to support serial consistency levels.")
            if self.fetch_size or self.paging_state:
                raise UnsupportedOperation(
                    "Automatic query paging may only be used with protocol version "
                    "2 or higher. Consider setting Cluster.protocol_version to 2.")
            write_short(f, len(self.query_params))
            for param in self.query_params:
                write_value(f, param)
            write_consistency_level(f, self.consistency_level)
        else:
            write_consistency_level(f, self.consistency_level)
            flags = _VALUES_FLAG
            if self.serial_consistency_level:
                flags |= _WITH_SERIAL_CONSISTENCY_FLAG
            if self.fetch_size:
                flags |= _PAGE_SIZE_FLAG
            if self.paging_state:
                flags |= _WITH_PAGING_STATE_FLAG
            write_byte(f, flags)
            write_short(f, len(self.query_params))
            for param in self.query_params:
                write_value(f, param)
            if self.fetch_size:
                write_int(f, self.fetch_size)
            if self.paging_state:
                write_longstring(f, self.paging_state)
            if self.serial_consistency_level:
                write_consistency_level(f, self.serial_consistency_level)


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
        for prepared, string_or_query_id, params in self.queries:
            if not prepared:
                write_byte(f, 0)
                write_longstring(f, string_or_query_id)
            else:
                write_byte(f, 1)
                write_short(f, len(string_or_query_id))
                f.write(string_or_query_id)
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


def write_header(f, version, flags, stream_id, opcode, length):
    """
    Write a CQL protocol frame header.
    """
    f.write(header_pack(version, flags, stream_id, opcode))
    write_int(f, length)


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
    if isinstance(s, six.text_type):
        s = s.encode('utf8')
    write_short(f, len(s))
    f.write(s)


def read_binary_longstring(f):
    size = read_int(f)
    contents = f.read(size)
    return contents


def read_longstring(f):
    return read_binary_longstring(f).decode('utf8')


def write_longstring(f, s):
    if isinstance(s, six.text_type):
        s = s.encode('utf8')
    write_int(f, len(s))
    f.write(s)


def read_stringlist(f):
    numstrs = read_short(f)
    return [read_string(f) for _ in range(numstrs)]


def write_stringlist(f, stringlist):
    write_short(f, len(stringlist))
    for s in stringlist:
        write_string(f, s)


def read_stringmap(f):
    numpairs = read_short(f)
    strmap = {}
    for _ in range(numpairs):
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
    for _ in range(numkeys):
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
