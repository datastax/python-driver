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

import logging


class NullHandler(logging.Handler):

    def emit(self, record):
        pass

logging.getLogger('cassandra').addHandler(NullHandler())

__version_info__ = (4, 0, 'dev0')
__version__ = '.'.join(map(str, __version_info__))


class ProtocolConstants(object):

    class Version(object):
        V3 = 3
        V4 = 4
        V5 = 5

    class Opcode(object):
        ERROR = 0x00
        STARTUP = 0x01
        READY = 0x02
        AUTHENTICATE = 0x03
        OPTIONS = 0x05
        SUPPORTED = 0x06
        QUERY = 0x07
        RESULT = 0x08
        PREPARE = 0x09
        EXECUTE = 0x0A
        REGISTER = 0x0B
        EVENT = 0X0C
        BATCH = 0x0D
        AUTH_CHALLENGE = 0x0E
        AUTH_RESPONSE = 0x0F
        AUTH_SUCCESS = 0x10

    class ErrorCode(object):
        SERVER_ERROR = 0x0000
        PROTOCOL_ERROR = 0x000A
        AUTH_ERROR = 0x0100
        UNAVAILABLE = 0x1000
        OVERLOADED = 0x1001
        IS_BOOTSTRAPPING = 0x1002
        TRUNCATE_ERROR = 0x1003
        WRITE_TIMEOUT = 0x1100
        READ_TIMEOUT = 0x1200
        READ_FAILURE = 0x1300
        FUNCTION_FAILURE = 0x1400
        WRITE_FAILURE = 0x1500
        SYNTAX_ERROR = 0x2000
        UNAUTHORIZED = 0x2100
        INVALID = 0x2200
        CONFIG_ERROR = 0x2300
        ALREADY_EXISTS = 0x2400
        UNPREPARED = 0x2500
        CDC_WRITE_FAILURE = 0x1600

    class ResultKind(object):
        VOID = 0x0001
        ROWS = 0x0002
        SET_KEYSPACE = 0x0003
        PREPARED = 0x0004
        SCHEMA_CHANGE = 0x0005

    class ConsistencyLevel(object):
        ANY = 0x0000
        ONE = 0x0001
        TWO = 0x0002
        THREE = 0x0003
        QUORUM = 0x0004
        ALL = 0x0005
        LOCAL_QUORUM = 0x0006
        EACH_QUORUM = 0x0007
        SERIAL = 0x0008
        LOCAL_SERIAL = 0x0009
        LOCAL_ONE = 0x000A

    class DataType(object):
        CUSTOM = 0x0000
        ASCII = 0x0001
        BIGINT = 0x0002
        BLOB = 0x0003
        BOOLEAN = 0x0004
        COUNTER = 0x0005
        DECIMAL = 0x0006
        DOUBLE = 0x0007
        FLOAT = 0x0008
        INT = 0x0009
        TEXT = 0x000A
        TIMESTAMP = 0x000B
        UUID = 0x000C
        VARCHAR = 0x000D
        VARINT = 0x000E
        TIMEUUID = 0x000F
        INET = 0x0010
        DATE = 0x0011
        TIME = 0x0012
        SMALLINT = 0x0013
        TINYINT = 0x0014
        DURATION = 0x0015
        LIST = 0x0020
        MAP = 0x0021
        SET = 0x0022
        UDT = 0x0030
        TUPLE = 0x0031

    class WriteType(object):
        SIMPLE = "SIMPLE"
        BATCH = "BATCH"
        UNLOGGED_BATCH = "UNLOGGED_BATCH"
        COUNTER = "COUNTER"
        BATCH_LOG = "BATCH_LOG"
        VIEW = "VIEW"
        CDC = "CDC"
        CAS = "CAS"

    class BatchType(object):
        LOGGED = 0x00
        UNLOGGED = 0x01
        COUNTER = 0x02

    class EventType(object):
        TOPOLOGY_CHANGE = "TOPOLOGY_CHANGE"
        STATUS_CHANGE = "STATUS_CHANGE"
        SCHEMA_CHANGE = "SCHEMA_CHANGE"

    class SchemaChangeType(object):
        CREATED = "CREATED"
        UPDATED = "UPDATED"
        DROPPED = "DROPPED"

    class SchemaChangeTarget(object):
        KEYSPACE = "KEYSPACE"
        TABLE = "TABLE"
        TYPE = "TYPE"
        FUNCTION = "FUNCTION"
        AGGREGATE = "AGGREGATE"

    class TopologyChangeType(object):
        NEW_NODE = "NEW_NODE"
        MOVED_NODE = "MOVED_NODE"
        REMOVED_NODE = "REMOVED_NODE"

    class StatusChangeType(object):
        UP = "UP"
        DOWN = "DOWN"

    class FrameFlag(object):
        COMPRESSED = 0x01
        TRACING = 0x02
        CUSTOM_PAYLOAD = 0x04
        WARNING = 0x08
        USE_BETA = 0x10

    class QueryFlag(object):
        VALUES = 0x01
        SKIP_METADATA = 0x02
        PAGE_SIZE = 0x04
        PAGING_STATE = 0x08
        SERIAL_CONSISTENCY = 0x10
        DEFAULT_TIMESTAMP = 0x20
        VALUE_NAMES = 0x40  # not used
        WITH_KEYSPACE = 0x80

    class PrepareFlag(object):
        WITH_KEYSPACE = 0x01

    class RowsFlag(object):
        GLOBAL_TABLES_SPEC = 0x01
        HAS_MORE_PAGES = 0x02
        NO_METADATA = 0x04
        METADATA_CHANGED = 0x08


class ConsistencyLevel(object):
    """
    Spcifies how many replicas must respond for an operation to be considered
    a success.  By default, ``ONE`` is used for all operations.
    """

    ANY = ProtocolConstants.ConsistencyLevel.ANY
    """
    Only requires that one replica receives the write *or* the coordinator
    stores a hint to replay later. Valid only for writes.
    """

    ONE = ProtocolConstants.ConsistencyLevel.ONE
    """
    Only one replica needs to respond to consider the operation a success
    """

    TWO = ProtocolConstants.ConsistencyLevel.TWO
    """
    Two replicas must respond to consider the operation a success
    """

    THREE = ProtocolConstants.ConsistencyLevel.THREE
    """
    Three replicas must respond to consider the operation a success
    """

    QUORUM = ProtocolConstants.ConsistencyLevel.QUORUM
    """
    ``ceil(RF/2)`` replicas must respond to consider the operation a success
    """

    ALL = ProtocolConstants.ConsistencyLevel.ALL
    """
    All replicas must respond to consider the operation a success
    """

    LOCAL_QUORUM = ProtocolConstants.ConsistencyLevel.LOCAL_QUORUM
    """
    Requires a quorum of replicas in the local datacenter
    """

    EACH_QUORUM = ProtocolConstants.ConsistencyLevel.EACH_QUORUM
    """
    Requires a quorum of replicas in each datacenter
    """

    SERIAL = ProtocolConstants.ConsistencyLevel.SERIAL
    """
    For conditional inserts/updates that utilize Cassandra's lightweight
    transactions, this requires consensus among all replicas for the
    modified data.
    """

    LOCAL_SERIAL = ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL
    """
    Like :attr:`~ConsistencyLevel.SERIAL`, but only requires consensus
    among replicas in the local datacenter.
    """

    LOCAL_ONE = ProtocolConstants.ConsistencyLevel.LOCAL_ONE
    """
    Sends a request only to replicas in the local datacenter and waits for
    one response.
    """

ConsistencyLevel.value_to_name = {
    ConsistencyLevel.ANY: 'ANY',
    ConsistencyLevel.ONE: 'ONE',
    ConsistencyLevel.TWO: 'TWO',
    ConsistencyLevel.THREE: 'THREE',
    ConsistencyLevel.QUORUM: 'QUORUM',
    ConsistencyLevel.ALL: 'ALL',
    ConsistencyLevel.LOCAL_QUORUM: 'LOCAL_QUORUM',
    ConsistencyLevel.EACH_QUORUM: 'EACH_QUORUM',
    ConsistencyLevel.SERIAL: 'SERIAL',
    ConsistencyLevel.LOCAL_SERIAL: 'LOCAL_SERIAL',
    ConsistencyLevel.LOCAL_ONE: 'LOCAL_ONE'
}

ConsistencyLevel.name_to_value = {
    'ANY': ConsistencyLevel.ANY,
    'ONE': ConsistencyLevel.ONE,
    'TWO': ConsistencyLevel.TWO,
    'THREE': ConsistencyLevel.THREE,
    'QUORUM': ConsistencyLevel.QUORUM,
    'ALL': ConsistencyLevel.ALL,
    'LOCAL_QUORUM': ConsistencyLevel.LOCAL_QUORUM,
    'EACH_QUORUM': ConsistencyLevel.EACH_QUORUM,
    'SERIAL': ConsistencyLevel.SERIAL,
    'LOCAL_SERIAL': ConsistencyLevel.LOCAL_SERIAL,
    'LOCAL_ONE': ConsistencyLevel.LOCAL_ONE
}


def consistency_value_to_name(value):
    return ConsistencyLevel.value_to_name[value] if value is not None else "Not Set"


class ProtocolVersion(object):
    """
    Defines native protocol versions supported by this driver.
    """
    V3 = ProtocolConstants.Version.V3
    """
    v3, supported in Cassandra 2.1-->3.x+;
    added support for protocol-level client-side timestamps (see :attr:`.Session.use_client_timestamp`),
    serial consistency levels for :class:`~.BatchStatement`, and an improved connection pool.
    """

    V4 = ProtocolConstants.Version.V4
    """
    v4, supported in Cassandra 2.2-->3.x+;
    added a number of new types, server warnings, new failure messages, and custom payloads. Details in the
    `project docs <https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec>`_
    """

    V5 = ProtocolConstants.Version.V5
    """
    v5, in beta from 3.x+
    """

    SUPPORTED_VERSIONS = (V5, V4, V3)
    """A tuple of all protocol versions."""

    BETA_VERSIONS = (V5,)
    """
    A tuple of all beta protocol versions
    """

    @classmethod
    def uses_int_query_flags(cls, version):
        return version >= cls.V5

    @classmethod
    def uses_prepare_flags(cls, version):
        return version >= cls.V5

    @classmethod
    def uses_prepared_metadata(cls, version):
        return version >= cls.V5

    @classmethod
    def uses_error_code_map(cls, version):
        return version >= cls.V5

    @classmethod
    def uses_keyspace_flag(cls, version):
        return version >= cls.V5


class WriteType(object):
    """
    For usage with :class:`.RetryPolicy`, this describe a type
    of write operation.
    """

    SIMPLE = ProtocolConstants.WriteType.SIMPLE
    """
    A write to a single partition key. Such writes are guaranteed to be atomic
    and isolated.
    """

    BATCH = ProtocolConstants.WriteType.BATCH
    """
    A write to multiple partition keys that used the distributed batch log to
    ensure atomicity.
    """

    UNLOGGED_BATCH = ProtocolConstants.WriteType.UNLOGGED_BATCH
    """
    A write to multiple partition keys that did not use the distributed batch
    log. Atomicity for such writes is not guaranteed.
    """

    COUNTER = ProtocolConstants.WriteType.COUNTER
    """
    A counter write (for one or multiple partition keys). Such writes should
    not be replayed in order to avoid overcount.
    """

    BATCH_LOG = ProtocolConstants.WriteType.BATCH_LOG
    """
    The initial write to the distributed batch log that Cassandra performs
    internally before a BATCH write.
    """

    CAS = ProtocolConstants.WriteType.CAS
    """
    A lighweight-transaction write, such as "DELETE ... IF EXISTS".
    """

    VIEW = ProtocolConstants.WriteType.VIEW
    """
    This WriteType is only seen in results for requests that were unable to
    complete MV operations.
    """

    CDC = ProtocolConstants.WriteType.CDC
    """
    This WriteType is only seen in results for requests that were unable to
    complete CDC operations.
    """


class SchemaChangeType(object):
    DROPPED = ProtocolConstants.SchemaChangeType.DROPPED
    CREATED = ProtocolConstants.SchemaChangeType.CREATED
    UPDATED = ProtocolConstants.SchemaChangeType.UPDATED


class SchemaTargetType(object):
    KEYSPACE = ProtocolConstants.SchemaChangeTarget.KEYSPACE
    TABLE = ProtocolConstants.SchemaChangeTarget.TABLE
    TYPE = ProtocolConstants.SchemaChangeTarget.TYPE
    FUNCTION = ProtocolConstants.SchemaChangeTarget.FUNCTION
    AGGREGATE = ProtocolConstants.SchemaChangeTarget.AGGREGATE


class SignatureDescriptor(object):

    def __init__(self, name, argument_types):
        self.name = name
        self.argument_types = argument_types

    @property
    def signature(self):
        """
        function signature string in the form 'name([type0[,type1[...]]])'

        can be used to uniquely identify overloaded function names within a keyspace
        """
        return self.format_signature(self.name, self.argument_types)

    @staticmethod
    def format_signature(name, argument_types):
        return "%s(%s)" % (name, ','.join(t for t in argument_types))

    def __repr__(self):
        return "%s(%s, %s)" % (self.__class__.__name__, self.name, self.argument_types)


class UserFunctionDescriptor(SignatureDescriptor):
    """
    Describes a User function by name and argument signature
    """

    name = None
    """
    name of the function
    """

    argument_types = None
    """
    Ordered list of CQL argument type names comprising the type signature
    """


class UserAggregateDescriptor(SignatureDescriptor):
    """
    Describes a User aggregate function by name and argument signature
    """

    name = None
    """
    name of the aggregate
    """

    argument_types = None
    """
    Ordered list of CQL argument type names comprising the type signature
    """


class DriverException(Exception):
    """
    Base for all exceptions explicitly raised by the driver.
    """
    pass


class RequestExecutionException(DriverException):
    """
    Base for request execution exceptions returned from the server.
    """
    pass


class Unavailable(RequestExecutionException):
    """
    There were not enough live replicas to satisfy the requested consistency
    level, so the coordinator node immediately failed the request without
    forwarding it to any replicas.
    """

    consistency = None
    """ The requested :class:`ConsistencyLevel` """

    required_replicas = None
    """ The number of replicas that needed to be live to complete the operation """

    alive_replicas = None
    """ The number of replicas that were actually alive """

    def __init__(self, summary_message, consistency=None, required_replicas=None, alive_replicas=None):
        self.consistency = consistency
        self.required_replicas = required_replicas
        self.alive_replicas = alive_replicas
        Exception.__init__(self, summary_message + ' info=' +
                           repr({'consistency': consistency_value_to_name(consistency),
                                 'required_replicas': required_replicas,
                                 'alive_replicas': alive_replicas}))


class Timeout(RequestExecutionException):
    """
    Replicas failed to respond to the coordinator node before timing out.
    """

    consistency = None
    """ The requested :class:`ConsistencyLevel` """

    required_responses = None
    """ The number of required replica responses """

    received_responses = None
    """
    The number of replicas that responded before the coordinator timed out
    the operation
    """

    def __init__(self, summary_message, consistency=None, required_responses=None,
                 received_responses=None, **kwargs):
        self.consistency = consistency
        self.required_responses = required_responses
        self.received_responses = received_responses

        info = {'consistency': consistency_value_to_name(consistency),
                'required_responses': required_responses,
                'received_responses': received_responses}
        info.update(kwargs)

        Exception.__init__(self, summary_message + ' info=' + repr(info))


class ReadTimeout(Timeout):
    """
    A subclass of :exc:`Timeout` for read operations.

    This indicates that the replicas failed to respond to the coordinator
    node before the configured timeout. This timeout is configured in
    ``cassandra.yaml`` with the ``read_request_timeout_in_ms``
    and ``range_request_timeout_in_ms`` options.
    """

    data_retrieved = None
    """
    A boolean indicating whether the requested data was retrieved
    by the coordinator from any replicas before it timed out the
    operation
    """

    def __init__(self, message, data_retrieved=None, **kwargs):
        Timeout.__init__(self, message, **kwargs)
        self.data_retrieved = data_retrieved


class WriteTimeout(Timeout):
    """
    A subclass of :exc:`Timeout` for write operations.

    This indicates that the replicas failed to respond to the coordinator
    node before the configured timeout. This timeout is configured in
    ``cassandra.yaml`` with the ``write_request_timeout_in_ms``
    option.
    """

    write_type = None
    """
    The type of write operation, enum on :class:`~cassandra.policies.WriteType`
    """

    def __init__(self, message, write_type=None, **kwargs):
        kwargs["write_type"] = write_type
        Timeout.__init__(self, message, **kwargs)
        self.write_type = write_type


class CDCWriteFailure(RequestExecutionException):
    """
    Hit limit on data in CDC folder, writes are rejected
    """
    def __init__(self, message):
        Exception.__init__(self, message)


class CoordinationFailure(RequestExecutionException):
    """
    Replicas sent a failure to the coordinator.
    """

    consistency = None
    """ The requested :class:`ConsistencyLevel` """

    required_responses = None
    """ The number of required replica responses """

    received_responses = None
    """
    The number of replicas that responded before the coordinator timed out
    the operation
    """

    failures = None
    """
    The number of replicas that sent a failure message
    """

    error_code_map = None
    """
    A map of inet addresses to error codes representing replicas that sent
    a failure message.  Only set when `protocol_version` is 5 or higher.
    """

    def __init__(self, summary_message, consistency=None, required_responses=None,
                 received_responses=None, failures=None, error_code_map=None):
        self.consistency = consistency
        self.required_responses = required_responses
        self.received_responses = received_responses
        self.failures = failures
        self.error_code_map = error_code_map

        info_dict = {
            'consistency': consistency_value_to_name(consistency),
            'required_responses': required_responses,
            'received_responses': received_responses,
            'failures': failures
        }

        if error_code_map is not None:
            # make error codes look like "0x002a"
            formatted_map = dict((addr, '0x%04x' % err_code)
                                 for (addr, err_code) in error_code_map.items())
            info_dict['error_code_map'] = formatted_map

        Exception.__init__(self, summary_message + ' info=' + repr(info_dict))


class ReadFailure(CoordinationFailure):
    """
    A subclass of :exc:`CoordinationFailure` for read operations.

    This indicates that the replicas sent a failure message to the coordinator.
    """

    data_retrieved = None
    """
    A boolean indicating whether the requested data was retrieved
    by the coordinator from any replicas before it timed out the
    operation
    """

    def __init__(self, message, data_retrieved=None, **kwargs):
        CoordinationFailure.__init__(self, message, **kwargs)
        self.data_retrieved = data_retrieved


class WriteFailure(CoordinationFailure):
    """
    A subclass of :exc:`CoordinationFailure` for write operations.

    This indicates that the replicas sent a failure message to the coordinator.
    """

    write_type = None
    """
    The type of write operation, enum on :class:`~cassandra.policies.WriteType`
    """

    def __init__(self, message, write_type=None, **kwargs):
        CoordinationFailure.__init__(self, message, **kwargs)
        self.write_type = write_type


class FunctionFailure(RequestExecutionException):
    """
    User Defined Function failed during execution
    """

    keyspace = None
    """
    Keyspace of the function
    """

    function = None
    """
    Name of the function
    """

    arg_types = None
    """
    List of argument type names of the function
    """

    def __init__(self, summary_message, keyspace, function, arg_types):
        self.keyspace = keyspace
        self.function = function
        self.arg_types = arg_types
        Exception.__init__(self, summary_message)


class RequestValidationException(DriverException):
    """
    Server request validation failed
    """
    pass


class ConfigurationException(RequestValidationException):
    """
    Server indicated request error due to current configuration
    """
    pass


class AlreadyExists(ConfigurationException):
    """
    An attempt was made to create a keyspace or table that already exists.
    """

    keyspace = None
    """
    The name of the keyspace that already exists, or, if an attempt was
    made to create a new table, the keyspace that the table is in.
    """

    table = None
    """
    The name of the table that already exists, or, if an attempt was
    make to create a keyspace, :const:`None`.
    """

    def __init__(self, keyspace=None, table=None):
        if table:
            message = "Table '%s.%s' already exists" % (keyspace, table)
        else:
            message = "Keyspace '%s' already exists" % (keyspace,)

        Exception.__init__(self, message)
        self.keyspace = keyspace
        self.table = table


class InvalidRequest(RequestValidationException):
    """
    A query was made that was invalid for some reason, such as trying to set
    the keyspace for a connection to a nonexistent keyspace.
    """
    pass


class Unauthorized(RequestValidationException):
    """
    The current user is not authorized to perform the requested operation.
    """
    pass


class AuthenticationFailed(DriverException):
    """
    Failed to authenticate.
    """
    pass


class OperationTimedOut(DriverException):
    """
    The operation took longer than the specified (client-side) timeout
    to complete.  This is not an error generated by Cassandra, only
    the driver.
    """

    errors = None
    """
    A dict of errors keyed by the :class:`~.Host` against which they occurred.
    """

    last_host = None
    """
    The last :class:`~.Host` this operation was attempted against.
    """

    def __init__(self, errors=None, last_host=None):
        self.errors = errors
        self.last_host = last_host
        message = "errors=%s, last_host=%s" % (self.errors, self.last_host)
        Exception.__init__(self, message)


class UnsupportedOperation(DriverException):
    """
    An attempt was made to use a feature that is not supported by the
    selected protocol version.  See :attr:`Cluster.protocol_version`
    for more details.
    """
    pass
