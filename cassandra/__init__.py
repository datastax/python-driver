__version_info__ = (1, 0, '0b7')
__version__ = '.'.join(map(str, __version_info__))


class ConsistencyLevel(object):
    """
    Spcifies how many replicas must respond for an operation to be considered
    a success.  By default, ``ONE`` is used for all operations.
    """

    ANY = 0
    """
    Only requires that one replica receives the write *or* the coordinator
    stores a hint to replay later. Valid only for writes.
    """

    ONE = 1
    """
    Only one replica needs to respond to consider the operation a success
    """

    TWO = 2
    """
    Two replicas must respond to consider the operation a success
    """

    THREE = 3
    """
    Three replicas must respond to consider the operation a success
    """

    QUORUM = 4
    """
    ``ceil(RF/2)`` replicas must respond to consider the operation a success
    """

    ALL = 5
    """
    All replicas must respond to consider the operation a success
    """

    LOCAL_QUORUM = 6
    """
    Requires a quorum of replicas in the local datacenter
    """

    EACH_QUORUM = 7
    """
    Requires a quorum of replicas in each datacenter
    """

ConsistencyLevel.value_to_name = {
    ConsistencyLevel.ANY: 'ANY',
    ConsistencyLevel.ONE: 'ONE',
    ConsistencyLevel.TWO: 'TWO',
    ConsistencyLevel.THREE: 'THREE',
    ConsistencyLevel.QUORUM: 'QUORUM',
    ConsistencyLevel.ALL: 'ALL',
    ConsistencyLevel.LOCAL_QUORUM: 'LOCAL_QUORUM',
    ConsistencyLevel.EACH_QUORUM: 'EACH_QUORUM'
}

ConsistencyLevel.name_to_value = {
    'ANY': ConsistencyLevel.ANY,
    'ONE': ConsistencyLevel.ONE,
    'TWO': ConsistencyLevel.TWO,
    'THREE': ConsistencyLevel.THREE,
    'QUORUM': ConsistencyLevel.QUORUM,
    'ALL': ConsistencyLevel.ALL,
    'LOCAL_QUORUM': ConsistencyLevel.LOCAL_QUORUM,
    'EACH_QUORUM': ConsistencyLevel.EACH_QUORUM
}


class Unavailable(Exception):
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

    def __init__(self, message, consistency=None, required_replicas=None, alive_replicas=None):
        Exception.__init__(self, message)
        self.consistency = consistency
        self.required_replicas = required_replicas
        self.alive_replicas = alive_replicas


class Timeout(Exception):
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

    def __init__(self, message, consistency=None, required_responses=None, received_responses=None):
        Exception.__init__(self, message)
        self.consistency = consistency
        self.required_responses = required_responses
        self.received_responses = received_responses


class ReadTimeout(Timeout):
    """
    A subclass of :exc:`Timeout` for read operations.
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
    """

    write_type = None
    """
    The type of write operation, enum on :class:`~cassandra.policies.WriteType`
    """

    def __init__(self, message, write_type=None, **kwargs):
        Timeout.__init__(self, message, **kwargs)
        self.write_type = write_type


class AlreadyExists(Exception):
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


class InvalidRequest(Exception):
    """
    A query was made that was invalid for some reason, such as trying to set
    the keyspace for a connection to a nonexistent keyspace.
    """
    pass


class Unauthorized(Exception):
    """
    The current user is not authorized to perfom the requested operation.
    """
    pass


class AuthenticationFailed(Exception):
    """
    Failed to authenticate.
    """
    pass


class OperationTimedOut(Exception):
    pass
