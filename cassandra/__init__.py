__version_info__ = (0, 1, 4)
__version__ = '.'.join(map(str, __version_info__))


class ConsistencyLevel(object):

    ANY = 0
    ONE = 1
    TWO = 2
    THREE = 3
    QUORUM = 4
    ALL = 5
    LOCAL_QUORUM = 6
    EACH_QUORUM = 7

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

    consistency = None
    required_replicas = None
    alive_replicas = None

    def __init__(self, message, consistency=None, required_replicas=None, alive_replicas=None):
        Exception.__init__(self, message)
        self.consistency = consistency
        self.required_replicas = required_replicas
        self.alive_replicas = alive_replicas


class Timeout(Exception):

    consistency = None
    required_responses = None
    received_responses = None

    def __init__(self, message, consistency=None, required_responses=None, received_responses=None):
        Exception.__init__(self, message)
        self.consistency = consistency
        self.required_responses = required_responses
        self.received_responses = received_responses


class ReadTimeout(Timeout):

    data_retrieved = None

    def __init__(self, message, data_retrieved=None, **kwargs):
        Timeout.__init__(self, message, **kwargs)
        self.data_retrieved = data_retrieved


class WriteTimeout(Timeout):

    write_type = None

    def __init__(self, message, write_type=None, **kwargs):
        Timeout.__init__(self, message, **kwargs)
        self.write_type = write_type


class AlreadyExists(Exception):

    keyspace = None
    table = None

    def __init__(self, keyspace=None, table=None):
        if table:
            message = "Table '%s.%s' already exists" % (keyspace, table)
        else:
            message = "Keyspace '%s' already exists" % (keyspace,)

        Exception.__init__(self, message)
        self.keyspace = keyspace
        self.table = table


class InvalidRequest(Exception):
    pass
