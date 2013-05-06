__version__ = '0.0.1-alpha'
__version_info__ = (0, 0, 1)


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
