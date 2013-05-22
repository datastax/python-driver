import struct

from cassandra import ConsistencyLevel
from cassandra.decoder import (cql_encoders, cql_encode_object,
                               cql_encode_map_collection,
                               cql_encode_set_collection,
                               cql_encode_list_collection)

class Query(object):

    retry_policy = None
    tracing_enabled = False
    consistency_level = ConsistencyLevel.ONE
    routing_key = None


class SimpleStatement(Query):

    def __init__(self, query_string):
        self._query_string = query_string
        self._routing_key = None

    @property
    def routing_key(self):
        return self._routing_key

    @routing_key.setter
    def set_routing_key(self, value):
        self._routing_key = "".join(struct.pack("HsB", len(component), component, 0)
                                    for component in value)

    @property
    def query_string(self):
        return self._query_string


class ColumnCollection(object):

    def __init__(self, sequence):
        self.sequence = sequence

    def __str__(self):
        s = self.sequence
        if isinstance(s, dict):
            return cql_encode_map_collection(s)
        elif isinstance(s, (set, frozenset)):
            return cql_encode_set_collection(s)
        else:
            return cql_encode_list_collection(s)


def bind_params(query, params):
    if isinstance(params, dict):
        return query % dict((k, cql_encoders.get(type(v), cql_encode_object)(v))
                            for k, v in params.iteritems())
    else:
        return query % tuple(cql_encoders.get(type(v), cql_encode_object)(v)
                             for v in params)
