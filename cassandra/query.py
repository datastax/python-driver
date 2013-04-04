import struct

from decoder import ConsistencyLevel

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
