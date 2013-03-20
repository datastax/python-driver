import struct

from decoder import ConsistencyLevel

class Query(object):

    retry_policy = None
    tracing_enabled = False
    consistency_level = ConsistencyLevel.name_to_value["ONE"]
    routing_key = None


class SimpleStatement(Query):

    query = None

    def __init__(self, query):
        self.query = query
        self._routing_key = None

    @property
    def routing_key(self):
        return self._routing_key

    @routing_key.setter
    def set_routing_key(self, value):
        self._routing_key = "".join(struct.pack("HsB", len(component), component, 0)
                                    for component in value)
