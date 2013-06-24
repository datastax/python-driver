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


class PreparedStatement(object):

    column_metadata = None
    query_id = None
    query = None
    keyspace = None

    routing_key = None
    routing_key_indexes = None

    consistency_level = None

    def __init__(self, column_metadata, query_id, routing_key_indexes, query, keyspace):
        self.column_metadata = column_metadata
        self.query_id = query_id
        self.routing_key_indexes = routing_key_indexes
        self.query = query
        self.keyspace = keyspace

    @classmethod
    def from_message(cls, query_id, column_metadata, cluster_metadata, query, keyspace):
        if not column_metadata:
            return PreparedStatement(column_metadata, query_id, None, query, keyspace)

        partition_key_columns = None
        routing_key_indexes = None

        ks_name, table_name, _, _ = column_metadata[0]
        ks_meta = cluster_metadata.keyspaces.get(ks_name)
        if ks_meta:
            table_meta = ks_meta.tables.get(table_name)
            if table_meta:
                partition_key_columns = table_meta.partition_key

                # make a map of {column_name: index} for each column in the statement
                statement_indexes = dict((c[2], i) for i, c in enumerate(column_metadata))

                # a list of which indexes in the statement correspond to partition key items
                try:
                    routing_key_indexes = [statement_indexes[c.name]
                                           for c in partition_key_columns]
                except KeyError:
                    pass  # we're missing a partition key component in the prepared
                          # statement; just leave routing_key_indexes as None

        return PreparedStatement(column_metadata, query_id, routing_key_indexes, query, keyspace)


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
