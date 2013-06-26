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
    _routing_key = None

    def __init__(self, retry_policy=None, tracing_enabled=False, consistency_level=ConsistencyLevel.ONE, routing_key=None):
        self.retry_policy = retry_policy
        self.tracing_enabled = tracing_enabled
        self.consistency_level = consistency_level
        self._routing_key = routing_key

    @property
    def routing_key(self):
        return self._routing_key

    @routing_key.setter
    def set_routing_key(self, value):
        self._routing_key = "".join(struct.pack("HsB", len(component), component, 0)
                                    for component in value)

class SimpleStatement(Query):

    def __init__(self, query_string, *args, **kwargs):
        Query.__init__(self, *args, **kwargs)
        self._query_string = query_string
        self._routing_key = None

    @property
    def query_string(self):
        return self._query_string


class PreparedStatement(object):

    column_metadata = None
    query_id = None
    md5_id = None
    query_string = None
    keyspace = None

    routing_key_indexes = None

    consistency_level = ConsistencyLevel.ONE

    def __init__(self, column_metadata, query_id, md5_id, routing_key_indexes, query, keyspace):
        self.column_metadata = column_metadata
        self.query_id = query_id
        self.md5_id = md5_id
        self.routing_key_indexes = routing_key_indexes
        self.query_string = query
        self.keyspace = keyspace

    @classmethod
    def from_message(cls, query_id, md5_id, column_metadata, cluster_metadata, query, keyspace):
        if not column_metadata:
            return PreparedStatement(column_metadata, query_id, md5_id, None, query, keyspace)

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

        return PreparedStatement(column_metadata, query_id, md5_id, routing_key_indexes, query, keyspace)

    def bind(self, values):
        return BoundStatement(self).bind(values)


class BoundStatement(Query):

    prepared_statement = None
    values = None
    _routing_key = None

    def __init__(self, prepared_statement, *args, **kwargs):
        Query.__init__(self, *args, **kwargs)
        self.prepared_statement = prepared_statement
        self.consistency_level = prepared_statement.consistency_level
        self.values = []

    def bind(self, values):
        col_meta = self.prepared_statement.column_metadata
        if len(values) > len(col_meta):
            raise ValueError(
                "Too many arguments provided to bind() (got %d, expected %d)" %
                (len(values), len(col_meta)))

        self.values = []
        for value, col_spec in zip(values, col_meta):
            if value is None:
                self.values.append(None)
            else:
                col_type = col_spec[-1]
                self.values.append(col_type.serialize(value))

        return self

    @property
    def routing_key(self):
        if not self.prepared_statement.routing_key_indexes:
            return None

        if self._routing_key is not None:
            return self._routing_key

        components = []
        for statement_index in self.prepared_statement.routing_key_indexes:
            val = self.values[statement_index]
            components.append(struct.pack("HsB", len(val), val, 0))

        self._routing_key = "".join(components)
        return self._routing_key


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
