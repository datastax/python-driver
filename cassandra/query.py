"""
This module holds classes for working with prepared statements and
specifying consistency levels and retry policies for individual
queries.
"""

import struct

from cassandra import ConsistencyLevel
from cassandra.decoder import (cql_encoders, cql_encode_object,
                               cql_encode_sequence)

class Query(object):
    """
    An abstract class representing a single query. There are two subclasses:
    :class:`.SimpleStatement` and :class:`.BoundStatement`.  These can
    be passed to :meth:`.Session.execute()`.
    """

    retry_policy = None
    """
    An instance of a :class:`cassandra.policies.RetryPolicy` or one of its
    subclasses.  This controls when a query will be retried and how it
    will be retried.
    """

    tracing_enabled = False
    """
    A boolean flag that may be set to :const:`True` to enable tracing on this
    query only.

    **Note**: query tracing is not yet supported by this driver
    """

    consistency_level = ConsistencyLevel.ONE
    """
    The :class:`.ConsistencyLevel` to be used for this operation.  Defaults
    to :attr:`.ConsistencyLevel.ONE`.
    """
    _routing_key = None

    def __init__(self, retry_policy=None, tracing_enabled=False, consistency_level=ConsistencyLevel.ONE, routing_key=None):
        self.retry_policy = retry_policy
        self.tracing_enabled = tracing_enabled
        self.consistency_level = consistency_level
        self._routing_key = routing_key

    def _get_routing_key(self):
        return self._routing_key

    def _set_routing_key(self, key):
        if isinstance(key, (list, tuple)):
            self._routing_key = "".join(struct.pack("HsB", len(component), component, 0)
                                        for component in key)
        else:
            self._routing_key = key

    def _del_routing_key(self):
        self._routing_key = None

    routing_key = property(
        _get_routing_key,
        _set_routing_key,
        _del_routing_key,
        """
        The :attr:`~.TableMetadata.partition_key` portion of the primary key,
        which can be used to determine which nodes are replicas for the query.

        If the partition key is a composite, a list or tuple must be passed in.
        Each key component should be in its packed (binary) format, so all
        components should be strings.
        """)

class SimpleStatement(Query):
    """
    A simple, un-prepared query.  All attributes of :class:`Query` apply
    to this class as well.
    """

    def __init__(self, query_string, *args, **kwargs):
        """
        `query_string` should be a literal CQL statement with the exception
        of parameter placeholders that will be filled through the
        `parameters` argument of :meth:`.Session.execute()`.
        """
        Query.__init__(self, *args, **kwargs)
        self._query_string = query_string

    @property
    def query_string(self):
        return self._query_string


class PreparedStatement(object):
    """
    A statement that has been prepared against at least one Cassandra node.
    Instances of this class should not be created directly, but through
    :meth:`.Session.prepare()`.
    """

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
        """
        Creates and returns a :class:`BoundStatement` instance using `values`.
        The `values` parameter *must* be a sequence, such as a tuple or list,
        even if there is only one value to bind.
        """
        return BoundStatement(self).bind(values)


class BoundStatement(Query):
    """
    A prepared statement that has been bound to a particular set of values.
    These may be created directly or through :meth:`.PreparedStatement.bind()`.

    All attributes of :class:`Query` apply to this class as well.
    """

    prepared_statement = None
    """
    The :class:`PreparedStatement` instance that this was created from.
    """

    values = None
    """
    The sequence of values that were bound to the prepared statement.
    """

    def __init__(self, prepared_statement, *args, **kwargs):
        """
        `prepared_statement` should be an instance of :class:`PreparedStatement`.
        All other ``*args`` and ``**kwargs`` will be passed to :class:`.Query`.
        """
        self.consistency_level = prepared_statement.consistency_level
        self.prepared_statement = prepared_statement
        self.values = []

        Query.__init__(self, *args, **kwargs)

    def bind(self, values):
        """
        Binds a sequence of values for the prepared statement parameters
        and returns this instance.  Note that `values` *must* be a
        sequence, even if you are only binding one value.
        """
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

        routing_indexes = self.prepared_statement.routing_key_indexes
        if len(routing_indexes) == 1:
            self._routing_key = self.values[routing_indexes[0]]
        else:
            components = []
            for statement_index in routing_indexes:
                val = self.values[statement_index]
                components.append(struct.pack("HsB", len(val), val, 0))

            self._routing_key = "".join(components)

        return self._routing_key


class ValueSequence(object):
    """
    A wrapper class that is used to specify that a sequence of values should
    be treated as a CQL list of values instead of a single column collection when used
    as part of the `parameters` argument for :meth:`.Session.execute()`.

    This is typically needed when supplying a list of keys to select.
    For example::

        >>> my_user_ids = ('alice', 'bob', 'charles')
        >>> query = "SELECT * FROM users WHERE user_id IN ?"
        >>> session.execute(query, parameters=[ValueSequence(my_user_ids)])

    """

    def __init__(self, sequence):
        self.sequence = sequence

    def __str__(self):
        return cql_encode_sequence(self.sequence)


def bind_params(query, params):
    if isinstance(params, dict):
        return query % dict((k, cql_encoders.get(type(v), cql_encode_object)(v))
                            for k, v in params.iteritems())
    else:
        return query % tuple(cql_encoders.get(type(v), cql_encode_object)(v)
                             for v in params)
