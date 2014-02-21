"""
This module holds classes for working with prepared statements and
specifying consistency levels and retry policies for individual
queries.
"""

from datetime import datetime, timedelta
import struct
import time

from cassandra import ConsistencyLevel, OperationTimedOut
from cassandra.cqltypes import unix_time_from_uuid1
from cassandra.decoder import (cql_encoders, cql_encode_object,
                               cql_encode_sequence, named_tuple_factory)

import logging
log = logging.getLogger(__name__)


class Statement(object):
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

    trace = None
    """
    If :meth:`.Session.execute()` is run with `trace` set to :const:`True`,
    this will be set to a :class:`.QueryTrace` instance.
    """

    consistency_level = ConsistencyLevel.ONE
    """
    The :class:`.ConsistencyLevel` to be used for this operation.  Defaults
    to :attr:`.ConsistencyLevel.ONE`.
    """

    _routing_key = None

    def __init__(self, retry_policy=None, consistency_level=None, routing_key=None):
        self.retry_policy = retry_policy
        if consistency_level is not None:
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

    @property
    def keyspace(self):
        """
        The string name of the keyspace this query acts on.
        """
        return None


class SimpleStatement(Statement):
    """
    A simple, un-prepared query.  All attributes of :class:`Statement` apply
    to this class as well.
    """

    def __init__(self, query_string, *args, **kwargs):
        """
        `query_string` should be a literal CQL statement with the exception
        of parameter placeholders that will be filled through the
        `parameters` argument of :meth:`.Session.execute()`.
        """
        Statement.__init__(self, *args, **kwargs)
        self._query_string = query_string

    @property
    def query_string(self):
        return self._query_string

    def __str__(self):
        consistency = ConsistencyLevel.value_to_name[self.consistency_level]
        return (u'<SimpleStatement query="%s", consistency=%s>' %
                (self.query_string, consistency))
    __repr__ = __str__


class PreparedStatement(object):
    """
    A statement that has been prepared against at least one Cassandra node.
    Instances of this class should not be created directly, but through
    :meth:`.Session.prepare()`.

    A :class:`.PreparedStatement` should be prepared only once. Re-preparing a statement
    may affect performance (as the operation requires a network roundtrip).
    """

    column_metadata = None
    query_id = None
    query_string = None
    keyspace = None

    routing_key_indexes = None

    consistency_level = ConsistencyLevel.ONE

    def __init__(self, column_metadata, query_id, routing_key_indexes, query, keyspace,
                 consistency_level=ConsistencyLevel.ONE):
        self.column_metadata = column_metadata
        self.query_id = query_id
        self.routing_key_indexes = routing_key_indexes
        self.query_string = query
        self.keyspace = keyspace
        self.consistency_level = consistency_level

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

    def bind(self, values):
        """
        Creates and returns a :class:`BoundStatement` instance using `values`.
        The `values` parameter **must** be a sequence, such as a tuple or list,
        even if there is only one value to bind.
        """
        return BoundStatement(self).bind(values)

    def __str__(self):
        consistency = ConsistencyLevel.value_to_name[self.consistency_level]
        return (u'<PreparedStatement query="%s", consistency=%s>' %
                (self.query_string, consistency))
    __repr__ = __str__


class BoundStatement(Statement):
    """
    A prepared statement that has been bound to a particular set of values.
    These may be created directly or through :meth:`.PreparedStatement.bind()`.

    All attributes of :class:`Statement` apply to this class as well.
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
        All other ``*args`` and ``**kwargs`` will be passed to :class:`.Statement`.
        """
        self.consistency_level = prepared_statement.consistency_level
        self.prepared_statement = prepared_statement
        self.values = []

        Statement.__init__(self, *args, **kwargs)

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

        self.raw_values = values
        self.values = []
        for value, col_spec in zip(values, col_meta):
            if value is None:
                self.values.append(None)
            else:
                col_type = col_spec[-1]

                try:
                    self.values.append(col_type.serialize(value))
                except (TypeError, struct.error):
                    col_name = col_spec[2]
                    expected_type = col_type
                    actual_type = type(value)

                    err = InvalidParameterTypeError(col_name=col_name,
                                                    expected_type=expected_type,
                                                    actual_type=actual_type)
                    raise err

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

    @property
    def keyspace(self):
        meta = self.prepared_statement.column_metadata
        if meta:
            return meta[0][0]
        else:
            return None

    def __str__(self):
        consistency = ConsistencyLevel.value_to_name[self.consistency_level]
        return (u'<BoundStatement query="%s", values=%s, consistency=%s>' %
                (self.prepared_statement.query_string, self.raw_values, consistency))
    __repr__ = __str__


class ValueSequence(object):
    """
    A wrapper class that is used to specify that a sequence of values should
    be treated as a CQL list of values instead of a single column collection when used
    as part of the `parameters` argument for :meth:`.Session.execute()`.

    This is typically needed when supplying a list of keys to select.
    For example::

        >>> my_user_ids = ('alice', 'bob', 'charles')
        >>> query = "SELECT * FROM users WHERE user_id IN %s"
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


class TraceUnavailable(Exception):
    """
    Raised when complete trace details cannot be fetched from Cassandra.
    """
    pass


class InvalidParameterTypeError(TypeError):
    """
    Raised when a used tries to bind a prepared statement with an argument of an
    invalid type.
    """

    def __init__(self, col_name, expected_type, actual_type):
        self.col_name = col_name
        self.expected_type = expected_type
        self.actual_type = actual_type

        values = (self.col_name, self.expected_type, self.actual_type)
        message = ('Received an argument of invalid type for column "%s". '
                   'Expected: %s, Got: %s' % values)

        super(InvalidParameterTypeError, self).__init__(message)


class QueryTrace(object):
    """
    A trace of the duration and events that occurred when executing
    an operation.
    """

    trace_id = None
    """
    :class:`uuid.UUID` unique identifier for this tracing session.  Matches
    the ``session_id`` column in ``system_traces.sessions`` and
    ``system_traces.events``.
    """

    request_type = None
    """
    A string that very generally describes the traced operation.
    """

    duration = None
    """
    A :class:`datetime.timedelta` measure of the duration of the query.
    """

    coordinator = None
    """
    The IP address of the host that acted as coordinator for this request.
    """

    parameters = None
    """
    A :class:`dict` of parameters for the traced operation, such as the
    specific query string.
    """

    started_at = None
    """
    A UTC :class:`datetime.datetime` object describing when the operation
    was started.
    """

    events = None
    """
    A chronologically sorted list of :class:`.TraceEvent` instances
    representing the steps the traced operation went through.  This
    corresponds to the rows in ``system_traces.events`` for this tracing
    session.
    """

    _session = None

    _SELECT_SESSIONS_FORMAT = "SELECT * FROM system_traces.sessions WHERE session_id = %s"
    _SELECT_EVENTS_FORMAT = "SELECT * FROM system_traces.events WHERE session_id = %s"
    _BASE_RETRY_SLEEP = 0.003

    def __init__(self, trace_id, session):
        self.trace_id = trace_id
        self._session = session

    def populate(self, max_wait=2.0):
        """
        Retrieves the actual tracing details from Cassandra and populates the
        attributes of this instance.  Because tracing details are stored
        asynchronously by Cassandra, this may need to retry the session
        detail fetch.  If the trace is still not available after `max_wait`
        seconds, :exc:`.TraceUnavailable` will be raised; if `max_wait` is
        :const:`None`, this will retry forever.
        """
        attempt = 0
        start = time.time()
        while True:
            time_spent = time.time() - start
            if max_wait is not None and time_spent >= max_wait:
                raise TraceUnavailable("Trace information was not available within %f seconds" % (max_wait,))

            session_results = self._execute(
                self._SELECT_SESSIONS_FORMAT, (self.trace_id,), time_spent, max_wait)

            if not session_results or session_results[0].duration is None:
                time.sleep(self._BASE_RETRY_SLEEP * (2 ** attempt))
                attempt += 1
                continue

            session_row = session_results[0]
            self.request_type = session_row.request
            self.duration = timedelta(microseconds=session_row.duration)
            self.started_at = session_row.started_at
            self.coordinator = session_row.coordinator
            self.parameters = session_row.parameters

            time_spent = time.time() - start
            event_results = self._execute(
                self._SELECT_EVENTS_FORMAT, (self.trace_id,), time_spent, max_wait)
            self.events = tuple(TraceEvent(r.activity, r.event_id, r.source, r.source_elapsed, r.thread)
                                for r in event_results)
            break

    def _execute(self, query, parameters, time_spent, max_wait):
        # in case the user switched the row factory, set it to namedtuple for this query
        future = self._session._create_response_future(query, parameters, trace=False)
        future.row_factory = named_tuple_factory
        future.send_request()

        timeout = (max_wait - time_spent) if max_wait is not None else None
        try:
            return future.result(timeout=timeout)
        except OperationTimedOut:
            raise TraceUnavailable("Trace information was not available within %f seconds" % (max_wait,))

    def __str__(self):
        return "%s [%s] coordinator: %s, started at: %s, duration: %s, parameters: %s" \
               % (self.request_type, self.trace_id, self.coordinator, self.started_at,
                  self.duration, self.parameters)


class TraceEvent(object):
    """
    Representation of a single event within a query trace.
    """

    description = None
    """
    A brief description of the event.
    """

    datetime = None
    """
    A UTC :class:`datetime.datetime` marking when the event occurred.
    """

    source = None
    """
    The IP address of the node this event occurred on.
    """

    source_elapsed = None
    """
    A :class:`datetime.timedelta` measuring the amount of time until
    this event occurred starting from when :attr:`.source` first
    received the query.
    """

    thread_name = None
    """
    The name of the thread that this event occurred on.
    """

    def __init__(self, description, timeuuid, source, source_elapsed, thread_name):
        self.description = description
        self.datetime = datetime.utcfromtimestamp(unix_time_from_uuid1(timeuuid))
        self.source = source
        if source_elapsed is not None:
            self.source_elapsed = timedelta(microseconds=source_elapsed)
        else:
            self.source_elapsed = None
        self.thread_name = thread_name

    def __str__(self):
        return "%s on %s[%s] at %s" % (self.description, self.source, self.thread_name, self.datetime)
