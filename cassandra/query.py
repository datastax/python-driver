# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module holds classes for working with prepared statements and
specifying consistency levels and retry policies for individual
queries.
"""

from collections import namedtuple
from datetime import datetime, timedelta
import re
import struct
import time
import six

from cassandra import ConsistencyLevel, OperationTimedOut
from cassandra.cqltypes import unix_time_from_uuid1
from cassandra.encoder import (cql_encoders, cql_encode_object,
                               cql_encode_sequence)
from cassandra.util import OrderedDict

import logging
log = logging.getLogger(__name__)


NON_ALPHA_REGEX = re.compile('[^a-zA-Z0-9]')
START_BADCHAR_REGEX = re.compile('^[^a-zA-Z0-9]*')
END_BADCHAR_REGEX = re.compile('[^a-zA-Z0-9_]*$')

_clean_name_cache = {}


def _clean_column_name(name):
    try:
        return _clean_name_cache[name]
    except KeyError:
        clean = NON_ALPHA_REGEX.sub("_", START_BADCHAR_REGEX.sub("", END_BADCHAR_REGEX.sub("", name)))
        _clean_name_cache[name] = clean
        return clean


def tuple_factory(colnames, rows):
    """
    Returns each row as a tuple

    Example::

        >>> from cassandra.query import named_tuple_factory
        >>> session = cluster.connect('mykeyspace')
        >>> session.row_factory = tuple_factory
        >>> rows = session.execute("SELECT name, age FROM users LIMIT 1")
        >>> print rows[0]
        ('Bob', 42)

    .. versionchanged:: 2.0.0
        moved from ``cassandra.decoder`` to ``cassandra.query``
    """
    return rows


def named_tuple_factory(colnames, rows):
    """
    Returns each row as a `namedtuple <https://docs.python.org/2/library/collections.html#collections.namedtuple>`_.
    This is the default row factory.

    Example::

        >>> from cassandra.query import named_tuple_factory
        >>> session = cluster.connect('mykeyspace')
        >>> session.row_factory = named_tuple_factory
        >>> rows = session.execute("SELECT name, age FROM users LIMIT 1")
        >>> user = rows[0]

        >>> # you can access field by their name:
        >>> print "name: %s, age: %d" % (user.name, user.age)
        name: Bob, age: 42

        >>> # or you can access fields by their position (like a tuple)
        >>> name, age = user
        >>> print "name: %s, age: %d" % (name, age)
        name: Bob, age: 42
        >>> name = user[0]
        >>> age = user[1]
        >>> print "name: %s, age: %d" % (name, age)
        name: Bob, age: 42

    .. versionchanged:: 2.0.0
        moved from ``cassandra.decoder`` to ``cassandra.query``
    """
    Row = namedtuple('Row', map(_clean_column_name, colnames))
    return [Row(*row) for row in rows]


def dict_factory(colnames, rows):
    """
    Returns each row as a dict.

    Example::

        >>> from cassandra.query import named_tuple_factory
        >>> session = cluster.connect('mykeyspace')
        >>> session.row_factory = dict_factory
        >>> rows = session.execute("SELECT name, age FROM users LIMIT 1")
        >>> print rows[0]
        {'age': 42, 'name': 'Bob'}

    .. versionchanged:: 2.0.0
        moved from ``cassandra.decoder`` to ``cassandra.query``
    """
    return [dict(zip(colnames, row)) for row in rows]


def ordered_dict_factory(colnames, rows):
    """
    Like :meth:`~cassandra.query.dict_factory`, but returns each row as an OrderedDict,
    so the order of the columns is preserved.

    .. versionchanged:: 2.0.0
        moved from ``cassandra.decoder`` to ``cassandra.query``
    """
    return [OrderedDict(zip(colnames, row)) for row in rows]


class Statement(object):
    """
    An abstract class representing a single query. There are three subclasses:
    :class:`.SimpleStatement`, :class:`.BoundStatement`, and :class:`.BatchStatement`.
    These can be passed to :meth:`.Session.execute()`.
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

    consistency_level = None
    """
    The :class:`.ConsistencyLevel` to be used for this operation.  Defaults
    to :const:`None`, which means that the default consistency level for
    the Session this is executed in will be used.
    """

    fetch_size = None
    """
    How many rows will be fetched at a time.  This overrides the default
    of :attr:`.Session.default_fetch_size`

    This only takes effect when protocol version 2 or higher is used.
    See :attr:`.Cluster.protocol_version` for details.

    .. versionadded:: 2.0.0
    """

    _serial_consistency_level = None
    _routing_key = None

    def __init__(self, retry_policy=None, consistency_level=None, routing_key=None,
                 serial_consistency_level=None, fetch_size=None):
        self.retry_policy = retry_policy
        if consistency_level is not None:
            self.consistency_level = consistency_level
        if serial_consistency_level is not None:
            self.serial_consistency_level = serial_consistency_level
        if fetch_size is not None:
            self.fetch_size = None
        self._routing_key = routing_key

    def _get_routing_key(self):
        return self._routing_key

    def _set_routing_key(self, key):
        if isinstance(key, (list, tuple)):
            self._routing_key = b"".join(struct.pack("HsB", len(component), component, 0)
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

    def _get_serial_consistency_level(self):
        return self._serial_consistency_level

    def _set_serial_consistency_level(self, serial_consistency_level):
        acceptable = (None, ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL)
        if serial_consistency_level not in acceptable:
            raise ValueError(
                "serial_consistency_level must be either ConsistencyLevel.SERIAL "
                "or ConsistencyLevel.LOCAL_SERIAL")

    def _del_serial_consistency_level(self):
        self._serial_consistency_level = None

    serial_consistency_level = property(
         _get_serial_consistency_level,
         _set_serial_consistency_level,
         _del_serial_consistency_level,
        """
        The serial consistency level is only used by conditional updates
        (``INSERT``, ``UPDATE`` and ``DELETE`` with an ``IF`` condition).  For
        those, the ``serial_consistency_level`` defines the consistency level of
        the serial phase (or "paxos" phase) while the normal
        :attr:`~.consistency_level` defines the consistency for the "learn" phase,
        i.e. what type of reads will be guaranteed to see the update right away.
        For example, if a conditional write has a :attr:`~.consistency_level` of
        :attr:`~.ConsistencyLevel.QUORUM` (and is successful), then a
        :attr:`~.ConsistencyLevel.QUORUM` read is guaranteed to see that write.
        But if the regular :attr:`~.consistency_level` of that write is
        :attr:`~.ConsistencyLevel.ANY`, then only a read with a
        :attr:`~.consistency_level` of :attr:`~.ConsistencyLevel.SERIAL` is
        guaranteed to see it (even a read with consistency
        :attr:`~.ConsistencyLevel.ALL` is not guaranteed to be enough).

        The serial consistency can only be one of :attr:`~.ConsistencyLevel.SERIAL`
        or :attr:`~.ConsistencyLevel.LOCAL_SERIAL`. While ``SERIAL`` guarantees full
        linearizability (with other ``SERIAL`` updates), ``LOCAL_SERIAL`` only
        guarantees it in the local data center.

        The serial consistency level is ignored for any query that is not a
        conditional update. Serial reads should use the regular
        :attr:`consistency_level`.

        Serial consistency levels may only be used against Cassandra 2.0+
        and the :attr:`~.Cluster.protocol_version` must be set to 2 or higher.

        .. versionadded:: 2.0.0
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
        consistency = ConsistencyLevel.value_to_name.get(self.consistency_level, 'Not Set')
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

    consistency_level = None
    serial_consistency_level = None

    def __init__(self, column_metadata, query_id, routing_key_indexes, query, keyspace,
                 consistency_level=None, serial_consistency_level=None, fetch_size=None):
        self.column_metadata = column_metadata
        self.query_id = query_id
        self.routing_key_indexes = routing_key_indexes
        self.query_string = query
        self.keyspace = keyspace
        self.consistency_level = consistency_level
        self.serial_consistency_level = serial_consistency_level
        self.fetch_size = fetch_size

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
        consistency = ConsistencyLevel.value_to_name.get(self.consistency_level, 'Not Set')
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
        self.serial_consistency_level = prepared_statement.serial_consistency_level
        self.prepared_statement = prepared_statement
        self.values = []

        Statement.__init__(self, *args, **kwargs)

    def bind(self, values):
        """
        Binds a sequence of values for the prepared statement parameters
        and returns this instance.  Note that `values` *must* be:
        * a sequence, even if you are only binding one value, or
        * a dict that relates 1-to-1 between dict keys and columns
        """
        if values is None:
            values = ()
        col_meta = self.prepared_statement.column_metadata

        # special case for binding dicts
        if isinstance(values, dict):
            dict_values = values
            values = []

            # sort values accordingly
            for col in col_meta:
                try:
                    values.append(dict_values[col[2]])
                except KeyError:
                    raise KeyError(
                        'Column name `%s` not found in bound dict.' %
                        (col[2]))

            # ensure a 1-to-1 dict keys to columns relationship
            if len(dict_values) != len(col_meta):
                # find expected columns
                columns = set()
                for col in col_meta:
                    columns.add(col[2])

                # generate error message
                if len(dict_values) > len(col_meta):
                    difference = set(dict_values.keys()).difference(columns)
                    msg = "Too many arguments provided to bind() (got %d, expected %d). " + \
                          "Unexpected keys %s."
                else:
                    difference = set(columns).difference(dict_values.keys())
                    msg = "Too few arguments provided to bind() (got %d, expected %d). " + \
                          "Expected keys %s."

                # exit with error message
                msg = msg % (len(values), len(col_meta), difference)
                raise ValueError(msg)

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

                    message = ('Received an argument of invalid type for column "%s". '
                               'Expected: %s, Got: %s' % (col_name, expected_type, actual_type))
                    raise TypeError(message)

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

            self._routing_key = b"".join(components)

        return self._routing_key

    @property
    def keyspace(self):
        meta = self.prepared_statement.column_metadata
        if meta:
            return meta[0][0]
        else:
            return None

    def __str__(self):
        consistency = ConsistencyLevel.value_to_name.get(self.consistency_level, 'Not Set')
        return (u'<BoundStatement query="%s", values=%s, consistency=%s>' %
                (self.prepared_statement.query_string, self.raw_values, consistency))
    __repr__ = __str__


class BatchType(object):
    """
    A BatchType is used with :class:`.BatchStatement` instances to control
    the atomicity of the batch operation.

    .. versionadded:: 2.0.0
    """

    LOGGED = None
    """
    Atomic batch operation.
    """

    UNLOGGED = None
    """
    Non-atomic batch operation.
    """

    COUNTER = None
    """
    Batches of counter operations.
    """

    def __init__(self, name, value):
        self.name = name
        self.value = value

    def __str__(self):
        return self.name

    def __repr__(self):
        return "BatchType.%s" % (self.name, )


BatchType.LOGGED = BatchType("LOGGED", 0)
BatchType.UNLOGGED = BatchType("UNLOGGED", 1)
BatchType.COUNTER = BatchType("COUNTER", 2)


class BatchStatement(Statement):
    """
    A protocol-level batch of operations which are applied atomically
    by default.

    .. versionadded:: 2.0.0
    """

    batch_type = None
    """
    The :class:`.BatchType` for the batch operation.  Defaults to
    :attr:`.BatchType.LOGGED`.
    """

    _statements_and_parameters = None

    def __init__(self, batch_type=BatchType.LOGGED, retry_policy=None,
                 consistency_level=None):
        """
        `batch_type` specifies The :class:`.BatchType` for the batch operation.
        Defaults to :attr:`.BatchType.LOGGED`.

        `retry_policy` should be a :class:`~.RetryPolicy` instance for
        controlling retries on the operation.

        `consistency_level` should be a :class:`~.ConsistencyLevel` value
        to be used for all operations in the batch.

        Example usage:

        .. code-block:: python

            insert_user = session.prepare("INSERT INTO users (name, age) VALUES (?, ?)")
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

            for (name, age) in users_to_insert:
                batch.add(insert_user, (name, age))

            session.execute(batch)

        You can also mix different types of operations within a batch:

        .. code-block:: python

            batch = BatchStatement()
            batch.add(SimpleStatement("INSERT INTO users (name, age) VALUES (%s, %s)", (name, age))
            batch.add(SimpleStatement("DELETE FROM pending_users WHERE name=%s", (name,))
            session.execute(batch)

        .. versionadded:: 2.0.0
        """
        self.batch_type = batch_type
        self._statements_and_parameters = []
        Statement.__init__(self, retry_policy=retry_policy, consistency_level=consistency_level)

    def add(self, statement, parameters=None):
        """
        Adds a :class:`.Statement` and optional sequence of parameters
        to be used with the statement to the batch.

        Like with other statements, parameters must be a sequence, even
        if there is only one item.
        """
        if isinstance(statement, six.string_types):
            if parameters:
                statement = bind_params(statement, parameters)
            self._statements_and_parameters.append((False, statement, ()))
        elif isinstance(statement, PreparedStatement):
            query_id = statement.query_id
            bound_statement = statement.bind(() if parameters is None else parameters)
            self._statements_and_parameters.append(
                (True, query_id, bound_statement.values))
        elif isinstance(statement, BoundStatement):
            if parameters:
                raise ValueError(
                    "Parameters cannot be passed with a BoundStatement "
                    "to BatchStatement.add()")
            self._statements_and_parameters.append(
                (True, statement.prepared_statement.query_id, statement.values))
        else:
            # it must be a SimpleStatement
            query_string = statement.query_string
            if parameters:
                query_string = bind_params(query_string, parameters)
            self._statements_and_parameters.append((False, query_string, ()))
        return self

    def add_all(self, statements, parameters):
        """
        Adds a sequence of :class:`.Statement` objects and a matching sequence
        of parameters to the batch.  :const:`None` can be used in place of
        parameters when no parameters are needed.
        """
        for statement, value in zip(statements, parameters):
            self.add(statement, parameters)

    def __str__(self):
        consistency = ConsistencyLevel.value_to_name.get(self.consistency_level, 'Not Set')
        return (u'<BatchStatement type=%s, statements=%d, consistency=%s>' %
                (self.batch_type, len(self._statements_and_parameters), consistency))
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
        return query % dict((k, cql_encoders.get(type(v), cql_encode_object)(v)) for k, v in six.iteritems(params))
    else:
        return query % tuple(cql_encoders.get(type(v), cql_encode_object)(v) for v in params)


class TraceUnavailable(Exception):
    """
    Raised when complete trace details cannot be fetched from Cassandra.
    """
    pass


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
                raise TraceUnavailable(
                    "Trace information was not available within %f seconds. Consider raising Session.max_trace_wait." % (max_wait,))

            log.debug("Attempting to fetch trace info for trace ID: %s", self.trace_id)
            session_results = self._execute(
                self._SELECT_SESSIONS_FORMAT, (self.trace_id,), time_spent, max_wait)

            if not session_results or session_results[0].duration is None:
                time.sleep(self._BASE_RETRY_SLEEP * (2 ** attempt))
                attempt += 1
                continue
            log.debug("Fetched trace info for trace ID: %s", self.trace_id)

            session_row = session_results[0]
            self.request_type = session_row.request
            self.duration = timedelta(microseconds=session_row.duration)
            self.started_at = session_row.started_at
            self.coordinator = session_row.coordinator
            self.parameters = session_row.parameters

            log.debug("Attempting to fetch trace events for trace ID: %s", self.trace_id)
            time_spent = time.time() - start
            event_results = self._execute(
                self._SELECT_EVENTS_FORMAT, (self.trace_id,), time_spent, max_wait)
            log.debug("Fetched trace events for trace ID: %s", self.trace_id)
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
