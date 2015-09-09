# Copyright 2013-2015 DataStax, Inc.
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

from bisect import bisect_right
from collections import defaultdict
from hashlib import md5
from itertools import islice, cycle
import json
import logging
import re
import six
from six.moves import zip
import sys
from threading import RLock

murmur3 = None
try:
    from cassandra.murmur3 import murmur3
except ImportError as e:
    pass

from cassandra import SignatureDescriptor
import cassandra.cqltypes as types
from cassandra.encoder import Encoder
from cassandra.marshal import varint_unpack
from cassandra.util import OrderedDict

log = logging.getLogger(__name__)

cql_keywords = set((
    'add', 'aggregate', 'all', 'allow', 'alter', 'and', 'apply', 'as', 'asc', 'ascii', 'authorize', 'batch', 'begin',
    'bigint', 'blob', 'boolean', 'by', 'called', 'clustering', 'columnfamily', 'compact', 'contains', 'count',
    'counter', 'create', 'custom', 'date', 'decimal', 'delete', 'desc', 'describe', 'distinct', 'double', 'drop',
    'entries', 'execute', 'exists', 'filtering', 'finalfunc', 'float', 'from', 'frozen', 'full', 'function',
    'functions', 'grant', 'if', 'in', 'index', 'inet', 'infinity', 'initcond', 'input', 'insert', 'int', 'into', 'json',
    'key', 'keys', 'keyspace', 'keyspaces', 'language', 'limit', 'list', 'login', 'map', 'modify', 'nan', 'nologin',
    'norecursive', 'nosuperuser', 'not', 'null', 'of', 'on', 'options', 'or', 'order', 'password', 'permission',
    'permissions', 'primary', 'rename', 'replace', 'returns', 'revoke', 'role', 'roles', 'schema', 'select', 'set',
    'sfunc', 'smallint', 'static', 'storage', 'stype', 'superuser', 'table', 'text', 'time', 'timestamp', 'timeuuid',
    'tinyint', 'to', 'token', 'trigger', 'truncate', 'ttl', 'tuple', 'type', 'unlogged', 'update', 'use', 'user',
    'users', 'using', 'uuid', 'values', 'varchar', 'varint', 'where', 'with', 'writetime'
))
"""
Set of keywords in CQL.

Derived from .../cassandra/src/java/org/apache/cassandra/cql3/Cql.g
"""

cql_keywords_unreserved = set((
    'aggregate', 'all', 'as', 'ascii', 'bigint', 'blob', 'boolean', 'called', 'clustering', 'compact', 'contains',
    'count', 'counter', 'custom', 'date', 'decimal', 'distinct', 'double', 'exists', 'filtering', 'finalfunc', 'float',
    'frozen', 'function', 'functions', 'inet', 'initcond', 'input', 'int', 'json', 'key', 'keys', 'keyspaces',
    'language', 'list', 'login', 'map', 'nologin', 'nosuperuser', 'options', 'password', 'permission', 'permissions',
    'returns', 'role', 'roles', 'sfunc', 'smallint', 'static', 'storage', 'stype', 'superuser', 'text', 'time',
    'timestamp', 'timeuuid', 'tinyint', 'trigger', 'ttl', 'tuple', 'type', 'user', 'users', 'uuid', 'values', 'varchar',
    'varint', 'writetime'
))
"""
Set of unreserved keywords in CQL.

Derived from .../cassandra/src/java/org/apache/cassandra/cql3/Cql.g
"""

cql_keywords_reserved = cql_keywords - cql_keywords_unreserved
"""
Set of reserved keywords in CQL.
"""


class Metadata(object):
    """
    Holds a representation of the cluster schema and topology.
    """

    cluster_name = None
    """ The string name of the cluster. """

    keyspaces = None
    """
    A map from keyspace names to matching :class:`~.KeyspaceMetadata` instances.
    """

    partitioner = None
    """
    The string name of the partitioner for the cluster.
    """

    token_map = None
    """ A :class:`~.TokenMap` instance describing the ring topology. """

    def __init__(self):
        self.keyspaces = {}
        self._hosts = {}
        self._hosts_lock = RLock()

    def export_schema_as_string(self):
        """
        Returns a string that can be executed as a query in order to recreate
        the entire schema.  The string is formatted to be human readable.
        """
        return "\n".join(ks.export_as_string() for ks in self.keyspaces.values())

    def rebuild_schema(self, ks_results, type_results, function_results,
                       aggregate_results, cf_results, col_results, triggers_result):
        """
        Rebuild the view of the current schema from a fresh set of rows from
        the system schema tables.

        For internal use only.
        """
        cf_def_rows = defaultdict(list)
        col_def_rows = defaultdict(lambda: defaultdict(list))
        usertype_rows = defaultdict(list)
        fn_rows = defaultdict(list)
        agg_rows = defaultdict(list)
        trigger_rows = defaultdict(lambda: defaultdict(list))

        for row in cf_results:
            cf_def_rows[row["keyspace_name"]].append(row)

        for row in col_results:
            ksname = row["keyspace_name"]
            cfname = row["columnfamily_name"]
            col_def_rows[ksname][cfname].append(row)

        for row in type_results:
            usertype_rows[row["keyspace_name"]].append(row)

        for row in function_results:
            fn_rows[row["keyspace_name"]].append(row)

        for row in aggregate_results:
            agg_rows[row["keyspace_name"]].append(row)

        for row in triggers_result:
            ksname = row["keyspace_name"]
            cfname = row["columnfamily_name"]
            trigger_rows[ksname][cfname].append(row)

        current_keyspaces = set()
        for row in ks_results:
            keyspace_meta = self._build_keyspace_metadata(row)
            try:
                keyspace_col_rows = col_def_rows.get(keyspace_meta.name, {})
                keyspace_trigger_rows = trigger_rows.get(keyspace_meta.name, {})
                for table_row in cf_def_rows.get(keyspace_meta.name, []):
                    table_meta = self._build_table_metadata(keyspace_meta, table_row, keyspace_col_rows, keyspace_trigger_rows)
                    keyspace_meta._add_table_metadata(table_meta)

                for usertype_row in usertype_rows.get(keyspace_meta.name, []):
                    usertype = self._build_usertype(keyspace_meta.name, usertype_row)
                    keyspace_meta.user_types[usertype.name] = usertype

                for fn_row in fn_rows.get(keyspace_meta.name, []):
                    fn = self._build_function(keyspace_meta.name, fn_row)
                    keyspace_meta.functions[fn.signature] = fn

                for agg_row in agg_rows.get(keyspace_meta.name, []):
                    agg = self._build_aggregate(keyspace_meta.name, agg_row)
                    keyspace_meta.aggregates[agg.signature] = agg
            except Exception:
                log.exception("Error while parsing metadata for keyspace %s. Metadata model will be incomplete.", keyspace_meta.name)
                keyspace_meta._exc_info = sys.exc_info()

            current_keyspaces.add(keyspace_meta.name)
            old_keyspace_meta = self.keyspaces.get(keyspace_meta.name, None)
            self.keyspaces[keyspace_meta.name] = keyspace_meta
            if old_keyspace_meta:
                self._keyspace_updated(keyspace_meta.name)
            else:
                self._keyspace_added(keyspace_meta.name)

        # remove not-just-added keyspaces
        removed_keyspaces = [name for name in self.keyspaces.keys()
                             if name not in current_keyspaces]
        self.keyspaces = dict((name, meta) for name, meta in self.keyspaces.items()
                              if name in current_keyspaces)
        for ksname in removed_keyspaces:
            self._keyspace_removed(ksname)

    def keyspace_changed(self, keyspace, ks_results):
        if not ks_results:
            if keyspace in self.keyspaces:
                del self.keyspaces[keyspace]
                self._keyspace_removed(keyspace)
            return

        keyspace_meta = self._build_keyspace_metadata(ks_results[0])
        old_keyspace_meta = self.keyspaces.get(keyspace, None)
        self.keyspaces[keyspace] = keyspace_meta
        if old_keyspace_meta:
            keyspace_meta.tables = old_keyspace_meta.tables
            keyspace_meta.user_types = old_keyspace_meta.user_types
            keyspace_meta.indexes = old_keyspace_meta.indexes
            keyspace_meta.functions = old_keyspace_meta.functions
            keyspace_meta.aggregates = old_keyspace_meta.aggregates
            if (keyspace_meta.replication_strategy != old_keyspace_meta.replication_strategy):
                self._keyspace_updated(keyspace)
        else:
            self._keyspace_added(keyspace)

    def usertype_changed(self, keyspace, name, type_results):
        if type_results:
            new_usertype = self._build_usertype(keyspace, type_results[0])
            self.keyspaces[keyspace].user_types[name] = new_usertype
        else:
            try:
                self.keyspaces[keyspace].user_types.pop(name, None)
            except KeyError:
                pass

    def function_changed(self, keyspace, function, function_results):
        if function_results:
            new_function = self._build_function(keyspace, function_results[0])
            self.keyspaces[keyspace].functions[function.signature] = new_function
        else:
            try:
                self.keyspaces[keyspace].functions.pop(function.signature, None)
            except KeyError:
                pass

    def aggregate_changed(self, keyspace, aggregate, aggregate_results):
        if aggregate_results:
            new_aggregate = self._build_aggregate(keyspace, aggregate_results[0])
            self.keyspaces[keyspace].aggregates[aggregate.signature] = new_aggregate
        else:
            try:
                self.keyspaces[keyspace].aggregates.pop(aggregate.signature, None)
            except KeyError:
                pass

    def table_changed(self, keyspace, table, cf_results, col_results, triggers_result):
        if cf_results:
            assert len(cf_results) == 1
            keyspace_meta = self.keyspaces[keyspace]
            table_meta = self._build_table_metadata(keyspace_meta, cf_results[0], {table: col_results}, {table: triggers_result})
            keyspace_meta._add_table_metadata(table_meta)
        else:
            try:
                self.keyspaces[keyspace]._drop_table_metadata(table)
            except KeyError:
                pass

    def _keyspace_added(self, ksname):
        if self.token_map:
            self.token_map.rebuild_keyspace(ksname, build_if_absent=False)

    def _keyspace_updated(self, ksname):
        if self.token_map:
            self.token_map.rebuild_keyspace(ksname, build_if_absent=False)

    def _keyspace_removed(self, ksname):
        if self.token_map:
            self.token_map.remove_keyspace(ksname)

    def _build_keyspace_metadata(self, row):
        name = row["keyspace_name"]
        try:
            durable_writes = row["durable_writes"]
            strategy_class = row["strategy_class"]
            strategy_options = json.loads(row["strategy_options"])
            ksm = KeyspaceMetadata(name, durable_writes, strategy_class, strategy_options)
        except Exception:
            log.exception("Error while parsing metadata for keyspace %s row(%s)", name, row)
            ksm = KeyspaceMetadata(name, False, 'UNKNOWN', {})
            ksm._exc_info = sys.exc_info()
        return ksm

    def _build_usertype(self, keyspace, usertype_row):
        type_classes = list(map(types.lookup_casstype, usertype_row['field_types']))
        return UserType(usertype_row['keyspace_name'], usertype_row['type_name'],
                        usertype_row['field_names'], type_classes)

    def _build_function(self, keyspace, function_row):
        return_type = types.lookup_casstype(function_row['return_type'])
        return Function(function_row['keyspace_name'], function_row['function_name'],
                        function_row['signature'], function_row['argument_names'],
                        return_type, function_row['language'], function_row['body'],
                        function_row['called_on_null_input'])

    def _build_aggregate(self, keyspace, aggregate_row):
        state_type = types.lookup_casstype(aggregate_row['state_type'])
        initial_condition = aggregate_row['initcond']
        if initial_condition is not None:
            initial_condition = state_type.deserialize(initial_condition, 3)
        return_type = types.lookup_casstype(aggregate_row['return_type'])
        return Aggregate(aggregate_row['keyspace_name'], aggregate_row['aggregate_name'],
                         aggregate_row['signature'], aggregate_row['state_func'], state_type,
                         aggregate_row['final_func'], initial_condition, return_type)

    def _build_table_metadata(self, keyspace_metadata, row, col_rows, trigger_rows):
        cfname = row["columnfamily_name"]
        cf_col_rows = col_rows.get(cfname, [])

        if not cf_col_rows:  # CASSANDRA-8487
            log.warning("Building table metadata with no column meta for %s.%s",
                        keyspace_metadata.name, cfname)

        table_meta = TableMetadata(keyspace_metadata, cfname)

        try:
            comparator = types.lookup_casstype(row["comparator"])
            table_meta.comparator = comparator

            if issubclass(comparator, types.CompositeType):
                column_name_types = comparator.subtypes
                is_composite_comparator = True
            else:
                column_name_types = (comparator,)
                is_composite_comparator = False

            num_column_name_components = len(column_name_types)
            last_col = column_name_types[-1]

            column_aliases = row.get("column_aliases", None)

            clustering_rows = [r for r in cf_col_rows
                               if r.get('type', None) == "clustering_key"]
            if len(clustering_rows) > 1:
                clustering_rows = sorted(clustering_rows, key=lambda row: row.get('component_index'))

            if column_aliases is not None:
                column_aliases = json.loads(column_aliases)
            else:
                column_aliases = [r.get('column_name') for r in clustering_rows]

            if is_composite_comparator:
                if issubclass(last_col, types.ColumnToCollectionType):
                    # collections
                    is_compact = False
                    has_value = False
                    clustering_size = num_column_name_components - 2
                elif (len(column_aliases) == num_column_name_components - 1
                        and issubclass(last_col, types.UTF8Type)):
                    # aliases?
                    is_compact = False
                    has_value = False
                    clustering_size = num_column_name_components - 1
                else:
                    # compact table
                    is_compact = True
                    has_value = column_aliases or not cf_col_rows
                    clustering_size = num_column_name_components

                    # Some thrift tables define names in composite types (see PYTHON-192)
                    if not column_aliases and hasattr(comparator, 'fieldnames'):
                        column_aliases = comparator.fieldnames
            else:
                is_compact = True
                if column_aliases or not cf_col_rows:
                    has_value = True
                    clustering_size = num_column_name_components
                else:
                    has_value = False
                    clustering_size = 0

            # partition key
            partition_rows = [r for r in cf_col_rows
                              if r.get('type', None) == "partition_key"]

            if len(partition_rows) > 1:
                partition_rows = sorted(partition_rows, key=lambda row: row.get('component_index'))

            key_aliases = row.get("key_aliases")
            if key_aliases is not None:
                key_aliases = json.loads(key_aliases) if key_aliases else []
            else:
                # In 2.0+, we can use the 'type' column. In 3.0+, we have to use it.
                key_aliases = [r.get('column_name') for r in partition_rows]

            key_validator = row.get("key_validator")
            if key_validator is not None:
                key_type = types.lookup_casstype(key_validator)
                key_types = key_type.subtypes if issubclass(key_type, types.CompositeType) else [key_type]
            else:
                key_types = [types.lookup_casstype(r.get('validator')) for r in partition_rows]

            for i, col_type in enumerate(key_types):
                if len(key_aliases) > i:
                    column_name = key_aliases[i]
                elif i == 0:
                    column_name = "key"
                else:
                    column_name = "key%d" % i

                col = ColumnMetadata(table_meta, column_name, col_type)
                table_meta.columns[column_name] = col
                table_meta.partition_key.append(col)

            # clustering key
            for i in range(clustering_size):
                if len(column_aliases) > i:
                    column_name = column_aliases[i]
                else:
                    column_name = "column%d" % i

                col = ColumnMetadata(table_meta, column_name, column_name_types[i])
                table_meta.columns[column_name] = col
                table_meta.clustering_key.append(col)

            # value alias (if present)
            if has_value:
                value_alias_rows = [r for r in cf_col_rows
                                    if r.get('type', None) == "compact_value"]

                if not key_aliases:  # TODO are we checking the right thing here?
                    value_alias = "value"
                else:
                    value_alias = row.get("value_alias", None)
                    if value_alias is None and value_alias_rows:  # CASSANDRA-8487
                        # In 2.0+, we can use the 'type' column. In 3.0+, we have to use it.
                        value_alias = value_alias_rows[0].get('column_name')

                default_validator = row.get("default_validator")
                if default_validator:
                    validator = types.lookup_casstype(default_validator)
                else:
                    if value_alias_rows:  # CASSANDRA-8487
                        validator = types.lookup_casstype(value_alias_rows[0].get('validator'))

                col = ColumnMetadata(table_meta, value_alias, validator)
                if value_alias:  # CASSANDRA-8487
                    table_meta.columns[value_alias] = col

            # other normal columns
            for col_row in cf_col_rows:
                column_meta = self._build_column_metadata(table_meta, col_row)
                table_meta.columns[column_meta.name] = column_meta

            if trigger_rows:
                for trigger_row in trigger_rows[cfname]:
                    trigger_meta = self._build_trigger_metadata(table_meta, trigger_row)
                    table_meta.triggers[trigger_meta.name] = trigger_meta

            table_meta.options = self._build_table_options(row)
            table_meta.is_compact_storage = is_compact
        except Exception:
            table_meta._exc_info = sys.exc_info()
            log.exception("Error while parsing metadata for table %s.%s row(%s) columns(%s)", keyspace_metadata.name, cfname, row, cf_col_rows)

        return table_meta

    def _build_table_options(self, row):
        """ Setup the mostly-non-schema table options, like caching settings """
        options = dict((o, row.get(o)) for o in TableMetadata.recognized_options if o in row)

        # the option name when creating tables is "dclocal_read_repair_chance",
        # but the column name in system.schema_columnfamilies is
        # "local_read_repair_chance".  We'll store this as dclocal_read_repair_chance,
        # since that's probably what users are expecting (and we need it for the
        # CREATE TABLE statement anyway).
        if "local_read_repair_chance" in options:
            val = options.pop("local_read_repair_chance")
            options["dclocal_read_repair_chance"] = val

        return options

    def _build_column_metadata(self, table_metadata, row):
        name = row["column_name"]
        data_type = types.lookup_casstype(row["validator"])
        is_static = row.get("type", None) == "static"
        column_meta = ColumnMetadata(table_metadata, name, data_type, is_static=is_static)
        index_meta = self._build_index_metadata(column_meta, row)
        column_meta.index = index_meta
        if index_meta:
            table_metadata.indexes[index_meta.name] = index_meta
        return column_meta

    def _build_index_metadata(self, column_metadata, row):
        index_name = row.get("index_name")
        index_type = row.get("index_type")
        if index_name or index_type:
            options = row.get("index_options")
            index_options = json.loads(options) if options else {}
            return IndexMetadata(column_metadata, index_name, index_type, index_options)
        else:
            return None

    def _build_trigger_metadata(self, table_metadata, row):
        name = row["trigger_name"]
        options = row["trigger_options"]
        trigger_meta = TriggerMetadata(table_metadata, name, options)
        return trigger_meta

    def rebuild_token_map(self, partitioner, token_map):
        """
        Rebuild our view of the topology from fresh rows from the
        system topology tables.
        For internal use only.
        """
        self.partitioner = partitioner
        if partitioner.endswith('RandomPartitioner'):
            token_class = MD5Token
        elif partitioner.endswith('Murmur3Partitioner'):
            token_class = Murmur3Token
        elif partitioner.endswith('ByteOrderedPartitioner'):
            token_class = BytesToken
        else:
            self.token_map = None
            return

        token_to_host_owner = {}
        ring = []
        for host, token_strings in six.iteritems(token_map):
            for token_string in token_strings:
                token = token_class(token_string)
                ring.append(token)
                token_to_host_owner[token] = host

        all_tokens = sorted(ring)
        self.token_map = TokenMap(
            token_class, token_to_host_owner, all_tokens, self)

    def get_replicas(self, keyspace, key):
        """
        Returns a list of :class:`.Host` instances that are replicas for a given
        partition key.
        """
        t = self.token_map
        if not t:
            return []
        try:
            return t.get_replicas(keyspace, t.token_class.from_key(key))
        except NoMurmur3:
            return []

    def can_support_partitioner(self):
        if self.partitioner.endswith('Murmur3Partitioner') and murmur3 is None:
            return False
        else:
            return True

    def add_or_return_host(self, host):
        """
        Returns a tuple (host, new), where ``host`` is a Host
        instance, and ``new`` is a bool indicating whether
        the host was newly added.
        """
        with self._hosts_lock:
            try:
                return self._hosts[host.address], False
            except KeyError:
                self._hosts[host.address] = host
                return host, True

    def remove_host(self, host):
        with self._hosts_lock:
            return bool(self._hosts.pop(host.address, False))

    def get_host(self, address):
        return self._hosts.get(address)

    def all_hosts(self):
        """
        Returns a list of all known :class:`.Host` instances in the cluster.
        """
        with self._hosts_lock:
            return self._hosts.values()


REPLICATION_STRATEGY_CLASS_PREFIX = "org.apache.cassandra.locator."


def trim_if_startswith(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


_replication_strategies = {}


class ReplicationStrategyTypeType(type):
    def __new__(metacls, name, bases, dct):
        dct.setdefault('name', name)
        cls = type.__new__(metacls, name, bases, dct)
        if not name.startswith('_'):
            _replication_strategies[name] = cls
        return cls


@six.add_metaclass(ReplicationStrategyTypeType)
class _ReplicationStrategy(object):
    options_map = None

    @classmethod
    def create(cls, strategy_class, options_map):
        if not strategy_class:
            return None

        strategy_name = trim_if_startswith(strategy_class, REPLICATION_STRATEGY_CLASS_PREFIX)

        rs_class = _replication_strategies.get(strategy_name, None)
        if rs_class is None:
            rs_class = _UnknownStrategyBuilder(strategy_name)
            _replication_strategies[strategy_name] = rs_class

        try:
            rs_instance = rs_class(options_map)
        except Exception as exc:
            log.warning("Failed creating %s with options %s: %s", strategy_name, options_map, exc)
            return None

        return rs_instance

    def make_token_replica_map(self, token_to_host_owner, ring):
        raise NotImplementedError()

    def export_for_schema(self):
        raise NotImplementedError()


ReplicationStrategy = _ReplicationStrategy


class _UnknownStrategyBuilder(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, options_map):
        strategy_instance = _UnknownStrategy(self.name, options_map)
        return strategy_instance


class _UnknownStrategy(ReplicationStrategy):
    def __init__(self, name, options_map):
        self.name = name
        self.options_map = options_map.copy() if options_map is not None else dict()
        self.options_map['class'] = self.name

    def __eq__(self, other):
        return (isinstance(other, _UnknownStrategy)
                and self.name == other.name
                and self.options_map == other.options_map)

    def export_for_schema(self):
        """
        Returns a string version of these replication options which are
        suitable for use in a CREATE KEYSPACE statement.
        """
        if self.options_map:
            return dict((str(key), str(value)) for key, value in self.options_map.items())
        return "{'class': '%s'}" % (self.name, )

    def make_token_replica_map(self, token_to_host_owner, ring):
        return {}


class SimpleStrategy(ReplicationStrategy):

    replication_factor = None
    """
    The replication factor for this keyspace.
    """

    def __init__(self, options_map):
        try:
            self.replication_factor = int(options_map['replication_factor'])
        except Exception:
            raise ValueError("SimpleStrategy requires an integer 'replication_factor' option")

    def make_token_replica_map(self, token_to_host_owner, ring):
        replica_map = {}
        for i in range(len(ring)):
            j, hosts = 0, list()
            while len(hosts) < self.replication_factor and j < len(ring):
                token = ring[(i + j) % len(ring)]
                host = token_to_host_owner[token]
                if host not in hosts:
                    hosts.append(host)
                j += 1

            replica_map[ring[i]] = hosts
        return replica_map

    def export_for_schema(self):
        """
        Returns a string version of these replication options which are
        suitable for use in a CREATE KEYSPACE statement.
        """
        return "{'class': 'SimpleStrategy', 'replication_factor': '%d'}" \
               % (self.replication_factor,)

    def __eq__(self, other):
        if not isinstance(other, SimpleStrategy):
            return False

        return self.replication_factor == other.replication_factor


class NetworkTopologyStrategy(ReplicationStrategy):

    dc_replication_factors = None
    """
    A map of datacenter names to the replication factor for that DC.
    """

    def __init__(self, dc_replication_factors):
        self.dc_replication_factors = dict(
            (str(k), int(v)) for k, v in dc_replication_factors.items())

    def make_token_replica_map(self, token_to_host_owner, ring):
        # note: this does not account for hosts having different racks
        replica_map = defaultdict(list)
        ring_len = len(ring)
        ring_len_range = range(ring_len)
        dc_rf_map = dict((dc, int(rf))
                         for dc, rf in self.dc_replication_factors.items() if rf > 0)
        dcs = dict((h, h.datacenter) for h in set(token_to_host_owner.values()))

        # build a map of DCs to lists of indexes into `ring` for tokens that
        # belong to that DC
        dc_to_token_offset = defaultdict(list)
        for i, token in enumerate(ring):
            host = token_to_host_owner[token]
            dc_to_token_offset[dcs[host]].append(i)

        # A map of DCs to an index into the dc_to_token_offset value for that dc.
        # This is how we keep track of advancing around the ring for each DC.
        dc_to_current_index = defaultdict(int)

        for i in ring_len_range:
            remaining = dc_rf_map.copy()
            replicas = replica_map[ring[i]]

            # go through each DC and find the replicas in that DC
            for dc in dc_to_token_offset.keys():
                if dc not in remaining:
                    continue

                # advance our per-DC index until we're up to at least the
                # current token in the ring
                token_offsets = dc_to_token_offset[dc]
                index = dc_to_current_index[dc]
                num_tokens = len(token_offsets)
                while index < num_tokens and token_offsets[index] < i:
                    index += 1
                dc_to_current_index[dc] = index

                # now add the next RF distinct token owners to the set of
                # replicas for this DC
                for token_offset in islice(cycle(token_offsets), index, index + num_tokens):
                    host = token_to_host_owner[ring[token_offset]]
                    if host in replicas:
                        continue

                    replicas.append(host)
                    dc_remaining = remaining[dc] - 1
                    if dc_remaining == 0:
                        del remaining[dc]
                        break
                    else:
                        remaining[dc] = dc_remaining

        return replica_map

    def export_for_schema(self):
        """
        Returns a string version of these replication options which are
        suitable for use in a CREATE KEYSPACE statement.
        """
        ret = "{'class': 'NetworkTopologyStrategy'"
        for dc, repl_factor in sorted(self.dc_replication_factors.items()):
            ret += ", '%s': '%d'" % (dc, repl_factor)
        return ret + "}"

    def __eq__(self, other):
        if not isinstance(other, NetworkTopologyStrategy):
            return False

        return self.dc_replication_factors == other.dc_replication_factors


class LocalStrategy(ReplicationStrategy):
    def __init__(self, options_map):
        pass

    def make_token_replica_map(self, token_to_host_owner, ring):
        return {}

    def export_for_schema(self):
        """
        Returns a string version of these replication options which are
        suitable for use in a CREATE KEYSPACE statement.
        """
        return "{'class': 'LocalStrategy'}"

    def __eq__(self, other):
        return isinstance(other, LocalStrategy)


class KeyspaceMetadata(object):
    """
    A representation of the schema for a single keyspace.
    """

    name = None
    """ The string name of the keyspace. """

    durable_writes = True
    """
    A boolean indicating whether durable writes are enabled for this keyspace
    or not.
    """

    replication_strategy = None
    """
    A :class:`.ReplicationStrategy` subclass object.
    """

    tables = None
    """
    A map from table names to instances of :class:`~.TableMetadata`.
    """

    indexes = None
    """
    A dict mapping index names to :class:`.IndexMetadata` instances.
    """

    user_types = None
    """
    A map from user-defined type names to instances of :class:`~cassandra.metadata.UserType`.

    .. versionadded:: 2.1.0
    """

    functions = None
    """
    A map from user-defined function signatures to instances of :class:`~cassandra.metadata.Function`.

    .. versionadded:: 2.6.0
    """

    aggregates = None
    """
    A map from user-defined aggregate signatures to instances of :class:`~cassandra.metadata.Aggregate`.

    .. versionadded:: 2.6.0
    """

    _exc_info = None
    """ set if metadata parsing failed """

    def __init__(self, name, durable_writes, strategy_class, strategy_options):
        self.name = name
        self.durable_writes = durable_writes
        self.replication_strategy = ReplicationStrategy.create(strategy_class, strategy_options)
        self.tables = {}
        self.indexes = {}
        self.user_types = {}
        self.functions = {}
        self.aggregates = {}

    def export_as_string(self):
        """
        Returns a CQL query string that can be used to recreate the entire keyspace,
        including user-defined types and tables.
        """
        cql = "\n\n".join([self.as_cql_query()]
                         + self.user_type_strings()
                         + [f.as_cql_query(True) for f in self.functions.values()]
                         + [a.as_cql_query(True) for a in self.aggregates.values()]
                         + [t.export_as_string() for t in self.tables.values()])
        if self._exc_info:
            import traceback
            ret = "/*\nWarning: Keyspace %s is incomplete because of an error processing metadata.\n" % \
                  (self.name)
            for line in traceback.format_exception(*self._exc_info):
                ret += line
            ret += "\nApproximate structure, for reference:\n(this should not be used to reproduce this schema)\n\n%s\n*/" % cql
            return ret
        return cql

    def as_cql_query(self):
        """
        Returns a CQL query string that can be used to recreate just this keyspace,
        not including user-defined types and tables.
        """
        ret = "CREATE KEYSPACE %s WITH replication = %s " % (
            protect_name(self.name),
            self.replication_strategy.export_for_schema())
        return ret + (' AND durable_writes = %s;' % ("true" if self.durable_writes else "false"))

    def user_type_strings(self):
        user_type_strings = []
        types = self.user_types.copy()
        keys = sorted(types.keys())
        for k in keys:
            if k in types:
                self.resolve_user_types(k, types, user_type_strings)
        return user_type_strings

    def resolve_user_types(self, key, types, user_type_strings):
        user_type = types.pop(key)
        for field_type in user_type.field_types:
            if field_type.cassname == 'UserType' and field_type.typename in types:
                self.resolve_user_types(field_type.typename, types, user_type_strings)
        user_type_strings.append(user_type.as_cql_query(formatted=True))

    def _add_table_metadata(self, table_metadata):
        self._drop_table_metadata(table_metadata.name)

        self.tables[table_metadata.name] = table_metadata
        for index_name, index_metadata in six.iteritems(table_metadata.indexes):
            self.indexes[index_name] = index_metadata

    def _drop_table_metadata(self, table_name):
        table_meta = self.tables.pop(table_name, None)
        if table_meta:
            for index_name in table_meta.indexes:
                self.indexes.pop(index_name, None)


class UserType(object):
    """
    A user defined type, as created by ``CREATE TYPE`` statements.

    User-defined types were introduced in Cassandra 2.1.

    .. versionadded:: 2.1.0
    """

    keyspace = None
    """
    The string name of the keyspace in which this type is defined.
    """

    name = None
    """
    The name of this type.
    """

    field_names = None
    """
    An ordered list of the names for each field in this user-defined type.
    """

    field_types = None
    """
    An ordered list of the types for each field in this user-defined type.
    """

    def __init__(self, keyspace, name, field_names, field_types):
        self.keyspace = keyspace
        self.name = name
        # non-frozen collections can return None
        self.field_names = field_names or []
        self.field_types = field_types or []

    def as_cql_query(self, formatted=False):
        """
        Returns a CQL query that can be used to recreate this type.
        If `formatted` is set to :const:`True`, extra whitespace will
        be added to make the query more readable.
        """
        ret = "CREATE TYPE %s.%s (%s" % (
            protect_name(self.keyspace),
            protect_name(self.name),
            "\n" if formatted else "")

        if formatted:
            field_join = ",\n"
            padding = "    "
        else:
            field_join = ", "
            padding = ""

        fields = []
        for field_name, field_type in zip(self.field_names, self.field_types):
            fields.append("%s %s" % (protect_name(field_name), field_type.cql_parameterized_type()))

        ret += field_join.join("%s%s" % (padding, field) for field in fields)
        ret += "\n);" if formatted else ");"
        return ret


class Aggregate(object):
    """
    A user defined aggregate function, as created by ``CREATE AGGREGATE`` statements.

    Aggregate functions were introduced in Cassandra 2.2

    .. versionadded:: 2.6.0
    """

    keyspace = None
    """
    The string name of the keyspace in which this aggregate is defined
    """

    name = None
    """
    The name of this aggregate
    """

    type_signature = None
    """
    An ordered list of the types for each argument to the aggregate
    """

    final_func = None
    """
    Name of a final function
    """

    initial_condition = None
    """
    Initial condition of the aggregate
    """

    return_type = None
    """
    Return type of the aggregate
    """

    state_func = None
    """
    Name of a state function
    """

    state_type = None
    """
    Type of the aggregate state
    """

    def __init__(self, keyspace, name, type_signature, state_func,
                 state_type, final_func, initial_condition, return_type):
        self.keyspace = keyspace
        self.name = name
        self.type_signature = type_signature
        self.state_func = state_func
        self.state_type = state_type
        self.final_func = final_func
        self.initial_condition = initial_condition
        self.return_type = return_type

    def as_cql_query(self, formatted=False):
        """
        Returns a CQL query that can be used to recreate this aggregate.
        If `formatted` is set to :const:`True`, extra whitespace will
        be added to make the query more readable.
        """
        sep = '\n' if formatted else ' '
        keyspace = protect_name(self.keyspace)
        name = protect_name(self.name)
        type_list = ', '.join(self.type_signature)
        state_func = protect_name(self.state_func)
        state_type = self.state_type.cql_parameterized_type()

        ret = "CREATE AGGREGATE %(keyspace)s.%(name)s(%(type_list)s)%(sep)s" \
              "SFUNC %(state_func)s%(sep)s" \
              "STYPE %(state_type)s" % locals()

        ret += ''.join((sep, 'FINALFUNC ', protect_name(self.final_func))) if self.final_func else ''
        ret += ''.join((sep, 'INITCOND ', Encoder().cql_encode_all_types(self.initial_condition)))\
               if self.initial_condition is not None else ''

        return ret

    @property
    def signature(self):
        return SignatureDescriptor.format_signature(self.name, self.type_signature)


class Function(object):
    """
    A user defined function, as created by ``CREATE FUNCTION`` statements.

    User-defined functions were introduced in Cassandra 2.2

    .. versionadded:: 2.6.0
    """

    keyspace = None
    """
    The string name of the keyspace in which this function is defined
    """

    name = None
    """
    The name of this function
    """

    type_signature = None
    """
    An ordered list of the types for each argument to the function
    """

    argument_names = None
    """
    An ordered list of the names of each argument to the function
    """

    return_type = None
    """
    Return type of the function
    """

    language = None
    """
    Language of the function body
    """

    body = None
    """
    Function body string
    """

    called_on_null_input = None
    """
    Flag indicating whether this function should be called for rows with null values
    (convenience function to avoid handling nulls explicitly if the result will just be null)
    """

    def __init__(self, keyspace, name, type_signature, argument_names,
                 return_type, language, body, called_on_null_input):
        self.keyspace = keyspace
        self.name = name
        self.type_signature = type_signature
        # type_signature (frozen<list<>>) will always be a list
        # argument_name is not frozen in C* < 3.0 and may return None
        self.argument_names = argument_names or []
        self.return_type = return_type
        self.language = language
        self.body = body
        self.called_on_null_input = called_on_null_input

    def as_cql_query(self, formatted=False):
        """
        Returns a CQL query that can be used to recreate this function.
        If `formatted` is set to :const:`True`, extra whitespace will
        be added to make the query more readable.
        """
        sep = '\n' if formatted else ' '
        keyspace = protect_name(self.keyspace)
        name = protect_name(self.name)
        arg_list = ', '.join(["%s %s" % (protect_name(n), t)
                             for n, t in zip(self.argument_names, self.type_signature)])
        typ = self.return_type.cql_parameterized_type()
        lang = self.language
        body = self.body
        on_null = "CALLED" if self.called_on_null_input else "RETURNS NULL"

        return "CREATE FUNCTION %(keyspace)s.%(name)s(%(arg_list)s)%(sep)s" \
               "%(on_null)s ON NULL INPUT%(sep)s" \
               "RETURNS %(typ)s%(sep)s" \
               "LANGUAGE %(lang)s%(sep)s" \
               "AS $$%(body)s$$;" % locals()

    @property
    def signature(self):
        return SignatureDescriptor.format_signature(self.name, self.type_signature)


class TableMetadata(object):
    """
    A representation of the schema for a single table.
    """

    keyspace = None
    """ An instance of :class:`~.KeyspaceMetadata`. """

    name = None
    """ The string name of the table. """

    partition_key = None
    """
    A list of :class:`.ColumnMetadata` instances representing the columns in
    the partition key for this table.  This will always hold at least one
    column.
    """

    clustering_key = None
    """
    A list of :class:`.ColumnMetadata` instances representing the columns
    in the clustering key for this table.  These are all of the
    :attr:`.primary_key` columns that are not in the :attr:`.partition_key`.

    Note that a table may have no clustering keys, in which case this will
    be an empty list.
    """

    @property
    def primary_key(self):
        """
        A list of :class:`.ColumnMetadata` representing the components of
        the primary key for this table.
        """
        return self.partition_key + self.clustering_key

    columns = None
    """
    A dict mapping column names to :class:`.ColumnMetadata` instances.
    """

    indexes = None
    """
    A dict mapping index names to :class:`.IndexMetadata` instances.
    """

    is_compact_storage = False

    options = None
    """
    A dict mapping table option names to their specific settings for this
    table.
    """

    recognized_options = (
        "comment",
        "read_repair_chance",
        "dclocal_read_repair_chance",  # kept to be safe, but see _build_table_options()
        "local_read_repair_chance",
        "replicate_on_write",
        "gc_grace_seconds",
        "bloom_filter_fp_chance",
        "caching",
        "compaction_strategy_class",
        "compaction_strategy_options",
        "min_compaction_threshold",
        "max_compaction_threshold",
        "compression_parameters",
        "min_index_interval",
        "max_index_interval",
        "index_interval",
        "speculative_retry",
        "rows_per_partition_to_cache",
        "memtable_flush_period_in_ms",
        "populate_io_cache_on_flush",
        "compression",
        "default_time_to_live")

    compaction_options = {
        "min_compaction_threshold": "min_threshold",
        "max_compaction_threshold": "max_threshold",
        "compaction_strategy_class": "class"}

    triggers = None
    """
    A dict mapping trigger names to :class:`.TriggerMetadata` instances.
    """

    _exc_info = None
    """ set if metadata parsing failed """

    @property
    def is_cql_compatible(self):
        """
        A boolean indicating if this table can be represented as CQL in export
        """
        # no such thing as DCT in CQL
        incompatible = issubclass(self.comparator, types.DynamicCompositeType)

        # no compact storage with more than one column beyond PK if there
        # are clustering columns
        incompatible |= (self.is_compact_storage and
                         len(self.columns) > len(self.primary_key) + 1 and
                         len(self.clustering_key) >= 1)

        return not incompatible

    def __init__(self, keyspace_metadata, name, partition_key=None, clustering_key=None, columns=None, triggers=None, options=None):
        self.keyspace = keyspace_metadata
        self.name = name
        self.partition_key = [] if partition_key is None else partition_key
        self.clustering_key = [] if clustering_key is None else clustering_key
        self.columns = OrderedDict() if columns is None else columns
        self.indexes = {}
        self.options = {} if options is None else options
        self.comparator = None
        self.triggers = OrderedDict() if triggers is None else triggers

    def export_as_string(self):
        """
        Returns a string of CQL queries that can be used to recreate this table
        along with all indexes on it.  The returned string is formatted to
        be human readable.
        """
        if self._exc_info:
            import traceback
            ret = "/*\nWarning: Table %s.%s is incomplete because of an error processing metadata.\n" % \
                  (self.keyspace.name, self.name)
            for line in traceback.format_exception(*self._exc_info):
                ret += line
            ret += "\nApproximate structure, for reference:\n(this should not be used to reproduce this schema)\n\n%s\n*/" % self.all_as_cql()
        elif not self.is_cql_compatible:
            # If we can't produce this table with CQL, comment inline
            ret = "/*\nWarning: Table %s.%s omitted because it has constructs not compatible with CQL (was created via legacy API).\n" % \
                  (self.keyspace.name, self.name)
            ret += "\nApproximate structure, for reference:\n(this should not be used to reproduce this schema)\n\n%s\n*/" % self.all_as_cql()
        else:
            ret = self.all_as_cql()

        return ret

    def all_as_cql(self):
        ret = self.as_cql_query(formatted=True)
        ret += ";"

        for col_meta in self.columns.values():
            if col_meta.index:
                ret += "\n%s;" % (col_meta.index.as_cql_query(),)

        for trigger_meta in self.triggers.values():
            ret += "\n%s;" % (trigger_meta.as_cql_query(),)
        return ret

    def as_cql_query(self, formatted=False):
        """
        Returns a CQL query that can be used to recreate this table (index
        creations are not included).  If `formatted` is set to :const:`True`,
        extra whitespace will be added to make the query human readable.
        """
        ret = "CREATE TABLE %s.%s (%s" % (
            protect_name(self.keyspace.name),
            protect_name(self.name),
            "\n" if formatted else "")

        if formatted:
            column_join = ",\n"
            padding = "    "
        else:
            column_join = ", "
            padding = ""

        columns = []
        for col in self.columns.values():
            columns.append("%s %s%s" % (protect_name(col.name), col.typestring, ' static' if col.is_static else ''))

        if len(self.partition_key) == 1 and not self.clustering_key:
            columns[0] += " PRIMARY KEY"

        ret += column_join.join("%s%s" % (padding, col) for col in columns)

        # primary key
        if len(self.partition_key) > 1 or self.clustering_key:
            ret += "%s%sPRIMARY KEY (" % (column_join, padding)

            if len(self.partition_key) > 1:
                ret += "(%s)" % ", ".join(protect_name(col.name) for col in self.partition_key)
            else:
                ret += protect_name(self.partition_key[0].name)

            if self.clustering_key:
                ret += ", %s" % ", ".join(protect_name(col.name) for col in self.clustering_key)

            ret += ")"

        # options
        ret += "%s) WITH " % ("\n" if formatted else "")

        option_strings = []
        if self.is_compact_storage:
            option_strings.append("COMPACT STORAGE")

        if self.clustering_key:
            cluster_str = "CLUSTERING ORDER BY "

            clustering_names = protect_names([c.name for c in self.clustering_key])

            if self.is_compact_storage and \
                    not issubclass(self.comparator, types.CompositeType):
                subtypes = [self.comparator]
            else:
                subtypes = self.comparator.subtypes

            inner = []
            for colname, coltype in zip(clustering_names, subtypes):
                ordering = "DESC" if issubclass(coltype, types.ReversedType) else "ASC"
                inner.append("%s %s" % (colname, ordering))

            cluster_str += "(%s)" % ", ".join(inner)
            option_strings.append(cluster_str)

        option_strings.extend(self._make_option_strings())

        join_str = "\n    AND " if formatted else " AND "
        ret += join_str.join(option_strings)

        return ret

    def _make_option_strings(self):
        ret = []
        options_copy = dict(self.options.items())

        actual_options = json.loads(options_copy.pop('compaction_strategy_options', '{}'))
        value = options_copy.pop("compaction_strategy_class", None)
        actual_options.setdefault("class", value)

        compaction_option_strings = ["'%s': '%s'" % (k, v) for k, v in actual_options.items()]
        ret.append('compaction = {%s}' % ', '.join(compaction_option_strings))

        for system_table_name in self.compaction_options.keys():
            options_copy.pop(system_table_name, None)  # delete if present
        options_copy.pop('compaction_strategy_option', None)

        if not options_copy.get('compression'):
            params = json.loads(options_copy.pop('compression_parameters', '{}'))
            param_strings = ["'%s': '%s'" % (k, v) for k, v in params.items()]
            ret.append('compression = {%s}' % ', '.join(param_strings))

        for name, value in options_copy.items():
            if value is not None:
                if name == "comment":
                    value = value or ""
                ret.append("%s = %s" % (name, protect_value(value)))

        return list(sorted(ret))


if six.PY3:
    def protect_name(name):
        return maybe_escape_name(name)
else:
    def protect_name(name):  # NOQA
        if isinstance(name, six.text_type):
            name = name.encode('utf8')
        return maybe_escape_name(name)


def protect_names(names):
    return [protect_name(n) for n in names]


def protect_value(value):
    if value is None:
        return 'NULL'
    if isinstance(value, (int, float, bool)):
        return str(value).lower()
    return "'%s'" % value.replace("'", "''")


valid_cql3_word_re = re.compile(r'^[a-z][0-9a-z_]*$')


def is_valid_name(name):
    if name is None:
        return False
    if name.lower() in cql_keywords_reserved:
        return False
    return valid_cql3_word_re.match(name) is not None


def maybe_escape_name(name):
    if is_valid_name(name):
        return name
    return escape_name(name)


def escape_name(name):
    return '"%s"' % (name.replace('"', '""'),)


class ColumnMetadata(object):
    """
    A representation of a single column in a table.
    """

    table = None
    """ The :class:`.TableMetadata` this column belongs to. """

    name = None
    """ The string name of this column. """

    data_type = None
    """
    The data type for the column in the form of an instance of one of
    the type classes in :mod:`cassandra.cqltypes`.
    """

    index = None
    """
    If an index exists on this column, this is an instance of
    :class:`.IndexMetadata`, otherwise :const:`None`.
    """

    is_static = False
    """
    If this column is static (available in Cassandra 2.1+), this will
    be :const:`True`, otherwise :const:`False`.
    """

    def __init__(self, table_metadata, column_name, data_type, index_metadata=None, is_static=False):
        self.table = table_metadata
        self.name = column_name
        self.data_type = data_type
        self.index = index_metadata
        self.is_static = is_static

    @property
    def typestring(self):
        """
        A string representation of the type for this column, such as "varchar"
        or "map<string, int>".
        """
        if issubclass(self.data_type, types.ReversedType):
            return self.data_type.subtypes[0].cql_parameterized_type()
        else:
            return self.data_type.cql_parameterized_type()

    def __str__(self):
        return "%s %s" % (self.name, self.data_type)


class IndexMetadata(object):
    """
    A representation of a secondary index on a column.
    """

    column = None
    """
    The column (:class:`.ColumnMetadata`) this index is on.
    """

    name = None
    """ A string name for the index. """

    index_type = None
    """ A string representing the type of index. """

    index_options = {}
    """ A dict of index options. """

    def __init__(self, column_metadata, index_name=None, index_type=None, index_options={}):
        self.column = column_metadata
        self.name = index_name
        self.index_type = index_type
        self.index_options = index_options

    def as_cql_query(self):
        """
        Returns a CQL query that can be used to recreate this index.
        """
        table = self.column.table
        if self.index_type != "CUSTOM":
            index_target = protect_name(self.column.name)
            if self.index_options is not None:
                option_keys = self.index_options.keys()
                if "index_keys" in option_keys:
                    index_target = 'keys(%s)' % (index_target,)
                elif "index_values" in option_keys:
                    # don't use any "function" for collection values
                    pass
                else:
                    # it might be a "full" index on a frozen collection, but
                    # we need to check the data type to verify that, because
                    # there is no special index option for full-collection
                    # indexes.
                    data_type = self.column.data_type
                    collection_types = ('map', 'set', 'list')
                    if data_type.typename == "frozen" and data_type.subtypes[0].typename in collection_types:
                        # no index option for full-collection index
                        index_target = 'full(%s)' % (index_target,)

            return "CREATE INDEX %s ON %s.%s (%s)" % (
                self.name,  # Cassandra doesn't like quoted index names for some reason
                protect_name(table.keyspace.name),
                protect_name(table.name),
                index_target)
        else:
            return "CREATE CUSTOM INDEX %s ON %s.%s (%s) USING '%s'" % (
                self.name,  # Cassandra doesn't like quoted index names for some reason
                protect_name(table.keyspace.name),
                protect_name(table.name),
                protect_name(self.column.name),
                self.index_options["class_name"])

    def export_as_string(self):
        """
        Returns a CQL query string that can be used to recreate this index.
        """
        return self.as_cql_query() + ';'


class TokenMap(object):
    """
    Information about the layout of the ring.
    """

    token_class = None
    """
    A subclass of :class:`.Token`, depending on what partitioner the cluster uses.
    """

    token_to_host_owner = None
    """
    A map of :class:`.Token` objects to the :class:`.Host` that owns that token.
    """

    tokens_to_hosts_by_ks = None
    """
    A map of keyspace names to a nested map of :class:`.Token` objects to
    sets of :class:`.Host` objects.
    """

    ring = None
    """
    An ordered list of :class:`.Token` instances in the ring.
    """

    _metadata = None

    def __init__(self, token_class, token_to_host_owner, all_tokens, metadata):
        self.token_class = token_class
        self.ring = all_tokens
        self.token_to_host_owner = token_to_host_owner

        self.tokens_to_hosts_by_ks = {}
        self._metadata = metadata
        self._rebuild_lock = RLock()

    def rebuild_keyspace(self, keyspace, build_if_absent=False):
        with self._rebuild_lock:
            current = self.tokens_to_hosts_by_ks.get(keyspace, None)
            if (build_if_absent and current is None) or (not build_if_absent and current is not None):
                replica_map = self.replica_map_for_keyspace(self._metadata.keyspaces[keyspace])
                self.tokens_to_hosts_by_ks[keyspace] = replica_map

    def replica_map_for_keyspace(self, ks_metadata):
        strategy = ks_metadata.replication_strategy
        if strategy:
            return strategy.make_token_replica_map(self.token_to_host_owner, self.ring)
        else:
            return None

    def remove_keyspace(self, keyspace):
        self.tokens_to_hosts_by_ks.pop(keyspace, None)

    def get_replicas(self, keyspace, token):
        """
        Get  a set of :class:`.Host` instances representing all of the
        replica nodes for a given :class:`.Token`.
        """
        tokens_to_hosts = self.tokens_to_hosts_by_ks.get(keyspace, None)
        if tokens_to_hosts is None:
            self.rebuild_keyspace(keyspace, build_if_absent=True)
            tokens_to_hosts = self.tokens_to_hosts_by_ks.get(keyspace, None)

        if tokens_to_hosts:
            # token range ownership is exclusive on the LHS (the start token), so
            # we use bisect_right, which, in the case of a tie/exact match,
            # picks an insertion point to the right of the existing match
            point = bisect_right(self.ring, token)
            if point == len(self.ring):
                return tokens_to_hosts[self.ring[0]]
            else:
                return tokens_to_hosts[self.ring[point]]
        return []


class Token(object):
    """
    Abstract class representing a token.
    """

    @classmethod
    def hash_fn(cls, key):
        return key

    @classmethod
    def from_key(cls, key):
        return cls(cls.hash_fn(key))

    def __cmp__(self, other):
        if self.value < other.value:
            return -1
        elif self.value == other.value:
            return 0
        else:
            return 1

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        return self.value < other.value

    def __hash__(self):
        return hash(self.value)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.value)
    __str__ = __repr__

MIN_LONG = -(2 ** 63)
MAX_LONG = (2 ** 63) - 1


class NoMurmur3(Exception):
    pass


class Murmur3Token(Token):
    """
    A token for ``Murmur3Partitioner``.
    """

    @classmethod
    def hash_fn(cls, key):
        if murmur3 is not None:
            h = int(murmur3(key))
            return h if h != MIN_LONG else MAX_LONG
        else:
            raise NoMurmur3()

    def __init__(self, token):
        """ `token` should be an int or string representing the token. """
        self.value = int(token)


class MD5Token(Token):
    """
    A token for ``RandomPartitioner``.
    """

    @classmethod
    def hash_fn(cls, key):
        if isinstance(key, six.text_type):
            key = key.encode('UTF-8')
        return abs(varint_unpack(md5(key).digest()))

    def __init__(self, token):
        """ `token` should be an int or string representing the token. """
        self.value = int(token)


class BytesToken(Token):
    """
    A token for ``ByteOrderedPartitioner``.
    """

    def __init__(self, token_string):
        """ `token_string` should be string representing the token. """
        if not isinstance(token_string, six.string_types):
            raise TypeError(
                "Tokens for ByteOrderedPartitioner should be strings (got %s)"
                % (type(token_string),))
        self.value = token_string


class TriggerMetadata(object):
    """
    A representation of a trigger for a table.
    """

    table = None
    """ The :class:`.TableMetadata` this trigger belongs to. """

    name = None
    """ The string name of this trigger. """

    options = None
    """
    A dict mapping trigger option names to their specific settings for this
    table.
    """
    def __init__(self, table_metadata, trigger_name, options=None):
        self.table = table_metadata
        self.name = trigger_name
        self.options = options

    def as_cql_query(self):
        ret = "CREATE TRIGGER %s ON %s.%s USING %s" % (
            protect_name(self.name),
            protect_name(self.table.keyspace.name),
            protect_name(self.table.name),
            protect_value(self.options['class'])
        )
        return ret
