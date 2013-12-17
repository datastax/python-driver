from bisect import bisect_left
from collections import defaultdict
try:
    from collections import OrderedDict
except ImportError:  # Python <2.7
    from cassandra.util import OrderedDict # NOQA
from hashlib import md5
import json
import logging
import re
from threading import RLock
import weakref

murmur3 = None
try:
    from murmur3 import murmur3
except ImportError:
    pass

import cassandra.cqltypes as types
from cassandra.marshal import varint_unpack
from cassandra.pool import Host

log = logging.getLogger(__name__)

_keywords = set((
    'select', 'from', 'where', 'and', 'key', 'insert', 'update', 'with',
    'limit', 'using', 'use', 'count', 'set',
    'begin', 'apply', 'batch', 'truncate', 'delete', 'in', 'create',
    'keyspace', 'schema', 'columnfamily', 'table', 'index', 'on', 'drop',
    'primary', 'into', 'values', 'timestamp', 'ttl', 'alter', 'add', 'type',
    'compact', 'storage', 'order', 'by', 'asc', 'desc', 'clustering',
    'token', 'writetime', 'map', 'list', 'to'
))

_unreserved_keywords = set((
    'key', 'clustering', 'ttl', 'compact', 'storage', 'type', 'values'
))


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

    def __init__(self, cluster):
        # use a weak reference so that the Cluster object can be GC'ed.
        # Normally the cycle detector would handle this, but implementing
        # __del__ disables that.
        self.cluster_ref = weakref.ref(cluster)
        self.keyspaces = {}
        self._hosts = {}
        self._hosts_lock = RLock()

    def export_schema_as_string(self):
        """
        Returns a string that can be executed as a query in order to recreate
        the entire schema.  The string is formatted to be human readable.
        """
        return "\n".join(ks.export_as_string() for ks in self.keyspaces.values())

    def rebuild_schema(self, keyspace, table, ks_results, cf_results, col_results):
        """
        Rebuild the view of the current schema from a fresh set of rows from
        the system schema tables.

        For internal use only.
        """
        cf_def_rows = defaultdict(list)
        col_def_rows = defaultdict(lambda: defaultdict(list))

        for row in cf_results:
            cf_def_rows[row["keyspace_name"]].append(row)

        for row in col_results:
            ksname = row["keyspace_name"]
            cfname = row["columnfamily_name"]
            col_def_rows[ksname][cfname].append(row)

        # either table or ks_results must be None
        if not table:
            # ks_results is not None
            added_keyspaces = set()
            for row in ks_results:
                keyspace_meta = self._build_keyspace_metadata(row)
                for table_row in cf_def_rows.get(keyspace_meta.name, []):
                    table_meta = self._build_table_metadata(
                        keyspace_meta, table_row, col_def_rows[keyspace_meta.name])
                    keyspace_meta.tables[table_meta.name] = table_meta

                added_keyspaces.add(keyspace_meta.name)
                self.keyspaces[keyspace_meta.name] = keyspace_meta

            if not keyspace:
                # remove not-just-added keyspaces
                self.keyspaces = dict((name, meta) for name, meta in self.keyspaces.items()
                                      if name in added_keyspaces)
            if self.token_map:
                self.token_map.rebuild(self.keyspaces.values())
        else:
            # keyspace is not None, table is not None
            try:
                keyspace_meta = self.keyspaces[keyspace]
            except KeyError:
                # we're trying to update a table in a keyspace we don't know
                # about, something went wrong.
                # TODO log error, submit schema refresh
                pass
            if keyspace in cf_def_rows:
                for table_row in cf_def_rows[keyspace]:
                    table_meta = self._build_table_metadata(
                        keyspace_meta, table_row, col_def_rows[keyspace])
                    keyspace_meta.tables[table_meta.name] = table_meta

    def _build_keyspace_metadata(self, row):
        name = row["keyspace_name"]
        durable_writes = row["durable_writes"]
        strategy_class = row["strategy_class"]
        strategy_options = json.loads(row["strategy_options"])
        return KeyspaceMetadata(name, durable_writes, strategy_class, strategy_options)

    def _build_table_metadata(self, keyspace_metadata, row, col_rows):
        cfname = row["columnfamily_name"]

        comparator = types.lookup_casstype(row["comparator"])
        if issubclass(comparator, types.CompositeType):
            column_name_types = comparator.subtypes
            is_composite = True
        else:
            column_name_types = (comparator,)
            is_composite = False

        num_column_name_components = len(column_name_types)
        last_col = column_name_types[-1]

        column_aliases = json.loads(row["column_aliases"])
        if is_composite:
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
                has_value = True
                clustering_size = num_column_name_components
        else:
            is_compact = True
            if column_aliases or not col_rows.get(cfname):
                has_value = True
                clustering_size = num_column_name_components
            else:
                has_value = False
                clustering_size = 0

        table_meta = TableMetadata(keyspace_metadata, cfname)
        table_meta.comparator = comparator

        # partition key
        key_aliases = row.get("key_aliases")
        key_aliases = json.loads(key_aliases) if key_aliases else []

        key_type = types.lookup_casstype(row["key_validator"])
        key_types = key_type.subtypes if issubclass(key_type, types.CompositeType) else [key_type]
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
            validator = types.lookup_casstype(row["default_validator"])
            if not key_aliases:  # TODO are we checking the right thing here?
                value_alias = "value"
            else:
                value_alias = row["value_alias"]

            col = ColumnMetadata(table_meta, value_alias, validator)
            table_meta.columns[value_alias] = col

        # other normal columns
        if col_rows:
            for col_row in col_rows[cfname]:
                column_meta = self._build_column_metadata(table_meta, col_row)
                table_meta.columns[column_meta.name] = column_meta

        table_meta.options = self._build_table_options(row, is_compact)
        return table_meta

    def _build_table_options(self, row, is_compact_storage):
        """ Setup the mostly-non-schema table options, like caching settings """
        options = dict((o, row.get(o)) for o in TableMetadata.recognized_options)
        options["is_compact_storage"] = is_compact_storage
        return options

    def _build_column_metadata(self, table_metadata, row):
        name = row["column_name"]
        data_type = types.lookup_casstype(row["validator"])
        column_meta = ColumnMetadata(table_metadata, name, data_type)
        index_meta = self._build_index_metadata(column_meta, row)
        column_meta.index = index_meta
        return column_meta

    def _build_index_metadata(self, column_metadata, row):
        index_name = row.get("index_name")
        index_type = row.get("index_type")
        if index_name or index_type:
            return IndexMetadata(column_metadata, index_name, index_type)
        else:
            return None

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
            if murmur3 is None:
                log.warning(
                    "The murmur3 C extension is not available, token awareness "
                    "cannot be supported for the Murmur3Partitioner")
        elif partitioner.endswith('ByteOrderedPartitioner'):
            token_class = BytesToken
        else:
            self.token_map = None
            return

        token_to_host_owner = {}
        ring = []
        for host, token_strings in token_map.iteritems():
            for token_string in token_strings:
                token = token_class(token_string)
                ring.append(token)
                token_to_host_owner[token] = host

        all_tokens = sorted(ring)
        self.token_map = TokenMap(
                token_class, token_to_host_owner, all_tokens,
                self.keyspaces.values())

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

    def add_host(self, address):
        cluster = self.cluster_ref()
        with self._hosts_lock:
            if address not in self._hosts:
                new_host = Host(address, cluster.conviction_policy_factory)
                self._hosts[address] = new_host
            else:
                return None

        return new_host

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


class ReplicationStrategy(object):

    @classmethod
    def create(cls, strategy_class, options_map):
        if not strategy_class:
            return None

        if strategy_class.endswith("OldNetworkTopologyStrategy"):
            return None
        elif strategy_class.endswith("NetworkTopologyStrategy"):
            return NetworkTopologyStrategy(options_map)
        elif strategy_class.endswith("SimpleStrategy"):
            repl_factor = options_map.get('replication_factor', None)
            if not repl_factor:
                return None
            return SimpleStrategy(repl_factor)
        elif strategy_class.endswith("LocalStrategy"):
            return LocalStrategy()

    def make_token_replica_map(self, token_to_host_owner, ring):
        raise NotImplementedError()

    def export_for_schema(self):
        raise NotImplementedError()


class SimpleStrategy(ReplicationStrategy):

    name = "SimpleStrategy"
    replication_factor = None

    def __init__(self, replication_factor):
        self.replication_factor = int(replication_factor)

    def make_token_replica_map(self, token_to_host_owner, ring):
        replica_map = {}
        for i in range(len(ring)):
            j, hosts = 0, set()
            while len(hosts) < self.replication_factor and j < len(ring):
                token = ring[(i + j) % len(ring)]
                hosts.add(token_to_host_owner[token])
                j += 1

            replica_map[ring[i]] = list(sorted(hosts))
        return replica_map

    def export_for_schema(self):
        return "{'class': 'SimpleStrategy', 'replication_factor': '%d'}" \
               % (self.replication_factor,)

class NetworkTopologyStrategy(ReplicationStrategy):

    name = "NetworkTopologyStrategy"
    dc_replication_factors = None

    def __init__(self, dc_replication_factors):
        self.dc_replication_factors = dc_replication_factors

    def make_token_replica_map(self, token_to_host_owner, ring):
        # note: this does not account for hosts having different racks
        replica_map = defaultdict(set)
        ring_len = len(ring)
        ring_len_range = range(ring_len)
        dc_rf_map = dict((dc, int(rf))
                         for dc, rf in self.dc_replication_factors.items() if rf > 0)
        dcs = dict((h, h.datacenter) for h in set(token_to_host_owner.values()))

        for i in ring_len_range:
            remaining = dc_rf_map.copy()
            for j in ring_len_range:
                token = ring[(i + j) % ring_len]
                host = token_to_host_owner[token]
                dc = dcs[host]
                if not dc in remaining:
                    # we already have all replicas for this DC
                    continue

                replica_map[ring[i]].add(host)

                if remaining[dc] == 1:
                    del remaining[dc]
                    if not remaining:
                        break
                else:
                    remaining[dc] -= 1

            replica_map[ring[i]] = list(sorted(replica_map[ring[i]]))

        return replica_map

    def export_for_schema(self):
        ret = "{'class': 'NetworkTopologyStrategy'"
        for dc, repl_factor in self.dc_replication_factors:
            ret += ", '%s': '%d'" % (dc, repl_factor)
        return ret + "}"


class LocalStrategy(ReplicationStrategy):

    name = "LocalStrategy"

    def make_token_replica_map(self, token_to_host_owner, ring):
        return {}

    def export_for_schema(self):
        return "{'class': 'LocalStrategy'}"


class KeyspaceMetadata(object):
    """
    A representation of the schema for a single keyspace.
    """

    name = None
    """ The string name of the keyspace """

    durable_writes = True
    """
    A boolean indicating whether durable writes are enabled for this keyspace
    or not
    """

    replication_strategy = None
    """
    A :class:`.ReplicationStrategy` subclass object.
    """

    tables = None
    """
    A map from table names to instances of :class:`~.TableMetadata`.
    """

    def __init__(self, name, durable_writes, strategy_class, strategy_options):
        self.name = name
        self.durable_writes = durable_writes
        self.replication_strategy = ReplicationStrategy.create(strategy_class, strategy_options)
        self.tables = {}

    def export_as_string(self):
        return "\n".join([self.as_cql_query()] + [t.as_cql_query() for t in self.tables.values()])

    def as_cql_query(self):
        ret = "CREATE KEYSPACE %s WITH REPLICATION = %s " % \
              (self.name, self.replication_strategy.export_for_schema())
        return ret + (' AND DURABLE_WRITES = %s;' % ("true" if self.durable_writes else "false"))


class TableMetadata(object):
    """
    A representation of the schema for a single table.
    """

    keyspace = None
    """ An instance of :class:`~.KeyspaceMetadata` """

    name = None
    """ The string name of the table """

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

    options = None
    """
    A dict mapping table option names to their specific settings for this
    table.
    """

    recognized_options = (
            "comment", "read_repair_chance",  # "local_read_repair_chance",
            "replicate_on_write", "gc_grace_seconds", "bloom_filter_fp_chance",
            "caching", "compaction_strategy_class", "compaction_strategy_options",
            "min_compaction_threshold", "max_compression_threshold",
            "compression_parameters")

    def __init__(self, keyspace_metadata, name, partition_key=None, clustering_key=None, columns=None, options=None):
        self.keyspace = keyspace_metadata
        self.name = name
        self.partition_key = [] if partition_key is None else partition_key
        self.clustering_key = [] if clustering_key is None else clustering_key
        self.columns = OrderedDict() if columns is None else columns
        self.options = options
        self.comparator = None

    def export_as_string(self):
        """
        Returns a string of CQL queries that can be used to recreate this table
        along with all indexes on it.  The returned string is formatted to
        be human readable.
        """
        ret = self.as_cql_query(formatted=True)
        ret += ";"

        for col_meta in self.columns.values():
            if col_meta.index:
                ret += "\n%s;" % (col_meta.index.as_cql_query(),)

        return ret

    def as_cql_query(self, formatted=False):
        """
        Returns a CQL query that can be used to recreate this table (index
        creations are not included).  If `formatted` is set to :const:`True`,
        extra whitespace will be added to make the query human readable.
        """
        ret = "CREATE TABLE %s.%s (%s" % (self.keyspace.name, self.name, "\n" if formatted else "")

        if formatted:
            column_join = ",\n"
            padding = "    "
        else:
            column_join = ", "
            padding = ""

        columns = []
        for col in self.columns.values():
            columns.append("%s %s" % (col.name, col.typestring))

        if len(self.partition_key) == 1 and not self.clustering_key:
            columns[0] += " PRIMARY KEY"

        ret += column_join.join("%s%s" % (padding, col) for col in columns)

        # primary key
        if len(self.partition_key) > 1 or self.clustering_key:
            ret += "%s%sPRIMARY KEY (" % (column_join, padding)

            if len(self.partition_key) > 1:
                ret += "(%s)" % ", ".join(col.name for col in self.partition_key)
            else:
                ret += self.partition_key[0].name

            if self.clustering_key:
                ret += ", %s" % ", ".join(col.name for col in self.clustering_key)

            ret += ")"

        # options
        ret += "%s) WITH " % ("\n" if formatted else "")

        option_strings = []
        if self.options.get("is_compact_storage"):
            option_strings.append("COMPACT STORAGE")

        if self.clustering_key:
            cluster_str = "CLUSTERING ORDER BY "

            clustering_names = self.protect_names([c.name for c in self.clustering_key])

            if self.options.get("is_compact_storage") and \
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

        option_strings.extend(map(self._make_option_str, self.recognized_options))
        option_strings = filter(lambda x: x is not None, option_strings)

        join_str = "\n    AND " if formatted else " AND "
        ret += join_str.join(option_strings)

        return ret

    def _make_option_str(self, name):
        value = self.options.get(name)
        if value is not None:
            if name == "comment":
                value = value or ""
            return "%s = %s" % (name, self.protect_value(value))

    def protect_name(self, name):
        if isinstance(name, unicode):
            name = name.encode('utf8')
        return self.maybe_escape_name(name)

    def protect_names(self, names):
        return map(self.protect_name, names)

    def protect_value(self, value):
        if value is None:
            return 'NULL'
        if isinstance(value, (int, float, bool)):
            return str(value)
        return "'%s'" % value.replace("'", "''")

    valid_cql3_word_re = re.compile(r'^[a-z][0-9a-z_]*$')

    def is_valid_name(self, name):
        if name is None:
            return False
        if name.lower() in _keywords - _unreserved_keywords:
            return False
        return self.valid_cql3_word_re.match(name) is not None

    def maybe_escape_name(self, name):
        if self.is_valid_name(name):
            return name
        return self.escape_name(name)

    def escape_name(self, name):
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

    index = None
    """
    If an index exists on this column, this is an instance of
    :class:`.IndexMetadata`, otherwise :const:`None`.
    """

    def __init__(self, table_metadata, column_name, data_type, index_metadata=None):
        self.table = table_metadata
        self.name = column_name
        self.data_type = data_type
        self.index = index_metadata

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

    def __init__(self, column_metadata, index_name=None, index_type=None):
        self.column = column_metadata
        self.name = index_name
        self.index_type = index_type

    def as_cql_query(self):
        """
        Returns a CQL query that can be used to recreate this index.
        """
        table = self.column.table
        return "CREATE INDEX %s ON %s.%s (%s)" % (self.name, table.keyspace.name, table.name, self.column.name)


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

    def __init__(self, token_class, token_to_host_owner, all_tokens, keyspaces):
        self.token_class = token_class
        self.ring = all_tokens
        self.token_to_host_owner = token_to_host_owner

        self.tokens_to_hosts_by_ks = {}
        self.rebuild(keyspaces)

    def rebuild(self, current_keyspaces):
        """
        Given an up-to-date list of :class:`.KeyspaceMetadata` instances, rebuild
        the per-keyspace replication map.
        """
        tokens_to_hosts_by_ks = {}
        for ks_metadata in current_keyspaces:
            strategy = ks_metadata.replication_strategy
            if strategy is None:
                token_to_hosts = defaultdict(set)
                for token, host in self.token_to_host_owner.items():
                    token_to_hosts[token].add(host)
                tokens_to_hosts_by_ks[ks_metadata.name] = token_to_hosts
            else:
                tokens_to_hosts_by_ks[ks_metadata.name] = \
                        strategy.make_token_replica_map(
                                self.token_to_host_owner, self.ring)

        self.tokens_to_hosts_by_ks = tokens_to_hosts_by_ks

    def get_replicas(self, keyspace, token):
        """
        Get  a set of :class:`.Host` instances representing all of the
        replica nodes for a given :class:`.Token`.
        """
        tokens_to_hosts = self.tokens_to_hosts_by_ks.get(keyspace, None)
        if tokens_to_hosts is None:
            return set()

        point = bisect_left(self.ring, token)
        if point == 0 and token != self.ring[0]:
            return tokens_to_hosts[self.ring[-1]]
        elif point == len(self.ring):
            return tokens_to_hosts[self.ring[0]]
        else:
            return tokens_to_hosts[self.ring[point]]


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

    def __hash__(self):
        return self.value

    def __repr__(self):
        return "<%s: %r>" % (self.__class__.__name__, self.value)
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
            h = murmur3(key)
            return h if h != MIN_LONG else MAX_LONG
        else:
            raise NoMurmur3()

    def __init__(self, token):
        """ `token` should be an int or string representing the token """
        self.value = int(token)


class MD5Token(Token):
    """
    A token for ``RandomPartitioner``.
    """

    @classmethod
    def hash_fn(cls, key):
        return abs(varint_unpack(md5(key).digest()))

    def __init__(self, token):
        """ `token` should be an int or string representing the token """
        self.value = int(token)


class BytesToken(Token):
    """
    A token for ``ByteOrderedPartitioner``.
    """

    def __init__(self, token_string):
        """ `token_string` should be string representing the token """
        if not isinstance(token_string, basestring):
            raise TypeError(
                "Tokens for ByteOrderedPartitioner should be strings (got %s)"
                % (type(token_string),))
        self.value = token_string
