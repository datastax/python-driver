from collections import defaultdict, OrderedDict
import json
import re
from threading import RLock

import cassandra.cqltypes as cqltypes
from cassandra.pool import Host

keywords = set((
    'select', 'from', 'where', 'and', 'key', 'insert', 'update', 'with',
    'limit', 'using', 'use', 'count', 'set',
    'begin', 'apply', 'batch', 'truncate', 'delete', 'in', 'create',
    'keyspace', 'schema', 'columnfamily', 'table', 'index', 'on', 'drop',
    'primary', 'into', 'values', 'timestamp', 'ttl', 'alter', 'add', 'type',
    'compact', 'storage', 'order', 'by', 'asc', 'desc', 'clustering',
    'token', 'writetime', 'map', 'list', 'to'
))

unreserved_keywords = set((
    'key', 'clustering', 'ttl', 'compact', 'storage', 'type', 'values'
))


class Metadata(object):

    def __init__(self, cluster):
        self.cluster = cluster
        self.cluster_name = None
        self.keyspaces = {}
        self._hosts = {}
        self._hosts_lock = RLock()

    def export_schema_as_string(self):
        return "\n".join(ks.export_as_string() for ks in self.keyspaces.values())

    def rebuild_schema(self, keyspace, table, ks_results, cf_results, col_results):
        cf_def_rows = defaultdict(list)
        col_def_rows = defaultdict(lambda: defaultdict(list))

        for row in cf_results.results:
            cf_def_rows[row["keyspace_name"]].append(row)

        for row in col_results.results:
            ksname = row["keyspace_name"]
            cfname = row["columnfamily_name"]
            col_def_rows[ksname][cfname].append(row)

        # either table or ks_results must be None
        if not table:
            # ks_results is not None
            added_keyspaces = set()
            for row in ks_results.results:
                keyspace_meta = self._build_keyspace_metadata(row)
                if ksname in cf_def_rows:
                    for table_row in cf_def_rows[keyspace_meta.name]:
                        table_meta = self._build_table_metadata(
                            keyspace_meta, table_row, col_def_rows[keyspace_meta.name])
                        keyspace_meta.tables[table_meta.name] = table_meta

                added_keyspaces.add(keyspace_meta.name)
                self.keyspaces[keyspace_meta.name] = keyspace_meta

            if not keyspace:
                # remove not-just-added keyspaces
                self.keyspaces = dict((name, meta) for name, meta in self.keyspaces.items()
                                      if name in added_keyspaces)
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
                    keyspace.tables[table_meta.name] = table_meta

    def _build_keyspace_metadata(self, row):
        name = row["keyspace_name"]
        durable_writes = row["durable_writes"]
        strategy_class = row["strategy_class"]
        strategy_options = json.loads(row["strategy_options"])
        return KeyspaceMetadata(name, durable_writes, strategy_class, strategy_options)

    def _build_table_metadata(self, keyspace_metadata, row, col_rows):
        cfname = row["columnfamily_name"]

        comparator = cqltypes.lookup_casstype(row["comparator"])
        if issubclass(comparator, cqltypes.CompositeType):
            column_name_types = comparator.subtypes
            is_composite = True
        else:
            column_name_types = (comparator,)
            is_composite = False

        num_column_name_components = len(column_name_types)
        last_col = column_name_types[-1]

        column_aliases = json.loads(row["column_aliases"])
        if is_composite:
            if issubclass(last_col, cqltypes.ColumnToCollectionType):
                # collections
                is_compact = False
                has_value = False
                clustering_size = num_column_name_components - 2
            elif (len(column_aliases) == num_column_name_components - 1
                    and issubclass(last_col, cqltypes.UTF8Type)):
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

        key_type = cqltypes.lookup_casstype(row["key_validator"])
        key_types = key_type.subtypes if issubclass(key_type, cqltypes.CompositeType) else [key_type]
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
            validator = cqltypes.lookup_casstype(row["default_validator"])
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
        data_type = cqltypes.lookup_casstype(row["validator"])
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
        # TODO
        pass

    def add_host(self, address):
        with self._hosts_lock:
            if address not in self._hosts:
                new_host = Host(address, self.cluster.conviction_policy_factory)
                self._hosts[address] = new_host
            else:
                return None

        new_host.monitor.register(self.cluster)
        return new_host

    def remove_host(self, host):
        with self._hosts_lock:
            return bool(self._hosts.pop(host.address, False))

    def get_host(self, address):
        return self._hosts.get(address)

    def all_hosts(self):
        with self._hosts_lock:
            return self._hosts.values()


class KeyspaceMetadata(object):

    def __init__(self, name, durable_writes, strategy_class, strategy_options):
        self.name = name
        self.durable_writes = durable_writes
        self.replication = strategy_options
        self.replication["class"] = strategy_class
        self.tables = {}

    def export_as_string(self):
        return "\n".join([self.as_cql_query()] + [t.as_cql_query() for t in self.tables.values()])

    def as_cql_query(self):
        ret = "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : '%s'" % \
                (self.name, self.replication["class"])
        for k, v in self.replication.items():
            if k != "class":
                ret += ", '%s': '%s'" % (k, v)
        ret += ' } AND DURABLE_WRITES = %s;' % ("true" if self.durable_writes else "false")


class TableMetadata(object):

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
        ret = self.as_cql_query(formatted=True)
        ret += ";"

        for col_meta in self.columns.values():
            if col_meta.index:
                ret += "\n%s;" % (col_meta.index.as_cql_query(),)

        return ret

    def as_cql_query(self, formatted=False):
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
                    not issubclass(self.comparator, cqltypes.CompositeType):
                subtypes = [self.comparator]
            else:
                subtypes = self.comparator.subtypes

            inner = []
            for colname, coltype in zip(clustering_names, subtypes):
                ordering = "DESC" if issubclass(coltype, cqltypes.ReversedType) else "ASC"
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
            return 'NULL' # this totally won't work
        if isinstance(value, bool):
            value = str(value).lower()
        elif isinstance(value, float):
            return '%f' % value
        elif isinstance(value, int):
            return str(value)
        return "'%s'" % value.replace("'", "''")

    valid_cql3_word_re = re.compile(r'^[a-z][0-9a-z_]*$')

    def is_valid_name(self, name):
        if name is None:
            return False
        if name.lower() in keywords - unreserved_keywords:
            return False
        return self.valid_cql3_word_re.match(name) is not None

    def maybe_escape_name(self, name):
        if self.is_valid_name(name):
            return name
        return self.escape_name(name)

    def escape_name(self, name):
        return '"%s"' % (name.replace('"', '""'),)


class ColumnMetadata(object):

    def __init__(self, table_metadata, column_name, data_type, index_metadata=None):
        self.table = table_metadata
        self.name = column_name
        self.data_type = data_type
        self.index = index_metadata

    @property
    def typestring(self):
        if issubclass(self.data_type, cqltypes.ReversedType):
            return self.data_type.subtypes[0].cql_parameterized_type()
        else:
            return self.data_type.cql_parameterized_type()

    def __str__(self):
        return "%s %s" % (self.name, self.data_type)


class IndexMetadata(object):

    def __init__(self, column_metadata, index_name=None, index_type=None):
        self.column = column_metadata
        self.name = index_name
        self.index_type = index_type

    def as_cql_query(self):
        table = self.column.table
        return "CREATE INDEX %s ON %s.%s (%s)" % (self.name, table.keyspace.name, table.name, self.column.name)
