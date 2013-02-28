from collections import defaultdict
import cqltypes

class Metadata(object):

    def __init__(self, cluster):
        self.cluster = cluster
        self.cluster_name = None
        self.hosts = {}
        self.keyspaces = {}

    def export_schema_as_string(self):
        return "\n".join([ks.export_as_string() for ks in self.keyspaces.values()])

    def reuild_schema(self, keyspace, table, ks_results, cf_results, col_results):
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
                ksname = keyspace_meta.name
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
        strategy_options = row["strategy_options"]
        return KeyspaceMetadata(name, durable_writes, strategy_class, strategy_options)

    def _build_table_metadata(self, keyspace_metadata, row, col_rows):
        cfname = row["columnfamily_name"]

        comparator = cqltypes.lookup_casstype(row["comparator"])
        if isinstance(comparator, cqltypes.CompositeType):
            column_name_types = comparator.subtypes
            is_composite = True
        else:
            column_name_types = (comparator,)
            is_composite = False

        num_column_name_components = len(column_name_types)
        last_col = column_name_types[-1]

        column_aliases = row["column_aliases"]
        if is_composite:
            if isinstance(last_col, cqltypes.ColumnToCollectionType):
                # collections
                is_compact = False
                has_value = False
                clustering_size = num_column_name_components - 2
            elif (len(column_aliases) == num_column_name_components - 1
                    and isinstance(last_col, cqltypes.UTF8Type)):
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
            if column_aliases or not col_rows:
                has_value = True
                clustering_size = num_column_name_components
            else:
                has_value = False
                clustering_size = 0

        table_meta = TableMetadata(keyspace_metadata, cfname)

        # partition key
        key_aliases = row["key_aliases"] or []
        key_type = cqltypes.lookup_casstype(row["key_validator"])
        key_types = key_type.subtypes if isinstance(key_type, cqltypes.CompositeType) else [key_type]
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
            if not row.get["key_aliases"]:
                value_alias = "value"
            else:
                value_alias = row["value_alias"]

            col = ColumnMetadata(table_meta, value_alias, validator)
            table_meta.columns[value_alias] = col

        if col_rows:
            for row in col_rows[cfname]:
                column_meta = self._build_column_metadata(table_meta, row)
                table_meta.columns[column_meta.name] = column_meta

        table_meta.options = self._build_table_options(is_compact)
        return table_meta

    def _build_table_options(self, row, is_compact_storage):
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
        return IndexMetadata(column_metadata, index_name, index_type)


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
            "comment", "read_repair_chance", "local_read_repair_chance",
            "replicate_on_write", "gc_grace_seconds", "bloom_filter_fp_chance",
            "caching", "compaction_strategy_class", "compaction_strategy_options",
            "min_compaction_threshold", "max_compression_threshold",
            "compression_parameters")

    def __init__(self, keyspace_metadata, name, partition_key=None, clustering_key=None, columns=None, options=None):
        self.keyspace = keyspace_metadata
        self.name = name
        self.partition_key = [] if partition_key is None else partition_key
        self.clustering_key = [] if clustering_key is None else clustering_key
        self.columns = {} if columns is None else columns
        self.options = options

    def _make_option_str(self, name):
        value = self.options.get(name)
        if value is not None:
            if name == "comment":
                value = "'%s'" % value
            return "%s = %s" % (name, value)

    def export_as_string(self):
        ret = self.as_cql_query(formatted=True)

        for col_meta in self.columns.values():
            if col_meta.index:
                ret += "\n%s" % (col_meta.index.as_cql_query(),)

    def as_cql_query(self, formatted=False):
        ret = "CREATE TABLE %s (%s" % (self.name, "\n" if formatted else "")

        if formatted:
            ret += "\n".join("    %s," % (col,) for col in self.columns.values())
        else:
            ret += ",".join(map(str, self.columns.values()))

        # primary key
        ret += "%sPRIMARY KEY (" % (" " * 4 if formatted else "")
        if len(self.partition_key) == 1:
            ret += self.partition_key[0].name
        else:
            ret += "(%s)" % ",".join(col.name for col in self.partition_key)

        ret += ", %s)\n" % ",".join(col.name for col in self.clustering_key)

        # options
        ret += ") WITH "

        option_strings = []
        if self.options.get("is_compact_storage"):
            option_strings.append("COMPACT STORAGE")

        option_strings.extend(map(self._make_option_str, self.recognized_options))

        join_str = "\n    AND " if formatted else " AND "
        ret += join_str.join(option_strings)

        return ret + ";"


class ColumnMetadata(object):

    def __init__(self, table_metadata, column_name, data_type, index_metadata=None):
        self.table = table_metadata
        self.name = column_name
        self.data_type = data_type
        self.index = index_metadata

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
