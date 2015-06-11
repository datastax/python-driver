# Copyright 2015 DataStax, Inc.
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

from collections import namedtuple
import json
import logging
import os
import six
import warnings

from cassandra import metadata
from cassandra.cqlengine import CQLEngineException, SizeTieredCompactionStrategy, LeveledCompactionStrategy
from cassandra.cqlengine import columns
from cassandra.cqlengine.connection import execute, get_cluster
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.named import NamedTable
from cassandra.cqlengine.usertype import UserType

CQLENG_ALLOW_SCHEMA_MANAGEMENT = 'CQLENG_ALLOW_SCHEMA_MANAGEMENT'

Field = namedtuple('Field', ['name', 'type'])

log = logging.getLogger(__name__)

# system keyspaces
schema_columnfamilies = NamedTable('system', 'schema_columnfamilies')


def create_keyspace(name, strategy_class, replication_factor, durable_writes=True, **replication_values):
    """
    *Deprecated - use :func:`create_keyspace_simple` or :func:`create_keyspace_network_topology` instead*

    Creates a keyspace

    If the keyspace already exists, it will not be modified.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*

    :param str name: name of keyspace to create
    :param str strategy_class: keyspace replication strategy class (:attr:`~.SimpleStrategy` or :attr:`~.NetworkTopologyStrategy`
    :param int replication_factor: keyspace replication factor, used with :attr:`~.SimpleStrategy`
    :param bool durable_writes: Write log is bypassed if set to False
    :param \*\*replication_values: Additional values to ad to the replication options map
    """
    if not _allow_schema_modification():
        return

    msg = "Deprecated. Use create_keyspace_simple or create_keyspace_network_topology instead"
    warnings.warn(msg, DeprecationWarning)
    log.warning(msg)

    cluster = get_cluster()

    if name not in cluster.metadata.keyspaces:
        # try the 1.2 method
        replication_map = {
            'class': strategy_class,
            'replication_factor': replication_factor
        }
        replication_map.update(replication_values)
        if strategy_class.lower() != 'simplestrategy':
            # Although the Cassandra documentation states for `replication_factor`
            # that it is "Required if class is SimpleStrategy; otherwise,
            # not used." we get an error if it is present.
            replication_map.pop('replication_factor', None)

        query = """
        CREATE KEYSPACE {0}
        WITH REPLICATION = {1}
        """.format(metadata.protect_name(name), json.dumps(replication_map).replace('"', "'"))

        if strategy_class != 'SimpleStrategy':
            query += " AND DURABLE_WRITES = {0}".format('true' if durable_writes else 'false')

        execute(query)


def create_keyspace_simple(name, replication_factor, durable_writes=True):
    """
    Creates a keyspace with SimpleStrategy for replica placement

    If the keyspace already exists, it will not be modified.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*

    :param str name: name of keyspace to create
    :param int replication_factor: keyspace replication factor, used with :attr:`~.SimpleStrategy`
    :param bool durable_writes: Write log is bypassed if set to False
    """
    _create_keyspace(name, durable_writes, 'SimpleStrategy',
                     {'replication_factor': replication_factor})


def create_keyspace_network_topology(name, dc_replication_map, durable_writes=True):
    """
    Creates a keyspace with NetworkTopologyStrategy for replica placement

    If the keyspace already exists, it will not be modified.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*

    :param str name: name of keyspace to create
    :param dict dc_replication_map: map of dc_names: replication_factor
    :param bool durable_writes: Write log is bypassed if set to False
    """
    _create_keyspace(name, durable_writes, 'NetworkTopologyStrategy', dc_replication_map)


def _create_keyspace(name, durable_writes, strategy_class, strategy_options):
    if not _allow_schema_modification():
        return

    cluster = get_cluster()

    if name not in cluster.metadata.keyspaces:
        log.info("Creating keyspace %s ", name)
        ks_meta = metadata.KeyspaceMetadata(name, durable_writes, strategy_class, strategy_options)
        execute(ks_meta.as_cql_query())
    else:
        log.info("Not creating keyspace %s because it already exists", name)


def delete_keyspace(name):
    msg = "Deprecated. Use drop_keyspace instead"
    warnings.warn(msg, DeprecationWarning)
    log.warning(msg)
    drop_keyspace(name)


def drop_keyspace(name):
    """
    Drops a keyspace, if it exists.

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    :param str name: name of keyspace to drop
    """
    if not _allow_schema_modification():
        return

    cluster = get_cluster()
    if name in cluster.metadata.keyspaces:
        execute("DROP KEYSPACE {0}".format(metadata.protect_name(name)))


def sync_table(model):
    """
    Inspects the model and creates / updates the corresponding table and columns.

    Any User Defined Types used in the table are implicitly synchronized.

    This function can only add fields that are not part of the primary key.

    Note that the attributes removed from the model are not deleted on the database.
    They become effectively ignored by (will not show up on) the model.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*
    """
    if not _allow_schema_modification():
        return

    if not issubclass(model, Model):
        raise CQLEngineException("Models must be derived from base Model.")

    if model.__abstract__:
        raise CQLEngineException("cannot create table from abstract model")

    cf_name = model.column_family_name()
    raw_cf_name = model._raw_column_family_name()

    ks_name = model._get_keyspace()

    cluster = get_cluster()

    keyspace = cluster.metadata.keyspaces[ks_name]
    tables = keyspace.tables

    syncd_types = set()
    for col in model._columns.values():
        udts = []
        columns.resolve_udts(col, udts)
        for udt in [u for u in udts if u not in syncd_types]:
            _sync_type(ks_name, udt, syncd_types)

    # check for an existing column family
    if raw_cf_name not in tables:
        log.debug("sync_table creating new table %s", cf_name)
        qs = get_create_table(model)

        try:
            execute(qs)
        except CQLEngineException as ex:
            # 1.2 doesn't return cf names, so we have to examine the exception
            # and ignore if it says the column family already exists
            if "Cannot add already existing column family" not in unicode(ex):
                raise
    else:
        log.debug("sync_table checking existing table %s", cf_name)
        # see if we're missing any columns
        fields = get_fields(model)
        field_names = [x.name for x in fields]
        model_fields = set()
        # # TODO: does this work with db_name??
        for name, col in model._columns.items():
            if col.primary_key or col.partition_key:
                continue  # we can't mess with the PK
            model_fields.add(name)
            if col.db_field_name in field_names:
                continue  # skip columns already defined

            # add missing column using the column def
            query = "ALTER TABLE {0} add {1}".format(cf_name, col.get_column_def())
            execute(query)

        db_fields_not_in_model = model_fields.symmetric_difference(field_names)
        if db_fields_not_in_model:
            log.info("Table %s has fields not referenced by model: %s", cf_name, db_fields_not_in_model)

        update_compaction(model)

    table = cluster.metadata.keyspaces[ks_name].tables[raw_cf_name]

    indexes = [c for n, c in model._columns.items() if c.index]

    for column in indexes:
        if table.columns[column.db_field_name].index:
            continue

        qs = ['CREATE INDEX index_{0}_{1}'.format(raw_cf_name, column.db_field_name)]
        qs += ['ON {0}'.format(cf_name)]
        qs += ['("{0}")'.format(column.db_field_name)]
        qs = ' '.join(qs)
        execute(qs)


def sync_type(ks_name, type_model):
    """
    Inspects the type_model and creates / updates the corresponding type.

    Note that the attributes removed from the type_model are not deleted on the database (this operation is not supported).
    They become effectively ignored by (will not show up on) the type_model.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*
    """
    if not _allow_schema_modification():
        return

    if not issubclass(type_model, UserType):
        raise CQLEngineException("Types must be derived from base UserType.")

    _sync_type(ks_name, type_model)


def _sync_type(ks_name, type_model, omit_subtypes=None):

    syncd_sub_types = omit_subtypes or set()
    for field in type_model._fields.values():
        udts = []
        columns.resolve_udts(field, udts)
        for udt in [u for u in udts if u not in syncd_sub_types]:
            _sync_type(ks_name, udt, syncd_sub_types)
            syncd_sub_types.add(udt)

    type_name = type_model.type_name()
    type_name_qualified = "%s.%s" % (ks_name, type_name)

    cluster = get_cluster()

    keyspace = cluster.metadata.keyspaces[ks_name]
    defined_types = keyspace.user_types

    if type_name not in defined_types:
        log.debug("sync_type creating new type %s", type_name_qualified)
        cql = get_create_type(type_model, ks_name)
        execute(cql)
        cluster.refresh_user_type_metadata(ks_name, type_name)
        type_model.register_for_keyspace(ks_name)
    else:
        defined_fields = defined_types[type_name].field_names
        model_fields = set()
        for field in type_model._fields.values():
            model_fields.add(field.db_field_name)
            if field.db_field_name not in defined_fields:
                execute("ALTER TYPE {0} ADD {1}".format(type_name_qualified, field.get_column_def()))

        type_model.register_for_keyspace(ks_name)

        if len(defined_fields) == len(model_fields):
            log.info("Type %s did not require synchronization", type_name_qualified)
            return

        db_fields_not_in_model = model_fields.symmetric_difference(defined_fields)
        if db_fields_not_in_model:
            log.info("Type %s has fields not referenced by model: %s", type_name_qualified, db_fields_not_in_model)


def get_create_type(type_model, keyspace):
    type_meta = metadata.UserType(keyspace,
                                  type_model.type_name(),
                                  (f.db_field_name for f in type_model._fields.values()),
                                  type_model._fields.values())
    return type_meta.as_cql_query()


def get_create_table(model):
    cf_name = model.column_family_name()
    qs = ['CREATE TABLE {0}'.format(cf_name)]

    # add column types
    pkeys = []  # primary keys
    ckeys = []  # clustering keys
    qtypes = []  # field types

    def add_column(col):
        s = col.get_column_def()
        if col.primary_key:
            keys = (pkeys if col.partition_key else ckeys)
            keys.append('"{0}"'.format(col.db_field_name))
        qtypes.append(s)

    for name, col in model._columns.items():
        add_column(col)

    qtypes.append('PRIMARY KEY (({0}){1})'.format(', '.join(pkeys), ckeys and ', ' + ', '.join(ckeys) or ''))

    qs += ['({0})'.format(', '.join(qtypes))]

    with_qs = []

    table_properties = ['bloom_filter_fp_chance', 'caching', 'comment',
                        'dclocal_read_repair_chance', 'default_time_to_live', 'gc_grace_seconds',
                        'index_interval', 'memtable_flush_period_in_ms', 'populate_io_cache_on_flush',
                        'read_repair_chance', 'replicate_on_write']
    for prop_name in table_properties:
        prop_value = getattr(model, '__{0}__'.format(prop_name), None)
        if prop_value is not None:
            # Strings needs to be single quoted
            if isinstance(prop_value, six.string_types):
                prop_value = "'{0}'".format(prop_value)
            with_qs.append("{0} = {1}".format(prop_name, prop_value))

    _order = ['"{0}" {1}'.format(c.db_field_name, c.clustering_order or 'ASC') for c in model._clustering_keys.values()]
    if _order:
        with_qs.append('clustering order by ({0})'.format(', '.join(_order)))

    compaction_options = get_compaction_options(model)
    if compaction_options:
        compaction_options = json.dumps(compaction_options).replace('"', "'")
        with_qs.append("compaction = {0}".format(compaction_options))

    # Add table properties.
    if with_qs:
        qs += ['WITH {0}'.format(' AND '.join(with_qs))]

    qs = ' '.join(qs)
    return qs


def get_compaction_options(model):
    """
    Generates dictionary (later converted to a string) for creating and altering
    tables with compaction strategy

    :param model:
    :return:
    """
    if not model.__compaction__:
        return {}

    result = {'class': model.__compaction__}

    def setter(key, limited_to_strategy=None):
        """
        sets key in result, checking if the key is limited to either SizeTiered or Leveled
        :param key: one of the compaction options, like "bucket_high"
        :param limited_to_strategy: SizeTieredCompactionStrategy, LeveledCompactionStrategy
        :return:
        """
        mkey = "__compaction_{0}__".format(key)
        tmp = getattr(model, mkey)
        if tmp and limited_to_strategy and limited_to_strategy != model.__compaction__:
            raise CQLEngineException("{0} is limited to {1}".format(key, limited_to_strategy))

        if tmp:
            # Explicitly cast the values to strings to be able to compare the
            # values against introspected values from Cassandra.
            result[key] = str(tmp)

    setter('tombstone_compaction_interval')
    setter('tombstone_threshold')

    setter('bucket_high', SizeTieredCompactionStrategy)
    setter('bucket_low', SizeTieredCompactionStrategy)
    setter('max_threshold', SizeTieredCompactionStrategy)
    setter('min_threshold', SizeTieredCompactionStrategy)
    setter('min_sstable_size', SizeTieredCompactionStrategy)

    setter('sstable_size_in_mb', LeveledCompactionStrategy)

    return result


def get_fields(model):
    # returns all fields that aren't part of the PK
    ks_name = model._get_keyspace()
    col_family = model._raw_column_family_name()
    field_types = ['regular', 'static']
    query = "select * from system.schema_columns where keyspace_name = %s and columnfamily_name = %s"
    tmp = execute(query, [ks_name, col_family])

    # Tables containing only primary keys do not appear to create
    # any entries in system.schema_columns, as only non-primary-key attributes
    # appear to be inserted into the schema_columns table
    try:
        return [Field(x['column_name'], x['validator']) for x in tmp if x['type'] in field_types]
    except KeyError:
        return [Field(x['column_name'], x['validator']) for x in tmp]
    # convert to Field named tuples


def get_table_settings(model):
    # returns the table as provided by the native driver for a given model
    cluster = get_cluster()
    ks = model._get_keyspace()
    table = model._raw_column_family_name()
    table = cluster.metadata.keyspaces[ks].tables[table]
    return table


def update_compaction(model):
    """Updates the compaction options for the given model if necessary.

    :param model: The model to update.

    :return: `True`, if the compaction options were modified in Cassandra,
        `False` otherwise.
    :rtype: bool
    """
    log.debug("Checking %s for compaction differences", model)
    table = get_table_settings(model)

    existing_options = table.options.copy()

    existing_compaction_strategy = existing_options['compaction_strategy_class']

    existing_options = json.loads(existing_options['compaction_strategy_options'])

    desired_options = get_compaction_options(model)

    desired_compact_strategy = desired_options.get('class', SizeTieredCompactionStrategy)

    desired_options.pop('class', None)

    do_update = False

    if desired_compact_strategy not in existing_compaction_strategy:
        do_update = True

    for k, v in desired_options.items():
        val = existing_options.pop(k, None)
        if val != v:
            do_update = True

    # check compaction_strategy_options
    if do_update:
        options = get_compaction_options(model)
        # jsonify
        options = json.dumps(options).replace('"', "'")
        cf_name = model.column_family_name()
        query = "ALTER TABLE {0} with compaction = {1}".format(cf_name, options)
        execute(query)
        return True

    return False


def drop_table(model):
    """
    Drops the table indicated by the model, if it exists.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*
    """
    if not _allow_schema_modification():
        return

    # don't try to delete non existant tables
    meta = get_cluster().metadata

    ks_name = model._get_keyspace()
    raw_cf_name = model._raw_column_family_name()

    try:
        meta.keyspaces[ks_name].tables[raw_cf_name]
        execute('DROP TABLE {0};'.format(model.column_family_name()))
    except KeyError:
        pass


def _allow_schema_modification():
    if not os.getenv(CQLENG_ALLOW_SCHEMA_MANAGEMENT):
        msg = CQLENG_ALLOW_SCHEMA_MANAGEMENT + " environment variable is not set. Future versions of this package will require this variable to enable management functions."
        warnings.warn(msg)
        log.warning(msg)

    return True
