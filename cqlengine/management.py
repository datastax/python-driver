import json
import warnings
import six
from cqlengine import SizeTieredCompactionStrategy, LeveledCompactionStrategy
from cqlengine import ONE
from cqlengine.named import NamedTable

from cqlengine.connection import execute, get_cluster
from cqlengine.exceptions import CQLEngineException

import logging
from collections import namedtuple
Field = namedtuple('Field', ['name', 'type'])

logger = logging.getLogger(__name__)
from cqlengine.models import Model

# system keyspaces
schema_columnfamilies = NamedTable('system', 'schema_columnfamilies')

def create_keyspace(name, strategy_class='SimpleStrategy', replication_factor=3, durable_writes=True, **replication_values):
    """
    creates a keyspace

    :param name: name of keyspace to create
    :param strategy_class: keyspace replication strategy class
    :param replication_factor: keyspace replication factor
    :param durable_writes: 1.2 only, write log is bypassed if set to False
    :param **replication_values: 1.2 only, additional values to ad to the replication data map
    """
    cluster = get_cluster()

    if name not in cluster.metadata.keyspaces:
        #try the 1.2 method
        replication_map = {
            'class': strategy_class,
            'replication_factor':replication_factor
        }
        replication_map.update(replication_values)
        if strategy_class.lower() != 'simplestrategy':
            # Although the Cassandra documentation states for `replication_factor`
            # that it is "Required if class is SimpleStrategy; otherwise,
            # not used." we get an error if it is present.
            replication_map.pop('replication_factor', None)

        query = """
        CREATE KEYSPACE {}
        WITH REPLICATION = {}
        """.format(name, json.dumps(replication_map).replace('"', "'"))

        if strategy_class != 'SimpleStrategy':
            query += " AND DURABLE_WRITES = {}".format('true' if durable_writes else 'false')

        execute(query)


def delete_keyspace(name):
    cluster = get_cluster()
    if name in cluster.metadata.keyspaces:
        execute("DROP KEYSPACE {}".format(name))

def create_table(model, create_missing_keyspace=True):
    raise CQLEngineException("create_table is deprecated, please use sync_table")

def sync_table(model, create_missing_keyspace=True):
    """
    Inspects the model and creates / updates the corresponding table and columns.

    Note that the attributes removed from the model are not deleted on the database.
    They become effectively ignored by (will not show up on) the model.

    :param create_missing_keyspace: (Defaults to True) Flags to us that we need to create missing keyspace
        mentioned in the model automatically.
    :type create_missing_keyspace: bool
    """

    if not issubclass(model, Model):
        raise CQLEngineException("Models must be derived from base Model.")

    if model.__abstract__:
        raise CQLEngineException("cannot create table from abstract model")


    #construct query string
    cf_name = model.column_family_name()
    raw_cf_name = model.column_family_name(include_keyspace=False)

    ks_name = model._get_keyspace()
    #create missing keyspace
    if create_missing_keyspace:
        create_keyspace(ks_name)

    cluster = get_cluster()

    keyspace = cluster.metadata.keyspaces[ks_name]
    tables = keyspace.tables

    #check for an existing column family
    if raw_cf_name not in tables:
        qs = get_create_table(model)

        try:
            execute(qs)
        except CQLEngineException as ex:
            # 1.2 doesn't return cf names, so we have to examine the exception
            # and ignore if it says the column family already exists
            if "Cannot add already existing column family" not in unicode(ex):
                raise
    else:
        # see if we're missing any columns
        fields = get_fields(model)
        field_names = [x.name for x in fields]
        for name, col in model._columns.items():
            if col.primary_key or col.partition_key: continue # we can't mess with the PK
            if col.db_field_name in field_names: continue # skip columns already defined

            # add missing column using the column def
            query = "ALTER TABLE {} add {}".format(cf_name, col.get_column_def())
            logger.debug(query)
            execute(query)

        update_compaction(model)


    table = cluster.metadata.keyspaces[ks_name].tables[raw_cf_name]

    indexes = [c for n,c in model._columns.items() if c.index]

    for column in indexes:
        if table.columns[column.db_field_name].index:
            continue

        qs = ['CREATE INDEX index_{}_{}'.format(raw_cf_name, column.db_field_name)]
        qs += ['ON {}'.format(cf_name)]
        qs += ['("{}")'.format(column.db_field_name)]
        qs = ' '.join(qs)
        execute(qs)

def get_create_table(model):
    cf_name = model.column_family_name()
    qs = ['CREATE TABLE {}'.format(cf_name)]

    #add column types
    pkeys = [] # primary keys
    ckeys = [] # clustering keys
    qtypes = [] # field types
    def add_column(col):
        s = col.get_column_def()
        if col.primary_key:
            keys = (pkeys if col.partition_key else ckeys)
            keys.append('"{}"'.format(col.db_field_name))
        qtypes.append(s)
    for name, col in model._columns.items():
        add_column(col)

    qtypes.append('PRIMARY KEY (({}){})'.format(', '.join(pkeys), ckeys and ', ' + ', '.join(ckeys) or ''))

    qs += ['({})'.format(', '.join(qtypes))]

    with_qs = []

    table_properties = ['bloom_filter_fp_chance', 'caching', 'comment',
        'dclocal_read_repair_chance', 'default_time_to_live', 'gc_grace_seconds',
        'index_interval', 'memtable_flush_period_in_ms', 'populate_io_cache_on_flush',
        'read_repair_chance', 'replicate_on_write']
    for prop_name in table_properties:
        prop_value = getattr(model, '__{}__'.format(prop_name), None)
        if prop_value is not None:
            # Strings needs to be single quoted
            if isinstance(prop_value, six.string_types):
                prop_value = "'{}'".format(prop_value)
            with_qs.append("{} = {}".format(prop_name, prop_value))

    _order = ['"{}" {}'.format(c.db_field_name, c.clustering_order or 'ASC') for c in model._clustering_keys.values()]
    if _order:
        with_qs.append('clustering order by ({})'.format(', '.join(_order)))

    compaction_options = get_compaction_options(model)
    if compaction_options:
        compaction_options = json.dumps(compaction_options).replace('"', "'")
        with_qs.append("compaction = {}".format(compaction_options))

    # Add table properties.
    if with_qs:
        qs += ['WITH {}'.format(' AND '.join(with_qs))]

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

    result = {'class':model.__compaction__}

    def setter(key, limited_to_strategy = None):
        """
        sets key in result, checking if the key is limited to either SizeTiered or Leveled
        :param key: one of the compaction options, like "bucket_high"
        :param limited_to_strategy: SizeTieredCompactionStrategy, LeveledCompactionStrategy
        :return:
        """
        mkey = "__compaction_{}__".format(key)
        tmp = getattr(model, mkey)
        if tmp and limited_to_strategy and limited_to_strategy != model.__compaction__:
            raise CQLEngineException("{} is limited to {}".format(key, limited_to_strategy))

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
    col_family = model.column_family_name(include_keyspace=False)

    query = "select * from system.schema_columns where keyspace_name = %s and columnfamily_name = %s"
    tmp = execute(query, [ks_name, col_family])

    # Tables containing only primary keys do not appear to create
    # any entries in system.schema_columns, as only non-primary-key attributes
    # appear to be inserted into the schema_columns table

    try:
        return [Field(x['column_name'], x['validator']) for x in tmp if x['type'] == 'regular']
    except KeyError:
        return [Field(x['column_name'], x['validator']) for x in tmp]
    # convert to Field named tuples


def get_table_settings(model):
    # returns the table as provided by the native driver for a given model
    cluster = get_cluster()
    ks = model._get_keyspace()
    table = model.column_family_name(include_keyspace=False)
    table = cluster.metadata.keyspaces[ks].tables[table]
    return table

def update_compaction(model):
    """Updates the compaction options for the given model if necessary.

    :param model: The model to update.

    :return: `True`, if the compaction options were modified in Cassandra,
        `False` otherwise.
    :rtype: bool
    """
    logger.debug("Checking %s for compaction differences", model)
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
        query = "ALTER TABLE {} with compaction = {}".format(cf_name, options)
        logger.debug(query)
        execute(query)
        return True

    return False


def delete_table(model):
    raise CQLEngineException("delete_table has been deprecated in favor of drop_table()")


def drop_table(model):

    # don't try to delete non existant tables
    meta = get_cluster().metadata

    ks_name = model._get_keyspace()
    raw_cf_name = model.column_family_name(include_keyspace=False)

    try:
        table = meta.keyspaces[ks_name].tables[raw_cf_name]
        execute('drop table {};'.format(model.column_family_name(include_keyspace=True)))
    except KeyError:
        pass



