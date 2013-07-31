import json

from cqlengine.connection import connection_manager, execute
from cqlengine.exceptions import CQLEngineException

import logging
from collections import namedtuple
Field = namedtuple('Field', ['name', 'type'])

logger = logging.getLogger(__name__)


def create_keyspace(name, strategy_class='SimpleStrategy', replication_factor=3, durable_writes=True, **replication_values):
    """
    creates a keyspace

    :param name: name of keyspace to create
    :param strategy_class: keyspace replication strategy class
    :param replication_factor: keyspace replication factor
    :param durable_writes: 1.2 only, write log is bypassed if set to False
    :param **replication_values: 1.2 only, additional values to ad to the replication data map
    """
    with connection_manager() as con:
        _, keyspaces = con.execute("""SELECT keyspace_name FROM system.schema_keyspaces""", {})
        if name not in [r[0] for r in keyspaces]:
            #try the 1.2 method
            replication_map = {
                'class': strategy_class,
                'replication_factor':replication_factor
            }
            replication_map.update(replication_values)

            query = """
            CREATE KEYSPACE {}
            WITH REPLICATION = {}
            """.format(name, json.dumps(replication_map).replace('"', "'"))

            if strategy_class != 'SimpleStrategy':
                query += " AND DURABLE_WRITES = {}".format('true' if durable_writes else 'false')

            execute(query)


def delete_keyspace(name):
    with connection_manager() as con:
        _, keyspaces = con.execute("""SELECT keyspace_name FROM system.schema_keyspaces""", {})
        if name in [r[0] for r in keyspaces]:
            execute("DROP KEYSPACE {}".format(name))


def create_table(model, create_missing_keyspace=True):

    if model.__abstract__:
        raise CQLEngineException("cannot create table from abstract model")

    #construct query string
    cf_name = model.column_family_name()
    raw_cf_name = model.column_family_name(include_keyspace=False)

    ks_name = model._get_keyspace()
    #create missing keyspace
    if create_missing_keyspace:
        create_keyspace(ks_name)

    with connection_manager() as con:
        tables = con.execute(
            "SELECT columnfamily_name from system.schema_columnfamilies WHERE keyspace_name = :ks_name",
            {'ks_name': ks_name}
        )
    tables = [x[0] for x in tables.results]

    #check for an existing column family
    #TODO: check system tables instead of using cql thrifteries
    if raw_cf_name not in tables:
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

        with_qs = ['read_repair_chance = {}'.format(model.__read_repair_chance__)]

        _order = ['"{}" {}'.format(c.db_field_name, c.clustering_order or 'ASC') for c in model._clustering_keys.values()]
        if _order:
            with_qs.append('clustering order by ({})'.format(', '.join(_order)))

        # add read_repair_chance
        qs += ['WITH {}'.format(' AND '.join(with_qs))]
        qs = ' '.join(qs)

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


    #get existing index names, skip ones that already exist
    with connection_manager() as con:
        _, idx_names = con.execute(
            "SELECT index_name from system.\"IndexInfo\" WHERE table_name=:table_name",
            {'table_name': raw_cf_name}
        )

    idx_names = [i[0] for i in idx_names]
    idx_names = filter(None, idx_names)

    indexes = [c for n,c in model._columns.items() if c.index]
    if indexes:
        for column in indexes:
            if column.db_index_name in idx_names: continue
            qs = ['CREATE INDEX index_{}_{}'.format(raw_cf_name, column.db_field_name)]
            qs += ['ON {}'.format(cf_name)]
            qs += ['("{}")'.format(column.db_field_name)]
            qs = ' '.join(qs)

            try:
                execute(qs)
            except CQLEngineException:
                # index already exists
                pass

def get_fields(model):
    # returns all fields that aren't part of the PK
    ks_name = model._get_keyspace()
    col_family = model.column_family_name(include_keyspace=False)

    with connection_manager() as con:
        query = "SELECT column_name, validator FROM system.schema_columns \
                 WHERE keyspace_name = :ks_name AND columnfamily_name = :col_family"

        logger.debug("get_fields %s %s", ks_name, col_family)

        tmp = con.execute(query, {'ks_name':ks_name, 'col_family':col_family})
    return [Field(x[0], x[1]) for x in tmp.results]
    # convert to Field named tuples

def delete_table(model):

    # don't try to delete non existant tables
    ks_name = model._get_keyspace()
    with connection_manager() as con:
        _, tables = con.execute(
            "SELECT columnfamily_name from system.schema_columnfamilies WHERE keyspace_name = :ks_name",
            {'ks_name': ks_name}
        )
    raw_cf_name = model.column_family_name(include_keyspace=False)
    if raw_cf_name not in [t[0] for t in tables]:
        return

    cf_name = model.column_family_name()
    execute('drop table {};'.format(cf_name))

