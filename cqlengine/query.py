from collections import namedtuple
import copy
from datetime import datetime
from hashlib import md5
from time import time
from uuid import uuid1
from cqlengine import BaseContainerColumn, BaseValueManager, Map

from cqlengine.connection import connection_manager
from cqlengine.exceptions import CQLEngineException
from cqlengine.functions import BaseQueryFunction

#CQL 3 reference:
#http://www.datastax.com/docs/1.1/references/cql/index

class QueryException(CQLEngineException): pass
class QueryOperatorException(QueryException): pass

class QueryOperator(object):
    # The symbol that identifies this operator in filter kwargs
    # ie: colname__<symbol>
    symbol = None

    # The comparator symbol this operator uses in cql
    cql_symbol = None

    def __init__(self, column, value):
        self.column = column
        self.value = value

        #the identifier is a unique key that will be used in string
        #replacement on query strings, it's created from a hash
        #of this object's id and the time
        self.identifier = uuid1().hex

        #perform validation on this operator
        self.validate_operator()
        self.validate_value()

    @property
    def cql(self):
        """
        Returns this operator's portion of the WHERE clause
        :param valname: the dict key that this operator's compare value will be found in
        """
        if isinstance(self.value, BaseQueryFunction):
            return '"{}" {} {}'.format(self.column.db_field_name, self.cql_symbol, self.value.to_cql(self.identifier))
        else:
            return '"{}" {} :{}'.format(self.column.db_field_name, self.cql_symbol, self.identifier)

    def validate_operator(self):
        """
        Checks that this operator can be used on the column provided
        """
        if self.symbol is None:
            raise QueryOperatorException(
                    "{} is not a valid operator, use one with 'symbol' defined".format(
                        self.__class__.__name__
                    )
                )
        if self.cql_symbol is None:
            raise QueryOperatorException(
                    "{} is not a valid operator, use one with 'cql_symbol' defined".format(
                        self.__class__.__name__
                    )
                )

    def validate_value(self):
        """
        Checks that the compare value works with this operator

        Doesn't do anything by default
        """
        pass

    def get_dict(self):
        """
        Returns this operators contribution to the cql.query arg dictionanry

        ie: if this column's name is colname, and the identifier is colval,
        this should return the dict: {'colval':<self.value>}
        SELECT * FROM column_family WHERE colname=:colval
        """
        if isinstance(self.value, BaseQueryFunction):
            return {self.identifier: self.column.to_database(self.value.get_value())}
        else:
            return {self.identifier: self.column.to_database(self.value)}

    @classmethod
    def get_operator(cls, symbol):
        if not hasattr(cls, 'opmap'):
            QueryOperator.opmap = {}
            def _recurse(klass):
                if klass.symbol:
                    QueryOperator.opmap[klass.symbol.upper()] = klass
                for subklass in klass.__subclasses__():
                    _recurse(subklass)
                pass
            _recurse(QueryOperator)
        try:
            return QueryOperator.opmap[symbol.upper()]
        except KeyError:
            raise QueryOperatorException("{} doesn't map to a QueryOperator".format(symbol))

class EqualsOperator(QueryOperator):
    symbol = 'EQ'
    cql_symbol = '='

class InOperator(EqualsOperator):
    symbol = 'IN'
    cql_symbol = 'IN'

class GreaterThanOperator(QueryOperator):
    symbol = "GT"
    cql_symbol = '>'

class GreaterThanOrEqualOperator(QueryOperator):
    symbol = "GTE"
    cql_symbol = '>='

class LessThanOperator(QueryOperator):
    symbol = "LT"
    cql_symbol = '<'

class LessThanOrEqualOperator(QueryOperator):
    symbol = "LTE"
    cql_symbol = '<='

class Consistency(object):
    ANY = 'ANY'
    ONE = 'ONE'
    QUORUM = 'QUORUM'
    LOCAL_QUORUM = 'LOCAL_QUORUM'
    EACH_QUORUM = 'EACH_QUORUM'
    ALL = 'ALL'

class BatchQuery(object):
    """
    Handles the batching of queries
    """

    def __init__(self, consistency=None, timestamp=None):
        self.queries = []
        self.consistency = consistency
        if timestamp is not None and not isinstance(timestamp, datetime):
            raise CQLEngineException('timestamp object must be an instance of datetime')
        self.timestamp = timestamp

    def add_query(self, query, params):
        self.queries.append((query, params))

    def execute(self):
        opener = 'BEGIN BATCH'
        if self.consistency:
            opener += ' USING CONSISTENCY {}'.format(self.consistency)
        if self.timestamp:
            epoch = datetime(1970, 1, 1)
            ts = long((self.timestamp - epoch).total_seconds() * 1000)
            opener += ' TIMESTAMP {}'.format(ts)

        query_list = [opener]
        parameters = {}
        for query, params in self.queries:
            query_list.append('  ' + query)
            parameters.update(params)

        query_list.append('APPLY BATCH;')

        with connection_manager() as con:
            con.execute('\n'.join(query_list), parameters)

        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        #don't execute if there was an exception
        if exc_type is not None: return
        self.execute()

class QuerySet(object):

    def __init__(self, model):
        super(QuerySet, self).__init__()
        self.model = model
        self.column_family_name = self.model.column_family_name()

        #Where clause filters
        self._where = []

        #ordering arguments
        self._order = None

        self._allow_filtering = False

        #CQL has a default limit of 10000, it's defined here
        #because explicit is better than implicit
        self._limit = 10000

        #see the defer and only methods
        self._defer_fields = []
        self._only_fields = []

        #results cache
        self._con = None
        self._cur = None
        self._result_cache = None
        self._result_idx = None

        self._batch = None

    def __unicode__(self):
        return self._select_query()

    def __str__(self):
        return str(self.__unicode__())

    def __call__(self, **kwargs):
        return self.filter(**kwargs)

    def __deepcopy__(self, memo):
        clone = self.__class__(self.model)
        for k,v in self.__dict__.items():
            if k in ['_con', '_cur', '_result_cache', '_result_idx']:
                clone.__dict__[k] = None
            else:
                clone.__dict__[k] = copy.deepcopy(v, memo)

        return clone

    def __len__(self):
        return self.count()
    
    def __del__(self):
        if self._con:
            self._con.close()
            self._con = None
            self._cur = None

    #----query generation / execution----

    def _validate_where_syntax(self):
        """ Checks that a filterset will not create invalid cql """

        #check that there's either a = or IN relationship with a primary key or indexed field
        equal_ops = [w for w in self._where if isinstance(w, EqualsOperator)]
        if not any([w.column.primary_key or w.column.index for w in equal_ops]):
            raise QueryException('Where clauses require either a "=" or "IN" comparison with either a primary key or indexed field')

        if not self._allow_filtering:
            #if the query is not on an indexed field
            if not any([w.column.index for w in equal_ops]):
                if not any([w.column._partition_key for w in equal_ops]):
                    raise QueryException('Filtering on a clustering key without a partition key is not allowed unless allow_filtering() is called on the querset')


        #TODO: abuse this to see if we can get cql to raise an exception

    def _where_clause(self):
        """ Returns a where clause based on the given filter args """
        self._validate_where_syntax()
        return ' AND '.join([f.cql for f in self._where])

    def _where_values(self):
        """ Returns the value dict to be passed to the cql query """
        values = {}
        for where in self._where:
            values.update(where.get_dict())
        return values

    def _select_query(self):
        """
        Returns a select clause based on the given filter args
        """
        fields = self.model._columns.keys()
        if self._defer_fields:
            fields = [f for f in fields if f not in self._defer_fields]
        elif self._only_fields:
            fields = [f for f in fields if f in self._only_fields]
        db_fields = [self.model._columns[f].db_field_name for f in fields]

        qs = ['SELECT {}'.format(', '.join(['"{}"'.format(f) for f in db_fields]))]
        qs += ['FROM {}'.format(self.column_family_name)]

        if self._where:
            qs += ['WHERE {}'.format(self._where_clause())]

        if self._order:
            qs += ['ORDER BY {}'.format(self._order)]

        if self._limit:
            qs += ['LIMIT {}'.format(self._limit)]

        if self._allow_filtering:
            qs += ['ALLOW FILTERING']

        return ' '.join(qs)

    #----Reads------

    def _execute_query(self):
        if self._batch:
            raise CQLEngineException("Only inserts, updates, and deletes are available in batch mode")
        if self._result_cache is None:
            self._con = connection_manager()
            self._cur = self._con.execute(self._select_query(), self._where_values())
            self._result_cache = [None]*self._cur.rowcount

    def _fill_result_cache_to_idx(self, idx):
        self._execute_query()
        if self._result_idx is None:
            self._result_idx = -1

        qty = idx - self._result_idx
        if qty < 1:
            return
        else:
            names = [i[0] for i in self._cur.description]
            for values in self._cur.fetchmany(qty):
                value_dict = dict(zip(names, values))
                self._result_idx += 1
                self._result_cache[self._result_idx] = self._construct_instance(value_dict)
                
            #return the connection to the connection pool if we have all objects
            if self._result_cache and self._result_cache[-1] is not None:
                self._con.close()
                self._con = None
                self._cur = None

    def __iter__(self):
        self._execute_query()

        for idx in range(len(self._result_cache)):
            instance = self._result_cache[idx]
            if instance is None:
                self._fill_result_cache_to_idx(idx)
            yield self._result_cache[idx]

    def __getitem__(self, s):
        self._execute_query()

        num_results = len(self._result_cache)

        if isinstance(s, slice):
            #calculate the amount of results that need to be loaded
            end = num_results if s.step is None else s.step
            if end < 0:
                end += num_results
            else:
                end -= 1
            self._fill_result_cache_to_idx(end)
            return self._result_cache[s.start:s.stop:s.step]
        else:
            #return the object at this index
            s = long(s)

            #handle negative indexing
            if s < 0: s += num_results

            if s >= num_results:
                raise IndexError
            else:
                self._fill_result_cache_to_idx(s)
                return self._result_cache[s]
            

    def _construct_instance(self, values):
        #translate column names to model names
        field_dict = {}
        db_map = self.model._db_map
        for key, val in values.items():
            if key in db_map:
                field_dict[db_map[key]] = val
            else:
                field_dict[key] = val
        instance = self.model(**field_dict)
        instance._is_persisted = True
        return instance

    def batch(self, batch_obj):
        """
        Adds a batch query to the mix
        :param batch_obj:
        :return:
        """
        if not isinstance(batch_obj, BatchQuery):
            raise CQLEngineException('batch_obj must be a BatchQuery instance')
        clone = copy.deepcopy(self)
        clone._batch = batch_obj
        return clone

    def first(self):
        try:
            return iter(self).next()
        except StopIteration:
            return None

    def all(self):
        clone = copy.deepcopy(self)
        clone._where = []
        return clone

    def _parse_filter_arg(self, arg):
        """
        Parses a filter arg in the format:
        <colname>__<op>
        :returns: colname, op tuple
        """
        statement = arg.split('__')
        if len(statement) == 1:
            return arg, None
        elif len(statement) == 2:
            return statement[0], statement[1]
        else:
            raise QueryException("Can't parse '{}'".format(arg))

    def filter(self, **kwargs):
        #add arguments to the where clause filters
        clone = copy.deepcopy(self)
        for arg, val in kwargs.items():
            col_name, col_op = self._parse_filter_arg(arg)
            #resolve column and operator
            try:
                column = self.model._columns[col_name]
            except KeyError:
                raise QueryException("Can't resolve column name: '{}'".format(col_name))

            #get query operator, or use equals if not supplied
            operator_class = QueryOperator.get_operator(col_op or 'EQ')
            operator = operator_class(column, val)

            clone._where.append(operator)

        return clone

    def get(self, **kwargs):
        """
        Returns a single instance matching this query, optionally with additional filter kwargs.

        A DoesNotExistError will be raised if there are no rows matching the query
        A MultipleObjectsFoundError will be raised if there is more than one row matching the queyr
        """
        if kwargs: return self.filter(**kwargs).get()
        self._execute_query()
        if len(self._result_cache) == 0:
            raise self.model.DoesNotExist
        elif len(self._result_cache) > 1:
            raise self.model.MultipleObjectsReturned(
                    '{} objects found'.format(len(self._result_cache)))
        else:
            return self[0]
        
    def order_by(self, colname):
        """
        orders the result set.
        ordering can only select one column, and it must be the second column in a composite primary key

        Default order is ascending, prepend a '-' to the column name for descending
        """
        if colname is None:
            clone = copy.deepcopy(self)
            clone._order = None
            return clone

        order_type = 'DESC' if colname.startswith('-') else 'ASC'
        colname = colname.replace('-', '')

        column = self.model._columns.get(colname)
        if column is None:
            raise QueryException("Can't resolve the column name: '{}'".format(colname))

        #validate the column selection
        if not column.primary_key:
            raise QueryException(
                "Can't order on '{}', can only order on (clustered) primary keys".format(colname))

        pks = [v for k,v in self.model._columns.items() if v.primary_key]
        if column == pks[0]:
            raise QueryException(
                "Can't order by the first primary key, clustering (secondary) keys only")

        clone = copy.deepcopy(self)
        clone._order = '"{}" {}'.format(column.db_field_name, order_type)
        return clone

    def count(self):
        """ Returns the number of rows matched by this query """
        if self._batch:
            raise CQLEngineException("Only inserts, updates, and deletes are available in batch mode")
        #TODO: check for previous query execution and return row count if it exists
        if self._result_cache is None:
            qs = ['SELECT COUNT(*)']
            qs += ['FROM {}'.format(self.column_family_name)]
            if self._where:
                qs += ['WHERE {}'.format(self._where_clause())]
            if self._allow_filtering:
                qs += ['ALLOW FILTERING']

            qs = ' '.join(qs)

            with connection_manager() as con:
                cur = con.execute(qs, self._where_values())
                return cur.fetchone()[0]
        else:
            return len(self._result_cache)

    def limit(self, v):
        """
        Sets the limit on the number of results returned
        CQL has a default limit of 10,000
        """
        if not (v is None or isinstance(v, (int, long))):
            raise TypeError
        if v == self._limit:
            return self

        if v < 0:
            raise QueryException("Negative limit is not allowed")

        clone = copy.deepcopy(self)
        clone._limit = v
        return clone

    def allow_filtering(self):
        """
        Enables the unwise practive of querying on a clustering
        key without also defining a partition key
        """
        clone = copy.deepcopy(self)
        clone._allow_filtering = True
        return clone

    def _only_or_defer(self, action, fields):
        clone = copy.deepcopy(self)
        if clone._defer_fields or clone._only_fields:
            raise QueryException("QuerySet alread has only or defer fields defined")

        #check for strange fields
        missing_fields = [f for f in fields if f not in self.model._columns.keys()]
        if missing_fields:
            raise QueryException(
                "Can't resolve fields {} in {}".format(
                    ', '.join(missing_fields), self.model.__name__))

        if action == 'defer':
            clone._defer_fields = fields
        elif action == 'only':
            clone._only_fields = fields
        else:
            raise ValueError

        return clone

    def only(self, fields):
        """ Load only these fields for the returned query """
        return self._only_or_defer('only', fields)

    def defer(self, fields):
        """ Don't load these fields for the returned query """
        return self._only_or_defer('defer', fields)

    def create(self, **kwargs):
        return self.model(**kwargs).batch(self._batch).save()

    #----delete---
    def delete(self, columns=[]):
        """
        Deletes the contents of a query
        """
        #validate where clause
        partition_key = self.model._primary_keys.values()[0]
        if not any([c.column == partition_key for c in self._where]):
            raise QueryException("The partition key must be defined on delete queries")
        qs = ['DELETE FROM {}'.format(self.column_family_name)]
        qs += ['WHERE {}'.format(self._where_clause())]
        qs = ' '.join(qs)

        if self._batch:
            self._batch.add_query(qs, self._where_values())
        else:
            with connection_manager() as con:
                con.execute(qs, self._where_values())

class DMLQuery(object):
    """
    A query object used for queries performing inserts, updates, or deletes

    this is usually instantiated by the model instance to be modified

    unlike the read query object, this is mutable
    """

    def __init__(self, model, instance=None, batch=None):
        self.model = model
        self.column_family_name = self.model.column_family_name()
        self.instance = instance
        self.batch = batch
        pass

    def batch(self, batch_obj):
        if not isinstance(batch_obj, BatchQuery):
            raise CQLEngineException('batch_obj must be a BatchQuery instance')
        self.batch = batch_obj
        return self

    def save(self):
        """
        Creates / updates a row.
        This is a blind insert call.
        All validation and cleaning needs to happen
        prior to calling this.
        """
        if self.instance is None:
            raise CQLEngineException("DML Query intance attribute is None")
        assert type(self.instance) == self.model

        #organize data
        value_pairs = []
        values = self.instance.as_dict()

        #get defined fields and their column names
        for name, col in self.model._columns.items():
            val = values.get(name)
            if val is None: continue
            value_pairs += [(col.db_field_name, val)]

        #construct query string
        field_names = zip(*value_pairs)[0]
        field_ids = {n:uuid1().hex for n in field_names}
        field_values = dict(value_pairs)
        query_values = {field_ids[n]:field_values[n] for n in field_names}

        qs = []
        if self.instance._can_update():
            qs += ["UPDATE {}".format(self.column_family_name)]
            qs += ["SET"]

            set_statements = []
            #get defined fields and their column names
            for name, col in self.model._columns.items():
                if not col.is_primary_key:
                    val = values.get(name)
                    if val is None: continue
                    if isinstance(col, BaseContainerColumn):
                        #remove value from query values, the column will handle it
                        query_values.pop(field_ids.get(name), None)

                        val_mgr = self.instance._values[name]
                        set_statements += col.get_update_statement(val, val_mgr.previous_value, query_values)
                        pass
                    else:
                        set_statements += ['"{}" = :{}'.format(col.db_field_name, field_ids[col.db_field_name])]
            qs += [', '.join(set_statements)]

            qs += ['WHERE']

            where_statements = []
            for name, col in self.model._primary_keys.items():
                where_statements += ['"{}" = :{}'.format(col.db_field_name, field_ids[col.db_field_name])]

            qs += [' AND '.join(where_statements)]

            # clear the qs if there are not set statements
            if not set_statements: qs = []

        else:
            qs += ["INSERT INTO {}".format(self.column_family_name)]
            qs += ["({})".format(', '.join(['"{}"'.format(f) for f in field_names]))]
            qs += ['VALUES']
            qs += ["({})".format(', '.join([':'+field_ids[f] for f in field_names]))]

        qs = ' '.join(qs)

        # skip query execution if it's empty
        # caused by pointless update queries
        if qs:
            if self.batch:
                self.batch.add_query(qs, query_values)
            else:
                with connection_manager() as con:
                    con.execute(qs, query_values)


        # delete nulled columns and removed map keys
        qs = ['DELETE']
        query_values = {}

        del_statements = []
        for k,v in self.instance._values.items():
            col = v.column
            if v.deleted:
                del_statements += ['"{}"'.format(col.db_field_name)]
            elif isinstance(col, Map):
                del_statements += col.get_delete_statement(v.value, v.previous_value, query_values)

        if del_statements:
            qs += [', '.join(del_statements)]

            qs += ['FROM {}'.format(self.column_family_name)]

            qs += ['WHERE']
            where_statements = []
            for name, col in self.model._primary_keys.items():
                field_id = uuid1().hex
                query_values[field_id] = field_values[name]
                where_statements += ['"{}" = :{}'.format(col.db_field_name, field_id)]
            qs += [' AND '.join(where_statements)]

            qs = ' '.join(qs)

            if self.batch:
                self.batch.add_query(qs, query_values)
            else:
                with connection_manager() as con:
                    con.execute(qs, query_values)

    def delete(self):
        """ Deletes one instance """
        if self.instance is None:
            raise CQLEngineException("DML Query intance attribute is None")
        field_values = {}
        qs = ['DELETE FROM {}'.format(self.column_family_name)]
        qs += ['WHERE']
        where_statements = []
        for name, col in self.model._primary_keys.items():
            field_id = uuid1().hex
            field_values[field_id] = col.to_database(getattr(self.instance, name))
            where_statements += ['"{}" = :{}'.format(col.db_field_name, field_id)]

        qs += [' AND '.join(where_statements)]
        qs = ' '.join(qs)

        if self.batch:
            self.batch.add_query(qs, field_values)
        else:
            with connection_manager() as con:
                con.execute(qs, field_values)


