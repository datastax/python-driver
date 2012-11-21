from collections import namedtuple
import copy

from cqlengine.connection import get_connection
from cqlengine.exceptions import QueryException

#CQL 3 reference:
#http://www.datastax.com/docs/1.1/references/cql/index

WhereFilter = namedtuple('WhereFilter', ['column', 'operator', 'value'])

class QueryOperatorException(QueryException): pass

class QueryOperator(object):
    symbol = None

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

class InOperator(QueryOperator):
    symbol = 'IN'

class GreaterThanOperator(QueryOperator):
    symbol = "GT"

class GreaterThanOrEqualOperator(QueryOperator):
    symbol = "GTE"

class LessThanOperator(QueryOperator):
    symbol = "LT"

class LessThanOrEqualOperator(QueryOperator):
    symbol = "LTE"

class QuerySet(object):
    #TODO: querysets should be immutable
    #TODO: querysets should be executed lazily
    #TODO: support specifying offset and limit (use slice) (maybe return a mutated queryset)
    #TODO: support specifying columns to exclude or select only

    #CQL supports ==, >, >=, <, <=, IN (a,b,c,..n)
    #REVERSE, LIMIT
    #ORDER BY

    def __init__(self, model, query_args={}):
        super(QuerySet, self).__init__()
        self.model = model
        self.column_family_name = self.model.objects.column_family_name

        #Where clause filters
        self._where = []

        #ordering arguments
        self._order = []

        #subset selection
        self._limit = None
        self._start = None

        #see the defer and only methods
        self._defer_fields = []
        self._only_fields = []

        self._cursor = None

    #----query generation / execution----
    def _execute_query(self):
        conn = get_connection()
        self._cursor = conn.cursor()

    def _where_clause(self):
        """
        Returns a where clause based on the given filter args
        """
        pass

    def _select_query(self):
        """
        Returns a select clause based on the given filter args
        """
        pass

    @property
    def cursor(self):
        if self._cursor is None:
            self._cursor = self._execute_query()
        return self._cursor

    #----Reads------
    def __iter__(self):
        if self._cursor is None:
            self._execute_query()
        return self

    def _get_next(self):
        """
        Gets the next cursor result
        Returns a db_field->value dict
        """
        cur = self._cursor
        values = cur.fetchone()
        if values is None: return
        names = [i[0] for i in cur.description]
        value_dict = dict(zip(names, values))
        return value_dict

    def next(self):
        values = self._get_next() 
        if values is None: raise StopIteration
        return values

    def first(self):
        pass

    def all(self):
        clone = copy.deepcopy(self)
        clone._where = []
        return clone

    def _parse_filter_arg(self, arg, val):
        statement = arg.split('__')
        if len(statement) == 1:
            return WhereFilter(arg, None, val)
        elif len(statement) == 2:
            return WhereFilter(statement[0], statement[1], val)
        else:
            raise QueryException("Can't parse '{}'".format(arg))

    def filter(self, **kwargs):
        #add arguments to the where clause filters
        clone = copy.deepcopy(self)
        for arg, val in kwargs.items():
            raw_statement = self._parse_filter_arg(arg, val)
            #resolve column and operator
            try:
                column = self.model._columns[raw_statement.column]
            except KeyError:
                raise QueryException("Can't resolve column name: '{}'".format(raw_statement.column))

            operator = QueryOperator.get_operator(raw_statement.operator)

            statement = WhereFilter(column, operator, val)
            clone._where.append(statement)

        return clone

    def count(self):
        """ Returns the number of rows matched by this query """
        qs = 'SELECT COUNT(*) FROM {}'.format(self.column_family_name)

    def find(self, pk):
        """
        loads one document identified by it's primary key
        """
        #TODO: make this a convenience wrapper of the filter method
        qs = 'SELECT * FROM {column_family} WHERE {pk_name}=:{pk_name}'
        qs = qs.format(column_family=self.column_family_name,
                       pk_name=self.model._pk_name)
        conn = get_connection()
        self._cursor = conn.cursor()
        self._cursor.execute(qs, {self.model._pk_name:pk})
        return self._get_next()

    def _only_or_defer(self, action, fields):
        clone = copy.deepcopy(self)
        if clone._defer_fields or clone._only_fields:
            raise QueryException("QuerySet alread has only or defer fields defined")

        #check for strange fields
        missing_fields = [f for f in fields if f not in self.model._columns.keys()]
        if missing_fields:
            raise QueryException("Can't resolve fields {} in {}".format(', '.join(missing_fields), self.model.__name__))

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

    #----writes----
    def save(self, instance):
        """
        Creates / updates a row.
        This is a blind insert call.
        All validation and cleaning needs to happen 
        prior to calling this.
        """
        assert type(instance) == self.model
        #organize data
        value_pairs = []

        #get pk
        col = self.model._columns[self.model._pk_name]
        values = instance.as_dict()
        value_pairs += [(col.db_field, values.get(self.model._pk_name))]

        #get defined fields and their column names
        for name, col in self.model._columns.items():
            if col.is_primary_key: continue
            value_pairs += [(col.db_field, values.get(name))]

        #add dynamic fields
        for key, val in values.items():
            if key in self.model._columns: continue
            value_pairs += [(key, val)]

        #construct query string
        field_names = zip(*value_pairs)[0]
        field_values = dict(value_pairs)
        qs = ["INSERT INTO {}".format(self.column_family_name)]
        qs += ["({})".format(', '.join(field_names))]
        qs += ['VALUES']
        qs += ["({})".format(', '.join([':'+f for f in field_names]))]
        qs = ' '.join(qs)

        conn = get_connection()
        cur = conn.cursor()
        cur.execute(qs, field_values)

    #----delete---
    def delete(self, columns=[]):
        """
        Deletes the contents of a query
        """

    def delete_instance(self, instance):
        """ Deletes one instance """
        pk_name = self.model._pk_name
        qs = ['DELETE FROM {}'.format(self.column_family_name)]
        qs += ['WHERE {0}=:{0}'.format(pk_name)]
        qs = ' '.join(qs)

        conn = get_connection()
        cur = conn.cursor()
        cur.execute(qs, {pk_name:instance.pk})

    def _create_column_family(self):
        #construct query string
        qs = ['CREATE TABLE {}'.format(self.column_family_name)]

        #add column types
        pkeys = []
        qtypes = []
        def add_column(col):
            s = '{} {}'.format(col.db_field, col.db_type)
            if col.primary_key: pkeys.append(col.db_field)
            qtypes.append(s)
        #add_column(self.model._columns[self.model._pk_name])
        for name, col in self.model._columns.items():
            add_column(col)

        qtypes.append('PRIMARY KEY ({})'.format(', '.join(pkeys)))

        qs += ['({})'.format(', '.join(qtypes))]
        qs = ' '.join(qs)

        #add primary key
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute(qs)
        except BaseException, e:
            if 'Cannot add already existing column family' not in e.message:
                raise

    def _delete_column_family(self):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute('drop table {};'.format(self.column_family_name))


