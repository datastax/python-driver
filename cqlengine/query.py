import copy

from cqlengine.connection import get_connection

#CQL 3 reference:
#http://www.datastax.com/docs/1.1/references/cql/index

class Query(object):

    pass

class QuerySet(object):
    #TODO: querysets should be immutable
    #TODO: querysets should be executed lazily
    #TODO: conflicting filter args should raise exception unless a force kwarg is supplied

    #CQL supports ==, >, >=, <, <=, IN (a,b,c,..n)
    #REVERSE, LIMIT
    #ORDER BY

    def __init__(self, model, query_args={}):
        super(QuerySet, self).__init__()
        self.model = model
        self.query_args = query_args
        self.column_family_name = self.model.objects.column_family_name

        self._cursor = None

    #----query generation / execution----
    def _execute_query(self):
        conn = get_connection()
        self._cursor = conn.cursor()

    def _generate_querystring(self):
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
        return QuerySet(self.model)

    def filter(self, **kwargs):
        qargs = copy.deepcopy(self.query_args)
        qargs.update(kwargs)
        return QuerySet(self.model, query_args=qargs)

    def exclude(self, **kwargs):
        """ Need to invert the logic for all kwargs """
        pass

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
    def delete(self):
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


