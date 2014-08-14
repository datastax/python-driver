import time
from datetime import datetime, timedelta
import six
from cqlengine.functions import QueryValue
from cqlengine.operators import BaseWhereOperator, InOperator


class StatementException(Exception): pass



import sys

class UnicodeMixin(object):
    if sys.version_info > (3, 0):
        __str__ = lambda x: x.__unicode__()
    else:
        __str__ = lambda x: six.text_type(x).encode('utf-8')

class ValueQuoter(UnicodeMixin):

    def __init__(self, value):
        self.value = value

    def __unicode__(self):
        from cassandra.encoder import cql_quote
        if isinstance(self.value, bool):
            return 'true' if self.value else 'false'
        elif isinstance(self.value, (list, tuple)):
            return '[' + ', '.join([cql_quote(v) for v in self.value]) + ']'
        elif isinstance(self.value, dict):
            return '{' + ', '.join([cql_quote(k) + ':' + cql_quote(v) for k,v in self.value.items()]) + '}'
        elif isinstance(self.value, set):
            return '{' + ', '.join([cql_quote(v) for v in self.value]) + '}'
        return cql_quote(self.value)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.value == other.value
        return False


class InQuoter(ValueQuoter):

    def __unicode__(self):
        from cassandra.encoder import cql_quote
        return '(' + ', '.join([cql_quote(v) for v in self.value]) + ')'


class BaseClause(UnicodeMixin):

    def __init__(self, field, value):
        self.field = field
        self.value = value
        self.context_id = None

    def __unicode__(self):
        raise NotImplementedError

    def __hash__(self):
        return hash(self.field) ^ hash(self.value)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.field == other.field and self.value == other.value
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def get_context_size(self):
        """ returns the number of entries this clause will add to the query context """
        return 1

    def set_context_id(self, i):
        """ sets the value placeholder that will be used in the query """
        self.context_id = i

    def update_context(self, ctx):
        """ updates the query context with this clauses values """
        assert isinstance(ctx, dict)
        ctx[str(self.context_id)] = self.value


class WhereClause(BaseClause):
    """ a single where statement used in queries """

    def __init__(self, field, operator, value, quote_field=True):
        """

        :param field:
        :param operator:
        :param value:
        :param quote_field: hack to get the token function rendering properly
        :return:
        """
        if not isinstance(operator, BaseWhereOperator):
            raise StatementException(
                "operator must be of type {}, got {}".format(BaseWhereOperator, type(operator))
            )
        super(WhereClause, self).__init__(field, value)
        self.operator = operator
        self.query_value = self.value if isinstance(self.value, QueryValue) else QueryValue(self.value)
        self.quote_field = quote_field

    def __unicode__(self):
        field = ('"{}"' if self.quote_field else '{}').format(self.field)
        return u'{} {} {}'.format(field, self.operator, six.text_type(self.query_value))

    def __hash__(self):
        return super(WhereClause, self).__hash__() ^ hash(self.operator)

    def __eq__(self, other):
        if super(WhereClause, self).__eq__(other):
            return self.operator.__class__ == other.operator.__class__
        return False

    def get_context_size(self):
        return self.query_value.get_context_size()

    def set_context_id(self, i):
        super(WhereClause, self).set_context_id(i)
        self.query_value.set_context_id(i)

    def update_context(self, ctx):
        if isinstance(self.operator, InOperator):
            ctx[str(self.context_id)] = InQuoter(self.value)
        else:
            self.query_value.update_context(ctx)


class AssignmentClause(BaseClause):
    """ a single variable st statement """

    def __unicode__(self):
        return u'"{}" = %({})s'.format(self.field, self.context_id)

    def insert_tuple(self):
        return self.field, self.context_id


class ContainerUpdateClause(AssignmentClause):

    def __init__(self, field, value, operation=None, previous=None, column=None):
        super(ContainerUpdateClause, self).__init__(field, value)
        self.previous = previous
        self._assignments = None
        self._operation = operation
        self._analyzed = False
        self._column = column

    def _to_database(self, val):
        return self._column.to_database(val) if self._column else val

    def _analyze(self):
        raise NotImplementedError

    def get_context_size(self):
        raise NotImplementedError

    def update_context(self, ctx):
        raise NotImplementedError


class SetUpdateClause(ContainerUpdateClause):
    """ updates a set collection """

    def __init__(self, field, value, operation=None, previous=None, column=None):
        super(SetUpdateClause, self).__init__(field, value, operation, previous, column=column)
        self._additions = None
        self._removals = None

    def __unicode__(self):
        qs = []
        ctx_id = self.context_id
        if self.previous is None and not (self._assignments or self._additions or self._removals):
            qs += ['"{}" = %({})s'.format(self.field, ctx_id)]
        if self._assignments:
            qs += ['"{}" = %({})s'.format(self.field, ctx_id)]
            ctx_id += 1
        if self._additions:
            qs += ['"{0}" = "{0}" + %({1})s'.format(self.field, ctx_id)]
            ctx_id += 1
        if self._removals:
            qs += ['"{0}" = "{0}" - %({1})s'.format(self.field, ctx_id)]

        return ', '.join(qs)

    def _analyze(self):
        """ works out the updates to be performed """
        if self.value is None or self.value == self.previous:
            pass
        elif self._operation == "add":
            self._additions = self.value
        elif self._operation == "remove":
            self._removals = self.value
        elif self.previous is None:
            self._assignments = self.value
        else:
            # partial update time
            self._additions = (self.value - self.previous) or None
            self._removals = (self.previous - self.value) or None
        self._analyzed = True

    def get_context_size(self):
        if not self._analyzed: self._analyze()
        if self.previous is None and not (self._assignments or self._additions or self._removals):
            return 1
        return int(bool(self._assignments)) + int(bool(self._additions)) + int(bool(self._removals))

    def update_context(self, ctx):
        if not self._analyzed: self._analyze()
        ctx_id = self.context_id
        if self.previous is None and not (self._assignments or self._additions or self._removals):
            ctx[str(ctx_id)] = self._to_database({})
        if self._assignments:
            ctx[str(ctx_id)] = self._to_database(self._assignments)
            ctx_id += 1
        if self._additions:
            ctx[str(ctx_id)] = self._to_database(self._additions)
            ctx_id += 1
        if self._removals:
            ctx[str(ctx_id)] = self._to_database(self._removals)


class ListUpdateClause(ContainerUpdateClause):
    """ updates a list collection """

    def __init__(self, field, value, operation=None, previous=None, column=None):
        super(ListUpdateClause, self).__init__(field, value, operation, previous, column=column)
        self._append = None
        self._prepend = None

    def __unicode__(self):
        if not self._analyzed: self._analyze()
        qs = []
        ctx_id = self.context_id
        if self._assignments is not None:
            qs += ['"{}" = %({})s'.format(self.field, ctx_id)]
            ctx_id += 1

        if self._prepend:
            qs += ['"{0}" = %({1})s + "{0}"'.format(self.field, ctx_id)]
            ctx_id += 1

        if self._append:
            qs += ['"{0}" = "{0}" + %({1})s'.format(self.field, ctx_id)]

        return ', '.join(qs)

    def get_context_size(self):
        if not self._analyzed: self._analyze()
        return int(self._assignments is not None) + int(bool(self._append)) + int(bool(self._prepend))

    def update_context(self, ctx):
        if not self._analyzed: self._analyze()
        ctx_id = self.context_id
        if self._assignments is not None:
            ctx[str(ctx_id)] = self._to_database(self._assignments)
            ctx_id += 1
        if self._prepend:
            # CQL seems to prepend element at a time, starting
            # with the element at idx 0, we can either reverse
            # it here, or have it inserted in reverse
            ctx[str(ctx_id)] = self._to_database(list(reversed(self._prepend)))
            ctx_id += 1
        if self._append:
            ctx[str(ctx_id)] = self._to_database(self._append)

    def _analyze(self):
        """ works out the updates to be performed """
        if self.value is None or self.value == self.previous:
            pass

        elif self._operation == "append":
            self._append = self.value

        elif self._operation == "prepend":
            # self.value is a Quoter but we reverse self._prepend later as if
            # it's a list, so we have to set it to the underlying list
            self._prepend = self.value.value

        elif self.previous is None:
            self._assignments = self.value

        elif len(self.value) < len(self.previous):
            # if elements have been removed,
            # rewrite the whole list
            self._assignments = self.value

        elif len(self.previous) == 0:
            # if we're updating from an empty
            # list, do a complete insert
            self._assignments = self.value
        else:

            # the max start idx we want to compare
            search_space = len(self.value) - max(0, len(self.previous)-1)

            # the size of the sub lists we want to look at
            search_size = len(self.previous)

            for i in range(search_space):
                #slice boundary
                j = i + search_size
                sub = self.value[i:j]
                idx_cmp = lambda idx: self.previous[idx] == sub[idx]
                if idx_cmp(0) and idx_cmp(-1) and self.previous == sub:
                    self._prepend = self.value[:i] or None
                    self._append = self.value[j:] or None
                    break

            # if both append and prepend are still None after looking
            # at both lists, an insert statement will be created
            if self._prepend is self._append is None:
                self._assignments = self.value

        self._analyzed = True


class MapUpdateClause(ContainerUpdateClause):
    """ updates a map collection """

    def __init__(self, field, value, operation=None, previous=None, column=None):
        super(MapUpdateClause, self).__init__(field, value, operation, previous, column=column)
        self._updates = None

    def _analyze(self):
        if self._operation == "update":
            self._updates = self.value.keys()
        else:
            if self.previous is None:
                self._updates = sorted([k for k, v in self.value.items()])
            else:
                self._updates = sorted([k for k, v in self.value.items() if v != self.previous.get(k)]) or None
        self._analyzed = True

    def get_context_size(self):
        if not self._analyzed: self._analyze()
        if self.previous is None and not self._updates:
            return 1
        return len(self._updates or []) * 2

    def update_context(self, ctx):
        if not self._analyzed: self._analyze()
        ctx_id = self.context_id
        if self.previous is None and not self._updates:
            ctx[str(ctx_id)] = {}
        else:
            for key in self._updates or []:
                val = self.value.get(key)
                ctx[str(ctx_id)] = self._column.key_col.to_database(key) if self._column else key
                ctx[str(ctx_id + 1)] = self._column.value_col.to_database(val) if self._column else val
                ctx_id += 2

    def __unicode__(self):
        if not self._analyzed: self._analyze()
        qs = []

        ctx_id = self.context_id
        if self.previous is None and not self._updates:
            qs += ['"int_map" = %({})s'.format(ctx_id)]
        else:
            for _ in self._updates or []:
                qs += ['"{}"[%({})s] = %({})s'.format(self.field, ctx_id, ctx_id + 1)]
                ctx_id += 2

        return ', '.join(qs)


class CounterUpdateClause(ContainerUpdateClause):

    def __init__(self, field, value, previous=None, column=None):
        super(CounterUpdateClause, self).__init__(field, value, previous=previous, column=column)
        self.previous = self.previous or 0

    def get_context_size(self):
        return 1

    def update_context(self, ctx):
        ctx[str(self.context_id)] = self._to_database(abs(self.value - self.previous))

    def __unicode__(self):
        delta = self.value - self.previous
        sign = '-' if delta < 0 else '+'
        return '"{0}" = "{0}" {1} %({2})s'.format(self.field, sign, self.context_id)


class BaseDeleteClause(BaseClause):
    pass


class FieldDeleteClause(BaseDeleteClause):
    """ deletes a field from a row """

    def __init__(self, field):
        super(FieldDeleteClause, self).__init__(field, None)

    def __unicode__(self):
        return '"{}"'.format(self.field)

    def update_context(self, ctx):
        pass

    def get_context_size(self):
        return 0


class MapDeleteClause(BaseDeleteClause):
    """ removes keys from a map """

    def __init__(self, field, value, previous=None):
        super(MapDeleteClause, self).__init__(field, value)
        self.value = self.value or {}
        self.previous = previous or {}
        self._analyzed = False
        self._removals = None

    def _analyze(self):
        self._removals = sorted([k for k in self.previous if k not in self.value])
        self._analyzed = True

    def update_context(self, ctx):
        if not self._analyzed: self._analyze()
        for idx, key in enumerate(self._removals):
            ctx[str(self.context_id + idx)] = key

    def get_context_size(self):
        if not self._analyzed: self._analyze()
        return len(self._removals)

    def __unicode__(self):
        if not self._analyzed: self._analyze()
        return ', '.join(['"{}"[%({})s]'.format(self.field, self.context_id + i) for i in range(len(self._removals))])


class BaseCQLStatement(UnicodeMixin):
    """ The base cql statement class """

    def __init__(self, table, consistency=None, timestamp=None, where=None):
        super(BaseCQLStatement, self).__init__()
        self.table = table
        self.consistency = consistency
        self.context_id = 0
        self.context_counter = self.context_id
        self.timestamp = timestamp

        self.where_clauses = []
        for clause in where or []:
            self.add_where_clause(clause)

    def add_where_clause(self, clause):
        """
        adds a where clause to this statement
        :param clause: the clause to add
        :type clause: WhereClause
        """
        if not isinstance(clause, WhereClause):
            raise StatementException("only instances of WhereClause can be added to statements")
        clause.set_context_id(self.context_counter)
        self.context_counter += clause.get_context_size()
        self.where_clauses.append(clause)

    def get_context(self):
        """
        returns the context dict for this statement
        :rtype: dict
        """
        ctx = {}
        for clause in self.where_clauses or []:
            clause.update_context(ctx)
        return ctx

    def get_context_size(self):
        return len(self.get_context())

    def update_context_id(self, i):
        self.context_id = i
        self.context_counter = self.context_id
        for clause in self.where_clauses:
            clause.set_context_id(self.context_counter)
            self.context_counter += clause.get_context_size()

    @property
    def timestamp_normalized(self):
        """
        we're expecting self.timestamp to be either a long, int, a datetime, or a timedelta
        :return:
        """
        if not self.timestamp:
            return None

        if isinstance(self.timestamp, six.integer_types):
            return self.timestamp

        if isinstance(self.timestamp, timedelta):
            tmp = datetime.now() + self.timestamp
        else:
            tmp = self.timestamp

        return int(time.mktime(tmp.timetuple()) * 1e+6 + tmp.microsecond)

    def __unicode__(self):
        raise NotImplementedError


    def __repr__(self):
        return self.__unicode__()

    @property
    def _where(self):
        return 'WHERE {}'.format(' AND '.join([six.text_type(c) for c in self.where_clauses]))


class SelectStatement(BaseCQLStatement):
    """ a cql select statement """

    def __init__(self,
                 table,
                 fields=None,
                 count=False,
                 consistency=None,
                 where=None,
                 order_by=None,
                 limit=None,
                 allow_filtering=False):

        """
        :param where
        :type where list of cqlengine.statements.WhereClause
        """
        super(SelectStatement, self).__init__(
            table,
            consistency=consistency,
            where=where
        )

        self.fields = [fields] if isinstance(fields, six.string_types) else (fields or [])
        self.count = count
        self.order_by = [order_by] if isinstance(order_by, six.string_types) else order_by
        self.limit = limit
        self.allow_filtering = allow_filtering

    def __unicode__(self):
        qs = ['SELECT']
        if self.count:
            qs += ['COUNT(*)']
        else:
            qs += [', '.join(['"{}"'.format(f) for f in self.fields]) if self.fields else '*']
        qs += ['FROM', self.table]

        if self.where_clauses:
            qs += [self._where]

        if self.order_by and not self.count:
            qs += ['ORDER BY {}'.format(', '.join(six.text_type(o) for o in self.order_by))]

        if self.limit:
            qs += ['LIMIT {}'.format(self.limit)]

        if self.allow_filtering:
            qs += ['ALLOW FILTERING']

        return ' '.join(qs)


class AssignmentStatement(BaseCQLStatement):
    """ value assignment statements """

    def __init__(self,
                 table,
                 assignments=None,
                 consistency=None,
                 where=None,
                 ttl=None,
                 timestamp=None):
        super(AssignmentStatement, self).__init__(
            table,
            consistency=consistency,
            where=where,
        )
        self.ttl = ttl
        self.timestamp = timestamp

        # add assignments
        self.assignments = []
        for assignment in assignments or []:
            self.add_assignment_clause(assignment)

    def update_context_id(self, i):
        super(AssignmentStatement, self).update_context_id(i)
        for assignment in self.assignments:
            assignment.set_context_id(self.context_counter)
            self.context_counter += assignment.get_context_size()

    def add_assignment_clause(self, clause):
        """
        adds an assignment clause to this statement
        :param clause: the clause to add
        :type clause: AssignmentClause
        """
        if not isinstance(clause, AssignmentClause):
            raise StatementException("only instances of AssignmentClause can be added to statements")
        clause.set_context_id(self.context_counter)
        self.context_counter += clause.get_context_size()
        self.assignments.append(clause)

    @property
    def is_empty(self):
        return len(self.assignments) == 0

    def get_context(self):
        ctx = super(AssignmentStatement, self).get_context()
        for clause in self.assignments:
            clause.update_context(ctx)
        return ctx


class InsertStatement(AssignmentStatement):
    """ an cql insert select statement """

    def add_where_clause(self, clause):
        raise StatementException("Cannot add where clauses to insert statements")

    def __unicode__(self):
        qs = ['INSERT INTO {}'.format(self.table)]

        # get column names and context placeholders
        fields = [a.insert_tuple() for a in self.assignments]
        columns, values = zip(*fields)

        qs += ["({})".format(', '.join(['"{}"'.format(c) for c in columns]))]
        qs += ['VALUES']
        qs += ["({})".format(', '.join(['%({})s'.format(v) for v in values]))]

        if self.ttl:
            qs += ["USING TTL {}".format(self.ttl)]

        if self.timestamp:
            qs += ["USING TIMESTAMP {}".format(self.timestamp_normalized)]

        return ' '.join(qs)


class UpdateStatement(AssignmentStatement):
    """ an cql update select statement """

    def __unicode__(self):
        qs = ['UPDATE', self.table]

        using_options = []

        if self.ttl:
            using_options += ["TTL {}".format(self.ttl)]

        if self.timestamp:
            using_options += ["TIMESTAMP {}".format(self.timestamp_normalized)]

        if using_options:
            qs += ["USING {}".format(" AND ".join(using_options))]

        qs += ['SET']
        qs += [', '.join([six.text_type(c) for c in self.assignments])]

        if self.where_clauses:
            qs += [self._where]

        return ' '.join(qs)


class DeleteStatement(BaseCQLStatement):
    """ a cql delete statement """

    def __init__(self, table, fields=None, consistency=None, where=None, timestamp=None):
        super(DeleteStatement, self).__init__(
            table,
            consistency=consistency,
            where=where,
            timestamp=timestamp
        )
        self.fields = []
        if isinstance(fields, six.string_types):
            fields = [fields]
        for field in fields or []:
            self.add_field(field)

    def update_context_id(self, i):
        super(DeleteStatement, self).update_context_id(i)
        for field in self.fields:
            field.set_context_id(self.context_counter)
            self.context_counter += field.get_context_size()

    def get_context(self):
        ctx = super(DeleteStatement, self).get_context()
        for field in self.fields:
            field.update_context(ctx)
        return ctx

    def add_field(self, field):
        if isinstance(field, six.string_types):
            field = FieldDeleteClause(field)
        if not isinstance(field, BaseClause):
            raise StatementException("only instances of AssignmentClause can be added to statements")
        field.set_context_id(self.context_counter)
        self.context_counter += field.get_context_size()
        self.fields.append(field)

    def __unicode__(self):
        qs = ['DELETE']
        if self.fields:
            qs += [', '.join(['{}'.format(f) for f in self.fields])]
        qs += ['FROM', self.table]

        delete_option = []

        if self.timestamp:
            delete_option += ["TIMESTAMP {}".format(self.timestamp_normalized)]

        if delete_option:
            qs += [" USING {} ".format(" AND ".join(delete_option))]

        if self.where_clauses:
            qs += [self._where]

        return ' '.join(qs)

