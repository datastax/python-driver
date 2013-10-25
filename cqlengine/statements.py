from cqlengine.operators import BaseWhereOperator, BaseAssignmentOperator


class StatementException(Exception): pass


class BaseClause(object):

    def __init__(self, field, value):
        self.field = field
        self.value = value
        self.context_id = None

    def __str__(self):
        return str(unicode(self))

    def get_context_size(self):
        """ returns the number of entries this clause will add to the query context """
        return 1

    def set_context_id(self, i):
        """ sets the value placeholder that will be used in the query """
        self.context_id = i

    def update_context(self, ctx):
        """ updates the query context with this clauses values """
        assert isinstance(ctx, dict)
        ctx[self.context_id] = self.value


class WhereClause(BaseClause):
    """ a single where statement used in queries """

    def __init__(self, field, operator, value):
        if not isinstance(operator, BaseWhereOperator):
            raise StatementException(
                "operator must be of type {}, got {}".format(BaseWhereOperator, type(operator))
            )
        super(WhereClause, self).__init__(field, value)
        self.operator = operator

    def __unicode__(self):
        return u'"{}" {} {}'.format(self.field, self.operator, self.context_id)


class AssignmentClause(BaseClause):
    """ a single variable st statement """

    def insert_tuple(self):
        return self.field, self.context_id


class BaseCQLStatement(object):
    """ The base cql statement class """

    def __init__(self, table, consistency=None, where=None):
        super(BaseCQLStatement, self).__init__()
        self.table = table
        self.consistency = consistency
        self.context_counter = 0

        self.where_clauses = []
        for clause in where or []:
            self.add_where_clause(clause)

    def add_where_clause(self, clause):
        if not isinstance(clause, WhereClause):
            raise StatementException("only instances of WhereClause can be added to statements")
        clause.set_context_id(self.context_counter)
        self.context_counter += clause.get_context_size()
        self.where_clauses.append(clause)

    def __str__(self):
        return str(unicode(self))

    @property
    def _where(self):
        return 'WHERE {}'.format(' AND '.join([unicode(c) for c in self.where_clauses]))


class SelectStatement(BaseCQLStatement):
    """ a cql select statement """

    def __init__(self,
                 table,
                 fields,
                 consistency=None,
                 where=None,
                 order_by=None,
                 limit=None,
                 allow_filtering=False):

        super(SelectStatement, self).__init__(
            table,
            consistency=consistency,
            where=where
        )

        self.fields = [fields] if isinstance(fields, basestring) else (fields or [])
        self.order_by = [order_by] if isinstance(order_by, basestring) else order_by
        self.limit = limit
        self.allow_filtering = allow_filtering

    def __unicode__(self):
        qs = ['SELECT']
        qs += [', '.join(['"{}"'.format(f) for f in self.fields]) if self.fields else '*']
        qs += ['FROM', self.table]

        if self.where_clauses:
            qs += [self._where]

        if self.order_by:
            qs += ['ORDER BY {}'.format(', '.join(unicode(o) for o in self.order_by))]

        if self.limit:
            qs += ['LIMIT {}'.format(self.limit)]

        if self.allow_filtering:
            qs += ['ALLOW FILTERING']

        return ' '.join(qs)


class DMLStatement(BaseCQLStatement):
    """ mutation statements with ttls """

    def __init__(self, table, consistency=None, where=None, ttl=None):
        super(DMLStatement, self).__init__(
            table,
            consistency=consistency,
            where=where)
        self.ttl = ttl


class AssignmentStatement(DMLStatement):
    """ value assignment statements """

    def __init__(self,
                 table,
                 assignments,
                 consistency=None,
                 where=None,
                 ttl=None):
        super(AssignmentStatement, self).__init__(
            table,
            consistency=consistency,
            where=where,
            ttl=ttl
        )

        # add assignments
        self.assignments = []
        for assignment in assignments or []:
            self.add_assignment_clause(assignment)

    def add_assignment_clause(self, clause):
        if not isinstance(clause, AssignmentClause):
            raise StatementException("only instances of AssignmentClause can be added to statements")
        clause.set_context_id(self.context_counter)
        self.context_counter += clause.get_context_size()
        self.assignments.append(clause)


class InsertStatement(AssignmentStatement):
    """ an cql insert select statement """

    def __init__(self, table, values, consistency=None):
        super(InsertStatement, self).__init__(
            table,
            consistency=consistency,
            where=None
        )
        self.values = values

    def add_where_clause(self, clause):
        raise StatementException("Cannot add where clauses to insert statements")

    def __unicode__(self):
        qs = ['INSERT INTO {}'.format(self.table)]

        # get column names and context placeholders
        fields = [a.insert_tuple() for a in self.assignments]
        columns, values = zip(*fields)

        qs += ["({})".format(', '.join(['"{}"'.format(c) for c in columns]))]
        qs += ['VALUES']
        qs += ["({})".format(', '.join([':{}'.format(v) for v in values]))]

        return ' '.join(qs)


class UpdateStatement(AssignmentStatement):
    """ an cql update select statement """

    def __init__(self, table, assignments, consistency=None, where=None, ttl=None):
        super(UpdateStatement, self).__init__(
            table,
            assignments,
            consistency=consistency,
            where=where,
            ttl=ttl
        )


class DeleteStatement(DMLStatement):
    """ a cql delete statement """

    def __init__(self, table, fields, consistency=None, where=None, ttl=None):
        super(DeleteStatement, self).__init__(
            table,
            consistency=consistency,
            where=where,
            ttl=ttl
        )
        self.fields = [fields] if isinstance(fields, basestring) else (fields or [])


