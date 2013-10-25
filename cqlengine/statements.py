from cqlengine.operators import BaseWhereOperator, BaseAssignmentOperator


class StatementException(Exception): pass


class BaseClause(object):

    def __init__(self, field, operator, value):
        self.field = field
        self.operator = operator
        self.value = value

    def __unicode__(self):
        return u'"{}" {} {}'.format(self.field, self.operator, self.value)

    def __str__(self):
        return str(unicode(self))


class WhereClause(BaseClause):
    """ a single where statement used in queries """

    def __init__(self, field, operator, value):
        if not isinstance(operator, BaseWhereOperator):
            raise StatementException(
                "operator must be of type {}, got {}".format(BaseWhereOperator, type(operator))
            )
        super(WhereClause, self).__init__(field, operator, value)


class AssignmentClause(BaseClause):
    """ a single variable st statement """

    def __init__(self, field, operator, value):
        if not isinstance(operator, BaseAssignmentOperator):
            raise StatementException(
                "operator must be of type {}, got {}".format(BaseAssignmentOperator, type(operator))
            )
        super(AssignmentClause, self).__init__(field, operator, value)


class BaseCQLStatement(object):
    """ The base cql statement class """

    def __init__(self, table, consistency=None, where=None):
        super(BaseCQLStatement, self).__init__()
        self.table = table
        self.consistency = consistency
        self.where_clauses = where or []

    def add_where_clause(self, clause):
        if not isinstance(clause, WhereClause):
            raise StatementException("only instances of WhereClause can be added to statements")
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
        self.assignments = assignments or []

    def add_assignment_clause(self, clause):
        if not isinstance(clause, AssignmentClause):
            raise StatementException("only instances of AssignmentClause can be added to statements")
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


