from cqlengine.operators import BaseWhereOperator


class StatementException(Exception): pass


class WhereClause(object):
    """ a single where statement used in queries """

    def __init__(self, field, operator, value):
        super(WhereClause, self).__init__()
        if not isinstance(operator, BaseWhereOperator):
            raise StatementException(
                "operator must be of type {}, got {}".format(BaseWhereOperator, type(operator))
            )

        self.field = field
        self.operator = operator
        self.value = value

    def __unicode__(self):
        return u'"{}" {} {}'.format(self.field, self.operator, self.value)

    def __str__(self):
        return str(unicode(self))


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


class SelectStatement(BaseCQLStatement):
    """ a cql select statement """

    def __init__(self,
                 table,
                 fields,
                 consistency=None,
                 where=None,
                 order_by=None,
                 limit=None,
                 allow_filtering=False
    ):
        super(SelectStatement, self).__init__(
            table,
            consistency=consistency,
            where=where
        )

        self.fields = [fields] if isinstance(fields, basestring) else fields
        self.order_by = [order_by] if isinstance(order_by, basestring) else order_by
        self.limit = limit
        self.allow_filtering = allow_filtering

    def __unicode__(self):
        qs = ['SELECT']
        qs += [', '.join(['"{}"'.format(f) for f in self.fields]) if self.fields else '*']
        qs += ['FROM', self.table]

        if self.where_clauses:
            qs += ['WHERE', ' AND '.join([unicode(c) for c in self.where_clauses])]

        if self.order_by:
            qs += ['ORDER BY {}'.format(', '.join(unicode(o) for o in self.order_by))]

        if self.limit:
            qs += ['LIMIT {}'.format(self.limit)]

        if self.allow_filtering:
            qs += ['ALLOW FILTERING']

        return ' '.join(qs)


class DMLStatement(BaseCQLStatement):
    """ mutation statements """

    def __init__(self, table, consistency=None, where=None, ttl=None):
        super(DMLStatement, self).__init__(
            table,
            consistency=consistency,
            where=where)
        self.ttl = ttl


class InsertStatement(DMLStatement):
    """ an cql insert select statement """

    def __init__(self, table, values, consistency=None):
        super(InsertStatement, self).__init__(
            table,
            consistency=consistency,
            where=None
        )

    def add_where_clause(self, clause):
        raise StatementException("Cannot add where clauses to insert statements")


class UpdateStatement(DMLStatement):
    """ an cql update select statement """

    def __init__(self, table, consistency=None, where=None):
        super(UpdateStatement, self).__init__(table, consistency, where)


class DeleteStatement(BaseCQLStatement):
    """ a cql delete statement """
