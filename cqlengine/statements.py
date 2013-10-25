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
        return u"{} {} {}".format(self.field, self.operator, self.value)

    def __str__(self):
        return str(unicode(self))


class BaseCQLStatement(object):
    """ The base cql statement class """

    def __init__(self, table, consistency=None):
        super(BaseCQLStatement, self).__init__()
        self.table = table
        self.consistency = consistency
        self.where_clauses = []


class SelectStatement(BaseCQLStatement):
    """ a cql select statement """

    def __init__(self, table, fields, consistency=None):
        super(SelectStatement, self).__init__(table, consistency)
        if isinstance(fields, basestring):
            fields = [fields]
        self.fields = fields

    def __unicode__(self):
        qs = ['SELECT']
        qs += [', '.join(self.fields) if self.fields else '*']
        return ' '.join(qs)


class DMLStatement(BaseCQLStatement):
    """ mutation statements """


class InsertStatement(BaseCQLStatement):
    """ an cql insert select statement """


class UpdateStatement(BaseCQLStatement):
    """ an cql update select statement """


class DeleteStatement(BaseCQLStatement):
    """ a cql delete statement """
