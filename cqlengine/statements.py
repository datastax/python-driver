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


class BaseCQLStatement(object):
    """ The base cql statement class """

    def __init__(self, table, consistency=None):
        super(BaseCQLStatement, self).__init__()
        self.table = table
        self.consistency = None
        self.where_clauses = []




class SelectStatement(BaseCQLStatement):
    """ a cql select statement """


class DMLStatement(BaseCQLStatement):
    """ mutation statements """


class InsertStatement(BaseCQLStatement):
    """ an cql insert select statement """


class UpdateStatement(BaseCQLStatement):
    """ an cql update select statement """


class DeleteStatement(BaseCQLStatement):
    """ a cql delete statement """
