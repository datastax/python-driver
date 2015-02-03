import six


class QueryOperatorException(Exception): pass

import sys

# move to central spot
class UnicodeMixin(object):
    if sys.version_info > (3, 0):
        __str__ = lambda x: x.__unicode__()
    else:
        __str__ = lambda x: six.text_type(x).encode('utf-8')


class BaseQueryOperator(UnicodeMixin):
    # The symbol that identifies this operator in kwargs
    # ie: colname__<symbol>
    symbol = None

    # The comparator symbol this operator uses in cql
    cql_symbol = None

    def __unicode__(self):
        if self.cql_symbol is None:
            raise QueryOperatorException("cql symbol is None")
        return self.cql_symbol

    @classmethod
    def get_operator(cls, symbol):
        if cls == BaseQueryOperator:
            raise QueryOperatorException("get_operator can only be called from a BaseQueryOperator subclass")
        if not hasattr(cls, 'opmap'):
            cls.opmap = {}
            def _recurse(klass):
                if klass.symbol:
                    cls.opmap[klass.symbol.upper()] = klass
                for subklass in klass.__subclasses__():
                    _recurse(subklass)
                pass
            _recurse(cls)
        try:
            return cls.opmap[symbol.upper()]
        except KeyError:
            raise QueryOperatorException("{} doesn't map to a QueryOperator".format(symbol))


class BaseWhereOperator(BaseQueryOperator):
    """ base operator used for where clauses """


class EqualsOperator(BaseWhereOperator):
    symbol = 'EQ'
    cql_symbol = '='


class InOperator(EqualsOperator):
    symbol = 'IN'
    cql_symbol = 'IN'


class GreaterThanOperator(BaseWhereOperator):
    symbol = "GT"
    cql_symbol = '>'


class GreaterThanOrEqualOperator(BaseWhereOperator):
    symbol = "GTE"
    cql_symbol = '>='


class LessThanOperator(BaseWhereOperator):
    symbol = "LT"
    cql_symbol = '<'


class LessThanOrEqualOperator(BaseWhereOperator):
    symbol = "LTE"
    cql_symbol = '<='


class BaseAssignmentOperator(BaseQueryOperator):
    """ base operator used for insert and delete statements """


class AssignmentOperator(BaseAssignmentOperator):
    cql_symbol = "="


class AddSymbol(BaseAssignmentOperator):
    cql_symbol = "+"
