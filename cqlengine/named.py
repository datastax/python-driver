from collections import defaultdict, namedtuple

from cqlengine.models import QuerySetDescriptor
from cqlengine.query import AbstractQueryableColumn


class NamedColumn(AbstractQueryableColumn):
    """ describes a named cql column """

    def __init__(self, name):
        self.name = name

    def _get_column(self):
        return self

    @property
    def cql(self):
        return self.name

    def to_database(self, val):
        return val


class NamedTable(object):
    """ describes a cql table """

    __abstract__ = False

    class ColumnContainer(dict):
        def __missing__(self, name):
            column = NamedColumn(name)
            self[name] = column
            return column
    _columns = ColumnContainer()

    objects = QuerySetDescriptor()

    def __init__(self, keyspace, name):
        self.keyspace = keyspace
        self.name = name

    @classmethod
    def column(cls, name):
        return NamedColumn(name)

    @classmethod
    def create(cls, **kwargs):
        return cls.objects.create(**kwargs)

    @classmethod
    def all(cls):
        return cls.objects.all()

    @classmethod
    def filter(cls, *args, **kwargs):
        return cls.objects.filter(*args, **kwargs)

    @classmethod
    def get(cls, *args, **kwargs):
        return cls.objects.get(*args, **kwargs)


class NamedKeyspace(object):
    """ Describes a cql keyspace """

    def __init__(self, name):
        self.name = name

    def table(self, name):
        """
        returns a table descriptor with the given
        name that belongs to this keyspace
        """
        return NamedTable(self.name, name)

