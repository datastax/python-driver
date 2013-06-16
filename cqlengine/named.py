from cqlengine.exceptions import CQLEngineException
from cqlengine.query import AbstractQueryableColumn, SimpleQuerySet


class QuerySetDescriptor(object):
    """
    returns a fresh queryset for the given model
    it's declared on everytime it's accessed
    """

    def __get__(self, obj, model):
        """ :rtype: ModelQuerySet """
        if model.__abstract__:
            raise CQLEngineException('cannot execute queries against abstract models')
        return SimpleQuerySet(model)

    def __call__(self, *args, **kwargs):
        """
        Just a hint to IDEs that it's ok to call this

        :rtype: ModelQuerySet
        """
        raise NotImplementedError


class NamedColumn(AbstractQueryableColumn):
    """
    A column that is not coupled to a model class, or type
    """

    def __init__(self, name):
        self.name = name

    def _get_column(self):
        return self

    @property
    def cql(self):
        return self.get_cql()

    def get_cql(self):
        return '"{}"'.format(self.name)

    def to_database(self, val):
        return val


class NamedTable(object):
    """
    A Table that is not coupled to a model class
    """

    __abstract__ = False

    objects = QuerySetDescriptor()

    def __init__(self, keyspace, name):
        self.keyspace = keyspace
        self.name = name

    @classmethod
    def column(cls, name):
        return NamedColumn(name)

    @classmethod
    def _get_column(cls, name):
        """
        Returns the column matching the given name

        :rtype: Column
        """
        return cls.column(name)

    # @classmethod
    # def create(cls, **kwargs):
    #     return cls.objects.create(**kwargs)

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
    """
    A keyspace
    """

    def __init__(self, name):
        self.name = name

    def table(self, name):
        """
        returns a table descriptor with the given
        name that belongs to this keyspace
        """
        return NamedTable(self.name, name)

