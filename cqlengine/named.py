from cqlengine.query import AbstractQueryableColumn


class NamedColumn(AbstractQueryableColumn):
    """ describes a named cql column """

    def __init__(self, name):
        self.name = name

    @property
    def cql(self):
        return self.name

    def to_database(self, val):
        return val


class NamedTable(object):
    """ describes a cql table """

    def __init__(self, keyspace, name):
        self.keyspace = keyspace
        self.name = name

    def column(self, name):
        return NamedColumn(name)


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

