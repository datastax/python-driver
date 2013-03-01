from datetime import datetime

from cqlengine.exceptions import ValidationError

class BaseQueryFunction(object):
    """
    Base class for filtering functions. Subclasses of these classes can
    be passed into .filter() and will be translated into CQL functions in
    the resulting query
    """

    _cql_string = None

    def __init__(self, value):
        self.value = value

    def to_cql(self, value_id):
        """
        Returns a function for cql with the value id as it's argument
        """
        return self._cql_string.format(value_id)

class MinTimeUUID(BaseQueryFunction):

    _cql_string = 'MinTimeUUID(:{})'

    def __init__(self, value):
        """
        :param value: the time to create a maximum time uuid from
        :type value: datetime
        """
        if not isinstance(value, datetime):
            raise ValidationError('datetime instance is required')
        super(MinTimeUUID, self).__init__(value)

class MaxTimeUUID(BaseQueryFunction):

    _cql_string = 'MaxTimeUUID(:{})'

    def __init__(self, value):
        """
        :param value: the time to create a minimum time uuid from
        :type value: datetime
        """
        if not isinstance(value, datetime):
            raise ValidationError('datetime instance is required')
        super(MaxTimeUUID, self).__init__(value)

