#column field types
import re
from uuid import uuid1, uuid4

from cqlengine.exceptions import ValidationError

class BaseValueManager(object):

    def __init__(self, instance, column, value):
        self.instance = instance
        self.column = column
        self.initial_value = value
        self.value = value

    def deleted(self):
        return self.value is None and self.initial_value is not None

    def getval(self):
        return self.value

    def setval(self, val):
        self.value = val

    def delval(self):
        self.value = None

    def get_property(self):
        _get = lambda slf: self.getval()
        _set = lambda slf, val: self.setval(val)
        _del = lambda slf: self.delval()

        if self.column.can_delete:
            return property(_get, _set, _del)
        else:
            return property(_get, _set)

class BaseColumn(object):

    #the cassandra type this column maps to
    db_type = None
    value_manager = BaseValueManager

    instance_counter = 0

    def __init__(self, primary_key=False, db_field=None, default=None, null=False):
        """
        :param primary_key: bool flag, there can be only one primary key per doc
        :param db_field: the fieldname this field will map to in the database
        :param default: the default value, can be a value or a callable (no args)
        :param null: boolean, is the field nullable?
        """
        self.primary_key = primary_key
        self.db_field = db_field
        self.default = default
        self.null = null

        self.value = None

        #keep track of instantiation order
        self.position = BaseColumn.instance_counter
        BaseColumn.instance_counter += 1

    def validate(self, value):
        """
        Returns a cleaned and validated value. Raises a ValidationError
        if there's a problem
        """
        if value is None:
            if self.has_default:
                return self.get_default()
            elif not self.null:
                raise ValidationError('null values are not allowed')
        return value

    def to_python(self, value):
        """
        Converts data from the database into python values
        raises a ValidationError if the value can't be converted
        """
        return value

    def to_database(self, value):
        """
        Converts python value into database value
        """
        if value is None and self.has_default:
            return self.get_default()
        return value

    @property
    def has_default(self):
        return bool(self.default)

    @property
    def is_primary_key(self):
        return self.primary_key

    @property
    def can_delete(self):
        return not self.primary_key

    #methods for replacing column definitions with properties that interact
    #with a column's value member
    #this will allow putting logic behind value access (lazy loading, etc)
    def _getval(self):
        """ This columns value getter """
        return self.value

    def _setval(self, val):
        """ This columns value setter """
        self.value = val

    def _delval(self):
        """ This columns value deleter """
        raise NotImplementedError

    def get_property(self, allow_delete=True):
        """
        Returns the property object that will set and get this
        column's value and be assigned to this column's model attribute
        """
        getval = lambda slf: self._getval()
        setval = lambda slf, val: self._setval(val)
        delval = lambda slf: self._delval()
        if not allow_delete:
            return property(getval, setval)
        return property(getval, setval, delval)

    def get_default(self):
        if self.has_default:
            if callable(self.default):
                return self.default()
            else:
                return self.default

    def get_column_def(self):
        """
        Returns a column definition for CQL table definition
        """
        dterms = [self.db_field, self.db_type]
        #if self.primary_key:
            #dterms.append('PRIMARY KEY')
        return ' '.join(dterms)

    def set_db_name(self, name):
        """
        Sets the column name during document class construction
        This value will be ignored if db_field is set in __init__
        """
        self.db_field = self.db_field or name

class Bytes(BaseColumn):
    db_type = 'blob'

class Ascii(BaseColumn):
    db_type = 'ascii'

class Text(BaseColumn):
    db_type = 'text'

class Integer(BaseColumn):
    db_type = 'int'

    def validate(self, value):
        val = super(Integer, self).validate(value)
        try:
            return long(val)
        except (TypeError, ValueError):
            raise ValidationError("{} can't be converted to integral value".format(value))

    def to_python(self, value):
        return self.validate(value)

    def to_database(self, value):
        return self.validate(value)

class DateTime(BaseColumn):
    db_type = 'timestamp'
    def __init__(self, **kwargs):
        super(DateTime, self).__init__(**kwargs)
        raise NotImplementedError

class UUID(BaseColumn):
    """
    Type 1 or 4 UUID
    """
    db_type = 'uuid'

    re_uuid = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')

    def __init__(self, default=lambda:uuid4(), **kwargs):
        super(UUID, self).__init__(default=default, **kwargs)

    def validate(self, value):
        val = super(UUID, self).validate(value)
        from uuid import UUID as _UUID
        if isinstance(val, _UUID): return val
        if not self.re_uuid.match(val):
            raise ValidationError("{} is not a valid uuid".format(value))
        return _UUID(val)

class Boolean(BaseColumn):
    db_type = 'boolean'

    def to_python(self, value):
        return bool(value)

    def to_database(self, value):
        return bool(value)

class Float(BaseColumn):
    db_type = 'double'

    def validate(self, value):
        try:
            return float(value)
        except (TypeError, ValueError):
            raise ValidationError("{} is not a valid float".format(value))

    def to_python(self, value):
        return self.validate(value)

    def to_database(self, value):
        return self.validate(value)

class Decimal(BaseColumn):
    db_type = 'decimal'
    #TODO: this
    def __init__(self, **kwargs):
        super(DateTime, self).__init__(**kwargs)
        raise NotImplementedError

class Counter(BaseColumn):
    def __init__(self, **kwargs):
        super(DateTime, self).__init__(**kwargs)
        raise NotImplementedError

#TODO: research supercolumns
#http://wiki.apache.org/cassandra/DataModel
#checkout composite columns:
#http://www.datastax.com/dev/blog/introduction-to-composite-columns-part-1
class List(BaseColumn):
    #checkout cql.cqltypes.ListType
    def __init__(self, **kwargs):
        super(DateTime, self).__init__(**kwargs)
        raise NotImplementedError

class Dict(BaseColumn):
    #checkout cql.cqltypes.MapType
    def __init__(self, **kwargs):
        super(DateTime, self).__init__(**kwargs)
        raise NotImplementedError

#checkout cql.cqltypes.SetType
#checkout cql.cqltypes.CompositeType
