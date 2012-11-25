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

    @property
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

    def __init__(self, primary_key=False, index=False, db_field=None, default=None, required=True):
        """
        :param primary_key: bool flag, indicates this column is a primary key. The first primary key defined
        on a model is the partition key, all others are cluster keys
        :param index: bool flag, indicates an index should be created for this column
        :param db_field: the fieldname this field will map to in the database
        :param default: the default value, can be a value or a callable (no args)
        :param required: boolean, is the field required?
        """
        self.primary_key = primary_key
        self.index = index
        self.db_field = db_field
        self.default = default
        self.required = required

        #the column name in the model definition
        self.column_name = None

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
            elif self.required:
                raise ValidationError('{} - None values are not allowed'.format(self.column_name or self.db_field))
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
        return '{} {}'.format(self.db_field_name, self.db_type)

    def set_column_name(self, name):
        """
        Sets the column name during document class construction
        This value will be ignored if db_field is set in __init__
        """
        self.column_name = name

    @property
    def db_field_name(self):
        """ Returns the name of the cql name of this column """
        return self.db_field or self.column_name

    @property
    def db_index_name(self):
        """ Returns the name of the cql index """
        return 'index_{}'.format(self.db_field_name)

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
    #TODO: decimal field
    def __init__(self, **kwargs):
        super(DateTime, self).__init__(**kwargs)
        raise NotImplementedError

class Counter(BaseColumn):
    #TODO: counter field
    def __init__(self, **kwargs):
        super(Counter, self).__init__(**kwargs)
        raise NotImplementedError

class ForeignKey(BaseColumn):
    #TODO: Foreign key field
    def __init__(self, **kwargs):
        super(ForeignKey, self).__init__(**kwargs)
        raise NotImplementedError

