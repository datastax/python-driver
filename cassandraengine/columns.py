#column field types
import re
from uuid import uuid1, uuid4

from cassandraengine.exceptions import ValidationError

class BaseColumn(object):

    #the cassandra type this column maps to
    db_type = None

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

    @property
    def has_default(self):
        return bool(self.default)

    @property
    def is_primary_key(self):
        return self.primary_key

    def get_default(self):
        if self.has_default:
            if callable(self.default):
                return self.default()
            else:
                return self.default

    def to_database(self, value):
        """
        Converts python value into database value
        """
        if value is None and self.has_default:
            return self.get_default()
        return value

    def get_column_def(self):
        """
        Returns a column definition for CQL table definition
        """
        dterms = [self.db_field, self.db_type]
        if self.primary_key:
            dterms.append('PRIMARY KEY')
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

