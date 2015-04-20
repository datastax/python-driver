# Copyright 2015 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from copy import deepcopy, copy
from datetime import date, datetime
import logging
import re
import six
import warnings

from cassandra.cqltypes import DateType
from cassandra.encoder import cql_quote

from cassandra.cqlengine import ValidationError

log = logging.getLogger(__name__)


class BaseValueManager(object):

    def __init__(self, instance, column, value):
        self.instance = instance
        self.column = column
        self.previous_value = deepcopy(value)
        self.value = value
        self.explicit = False

    @property
    def deleted(self):
        return self.value is None and self.previous_value is not None

    @property
    def changed(self):
        """
        Indicates whether or not this value has changed.

        :rtype: boolean

        """
        return self.value != self.previous_value

    def reset_previous_value(self):
        self.previous_value = copy(self.value)

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


class ValueQuoter(object):
    """
    contains a single value, which will quote itself for CQL insertion statements
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        raise NotImplementedError

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.value == other.value
        return False


class Column(object):

    # the cassandra type this column maps to
    db_type = None
    value_manager = BaseValueManager

    instance_counter = 0

    primary_key = False
    """
    bool flag, indicates this column is a primary key. The first primary key defined
    on a model is the partition key (unless partition keys are set), all others are cluster keys
    """

    partition_key = False

    """
    indicates that this column should be the partition key, defining
    more than one partition key column creates a compound partition key
    """

    index = False
    """
    bool flag, indicates an index should be created for this column
    """

    db_field = None
    """
    the fieldname this field will map to in the database
    """

    default = None
    """
    the default value, can be a value or a callable (no args)
    """

    required = False
    """
    boolean, is the field required? Model validation will raise and
    exception if required is set to True and there is a None value assigned
    """

    clustering_order = None
    """
    only applicable on clustering keys (primary keys that are not partition keys)
    determines the order that the clustering keys are sorted on disk
    """

    polymorphic_key = False
    """
    *Deprecated*

    see :attr:`~.discriminator_column`
    """

    discriminator_column = False
    """
    boolean, if set to True, this column will be used for discriminating records
    of inherited models.

    Should only be set on a column of an abstract model being used for inheritance.

    There may only be one discriminator column per model. See :attr:`~.__discriminator_value__`
    for how to specify the value of this column on specialized models.
    """

    static = False
    """
    boolean, if set to True, this is a static column, with a single value per partition
    """

    def __init__(self,
                 primary_key=False,
                 partition_key=False,
                 index=False,
                 db_field=None,
                 default=None,
                 required=False,
                 clustering_order=None,
                 polymorphic_key=False,
                 discriminator_column=False,
                 static=False):
        self.partition_key = partition_key
        self.primary_key = partition_key or primary_key
        self.index = index
        self.db_field = db_field
        self.default = default
        self.required = required
        self.clustering_order = clustering_order

        if polymorphic_key:
            msg = "polymorphic_key is deprecated. Use discriminator_column instead."
            warnings.warn(msg, DeprecationWarning)
            log.warning(msg)

        self.discriminator_column = discriminator_column or polymorphic_key
        self.polymorphic_key = self.discriminator_column

        # the column name in the model definition
        self.column_name = None
        self.static = static

        self.value = None

        # keep track of instantiation order
        self.position = Column.instance_counter
        Column.instance_counter += 1

    def validate(self, value):
        """
        Returns a cleaned and validated value. Raises a ValidationError
        if there's a problem
        """
        if value is None:
            if self.required:
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
        return self.default is not None

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
        static = "static" if self.static else ""
        return '{} {} {}'.format(self.cql, self.db_type, static)

    # TODO: make columns use cqltypes under the hood
    # until then, this bridges the gap in using types along with cassandra.metadata for CQL generation
    def cql_parameterized_type(self):
        return self.db_type

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

    @property
    def cql(self):
        return self.get_cql()

    def get_cql(self):
        return '"{}"'.format(self.db_field_name)

    def _val_is_null(self, val):
        """ determines if the given value equates to a null value for the given column type """
        return val is None

    @property
    def sub_columns(self):
        return []


class Blob(Column):
    """
    Stores a raw binary value
    """
    db_type = 'blob'

    def to_database(self, value):

        if not isinstance(value, (six.binary_type, bytearray)):
            raise Exception("expecting a binary, got a %s" % type(value))

        val = super(Bytes, self).to_database(value)
        return bytearray(val)

    def to_python(self, value):
        return value

Bytes = Blob


class Ascii(Column):
    """
    Stores a US-ASCII character string
    """
    db_type = 'ascii'


class Inet(Column):
    """
    Stores an IP address in IPv4 or IPv6 format
    """
    db_type = 'inet'


class Text(Column):
    """
    Stores a UTF-8 encoded string
    """
    db_type = 'text'

    def __init__(self, min_length=None, max_length=None, **kwargs):
        """
        :param int min_length: Sets the minimum length of this string, for validation purposes.
            Defaults to 1 if this is a ``required`` column. Otherwise, None.
        :param int max_lemgth: Sets the maximum length of this string, for validation purposes.
        """
        self.min_length = min_length or (1 if kwargs.get('required', False) else None)
        self.max_length = max_length
        super(Text, self).__init__(**kwargs)

    def validate(self, value):
        value = super(Text, self).validate(value)
        if value is None:
            return
        if not isinstance(value, (six.string_types, bytearray)) and value is not None:
            raise ValidationError('{} {} is not a string'.format(self.column_name, type(value)))
        if self.max_length:
            if len(value) > self.max_length:
                raise ValidationError('{} is longer than {} characters'.format(self.column_name, self.max_length))
        if self.min_length:
            if len(value) < self.min_length:
                raise ValidationError('{} is shorter than {} characters'.format(self.column_name, self.min_length))
        return value


class Integer(Column):
    """
    Stores a 32-bit signed integer value
    """

    db_type = 'int'

    def validate(self, value):
        val = super(Integer, self).validate(value)
        if val is None:
            return
        try:
            return int(val)
        except (TypeError, ValueError):
            raise ValidationError("{} {} can't be converted to integral value".format(self.column_name, value))

    def to_python(self, value):
        return self.validate(value)

    def to_database(self, value):
        return self.validate(value)


class BigInt(Integer):
    """
    Stores a 64-bit signed long value
    """
    db_type = 'bigint'


class VarInt(Column):
    """
    Stores an arbitrary-precision integer
    """
    db_type = 'varint'

    def validate(self, value):
        val = super(VarInt, self).validate(value)
        if val is None:
            return
        try:
            return int(val)
        except (TypeError, ValueError):
            raise ValidationError(
                "{} {} can't be converted to integral value".format(self.column_name, value))

    def to_python(self, value):
        return self.validate(value)

    def to_database(self, value):
        return self.validate(value)


class CounterValueManager(BaseValueManager):
    def __init__(self, instance, column, value):
        super(CounterValueManager, self).__init__(instance, column, value)
        self.value = self.value or 0
        self.previous_value = self.previous_value or 0


class Counter(Integer):
    """
    Stores a counter that can be inremented and decremented
    """
    db_type = 'counter'

    value_manager = CounterValueManager

    def __init__(self,
                 index=False,
                 db_field=None,
                 required=False):
        super(Counter, self).__init__(
            primary_key=False,
            partition_key=False,
            index=index,
            db_field=db_field,
            default=0,
            required=required,
        )


class DateTime(Column):
    """
    Stores a datetime value
    """
    db_type = 'timestamp'

    def to_python(self, value):
        if value is None:
            return
        if isinstance(value, datetime):
            return value
        elif isinstance(value, date):
            return datetime(*(value.timetuple()[:6]))
        try:
            return datetime.utcfromtimestamp(value)
        except TypeError:
            return datetime.utcfromtimestamp(DateType.deserialize(value))

    def to_database(self, value):
        value = super(DateTime, self).to_database(value)
        if value is None:
            return
        if not isinstance(value, datetime):
            if isinstance(value, date):
                value = datetime(value.year, value.month, value.day)
            else:
                raise ValidationError("{} '{}' is not a datetime object".format(self.column_name, value))
        epoch = datetime(1970, 1, 1, tzinfo=value.tzinfo)
        offset = epoch.tzinfo.utcoffset(epoch).total_seconds() if epoch.tzinfo else 0

        return int(((value - epoch).total_seconds() - offset) * 1000)


class Date(Column):
    """
    *Note: this type is overloaded, and will likely be changed or removed to accommodate distinct date type
    in a future version*

    Stores a date value, with no time-of-day
    """
    db_type = 'timestamp'

    def to_python(self, value):
        if value is None:
            return
        if isinstance(value, datetime):
            return value.date()
        elif isinstance(value, date):
            return value
        try:
            return datetime.utcfromtimestamp(value).date()
        except TypeError:
            return datetime.utcfromtimestamp(DateType.deserialize(value)).date()

    def to_database(self, value):
        value = super(Date, self).to_database(value)
        if value is None:
            return
        if isinstance(value, datetime):
            value = value.date()
        if not isinstance(value, date):
            raise ValidationError("{} '{}' is not a date object".format(self.column_name, repr(value)))

        return int((value - date(1970, 1, 1)).total_seconds() * 1000)


class UUID(Column):
    """
    Stores a type 1 or 4 UUID
    """
    db_type = 'uuid'

    re_uuid = re.compile(r'[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}')

    def validate(self, value):
        val = super(UUID, self).validate(value)
        if val is None:
            return
        from uuid import UUID as _UUID
        if isinstance(val, _UUID):
            return val
        if isinstance(val, six.string_types) and self.re_uuid.match(val):
            return _UUID(val)
        raise ValidationError("{} {} is not a valid uuid".format(self.column_name, value))

    def to_python(self, value):
        return self.validate(value)

    def to_database(self, value):
        return self.validate(value)

from uuid import UUID as pyUUID, getnode


class TimeUUID(UUID):
    """
    UUID containing timestamp
    """

    db_type = 'timeuuid'

    @classmethod
    def from_datetime(self, dt):
        """
        generates a UUID for a given datetime

        :param dt: datetime
        :type dt: datetime
        :return:
        """
        global _last_timestamp

        epoch = datetime(1970, 1, 1, tzinfo=dt.tzinfo)
        offset = epoch.tzinfo.utcoffset(epoch).total_seconds() if epoch.tzinfo else 0
        timestamp = (dt - epoch).total_seconds() - offset

        node = None
        clock_seq = None

        nanoseconds = int(timestamp * 1e9)
        timestamp = int(nanoseconds // 100) + 0x01b21dd213814000

        if clock_seq is None:
            import random
            clock_seq = random.randrange(1 << 14)  # instead of stable storage
        time_low = timestamp & 0xffffffff
        time_mid = (timestamp >> 32) & 0xffff
        time_hi_version = (timestamp >> 48) & 0x0fff
        clock_seq_low = clock_seq & 0xff
        clock_seq_hi_variant = (clock_seq >> 8) & 0x3f
        if node is None:
            node = getnode()
        return pyUUID(fields=(time_low, time_mid, time_hi_version,
                              clock_seq_hi_variant, clock_seq_low, node), version=1)


class Boolean(Column):
    """
    Stores a boolean True or False value
    """
    db_type = 'boolean'

    def validate(self, value):
        """ Always returns a Python boolean. """
        value = super(Boolean, self).validate(value)

        if value is not None:
            value = bool(value)

        return value

    def to_python(self, value):
        return self.validate(value)


class BaseFloat(Column):
    def validate(self, value):
        value = super(BaseFloat, self).validate(value)
        if value is None:
            return
        try:
            return float(value)
        except (TypeError, ValueError):
            raise ValidationError("{} {} is not a valid float".format(self.column_name, value))

    def to_python(self, value):
        return self.validate(value)

    def to_database(self, value):
        return self.validate(value)


class Float(BaseFloat):
    """
    Stores a single-precision floating-point value
    """
    db_type = 'float'

    def __init__(self, double_precision=None, **kwargs):
        if double_precision is None or bool(double_precision):
            msg = "Float(double_precision=True) is deprecated. Use Double() type instead."
            double_precision = True
            warnings.warn(msg, DeprecationWarning)
            log.warning(msg)

        self.db_type = 'double' if double_precision else 'float'

        super(Float, self).__init__(**kwargs)


class Double(BaseFloat):
    """
    Stores a double-precision floating-point value
    """
    db_type = 'double'


class Decimal(Column):
    """
    Stores a variable precision decimal value
    """
    db_type = 'decimal'

    def validate(self, value):
        from decimal import Decimal as _Decimal
        from decimal import InvalidOperation
        val = super(Decimal, self).validate(value)
        if val is None:
            return
        try:
            return _Decimal(val)
        except InvalidOperation:
            raise ValidationError("{} '{}' can't be coerced to decimal".format(self.column_name, val))

    def to_python(self, value):
        return self.validate(value)

    def to_database(self, value):
        return self.validate(value)


class BaseContainerColumn(Column):
    """
    Base Container type for collection-like columns.

    https://cassandra.apache.org/doc/cql3/CQL.html#collections
    """

    def __init__(self, value_type, **kwargs):
        """
        :param value_type: a column class indicating the types of the value
        """
        inheritance_comparator = issubclass if isinstance(value_type, type) else isinstance
        if not inheritance_comparator(value_type, Column):
            raise ValidationError('value_type must be a column class')
        if inheritance_comparator(value_type, BaseContainerColumn):
            raise ValidationError('container types cannot be nested')
        if value_type.db_type is None:
            raise ValidationError('value_type cannot be an abstract column type')

        if isinstance(value_type, type):
            self.value_type = value_type
            self.value_col = self.value_type()
        else:
            self.value_col = value_type
            self.value_type = self.value_col.__class__

        super(BaseContainerColumn, self).__init__(**kwargs)

    def validate(self, value):
        value = super(BaseContainerColumn, self).validate(value)
        # It is dangerous to let collections have more than 65535.
        # See: https://issues.apache.org/jira/browse/CASSANDRA-5428
        if value is not None and len(value) > 65535:
            raise ValidationError("{} Collection can't have more than 65535 elements.".format(self.column_name))
        return value

    def _val_is_null(self, val):
        return not val

    @property
    def sub_columns(self):
        return [self.value_col]


class BaseContainerQuoter(ValueQuoter):

    def __nonzero__(self):
        return bool(self.value)


class Set(BaseContainerColumn):
    """
    Stores a set of unordered, unique values

    http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_set_t.html
    """
    class Quoter(BaseContainerQuoter):

        def __str__(self):
            cq = cql_quote
            return '{' + ', '.join([cq(v) for v in self.value]) + '}'

    def __init__(self, value_type, strict=True, default=set, **kwargs):
        """
        :param value_type: a column class indicating the types of the value
        :param strict: sets whether non set values will be coerced to set
            type on validation, or raise a validation error, defaults to True
        """
        self.strict = strict
        self.db_type = 'set<{}>'.format(value_type.db_type)
        super(Set, self).__init__(value_type, default=default, **kwargs)

    def validate(self, value):
        val = super(Set, self).validate(value)
        if val is None:
            return
        types = (set,) if self.strict else (set, list, tuple)
        if not isinstance(val, types):
            if self.strict:
                raise ValidationError('{} {} is not a set object'.format(self.column_name, val))
            else:
                raise ValidationError('{} {} cannot be coerced to a set object'.format(self.column_name, val))

        if None in val:
            raise ValidationError("{} None not allowed in a set".format(self.column_name))

        return {self.value_col.validate(v) for v in val}

    def to_python(self, value):
        if value is None:
            return set()
        return {self.value_col.to_python(v) for v in value}

    def to_database(self, value):
        if value is None:
            return None

        if isinstance(value, self.Quoter):
            return value
        return self.Quoter({self.value_col.to_database(v) for v in value})


class List(BaseContainerColumn):
    """
    Stores a list of ordered values

    http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_list_t.html
    """
    class Quoter(BaseContainerQuoter):

        def __str__(self):
            cq = cql_quote
            return '[' + ', '.join([cq(v) for v in self.value]) + ']'

        def __nonzero__(self):
            return bool(self.value)

    def __init__(self, value_type, default=list, **kwargs):
        """
        :param value_type: a column class indicating the types of the value
        """
        self.db_type = 'list<{}>'.format(value_type.db_type)
        return super(List, self).__init__(value_type=value_type, default=default, **kwargs)

    def validate(self, value):
        val = super(List, self).validate(value)
        if val is None:
            return
        if not isinstance(val, (set, list, tuple)):
            raise ValidationError('{} {} is not a list object'.format(self.column_name, val))
        if None in val:
            raise ValidationError("{} None is not allowed in a list".format(self.column_name))
        return [self.value_col.validate(v) for v in val]

    def to_python(self, value):
        if value is None:
            return []
        return [self.value_col.to_python(v) for v in value]

    def to_database(self, value):
        if value is None:
            return None
        if isinstance(value, self.Quoter):
            return value
        return self.Quoter([self.value_col.to_database(v) for v in value])


class Map(BaseContainerColumn):
    """
    Stores a key -> value map (dictionary)

    http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_map_t.html
    """
    class Quoter(BaseContainerQuoter):

        def __str__(self):
            cq = cql_quote
            return '{' + ', '.join([cq(k) + ':' + cq(v) for k, v in self.value.items()]) + '}'

        def get(self, key):
            return self.value.get(key)

        def keys(self):
            return self.value.keys()

        def items(self):
            return self.value.items()

    def __init__(self, key_type, value_type, default=dict, **kwargs):
        """
        :param key_type: a column class indicating the types of the key
        :param value_type: a column class indicating the types of the value
        """

        self.db_type = 'map<{}, {}>'.format(key_type.db_type, value_type.db_type)

        inheritance_comparator = issubclass if isinstance(key_type, type) else isinstance
        if not inheritance_comparator(key_type, Column):
            raise ValidationError('key_type must be a column class')
        if inheritance_comparator(key_type, BaseContainerColumn):
            raise ValidationError('container types cannot be nested')
        if key_type.db_type is None:
            raise ValidationError('key_type cannot be an abstract column type')

        if isinstance(key_type, type):
            self.key_type = key_type
            self.key_col = self.key_type()
        else:
            self.key_col = key_type
            self.key_type = self.key_col.__class__
        super(Map, self).__init__(value_type, default=default, **kwargs)

    def validate(self, value):
        val = super(Map, self).validate(value)
        if val is None:
            return
        if not isinstance(val, dict):
            raise ValidationError('{} {} is not a dict object'.format(self.column_name, val))
        return {self.key_col.validate(k): self.value_col.validate(v) for k, v in val.items()}

    def to_python(self, value):
        if value is None:
            return {}
        if value is not None:
            return {self.key_col.to_python(k): self.value_col.to_python(v) for k, v in value.items()}

    def to_database(self, value):
        if value is None:
            return None
        if isinstance(value, self.Quoter):
            return value
        return self.Quoter({self.key_col.to_database(k): self.value_col.to_database(v) for k, v in value.items()})

    @property
    def sub_columns(self):
        return [self.key_col, self.value_col]


class UDTValueManager(BaseValueManager):
    @property
    def changed(self):
        return self.value != self.previous_value or self.value.has_changed_fields()

    def reset_previous_value(self):
        self.value.reset_changed_fields()
        self.previous_value = copy(self.value)


class UserDefinedType(Column):
    """
    User Defined Type column

    http://www.datastax.com/documentation/cql/3.1/cql/cql_using/cqlUseUDT.html

    These columns are represented by a specialization of :class:`cassandra.cqlengine.usertype.UserType`.

    Please see :ref:`user_types` for examples and discussion.
    """

    value_manager = UDTValueManager

    def __init__(self, user_type, **kwargs):
        """
        :param type user_type: specifies the :class:`~.UserType` model of the column
        """
        self.user_type = user_type
        self.db_type = "frozen<%s>" % user_type.type_name()
        super(UserDefinedType, self).__init__(**kwargs)

    @property
    def sub_columns(self):
        return list(self.user_type._fields.values())


def resolve_udts(col_def, out_list):
    for col in col_def.sub_columns:
        resolve_udts(col, out_list)
    if isinstance(col_def, UserDefinedType):
        out_list.append(col_def.user_type)


class _PartitionKeysToken(Column):
    """
    virtual column representing token of partition columns.
    Used by filter(pk__token=Token(...)) filters
    """

    def __init__(self, model):
        self.partition_columns = model._partition_keys.values()
        super(_PartitionKeysToken, self).__init__(partition_key=True)

    @property
    def db_field_name(self):
        return 'token({})'.format(', '.join(['"{}"'.format(c.db_field_name) for c in self.partition_columns]))

    def to_database(self, value):
        from cqlengine.functions import Token
        assert isinstance(value, Token)
        value.set_columns(self.partition_columns)
        return value

    def get_cql(self):
        return "token({})".format(", ".join(c.cql for c in self.partition_columns))
