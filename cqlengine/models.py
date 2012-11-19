from collections import OrderedDict

from cqlengine import columns
from cqlengine.exceptions import ModelException
from cqlengine.manager import Manager

class BaseModel(object):
    """
    The base model class, don't inherit from this, inherit from Model, defined below
    """

    #table names will be generated automatically from it's model name and package
    #however, you can alse define them manually here
    db_name = None 

    def __init__(self, **values):
        self._values = {}
        for name, column in self._columns.items():
            value_mngr = column.value_manager(self, column, values.get(name, None))
            self._values[name] = value_mngr

    @classmethod
    def find(cls, pk):
        """ Loads a document by it's primary key """
        cls.objects.find(pk)

    @property
    def pk(self):
        """ Returns the object's primary key """
        return getattr(self, self._pk_name)

    def validate(self):
        """ Cleans and validates the field values """
        for name, col in self._columns.items():
            val = col.validate(getattr(self, name))
            setattr(self, name, val)

    def as_dict(self):
        """ Returns a map of column names to cleaned values """
        values = self._dynamic_columns or {}
        for name, col in self._columns.items():
            values[name] = col.to_database(getattr(self, name, None))
        return values

    def save(self):
        is_new = self.pk is None
        self.validate()
        self.objects._save_instance(self)
        return self

    def delete(self):
        """ Deletes this instance """
        self.objects._delete_instance(self)


class ModelMetaClass(type):

    def __new__(cls, name, bases, attrs):
        """
        """
        #move column definitions into _columns dict
        #and set default column names
        _columns = OrderedDict()
        pk_name = None

        def _transform_column(col_name, col_obj):
            _columns[col_name] = col_obj
            col_obj.set_db_name(col_name)
            #set properties
            _get = lambda self: self._values[col_name].getval()
            _set = lambda self, val: self._values[col_name].setval(val)
            _del = lambda self: self._values[col_name].delval()
            if col_obj.can_delete:
                attrs[col_name] = property(_get, _set)
            else:
                attrs[col_name] = property(_get, _set, _del)


        column_definitions = [(k,v) for k,v in attrs.items() if isinstance(v, columns.BaseColumn)]
        column_definitions = sorted(column_definitions, lambda x,y: cmp(x[1].position, y[1].position))

        #prepend primary key if none has been defined
        if not any([v.primary_key for k,v in column_definitions]):
            k,v = 'id', columns.UUID(primary_key=True)
            column_definitions = [(k,v)] + column_definitions

        #transform column definitions
        for k,v in column_definitions:
            if pk_name is None and v.primary_key:
                pk_name = k
            _transform_column(k,v)
        
        #setup primary key shortcut
        if pk_name != 'pk':
            attrs['pk'] = attrs[pk_name]

        #check for duplicate column names
        col_names = set()
        for k,v in _columns.items():
            if v.db_field in col_names:
                raise ModelException("{} defines the column {} more than once".format(name, v.db_field))
            col_names.add(v.db_field)

        #get column family name
        cf_name = attrs.pop('db_name', name)

        #create db_name -> model name map for loading
        db_map = {}
        for name, col in _columns.items():
            db_map[col.db_field] = name

        #add management members to the class
        attrs['_columns'] = _columns
        attrs['_db_map'] = db_map
        attrs['_pk_name'] = pk_name
        attrs['_dynamic_columns'] = {}

        #create the class and add a manager to it
        klass = super(ModelMetaClass, cls).__new__(cls, name, bases, attrs)
        klass.objects = Manager(klass)
        return klass


class Model(BaseModel):
    """
    the db name for the column family can be set as the attribute db_name, or
    it will be genertaed from the class name
    """
    __metaclass__ = ModelMetaClass


