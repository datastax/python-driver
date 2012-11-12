
from cassandraengine import columns
from cassandraengine.exceptions import ModelException
from cassandraengine.manager import Manager

class BaseModel(object):
    """
    The base model class, don't inherit from this, inherit from Model, defined below
    """

    #table names will be generated automatically from it's model name and package
    #however, you can alse define them manually here
    db_name = None

    def __init__(self, **values):
        #set columns from values
        for k,v in values.items():
            if k in self._columns:
                setattr(self, k, v)
            else:
                self._dynamic_columns[k] = v

        #set excluded columns to None
        for k in self._columns.keys():
            if k not in values:
                setattr(self, k, None)

    @classmethod
    def find(cls, pk):
        """ Loads a document by it's primary key """
        cls.objects.find(pk)

    @property
    def pk(self):
        """ Returns the object's primary key """
        return getattr(self, self._pk_name)

    #dynamic column methods
    def __getitem__(self, key):
        return self._dynamic_columns[key]

    def __setitem__(self, key, val):
        self._dynamic_columns[key] = val

    def __delitem__(self, key):
        del self._dynamic_columns[key]

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
        _columns = {}
        pk_name = None

        def _transform_column(col_name, col_obj):
            _columns[col_name] = col_obj
            col_obj.set_db_name(col_name)
            allow_delete = not col_obj.primary_key
            attrs[col_name] = col_obj.get_property(allow_delete=allow_delete)

        #transform column definitions
        for k,v in attrs.items():
            if isinstance(v, columns.BaseColumn):
                if v.is_primary_key:
                    if pk_name:
                        raise ModelException("More than one primary key defined for {}".format(name))
                    pk_name = k
                _transform_column(k,v)

        #set primary key if it's not already defined
        if not pk_name:
            k,v = 'id', columns.UUID(primary_key=True)
            _transform_column(k,v)
            pk_name = k
        
        #setup primary key shortcut
        if pk_name != 'pk':
            attrs['pk'] = _columns[pk_name].get_property(allow_delete=False)

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

        attrs['_columns'] = _columns
        attrs['_db_map'] = db_map
        attrs['_pk_name'] = pk_name
        attrs['_dynamic_columns'] = {}

        klass = super(ModelMetaClass, cls).__new__(cls, name, bases, attrs)
        klass.objects = Manager(klass)
        return klass


class Model(BaseModel):
    """
    the db name for the column family can be set as the attribute db_name, or
    it will be genertaed from the class name
    """
    __metaclass__ = ModelMetaClass


