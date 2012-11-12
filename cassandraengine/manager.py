#manager class

from cassandraengine.query import QuerySet

class Manager(object):

    def __init__(self, model):
        super(Manager, self).__init__()
        self.model = model

    @property
    def column_family_name(self):
        """
        Returns the column family name if it's been defined
        otherwise, it creates it from the module and class name
        """
        if self.model.db_name:
            return self.model.db_name
        cf_name = self.model.__module__ + '.' + self.model.__name__
        cf_name = cf_name.replace('.', '_')
        #trim to less than 48 characters or cassandra will complain
        cf_name = cf_name[-48:]
        return cf_name

    def __call__(self, **kwargs):
        """ filter shortcut """
        return self.filter(**kwargs)

    def find(self, pk):
        """
        Returns the row corresponding to the primary key value given
        """
        values = QuerySet(self.model).find(pk)
        if values is None: return

        #change the column names to model names
        #in case they are different
        field_dict = {}
        db_map = self.model._db_map
        for key, val in values.items():
            if key in db_map:
                field_dict[db_map[key]] = val
            else:
                field_dict[key] = val
        return self.model(**field_dict)

    def all(self):
        return QuerySet(self.model).all()

    def filter(self, **kwargs):
        return QuerySet(self.model).filter(**kwargs)

    def exclude(self, **kwargs):
        return QuerySet(self.model).exclude(**kwargs)

    def create(self, **kwargs):
        return self.model(**kwargs).save()

    def delete(self, **kwargs):
        pass

    #----single instance methods----
    def _save_instance(self, instance):
        """
        The business end of save, this is called by the models
        save method and calls the Query save method. This should
        only be called by the model saving itself
        """
        QuerySet(self.model).save(instance)

    def _delete_instance(self, instance):
        """ Deletes a single instance """
        QuerySet(self.model).delete_instance(instance)

    #----column family create/delete----
    def _create_column_family(self):
        QuerySet(self.model)._create_column_family()

    def _delete_column_family(self):
        QuerySet(self.model)._delete_column_family()
