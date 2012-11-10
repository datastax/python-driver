
class BaseDocument(object):
    pass

class DocumentMetaClass(type):

    def __new__(cls, name, bases, attrs):
        """
        """
        #TODO:assert primary key exists
        #TODO:get column family name
        #TODO:check that all resolved column family names are unique
        return super(DocumentMetaClass, cls).__new__(cls, name, bases, attrs)

class Document(BaseDocument):
    """
    """
    __metaclass__ = DocumentMetaClass
