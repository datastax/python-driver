#cqlengine exceptions
class CQLEngineException(Exception): pass
class ModelException(CQLEngineException): pass
class ValidationError(CQLEngineException): pass

class UndefinedKeyspaceException(CQLEngineException): pass
