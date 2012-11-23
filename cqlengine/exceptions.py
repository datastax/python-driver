#cqlengine exceptions
class CQLEngineException(BaseException): pass
class ModelException(CQLEngineException): pass
class ValidationError(CQLEngineException): pass

