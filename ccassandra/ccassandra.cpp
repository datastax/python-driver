#include <map>

#include "python.hpp"
#include "marshal.hpp"
#include "cql_types.hpp"


typedef std::map<PyObject*, pyccassandra::CqlType*> PyCqlTypeMap;


/// Mapping of pure-Python Cassandra driver CQL types to internal types.
static PyCqlTypeMap pyCassandraCqlTypeMapping;


static PyObject* ccassandra_deserialize_cqltype(PyObject*,
                                                PyObject* args,
                                                PyObject* kwargs)
{
    Py_ssize_t dataLength;
    const unsigned char* data;
    PyObject* pyCqlType;
    const char* keywords[] =
    {
        "data",
        "cql_type",
        NULL,
    };

    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "s#O",
                                     const_cast<char**>(keywords),
                                     &data,
                                     &dataLength,
                                     &pyCqlType))
        return NULL;

    // Find the corresponding CQL type implementation.
    const PyCqlTypeMap::iterator it =
        pyCassandraCqlTypeMapping.find(pyCqlType);

    if (it == pyCassandraCqlTypeMapping.end())
    {
        PyErr_SetString(PyExc_ValueError, "unsupported CQL type");
        return NULL;
    }

    // Deserialize.
    pyccassandra::Buffer buffer(data, Py_ssize_t(dataLength));
    return it->second->Deserialize(buffer);
}


static PyMethodDef CcassandraMethods[] =
{
    {
        "deserialize_cqltype",
        (PyCFunction)ccassandra_deserialize_cqltype,
        METH_VARARGS | METH_KEYWORDS,
        "Deserialize a CQL type"
    },
    {
        NULL,
        NULL,
        0,
        NULL
    }
};


PyMODINIT_FUNC initccassandra(void)
{
    // Import the types on which we depend.
#define IMPORT_PYTHON_MODULE(_to, _name)            \
    PyObject* _to = PyImport_ImportModule(_name);   \
    if (!_to)                                       \
        return
#define STORE_ATTR(_to, _from, _name)                       \
    PyObject* _to = PyObject_GetAttrString(_from, _name);   \
    if (!_to) \
        return

    IMPORT_PYTHON_MODULE(pyUuid, "uuid");
    STORE_ATTR(pyUuidUuid, pyUuid, "UUID");

    IMPORT_PYTHON_MODULE(pyDatetime, "datetime");
    STORE_ATTR(pyDatetimeDatetime, pyDatetime, "datetime");

    IMPORT_PYTHON_MODULE(pyDecimal, "decimal");
    STORE_ATTR(pyDecimalDecimal, pyDecimal, "Decimal");
    
    // Import the pure Python Cassandra driver from Datastax and set up the
    // mapping.
    IMPORT_PYTHON_MODULE(pyCassandraCqltypes, "cassandra.cqltypes");

#define MAP_SIMPLE_CQL_TYPE(_py, _cpp)                                  \
    {                                                                   \
        STORE_ATTR(pyCqlType, pyCassandraCqltypes, _py);                \
        pyCassandraCqlTypeMapping[pyCqlType] = new pyccassandra::_cpp(); \
    }

    MAP_SIMPLE_CQL_TYPE("Int32Type", CqlInt32Type);
    MAP_SIMPLE_CQL_TYPE("LongType", CqlLongType);
    MAP_SIMPLE_CQL_TYPE("FloatType", CqlFloatType);
    MAP_SIMPLE_CQL_TYPE("DoubleType", CqlDoubleType);
    MAP_SIMPLE_CQL_TYPE("BooleanType", CqlBooleanType);
    MAP_SIMPLE_CQL_TYPE("BytesType", CqlBytesType);
    MAP_SIMPLE_CQL_TYPE("AsciiType", CqlAsciiType);
    MAP_SIMPLE_CQL_TYPE("UTF8Type", CqlUtf8Type);
    MAP_SIMPLE_CQL_TYPE("VarcharType", CqlVarcharType);
    MAP_SIMPLE_CQL_TYPE("CounterColumnType", CqlCounterColumnType);
    MAP_SIMPLE_CQL_TYPE("InetAddressType", CqlInetAddressType);
    MAP_SIMPLE_CQL_TYPE("IntegerType", CqlIntegerType);

#undef MAP_SIMPLE_CQL_TYPE

    {
        STORE_ATTR(pyUuidCqlType, pyCassandraCqltypes, "UUIDType");
        STORE_ATTR(pyTimeUuidCqlType, pyCassandraCqltypes, "TimeUUIDType");
        pyccassandra::CqlUuidType* type =
            new pyccassandra::CqlUuidType(pyUuidUuid);
        pyCassandraCqlTypeMapping[pyUuidCqlType] = type;
        pyCassandraCqlTypeMapping[pyTimeUuidCqlType] = type;
    }

    {
        STORE_ATTR(pyDateCqlType, pyCassandraCqltypes, "DateType");
        STORE_ATTR(pyTimestampCqlType, pyCassandraCqltypes, "TimestampType");
        pyccassandra::CqlType* type =
            new pyccassandra::CqlDateType(pyDatetimeDatetime);
        pyCassandraCqlTypeMapping[pyDateCqlType] = type;
        pyCassandraCqlTypeMapping[pyTimestampCqlType] = type;
    }

    {
        STORE_ATTR(pyCqlType, pyCassandraCqltypes, "DecimalType");
        pyCassandraCqlTypeMapping[pyCqlType] =
            new pyccassandra::CqlDecimalType(pyDecimalDecimal);
    }

    // Initialize the module.
    PyObject* module;

    if (!(module = Py_InitModule("ccassandra", CcassandraMethods)))
        return;
}
