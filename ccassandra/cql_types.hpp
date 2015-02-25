#ifndef __PYCCASSANDRA_CQLTYPES
#define __PYCCASSANDRA_CQLTYPES
#include "buffer.hpp"
#include "python.hpp"


namespace pyccassandra
{
    /// CQL type.
    class CqlType
    {
    public:
        CqlType() {}
        virtual ~CqlType();


        /// Deserialize the data type from a buffer.

        /// @param buffer Buffer.
        /// @returns a pointer to the deserialized Python object representation
        /// of the value if successful, otherwise NULL, in which case the
        /// proper Python exception has been set.
        virtual PyObject* Deserialize(Buffer& buffer) = 0;
    private:
        CqlType(CqlType&);
        CqlType& operator =(CqlType&);
    };


#define DECLARE_SIMPLE_CQL_TYPE_CLASS(_cls)                 \
    class _cls                                              \
        :   public CqlType                                  \
    {                                                       \
    public:                                                 \
        _cls()                                              \
            :   CqlType()                                   \
        {}                                                  \
        virtual ~_cls() {}                                  \
        virtual PyObject* Deserialize(Buffer& buffer);      \
    }


    /// 32-bit signed integer CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlInt32Type);


    /// 64-bit signed integer CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlLongType);


    /// Counter column CQL type.
    typedef CqlLongType CqlCounterColumnType;


    /// 32-bit floating point CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlFloatType);


    /// 64-bit floating point CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlDoubleType);


    /// Boolean CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlBooleanType);


    /// Bytes CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlBytesType);


    /// ASCII CQL type.
    typedef CqlBytesType CqlAsciiType;


    /// UTF-8 CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlUtf8Type);


    /// Varchar CQL type.
    typedef CqlUtf8Type CqlVarcharType;


    /// Inet address CQL type.
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlInetAddressType);


    /// Integer CQL type (varint.)
    DECLARE_SIMPLE_CQL_TYPE_CLASS(CqlIntegerType);


    /// UUID CQL type.
    class CqlUuidType
        :   public CqlType
    {
    public:
        CqlUuidType(PyObject* pythonUuidType)
            :   CqlType(),
                _pythonUuidType(pythonUuidType)
        {}
        virtual ~CqlUuidType() {}
        virtual PyObject* Deserialize(Buffer& buffer);
    private:
        PyObject* _pythonUuidType;
    };


    /// Date CQL type.
    class CqlDateType
        :   public CqlType
    {
    public:
        CqlDateType(PyObject* pythonDatetimeType)
            :   CqlType(),
                _pythonDatetimeUtcFromTimestamp(
                    PyObject_GetAttrString(pythonDatetimeType,
                                           "utcfromtimestamp")
                )
        {}
        virtual ~CqlDateType() {}
        virtual PyObject* Deserialize(Buffer& buffer);
    private:
        PyObject* _pythonDatetimeUtcFromTimestamp;
    };

    
    /// Timestamp CQL type.
    typedef CqlDateType CqlTimestampType;


    /// Decimal CQL type.
    class CqlDecimalType
        :   public CqlType
    {
    public:
        CqlDecimalType(PyObject* pythonDecimalType)
            :   CqlType(),
                _pythonDecimalType(pythonDecimalType)
        {}
        virtual ~CqlDecimalType() {}
        virtual PyObject* Deserialize(Buffer& buffer);
    private:
        PyObject* _pythonDecimalType;
    };


#undef DECLARE_SIMPLE_CQL_TYPE_CLASS
}
#endif
