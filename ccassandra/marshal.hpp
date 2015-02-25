#ifndef __PYCASSANDRA_MARSHAL
#define __PYCASSANDRA_MARSHAL
#include <stdint.h>
#include <iostream>

#include "python.hpp"
#include "config.hpp"


namespace pyccassandra
{
    /// Unpack a 64-bit unsigned integer.
    inline uint64_t UnpackUint64(const unsigned char* data)
    {
        uint64_t val = *((uint64_t*)data);

#if IS_LITTLE_ENDIAN
        val = (((val << 8) & 0xff00ff00ff00ff00ull) |
               ((val >> 8) & 0x00ff00ff00ff00ffull));
        val = (((val << 16) & 0xffff0000ffff0000ull) |
               ((val >> 16) & 0x0000ffff0000ffffull));
        return (val << 32) | (val >> 32);
#else
        return val;
#endif
    }


    /// Unpack a 64-bit signed integer.
    inline int64_t UnpackInt64(const unsigned char* data)
    {
        int64_t val = *((int64_t*)data);

#if IS_LITTLE_ENDIAN
        val = (((val << 8) & 0xff00ff00ff00ff00ull) |
               ((val >> 8) & 0x00ff00ff00ff00ffull));
        val = (((val << 16) & 0xffff0000ffff0000ull) |
               ((val >> 16) & 0x0000ffff0000ffffull));
        return (val << 32) | ((val >> 32) & 0xffffffffull);
#else
        return val;
#endif
    }


    /// Unpack a 32-bit unsigned integer.
    inline uint32_t UnpackUint32(const unsigned char* data)
    {
        uint32_t val = *((uint32_t*)data);

#if IS_LITTLE_ENDIAN
        val = ((val << 8) & 0xff00ff00) | ((val >> 8) & 0xff00ff); 
        return (val << 16) | (val >> 16);
#else
        return val;
#endif
    }


    /// Unpack a 32-bit signed integer.
    inline int32_t UnpackInt32(const unsigned char* data)
    {
        int32_t val = *((int32_t*)data);

#if IS_LITTLE_ENDIAN
        val = ((val << 8) & 0xff00ff00) | ((val >> 8) & 0xff00ff);
        return (val << 16) | ((val >> 16) & 0xffff);
#else
        return val;
#endif
    }


    /// Unpack a 16-bit unsigned integer.
    inline uint16_t UnpackUint16(const unsigned char* data)
    {
        uint16_t val = *((uint16_t*)data);

#if IS_LITTLE_ENDIAN
        return (val << 8) | (val >> 8);
#else
        return val;
#endif
    }

    
    /// Unpack a 16-bit signed integer.
    inline int16_t UnpackInt16(const unsigned char* data)
    {
        int16_t val = *((uint16_t*)data);

#if IS_LITTLE_ENDIAN
        return (val << 8) | ((val >> 8) & 0xff);
#else
        return val;
#endif
    }


    /// Unpack an 8-bit unsigned integer.
    inline uint8_t UnpackUint8(const unsigned char* data)
    {
        return *data;
    }


    /// Unpack an 8-bit signed integer.
    inline int8_t UnpackInt8(const unsigned char* data)
    {
        return *((int8_t*)(data));
    }


    /// Unpack a 64-bit floating point number.
    inline double UnpackFloat64(const unsigned char* data)
    {
        const uint64_t representation = UnpackUint64(data);
        return *((const double*)(&representation));
    }


    /// Unpack a 32-bit floating point number.
    inline float UnpackFloat32(const unsigned char* data)
    {
        const uint32_t representation = UnpackUint32(data);
        return *((const float*)(&representation));
    }


    /// Unmarshal a 64-bit unsigned integer to a Python object.
    inline PyObject* UnmarshalUint64(const unsigned char* data)
    {
        return PyLong_FromUnsignedLongLong(UnpackUint64(data));
    }


    /// Unmarshal a 64-bit signed integer to a Python object.
    inline PyObject* UnmarshalInt64(const unsigned char* data)
    {
        return PyLong_FromLongLong(UnpackInt64(data));
    }


    /// Unmarshal a 32-bit unsigned integer to a Python object.
    inline PyObject* UnmarshalUint32(const unsigned char* data)
    {
        return PyInt_FromSize_t(UnpackUint32(data));
    }


    /// Unmarshal a 32-bit signed integer to a Python object.
    inline PyObject* UnmarshalInt32(const unsigned char* data)
    {
        return PyInt_FromLong(UnpackInt32(data));
    }


    /// Unmarshal a 16-bit unsigned integer to a Python object.
    inline PyObject* UnmarshalUint16(const unsigned char* data)
    {
        return PyInt_FromLong(UnpackUint16(data));
    }


    /// Unmarshal a 16-bit signed integer to a Python object.
    inline PyObject* UnmarshalInt16(const unsigned char* data)
    {
        return PyInt_FromLong(UnpackInt16(data));
    }


    /// Unmarshal an 8-bit unsigned integer to a Python object.
    inline PyObject* UnmarshalUint8(const unsigned char* data)
    {
        return PyInt_FromLong(UnpackUint8(data));
    }


    /// Unmarshal an 8-bit signed integer to a Python object.
    inline PyObject* UnmarshalInt8(const unsigned char* data)
    {
        return PyInt_FromLong(UnpackInt8(data));
    }


    /// Unmarshal a 64-bit floating point number to a Python object.
    inline PyObject* UnmarshalFloat64(const unsigned char* data)
    {
        return PyFloat_FromDouble(UnpackFloat64(data));
    }
    

    /// Unmarshal a 32-bit floating point number to a Python object.
    inline PyObject* UnmarshalFloat32(const unsigned char* data)
    {
        return PyFloat_FromDouble(UnpackFloat32(data));
    }


    /// Unmarshal a boolean value to a Python object.
    inline PyObject* UnmarshalBoolean(const unsigned char* data)
    {
        return (*data ? Py_True : Py_False);
    }


    /// Unmarshal a variable length integer value to a Python object.
    inline PyObject* UnmarshalVarint(const unsigned char* data,
                                     std::size_t size)
    {
        if (size == 0)
            return PyLong_FromLong(0);

        // Allocate a buffer big enough to hold the hex encoded varint. This,
        // sadly, seems to be the only reliable way of getting Python to eat a
        // variable length integer.
        char hexData[size * 2 + 2];

        // Write out the hex data as best as we can.
        std::size_t dataResidual = size;
        const unsigned char* dataPosition = data;
        char* hexPosition = hexData + 1;
        while (dataResidual--)
        {
            const unsigned char in = *dataPosition++;
            const uint8_t high = in >> 4;
            const uint8_t low = in & 0xf;

            *hexPosition++ = (high > 9 ? 87 + high : 48 + high);
            *hexPosition++ = (low > 9 ? 87 + low : 48 + low);
        }

        *hexPosition = 0;

        // Create the naive Python representation.
        PyObject* repr = PyInt_FromString(hexData + 1, NULL, 16);
        if (!repr)
            return NULL;

        // If this is a negative number, we need to do some work here.
        if (data[0] & 128)
        {
            // Let's construct the subtractor to get the resulting negative
            // number.
            hexData[0] = '1';
            memset(hexData + 1, '0', size * 2);

            PyObject* result = NULL;
            PyObject* subt = PyInt_FromString(hexData, NULL, 16);
            if (subt)
            {
                result = PyNumber_Subtract(repr, subt);
                Py_DECREF(subt);
            }

            Py_DECREF(repr);

            return result;
        }
        else
            return repr;
    }
}
#endif
