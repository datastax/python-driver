#ifndef __PYCASSANDRA_CONFIG
#define __PYCASSANDRA_CONFIG

// Endianness test.
//
// Currently expects either Clang or GCC, which both set __LITTLE_ENDIAN__ or
// __BIG_ENDIAN__ to 1 depending on the endianness (we don't give a crap about
// PGP byte ordering):
//
// gcc -E -dM - < /dev/null |grep ENDIAN
#if __LITTLE_ENDIAN__
    #define IS_LITTLE_ENDIAN 1
#elif __BIG_ENDIAN__
    #define IS_LITTLE_ENDIAN 0
#else
    #error Unsupported compiler or endianness.
#endif

// Type assertions.
#if CHAR_BIT != 8
    #error Only octets are supported.
#endif

#endif
