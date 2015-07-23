from libc.stdint cimport (int8_t, int16_t, int32_t, int64_t,
                          uint8_t, uint16_t, uint32_t, uint64_t)

cpdef bytes int64_pack(int64_t x)
cpdef bytes int32_pack(int32_t x)
cpdef bytes int16_pack(int16_t x)
cpdef bytes int8_pack(int8_t x)

cpdef int64_t int64_unpack(const char *buf)
cpdef int32_t int32_unpack(const char *buf)
cpdef int16_t int16_unpack(const char *buf)
cpdef int8_t  int8_unpack(const char *buf)

cpdef bytes uint64_pack(uint64_t x)
cpdef bytes uint32_pack(uint32_t x)
cpdef bytes uint16_pack(uint16_t x)
cpdef bytes uint8_pack(uint8_t x)

cpdef uint64_t uint64_unpack(const char *buf)
cpdef uint32_t uint32_unpack(const char *buf)
cpdef uint16_t uint16_unpack(const char *buf)
cpdef uint8_t  uint8_unpack(const char *buf)

cpdef bytes double_pack(double x)
cpdef bytes float_pack(float x)

cpdef double double_unpack(const char *buf)
cpdef float float_unpack(const char *buf)

