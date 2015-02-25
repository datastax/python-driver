#ifndef __PYCCASSANDRA_BUFFER
#define __PYCCASSANDRA_BUFFER
#include <cstddef>


namespace pyccassandra
{
    /// Buffer.

    /// Internal buffer structure for accessing raw data safely. The data
    /// passed to the buffer must remain available for the entire life time of
    /// the buffer.
    class Buffer
    {
    public:
        /// Initialize a buffer.

        /// @param data Data.
        /// @param size Data size in octets.
        Buffer(const unsigned char* data, std::size_t size)
            :   _position(data),
                _residual(size)
        {

        }


        /// Current data position.
        inline const unsigned char* Position() const
        {
            return _position;
        }


        /// Residual data left in the buffer.
        inline const std::size_t& Residual() const
        {
            return _residual;
        }


        /// Advance the current data position.
        inline void Advance(const std::size_t& amount)
        {
            _position += amount;
            _residual -= amount;
        }


        /// Consume an amount of data from the buffer.

        /// @param amount Amount.
        /// @returns a pointer to start of the consumed data if sufficient data
        /// is available, otherwise NULL.
        inline const unsigned char* Consume(const std::size_t& amount)
        {
            if (_residual < amount)
                return NULL;

            const unsigned char* pos = _position;
            _position += amount;
            _residual -= amount;
            return pos;
        }
    private:
        Buffer(Buffer&);
        Buffer& operator =(Buffer&);


        /// Data position.
        const unsigned char* _position;


        /// Residual data size including the current data position.
        std::size_t _residual;
    };
}
#endif
