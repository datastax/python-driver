from cassandra.bytesio cimport BytesIOReader

def test_read1(assert_equal, assert_raises):
    cdef BytesIOReader reader = BytesIOReader(b'abcdef')
    assert_equal(reader.read(2)[:2], b'ab')
    assert_equal(reader.read(2)[:2], b'cd')
    assert_equal(reader.read(0)[:0], b'')
    assert_equal(reader.read(2)[:2], b'ef')

def test_read2(assert_equal, assert_raises):
    cdef BytesIOReader reader = BytesIOReader(b'abcdef')
    reader.read(5)
    reader.read(1)

def test_read3(assert_equal, assert_raises):
    cdef BytesIOReader reader = BytesIOReader(b'abcdef')
    reader.read(6)

def test_read_eof(assert_equal, assert_raises):
    cdef BytesIOReader reader = BytesIOReader(b'abcdef')
    reader.read(5)
    # cannot convert reader.read to an object, do it manually
    # assert_raises(EOFError, reader.read, 2)
    try:
        reader.read(2)
    except EOFError:
        pass
    else:
        raise Exception("Expected an EOFError")
    reader.read(1) # see that we can still read this
