"""
Module containing the definitions and declarations (parsing.pxd) for parsers.
"""

cdef class ParseDesc:
    """Description of what structure to parse"""

    def __init__(self, colnames, coltypes, datatypes, protocol_version):
        self.colnames = colnames
        self.coltypes = coltypes
        self.datatypes = datatypes
        self.protocol_version = protocol_version
        self.rowsize = len(colnames)


cdef class ColumnParser:
    """Decode a ResultMessage into a set of columns"""

    cpdef parse_rows(self, BytesIOReader reader, ParseDesc desc):
        raise NotImplementedError


cdef class RowParser:
    """Parser for a single row"""

    cpdef unpack_row(self, BytesIOReader reader, ParseDesc desc):
        """
        Unpack a single row of data in a ResultMessage.
        """
        raise NotImplementedError
