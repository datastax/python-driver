# -- cython: profile=True

from cassandra.protocol import ResultMessage, ProtocolHandler

from cassandra.parsing cimport ParseDesc, ColumnParser
from cassandra.deserializers import make_deserializers
from cassandra.objparser import ListParser


include "ioutils.pyx"


def make_recv_results_rows(ColumnParser colparser):
    def recv_results_rows(cls, f, protocol_version, user_type_map):
        """
        Parse protocol data given as a BytesIO f into a set of columns (e.g. list of tuples)
        This is used as the recv_results_rows method of (Fast)ResultMessage
        """
        paging_state, column_metadata = cls.recv_results_metadata(f, user_type_map)

        colnames = [c[2] for c in column_metadata]
        coltypes = [c[3] for c in column_metadata]

        desc = ParseDesc(colnames, coltypes, make_deserializers(coltypes),
                         protocol_version)
        reader = BytesIOReader(f.read())
        parsed_rows = colparser.parse_rows(reader, desc)

        return (paging_state, (colnames, parsed_rows))

    return recv_results_rows


def make_protocol_handler(colparser=ListParser()):
    """
    Given a column parser to deserialize ResultMessages, return a suitable
    Cython-based protocol handler.

    There are three Cython-based protocol handlers (least to most performant):

        1. objparser.ListParser
            this parser decodes result messages into a list of tuples

        2. objparser.LazyParser
            this parser decodes result messages lazily by returning an iterator

        3. numpyparser.NumPyParser
            this parser decodes result messages into NumPy arrays

    The default is to use objparser.ListParser
    """
    # TODO: It may be cleaner to turn ProtocolHandler and ResultMessage into
    # TODO:     instances and use methods instead of class methods

    class FastResultMessage(ResultMessage):
        """
        Cython version of Result Message that has a faster implementation of
        recv_results_row.
        """
        # type_codes = ResultMessage.type_codes.copy()
        code_to_type = dict((v, k) for k, v in ResultMessage.type_codes.items())
        recv_results_rows = classmethod(make_recv_results_rows(colparser))

    class CythonProtocolHandler(ProtocolHandler):
        """
        Use FastResultMessage to decode query result message messages.
        """

        my_opcodes = ProtocolHandler.message_types_by_opcode.copy()
        my_opcodes[FastResultMessage.opcode] = FastResultMessage
        message_types_by_opcode = my_opcodes

    return CythonProtocolHandler
