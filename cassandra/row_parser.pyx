# -- cython: profile=True

from cassandra.parsing cimport ParseDesc, ColumnParser
from cassandra.deserializers import make_deserializers

include "ioutils.pyx"

def make_recv_results_rows(ColumnParser colparser):
    def recv_results_rows(cls, f, int protocol_version, user_type_map):
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
