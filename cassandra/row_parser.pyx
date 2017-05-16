# Copyright 2013-2017 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from cassandra.parsing cimport ParseDesc, ColumnParser
from cassandra.obj_parser import TupleRowParser
from cassandra.deserializers import make_deserializers

include "ioutils.pyx"

def make_recv_results_rows(ColumnParser colparser):
    def recv_results_rows(cls, f, int protocol_version, user_type_map, result_metadata):
        """
        Parse protocol data given as a BytesIO f into a set of columns (e.g. list of tuples)
        This is used as the recv_results_rows method of (Fast)ResultMessage
        """
        paging_state, column_metadata = cls.recv_results_metadata(f, user_type_map)

        column_metadata = column_metadata or result_metadata

        colnames = [c[2] for c in column_metadata]
        coltypes = [c[3] for c in column_metadata]

        desc = ParseDesc(colnames, coltypes, make_deserializers(coltypes),
                         protocol_version)
        reader = BytesIOReader(f.read())
        try:
            parsed_rows = colparser.parse_rows(reader, desc)
        except Exception as e:
            # Use explicitly the TupleRowParser to display better error messages for column decoding failures
            rowparser = TupleRowParser()
            reader.buf_ptr = reader.buf
            reader.pos = 0
            rowcount = read_int(reader)
            for i in range(rowcount):
                rowparser.unpack_row(reader, desc)

        return (paging_state, coltypes, (colnames, parsed_rows))

    return recv_results_rows
