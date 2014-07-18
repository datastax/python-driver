# Copyright 2013-2014 DataStax, Inc.
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
from decimal import Decimal
import datetime
from uuid import UUID
import pytz

DATA_TYPE_PRIMITIVES = [
    'ascii',
    'bigint',
    'blob',
    'boolean',
    # 'counter', counters are not allowed inside tuples
    'decimal',
    'double',
    'float',
    'inet',
    'int',
    'text',
    'timestamp',
    'timeuuid',
    'uuid',
    'varchar',
    'varint',
]

DATA_TYPE_NON_PRIMITIVE_NAMES = [
    'list',
    'set',
    'map',
    'tuple'
]


def get_sample_data():
    """
    Create a standard set of sample inputs for testing.
    """

    sample_data = {}

    for datatype in DATA_TYPE_PRIMITIVES:
        if datatype == 'ascii':
            sample_data[datatype] = 'ascii'

        elif datatype == 'bigint':
            sample_data[datatype] = 2 ** 63 - 1

        elif datatype == 'blob':
            sample_data[datatype] = bytearray(b'hello world')

        elif datatype == 'boolean':
            sample_data[datatype] = True

        elif datatype == 'counter':
            # Not supported in an insert statement
            pass

        elif datatype == 'decimal':
            sample_data[datatype] = Decimal('12.3E+7')

        elif datatype == 'double':
            sample_data[datatype] = 1.23E+8

        elif datatype == 'float':
            sample_data[datatype] = 3.4028234663852886e+38

        elif datatype == 'inet':
            sample_data[datatype] = '123.123.123.123'

        elif datatype == 'int':
            sample_data[datatype] = 2147483647

        elif datatype == 'text':
            sample_data[datatype] = 'text'

        elif datatype == 'timestamp':
            sample_data[datatype] = datetime.datetime.fromtimestamp(872835240, tz=pytz.timezone('America/New_York')).astimezone(pytz.UTC).replace(tzinfo=None)

        elif datatype == 'timeuuid':
            sample_data[datatype] = UUID('FE2B4360-28C6-11E2-81C1-0800200C9A66')

        elif datatype == 'uuid':
            sample_data[datatype] = UUID('067e6162-3b6f-4ae2-a171-2470b63dff00')

        elif datatype == 'varchar':
            sample_data[datatype] = 'varchar'

        elif datatype == 'varint':
            sample_data[datatype] = int(str(2147483647) + '000')

        else:
            raise Exception('Missing handling of %s.'  % datatype)

    return sample_data

SAMPLE_DATA = get_sample_data()

def get_sample(datatype):
    """
    Helper method to access created sample data
    """

    return SAMPLE_DATA[datatype]
