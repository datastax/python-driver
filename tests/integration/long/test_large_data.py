from struct import pack
import unittest

import cassandra

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.decoder import dict_factory
from cassandra.query import SimpleStatement
from tests.integration.long.utils import create_schema


# Converts an integer to an string of letters
def create_column_name(i):
    letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

    column_name = ''
    while True:
        column_name += letters[i % 10]
        i /= 10
        if not i:
            break

    return column_name


class LargeDataTests(unittest.TestCase):

    def setUp(self):
        self.keyspace = 'large_data'

    def wide_rows(self, session, table, key):
        # Write
        for i in range(100000):
            statement = SimpleStatement('INSERT INTO %s (k, i) VALUES (%s, %s)'
                                        % (table, key, i),
                                        consistency_level=ConsistencyLevel.QUORUM)
            session.execute(statement)

        # Read
        results = session.execute('SELECT i FROM %s WHERE k=%s' % (table, key))

        # Verify
        i = 0
        for row in results:
            self.assertEqual(row['i'], i)
            i += 1


    def wide_batch_rows(self, session, table, key):
        # Write
        statement = 'BEGIN BATCH '
        for i in range(2000):
            statement += 'INSERT INTO %s (k, i) VALUES (%s, %s) ' % (table, key, i)
        statement += 'APPLY BATCH'
        statement = SimpleStatement(statement, consistency_level=ConsistencyLevel.QUORUM)
        session.execute(statement)

        # Read
        results = session.execute('SELECT i FROM %s WHERE k=%s' % (table, key))

        # Verify
        i = 0
        for row in results:
            self.assertEqual(row['i'], i)
            i += 1



    def wide_byte_rows(self, session, table, key):
        # Build small ByteBuffer sample
        bb = '0xCAFE'

        # Write
        for i in range(1000000):
            statement = SimpleStatement('INSERT INTO %s (k, i) VALUES (%s, %s)'
                                        % (table, key, str(bb)),
                                        consistency_level=ConsistencyLevel.QUORUM)
            session.execute(statement)

        # Read
        results = session.execute('SELECT i FROM %s WHERE k=%s' % (table, key))

        # Verify
        bb = pack('>H', 0xCAFE)
        i = 0
        for row in results:
            self.assertEqual(row['i'], bb)
            i += 1


    def large_text(self, session, table, key):
        # Create ultra-long text
        text = ''
        for i in range(1000000):
            text += str(i)

        # Write
        session.execute(SimpleStatement("INSERT INTO %s (k, txt) VALUES (%s, '%s')"
                                        % (table, key, text),
                                        consistency_level=ConsistencyLevel.QUORUM))

        # Read
        result = session.execute('SELECT * FROM %s WHERE k=%s' % (table, key))

        # Verify
        for row in result:
            self.assertEqual(row['txt'], text)


    def wide_table(self, session, table, key):
        # Write
        insert_statement = 'INSERT INTO %s (key, '

        column_names = []
        for i in range(330):
            column_names.append(create_column_name(i))
        insert_statement += ', '.join(column_names)

        insert_statement += ') VALUES (%s, '

        values = []
        for i in range(330):
            values.append(str(i))
        insert_statement += ', '.join(values)

        insert_statement += ')'

        insert_statement = insert_statement % (table, key)

        session.execute(SimpleStatement(insert_statement, consistency_level=ConsistencyLevel.QUORUM))

        # Read
        result = session.execute('SELECT * FROM %s WHERE key=%s' % (table, key))

        # Verify
        for row in result:
            for i in range(330):
                self.assertEqual(row[create_column_name(i)], i)




    def test_wide_rows(self):
        table = 'wide_rows'

        cluster = Cluster()
        session = cluster.connect()
        session.row_factory = dict_factory

        create_schema(session, self.keyspace)
        session.execute('CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))' % table)

        self.wide_rows(session, table, 0)


    def test_wide_batch_rows(self):
        table = 'wide_batch_rows'

        cluster = Cluster()
        session = cluster.connect()
        session.row_factory = dict_factory

        create_schema(session, self.keyspace)
        session.execute('CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))' % table)

        self.wide_batch_rows(session, table, 0)


    def test_wide_byte_rows(self):
        table = 'wide_byte_rows'

        cluster = Cluster()
        session = cluster.connect()
        session.row_factory = dict_factory

        create_schema(session, self.keyspace)
        session.execute('CREATE TABLE %s (k INT, i BLOB, PRIMARY KEY(k, i))' % table)

        self.wide_byte_rows(session, table, 0)


    def test_large_text(self):
        table = 'large_text'

        cluster = Cluster()
        session = cluster.connect()
        session.row_factory = dict_factory

        create_schema(session, self.keyspace)
        session.execute('CREATE TABLE %s (k int PRIMARY KEY, txt text)' % table)

        self.large_text(session, table, 0)


    def test_wide_table(self):
        table = 'wide_table'

        cluster = Cluster()
        session = cluster.connect()
        session.row_factory = dict_factory

        create_schema(session, self.keyspace)
        table_declaration = 'CREATE TABLE %s (key INT PRIMARY KEY, '
        column_names = []
        for i in range(330):
            column_names.append(create_column_name(i))
        table_declaration += ' INT, '.join(column_names)
        table_declaration += ' INT)'
        session.execute(table_declaration % table)

        self.wide_table(session, table, 0)
