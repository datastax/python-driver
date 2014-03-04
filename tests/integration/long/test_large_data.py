import Queue
from struct import pack
import unittest

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

    def make_session_and_keyspace(self):
        cluster = Cluster()
        session = cluster.connect()
        session.default_timeout = 20.0  # increase the default timeout
        session.row_factory = dict_factory

        create_schema(session, self.keyspace)
        return session

    def batch_futures(self, session, statement_generator):
        futures = Queue.Queue(maxsize=121)
        for i, statement in enumerate(statement_generator):
            if i > 0 and i % 120 == 0:
                # clear the existing queue
                while True:
                    try:
                        futures.get_nowait().result()
                    except Queue.Empty:
                        break

            future = session.execute_async(statement)
            futures.put_nowait(future)

        while True:
            try:
                futures.get_nowait().result()
            except Queue.Empty:
                break

    def test_wide_rows(self):
        table = 'wide_rows'
        session = self.make_session_and_keyspace()
        session.execute('CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))' % table)

        # Write via async futures
        self.batch_futures(
            session,
            (SimpleStatement('INSERT INTO %s (k, i) VALUES (0, %s)' % (table, i),
                            consistency_level=ConsistencyLevel.QUORUM)
             for i in range(1000000)))

        # Read
        results = session.execute('SELECT i FROM %s WHERE k=%s' % (table, 0))

        # Verify
        for i, row in enumerate(results):
            self.assertEqual(row['i'], i)

    def test_wide_batch_rows(self):
        table = 'wide_batch_rows'
        session = self.make_session_and_keyspace()
        session.execute('CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))' % table)

        # Write
        statement = 'BEGIN BATCH '
        for i in range(2000):
            statement += 'INSERT INTO %s (k, i) VALUES (%s, %s) ' % (table, 0, i)
        statement += 'APPLY BATCH'
        statement = SimpleStatement(statement, consistency_level=ConsistencyLevel.QUORUM)
        session.execute(statement)

        # Read
        results = session.execute('SELECT i FROM %s WHERE k=%s' % (table, 0))

        # Verify
        for i, row in enumerate(results):
            self.assertEqual(row['i'], i)

    def test_wide_byte_rows(self):
        table = 'wide_byte_rows'
        session = self.make_session_and_keyspace()
        session.execute('CREATE TABLE %s (k INT, i INT, v BLOB, PRIMARY KEY(k, i))' % table)

        # Build small ByteBuffer sample
        bb = '0xCAFE'

        # Write
        self.batch_futures(
            session,
            (SimpleStatement('INSERT INTO %s (k, i, v) VALUES (0, %s, %s)' % (table, i, str(bb)),
                            consistency_level=ConsistencyLevel.QUORUM)
             for i in range(1000000)))

        # Read
        results = session.execute('SELECT i, v FROM %s WHERE k=%s' % (table, 0))

        # Verify
        bb = pack('>H', 0xCAFE)
        for row in results:
            self.assertEqual(row['v'], bb)

    def test_large_text(self):
        table = 'large_text'
        session = self.make_session_and_keyspace()
        session.execute('CREATE TABLE %s (k int PRIMARY KEY, txt text)' % table)

        # Create ultra-long text
        text = 'a' * 1000000

        # Write
        session.execute(SimpleStatement("INSERT INTO %s (k, txt) VALUES (%s, '%s')"
                                        % (table, 0, text),
                                        consistency_level=ConsistencyLevel.QUORUM))

        # Read
        result = session.execute('SELECT * FROM %s WHERE k=%s' % (table, 0))

        # Verify
        for row in result:
            self.assertEqual(row['txt'], text)

    def test_wide_table(self):
        table = 'wide_table'
        session = self.make_session_and_keyspace()
        table_declaration = 'CREATE TABLE %s (key INT PRIMARY KEY, '
        table_declaration += ' INT, '.join(create_column_name(i) for i in range(330))
        table_declaration += ' INT)'
        session.execute(table_declaration % table)

        # Write
        insert_statement = 'INSERT INTO %s (key, '
        insert_statement += ', '.join(create_column_name(i) for i in range(330))
        insert_statement += ') VALUES (%s, '
        insert_statement += ', '.join(str(i) for i in range(330))
        insert_statement += ')'
        insert_statement = insert_statement % (table, 0)

        session.execute(SimpleStatement(insert_statement, consistency_level=ConsistencyLevel.QUORUM))

        # Read
        result = session.execute('SELECT * FROM %s WHERE key=%s' % (table, 0))

        # Verify
        for row in result:
            for i in range(330):
                self.assertEqual(row[create_column_name(i)], i)
