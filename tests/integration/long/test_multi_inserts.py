
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import os
from cassandra.cluster import Cluster


class StressInsertsTests(unittest.TestCase):

    '''
    Test case for PYTHON-124: Repeated inserts may exhaust all connections
    causing NoConnectionsAvailable, in_flight never decreased
    '''

    def setUp(self):
        """
        USE test1rf;
        DROP TABLE IF EXISTS cd;
        CREATE TABLE cd (
        id text,
        title text,
        tracks list<text>,
        PRIMARY KEY (id)
        );
        """
        self.cluster=Cluster()
        self.session = self.cluster.connect('test1rf')

        ddl1 = '''
            DROP TABLE IF EXISTS cd'''
        self.session.execute(ddl1)

        ddl2 = '''
            CREATE TABLE test1rf.cd (
                id text,
                title text,
                tracks list<text>,
                PRIMARY KEY (id) )'''
        self.session.execute(ddl2)

    def tearDown(self):
        """
        DROP TABLE test1rf.cd
        Shutdown cluster
        """
        self.session.execute("DROP TABLE test1rf.cd")
        self.cluster.shutdown()

    def test_in_flight_is_one(self):
        """
        Verify that in_flight value stays equal to one while doing multiple inserts.
        The number of inserts can be set through INSERTS_ITERATIONS environmental variable.
        Default value is 1000000.
        """
        prepared = self.session.prepare ("INSERT INTO cd (id,title,tracks) VALUES (?,?,?)")
        iterations = int(os.getenv("INSERT_ITERATIONS", 1000000))
        i=0
        no_leaking_connections = True
        while i < iterations:
            titul = "Title#%d" % (i,)
            id = str(i)
            tracks = ['a','b','c' ]
            bound = prepared.bind( (id,titul,tracks), )
            self.session.execute( bound )
            for pool in self.session._pools.values():
                if not no_leaking_connections:
                    break
                for conn in pool._connections:
                    if not no_leaking_connections:
                        break
                    if conn.in_flight > 1:
                        print self.session.get_pool_state()
                        no_leaking_connections = False
                        break

            if not no_leaking_connections:
                break
            i=i+1

        self.assertTrue(no_leaking_connections, 'Detected leaking connection')