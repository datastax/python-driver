#!/usr/bin/env python

import logging

log = logging.getLogger()
log.setLevel('DEBUG')
log.addHandler(logging.StreamHandler())

from cassandra.cluster import Cluster

KEYSPACE = "testkeyspace"

def main():
    c = Cluster()
    s = c.connect()

    rows = s.execute("SELECT keyspace_name FROM system.schema_keyspaces")
    if KEYSPACE in [row[0] for row in rows]:
        print "dropping existing keyspace..."
        s.execute("DROP KEYSPACE " + KEYSPACE)

    print "creating keyspace..."
    s.execute("""
        CREATE KEYSPACE %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """ % KEYSPACE)

    print "setting keyspace..."
    s.set_keyspace(KEYSPACE)

    print "creating table..."
    s.execute("""
        CREATE TABLE mytable (
            thekey text,
            col1 text,
            col2 text,
            PRIMARY KEY (thekey, col1)
        )
        """)

    for i in range(10):
        print "inserting row", i
        s.execute("""
            INSERT INTO mytable (thekey, col1, col2)
            VALUES ('key%d', 'a', 'b')
            """ % i)

    future = s.execute_async("SELECT * FROM mytable")
    print "key\tcol1\tcol2"
    print "---\t----\t----"

    try:
        rows = future.deliver()
    except:
        log.exeception()

    for row in rows:
        print '\t'.join(row)

    s.execute("DROP KEYSPACE " + KEYSPACE)

if __name__ == "__main__":
    main()
