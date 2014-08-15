import os
from unittest import TestCase, skipUnless

from cqlengine import Model, Integer
from cqlengine.management import sync_table
from cqlengine.tests import base
import resource
import gc

class LoadTest(Model):
    __keyspace__ = 'test'
    k = Integer(primary_key=True)
    v = Integer()


@skipUnless("LOADTEST" in os.environ, "LOADTEST not on")
def test_lots_of_queries():
    sync_table(LoadTest)
    import objgraph
    gc.collect()
    objgraph.show_most_common_types()

    print("Starting...")

    for i in range(1000000):
        if i % 25000 == 0:
            # print memory statistic
            print("Memory usage: %s" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

        LoadTest.create(k=i, v=i)

    objgraph.show_most_common_types()

    raise Exception("you shouldn't be here")
