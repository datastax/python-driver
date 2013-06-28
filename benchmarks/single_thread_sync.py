from base import benchmark

def execute(session, query, values, num_queries):
    for i in xrange(num_queries):
        session.execute(query, values)

if __name__ == "__main__":
    benchmark(execute)
