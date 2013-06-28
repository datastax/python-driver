from base import benchmark

import logging

log = logging.getLogger(__name__)

def execute(session, query, values, num_queries):

    futures = []

    for i in range(num_queries):
        future = session.execute_async(query, values)
        futures.append(future)

    for future in futures:
        future.result()


if __name__ == "__main__":
    benchmark(execute)
