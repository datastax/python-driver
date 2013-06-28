from base import benchmark

import logging
import Queue

log = logging.getLogger(__name__)

def execute(session, query, values, num_queries):

    futures = Queue.Queue(maxsize=121)

    for i in range(num_queries):
        if i > 0 and i % 120 == 0:
            # clear the existing queue
            while True:
                try:
                    futures.get_nowait().result()
                except Queue.Empty:
                    break

        future = session.execute_async(query, values)
        futures.put_nowait(future)

    while True:
        try:
            futures.get_nowait().result()
        except Queue.Empty:
            break


if __name__ == "__main__":
    benchmark(execute)
