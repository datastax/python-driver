import logging
import Queue
from threading import Thread

from base import benchmark

log = logging.getLogger(__name__)

def execute(session, query, values, num_queries, num_threads):

    per_thread = num_queries / num_threads

    def run():
        futures = Queue.Queue(maxsize=121)

        for i in range(per_thread):
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

    threads = []
    for i in range(num_threads):
        thread = Thread(target=run)
        thread.daemon = True
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        while thread.is_alive():
            thread.join(timeout=0.5)


if __name__ == "__main__":
    benchmark(execute)
