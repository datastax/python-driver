import logging
from base import benchmark, BenchmarkThread
from six.moves import queue

log = logging.getLogger(__name__)


class Runner(BenchmarkThread):

    def run(self):
        futures = queue.Queue(maxsize=121)

        self.start_profile()

        for i in range(self.num_queries):
            if i > 0 and i % 120 == 0:
                # clear the existing queue
                while True:
                    try:
                        futures.get_nowait().result()
                    except queue.Empty:
                        break

            future = self.session.execute_async(self.query, self.values)
            futures.put_nowait(future)

        while True:
            try:
                futures.get_nowait().result()
            except queue.Empty:
                break

        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)
