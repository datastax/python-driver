import logging
import Queue

from base import benchmark, BenchmarkThread

log = logging.getLogger(__name__)

class Runner(BenchmarkThread):

    def run(self):
        futures = Queue.Queue(maxsize=121)

        self.start_profile()

        for i in range(self.num_queries):
            if i >= 120:
                old_future = futures.get_nowait()
                old_future.result()

            future = self.session.execute_async(self.query, self.values)
            futures.put_nowait(future)

        while True:
            try:
                futures.get_nowait().result()
            except Queue.Empty:
                break

        self.finish_profile


if __name__ == "__main__":
    benchmark(Runner)
