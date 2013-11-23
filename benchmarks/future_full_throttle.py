import logging

from base import benchmark, BenchmarkThread

log = logging.getLogger(__name__)

class Runner(BenchmarkThread):

    def run(self):
        futures = []

        self.start_profile()

        for i in range(self.num_queries):
            future = self.session.execute_async(self.query, self.values)
            futures.append(future)

        for future in futures:
            future.result()

        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)
