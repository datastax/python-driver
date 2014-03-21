import logging

from itertools import count
from threading import Event

from base import benchmark, BenchmarkThread
from six.moves import range

log = logging.getLogger(__name__)


sentinel = object()


class Runner(BenchmarkThread):

    def __init__(self, *args, **kwargs):
        BenchmarkThread.__init__(self, *args, **kwargs)
        self.num_started = count()
        self.num_finished = count()
        self.event = Event()

    def insert_next(self, previous_result=sentinel):
        if previous_result is not sentinel:
            if isinstance(previous_result, BaseException):
                log.error("Error on insert: %r", previous_result)
            if next(self.num_finished) >= self.num_queries:
                self.event.set()

        if next(self.num_started) <= self.num_queries:
            future = self.session.execute_async(self.query, self.values)
            future.add_callbacks(self.insert_next, self.insert_next)

    def run(self):
        self.start_profile()

        for _ in range(min(120, self.num_queries)):
            self.insert_next()

        self.event.wait()

        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)
