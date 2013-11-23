from itertools import count
import logging
from threading import Event

from base import benchmark, BenchmarkThread

log = logging.getLogger(__name__)

initial = object()

class Runner(BenchmarkThread):

    def __init__(self, *args, **kwargs):
        BenchmarkThread.__init__(self, *args, **kwargs)
        self.num_started = count()
        self.num_finished = count()
        self.event = Event()

    def handle_error(self, exc):
        log.error("Error on insert: %r", exc)

    def insert_next(self, previous_result):
        current_num = self.num_started.next()

        if previous_result is not initial:
            num = next(self.num_finished)
            if num >= self.num_queries:
                self.event.set()

        if current_num <= self.num_queries:
            future = self.session.execute_async(self.query, self.values)
            future.add_callbacks(self.insert_next, self.handle_error)

    def run(self):
        self.start_profile()

        for i in range(120):
            self.insert_next(initial)

        self.event.wait()

        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)
