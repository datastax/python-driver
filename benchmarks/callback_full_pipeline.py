from base import benchmark

import logging
from itertools import count
from threading import Event, Thread

log = logging.getLogger(__name__)

initial = object()

class Runner(Thread):

    def __init__(self, session, query, values, num_queries, *args, **kwargs):
        self.session = session
        self.query = query
        self.values = values
        self.num_queries = num_queries
        self.num_started = count()
        self.num_finished = count()
        self.event = Event()
        Thread.__init__(self)

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
        for i in range(120):
            self.insert_next(initial)

        self.event.wait()

def execute(session, query, values, num_queries, num_threads):

    per_thread = num_queries / num_threads
    threads = []
    for i in range(num_threads):
        thread = Runner(session, query, values, per_thread)
        thread.daemon = True
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        while thread.is_alive():
            thread.join(timeout=0.5)


if __name__ == "__main__":
    benchmark(execute)
