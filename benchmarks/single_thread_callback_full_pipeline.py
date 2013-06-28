from base import benchmark

import logging
from itertools import count
from threading import Event

log = logging.getLogger(__name__)

initial = object()

def execute(session, query, values, num_queries):

    num_started = count()
    num_finished = count()
    event = Event()

    def handle_error(exc):
        log.error("Error on insert: %r", exc)

    def insert_next(previous_result):
        current_num = num_started.next()

        if previous_result is not initial:
            num = next(num_finished)
            if num >= num_queries:
                event.set()

        if current_num <= num_queries:
            future = session.execute_async(query, values)
            future.add_callbacks(insert_next, handle_error)

    for i in range(120):
        insert_next(initial)

    event.wait()

if __name__ == "__main__":
    benchmark(execute)
