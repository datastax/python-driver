from threading import Thread

from base import benchmark

def execute(session, query, values, num_queries, num_threads):

    per_thread = num_queries / num_threads

    def run():
        for i in xrange(per_thread):
            session.execute(query, values)

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
