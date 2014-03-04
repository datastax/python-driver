from base import benchmark, BenchmarkThread

class Runner(BenchmarkThread):

    def run(self):
        self.start_profile()

        for i in xrange(self.num_queries):
            self.session.execute(self.query, self.values)

        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)
