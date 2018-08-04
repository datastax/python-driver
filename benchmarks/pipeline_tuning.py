# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from base import benchmark, BenchmarkThread, parse_options

import os
from pprint import pprint
from six.moves import range
import sys
import time

dirname = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dirname)
sys.path.append(os.path.join(dirname, '..'))

from cassandra.concurrent import WritePipeline, ReadPipeline


def mean(numbers):
    return float(sum(numbers)) / max(len(numbers), 1)


class Runner(BenchmarkThread):

    def run(self):
        options, args = parse_options()
        results = []

        # test for the most performant max_in_flight_requests value from
        # 100 to 5000 in increments of 100
        for max_in_flight_requests in range(100, 5000, 100):
            # test for the most performant buffer value from
            # 100 to 50000 in increments of 100
            for buffer_size in range(100, 50000, 100):
                write_pipeline = WritePipeline(self.session,
                                               max_in_flight_requests=max_in_flight_requests,
                                               max_unsent_write_requests=buffer_size,
                                               allow_non_performant_queries=True)
                read_pipeline = ReadPipeline(self.session,
                                             max_in_flight_requests=max_in_flight_requests,
                                             max_unconsumed_read_responses=buffer_size,
                                             allow_non_performant_queries=True)

                query = self.query

                # uncomment to test with prepared statements
                # prepared_statement = self.session.prepare(
                #     self.query.replace("'{key}'", '?'))
                # query = prepared_statement
                # self.values = ('key',)

                start_time = time.time()

                for _ in range(self.num_queries):
                    if options.read:
                        read_pipeline.execute(query, self.values)
                    else:
                        write_pipeline.execute(query, self.values)

                if options.read:
                    for _ in read_pipeline.results():
                        pass
                else:
                    write_pipeline.confirm()

                elapsed_time = time.time() - start_time
                result = (elapsed_time, max_in_flight_requests, buffer_size)
                results.append(result)
                print result

        # sort the list by the shortest runtimes and choose the top results
        results.sort()
        top_n = 10
        optimal_runtimes = results[:top_n]

        # create an average value of max_in_flight_requests for the top
        # results and round to the nearest tenth unit
        best_average_max_in_flight_requests = \
            mean([result[1] for result in optimal_runtimes])
        best_average_max_in_flight_requests = int(
            round(best_average_max_in_flight_requests, -1))

        # create an average value of buffer sizes for the top
        # results and round to the nearest tenth unit
        best_average_buffer_size = \
            mean([result[2] for result in optimal_runtimes])
        best_average_buffer_size = int(round(best_average_buffer_size, -1))

        # Pretty print the top N results
        print 'Top %s results' % top_n
        pprint(optimal_runtimes)

        # Print the average value of the parameter values used for the fastest
        # runs
        print '-' * 20
        print 'Best average max_in_flight_requests value:', best_average_max_in_flight_requests
        if options.read:
            print 'Best average max_unconsumed_read_responses value:', best_average_buffer_size
        else:
            print 'Best average max_unsent_write_requests value:', best_average_buffer_size
        print '-' * 20
        print


if __name__ == "__main__":
    benchmark(Runner)
