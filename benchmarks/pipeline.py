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
from six.moves import range
import sys

dirname = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dirname)
sys.path.append(os.path.join(dirname, '..'))

from cassandra.concurrent import WritePipeline, ReadPipeline


class Runner(BenchmarkThread):

    def run(self):
        options, args = parse_options()
        write_pipeline = WritePipeline(self.session,
                                       allow_non_performant_queries=True)
        read_pipeline = ReadPipeline(self.session,
                                     allow_non_performant_queries=True)
        self.start_profile()

        for _ in range(self.num_queries):
            if options.read:
                read_pipeline.execute(self.query, self.values)
            else:
                write_pipeline.execute(self.query, self.values)

        if options.read:
            for _ in read_pipeline.results():
                pass
        else:
            write_pipeline.confirm()

        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)
