# Copyright 2013-2016 DataStax, Inc.
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

import logging

from base import benchmark, BenchmarkThread

log = logging.getLogger(__name__)

class Runner(BenchmarkThread):

    def run(self):
        futures = []

        self.start_profile()

        for i in range(self.num_queries):
            key = "{0}-{1}".format(self.thread_num, i)
            future = self.run_query(key)
            futures.append(future)

        for future in futures:
            future.result()

        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)
