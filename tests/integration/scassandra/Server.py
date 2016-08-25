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
# limitations under the License

import subprocess


class ScassandraServer(object):

    def __init__(self, jar_path, binary_port=8042, admin_port=8043):
        self.binary_port = binary_port
        self.admin_port = admin_port
        self.jar_path = jar_path

    def start(self):
        self.proc = subprocess.Popen(['java', '-jar', '-Dscassandra.binary.port={0}'.format(self.binary_port), '-Dscassandra.admin.port={0}'.format(self.admin_port), '-Dscassandra.log.level=INFO', self.jar_path], shell=False)

    def stop(self):
        self.proc.terminate()
