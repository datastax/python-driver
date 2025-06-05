# Copyright 2025 ScyllaDB, Inc.
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
from typing import Optional


class ApplicationInfoBase:
    """
    A class that holds application information and adds it to startup message options
    """
    def add_startup_options(self, options: dict[str, str]):
        raise NotImplementedError()


class ApplicationInfo(ApplicationInfoBase):
    application_name: Optional[str]
    application_version: Optional[str]
    client_id: Optional[str]

    def __init__(
            self,
            application_name: Optional[str] = None,
            application_version: Optional[str] = None,
            client_id: Optional[str] = None
    ):
        if application_name and not isinstance(application_name, str):
            raise TypeError('application_name must be a string')
        if application_version and not isinstance(application_version, str):
            raise TypeError('application_version must be a string')
        if client_id and not isinstance(client_id, str):
            raise TypeError('client_id must be a string')

        self.application_name = application_name
        self.application_version = application_version
        self.client_id = client_id

    def add_startup_options(self, options: dict[str, str]):
        if self.application_name:
            options['APPLICATION_NAME'] = self.application_name
        if self.application_version:
            options['APPLICATION_VERSION'] = self.application_version
        if self.client_id:
            options['CLIENT_ID'] = self.client_id
