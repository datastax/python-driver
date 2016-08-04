
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

import json
import requests


class ScassandraClient(object):

    def __init__(self, admin_addr="127.0.0.1:8043"):
        self.admin_addr = admin_addr

    def prime_metadata(self):

        self.submit_request(self.retrieve_system_peers_query())
        self.submit_request(self.retrieve_schema_column_family())
        self.submit_request(self.retrieve_schema_columns())
        self.submit_request(self.retrieve_system_local_query())
        self.submit_request(self.retrieve_schema_keyspaces())
        self.submit_request(PrimeQuery("SELECT * FROM system.schema_usertypes", result="invalid"))
        self.submit_request(PrimeQuery("SELECT * FROM system.schema_aggregates", result="invalid"))
        self.submit_request(PrimeQuery("SELECT * FROM system.schema_functions", result="invalid"))
        self.submit_request(PrimeQuery("SELECT * FROM system.schema_triggers", result="invalid"))

    def submit_request(self, query):
        result = requests.post("http://{0}/{1}".format(self.admin_addr, query.type), json=query.fetch_json())
        print(result)
        return result

    def retrieve_system_local_query(self):
        system_local_row = {}
        system_local_row["cluster_name"] = "custom cluster name"
        system_local_row["partitioner"] = "org.apache.cassandra.dht.Murmur3Partitioner"
        system_local_row["data_center"] = "dc1"
        system_local_row["rack"] = "rc1"
        system_local_row["release_version"] = "2.0.1"
        tokens = ["1743244960790844724"]
        system_local_row["tokens"] = tokens
        column_types = {"tokens": "set<text>"}
        system_local = PrimeQuery("SELECT * FROM system.local WHERE key='local'", rows=[system_local_row], column_types=column_types)
        return system_local

    def retrieve_system_peers_query(self):
        peer_1_row = {}
        peer_1_row["peer"] = "127.0.0.1"
        peer_1_row["data_center"] = "datacenter1"
        peer_1_row["host_id"] = "8db34a1c-bbb5-4f6e-b31e-e15d75b05620"
        peer_1_row["preferred_ip"] = "None"
        peer_1_row["rack"] = "rack1"
        peer_1_row["release_version"] = "2.0.1"
        peer_1_row["rpc_address"] = "127.0.0.1"
        peer_1_row["schema_version"] = "7f1c0a6e-ea18-343e-b344-a7cb80640dca"
        tokens = ["1743244960790844724"]
        peer_1_row["tokens"] = tokens
        column_types = {"tokens": "set<text>"}
        peers = PrimeQuery("SELECT * FROM system.peers", rows=[peer_1_row], column_types=column_types)
        return peers

    def retrieve_schema_keyspaces(self):
        schema_keyspaces_rows = {}
        schema_keyspaces_rows["keyspace_name"] = "system"
        schema_keyspaces_rows["durable_writes"] = "True"
        schema_keyspaces_rows["strategy_class"] = "org.apache.cassandra.locator.LocalStrategy"
        schema_keyspaces_rows["strategy_options"] = "{}"
        sks = PrimeQuery("SELECT * FROM system.schema_keyspaces", rows=[schema_keyspaces_rows])
        return sks

    def retrieve_schema_column_family(self):
        scf_1_row = {}
        scf_1_row["comment"] = "ColumnFamily definitions"
        scf_1_row["keyspace_name"] = "system"
        scf = PrimeQuery("SELECT * FROM system.schema_columnfamilies", rows=[scf_1_row])
        return scf

    def retrieve_schema_columns(self):
        schema_columns_row_1 = {}
        schema_columns_row_1["index_options"] = "null"
        schema_columns_row_1["index_name"] = "null"
        schema_columns_row_1["keyspace_name"] = "null"
        schema_columns_row_1["index_type"] = "validator"
        schema_columns_row_1["validator"] = "org.apache.cassandra.db.marshal.UTF8Type"
        schema_columns_row_1["columnfamily_name"] = "schema_columnfamilies"
        schema_columns_row_1["component_index"] = "1"
        schema_columns_row_1["type"] = "regular"
        schema_columns_row_1["column_name"] = "compaction_strategy_class"
        sc = PrimeQuery("SELECT * FROM system.schema_columns", rows=[schema_columns_row_1])
        return sc


class PrimeQuery(object):

    def __init__(self, expected_query, result="success", rows=None, column_types=None):
        self.expected_query = expected_query
        self.rows = rows
        self.result = result
        self.column_types = column_types
        self.type = "prime-query-single"

    def fetch_json(self):
        json_dict = {}
        then = {}
        when = {}
        when['query'] = self.expected_query
        then['result'] = self.result
        if self.rows:
            then['rows'] = self.rows
        if self.column_types:
            then['column_types'] = self.column_types
        json_dict['when'] = when
        json_dict['then'] = then
        return json_dict



