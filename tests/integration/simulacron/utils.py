
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

import requests
import json

from tests.integration import CASSANDRA_VERSION
import subprocess
import time


class SimulacronServer(object):

    def __init__(self, jar_path):
        self.jar_path = jar_path

    def start(self):
        self.proc = subprocess.Popen(['java', '-jar', self.jar_path], shell=False)

    def stop(self):
        self.proc.terminate()

jar_path = "/home/jaume/workspace/simulacron/standalone/target/standalone-0.1-SNAPSHOT.jar"
server_simulacron = SimulacronServer(jar_path)

def start_simulacron():
    server_simulacron.start()

    #Check logs for this
    time.sleep(5)

def stopt_simulacron():
    server_simulacron.stop()


class SimulacronClient(object):

    def __init__(self, admin_addr="127.0.0.1:8187"):
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
        result = requests.post("http://{0}/{1}{2}".format(
            self.admin_addr, query.path, query.fetch_url_params()),
            json=query.fetch_json())

        return result

    def prime_server_versions(self):
        system_local_row = {}
        system_local_row["cql_version"] = "3.4.4"
        system_local_row["release_version"] = "3.1.1" + "-SNAPSHOT"
        column_types = {"cql_version": "ascii", "release_version": "ascii"}
        system_local = PrimeQuery("SELECT cql_version, release_version FROM system.local",
                                  rows=[system_local_row],
                                  column_types=column_types,
                                  then={"delay_in_ms": 1})

        self.submit_request(system_local)

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

NO_THEN = object()

class PrimeQuery(object):

    def __init__(self, expected_query, result="success", rows=None, column_types=None, then=None):
        self.expected_query = expected_query
        self.rows = rows
        self.result = result
        self.column_types = column_types
        self.path = "prime-query-single"
        self.then = then

    def fetch_json(self):
        json_dict = {}
        then = {}
        when = {}
        when['query'] = self.expected_query
        then['result'] = self.result
        if self.rows is not None:
            then['rows'] = self.rows

        if self.column_types is not None:
            then['column_types'] = self.column_types

        if self.then is not None and self.then is not NO_THEN:
            then.update(self.then)

        json_dict['when'] = when
        if self.then is not NO_THEN:
            json_dict['then'] = then

        return json_dict

    def set_node(self, cluster_id, datacenter_id, node_id):
        self.cluster_id = cluster_id
        self.datacenter_id = datacenter_id
        self.node_id = node_id

        if self.cluster_id is not None:
            self.path += "/{}".format(self.cluster_id)

        if self.cluster_id is not None:
            self.path += "/{}".format(self.datacenter_id)

        if self.cluster_id is not None:
            self.path += "/{}".format(self.node_id)

    def fetch_url_params(self):
        return ""


class ClusterQuery(object):
    def __init__(self, cluster_name, cassandra_version, data_centers=1, json_dict=None):
        self.cluster_name = cluster_name
        self.cassandra_version = cassandra_version
        self.data_centers = data_centers
        if json_dict is None:
            self.json_dict = {}
        else:
            self.json_dict = json_dict

        self.path = "cluster"

    def fetch_json(self):
        return self.json_dict

    def fetch_url_params(self):
        return "?cluster_name={0}&cassandra_version={1}&data_centers={2}".\
            format(self.cluster_name, self.cassandra_version,  self.data_centers)


def prime_driver_defaults():
    client_simulacron = SimulacronClient()
    client_simulacron.prime_server_versions()


def prime_singledc():
    cluster_query = ClusterQuery("singledc", CASSANDRA_VERSION, 3)
    client_simulacron = SimulacronClient()
    response = client_simulacron.submit_request(cluster_query)
    return SimulacronCluster(response.text)


def start_and_prime_singledc():
    start_simulacron()
    prime_singledc()
    prime_driver_defaults()

default_column_types = {
      "key": "bigint",
      "description": "ascii",
      "dates": "map<ascii,date>"
    }

default_row = {}
default_row["key"] = 2
default_row["description"] = "whatever_description"
default_row["dates"] = {"whatever_text" : "2014-08-01"}

def prime_query(query, rows=[default_row], column_types=default_column_types, then=None):
    client_simulacron = SimulacronClient()
    query = PrimeQuery(query, rows=rows, column_types=column_types, then=then)
    response = client_simulacron.submit_request(query)
    return response


class SimulacronCluster(object):
    def __init__(self, json_text):
        self.json = json_text
        self.o = json.loads(json_text)

    def get_cluster_id(self):
        return self.o["id"]

    def get_cluster_name(self):
        return self.o["name"]

    def get_data_centers_ids(self):
        return [dc["id"] for dc in self.o["data_centers"]]

    def get_data_centers_names(self):
        return [dc["name"] for dc in self.o["data_centers"]]

    def get_node_ids(self, datacenter_id):
        datacenter = filter(lambda x: x["id"] ==  datacenter_id, self.o["data_centers"]).pop()
        return [node["id"] for node in datacenter["nodes"]]
