# Copyright 2013-2017 DataStax, Inc.
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

from six.moves.urllib.request import build_opener, Request, HTTPHandler
import json
from tests.integration import CASSANDRA_VERSION, SIMULACRON_JAR
import subprocess
import time
import nose

DEFAULT_CLUSTER = "python_simulacron_cluster"


class SimulacronCluster(object):
    """
    Represents a Cluster object as returned by simulacron
    """
    def __init__(self, json_text):
        self.json = json_text
        self.o = json.loads(json_text)

    @property
    def cluster_id(self):
        return self.o["id"]

    @property
    def cluster_name(self):
        return self.o["name"]

    @property
    def data_center_ids(self):
        return [dc["id"] for dc in self.o["data_centers"]]

    @property
    def data_centers_names(self):
        return [dc["name"] for dc in self.o["data_centers"]]

    def get_node_ids(self, datacenter_id):
        datacenter = list(filter(lambda x: x["id"] == datacenter_id, self.o["data_centers"])).pop()
        return [node["id"] for node in datacenter["nodes"]]


class SimulacronServer(object):
    """
    Class for starting and stopping the server from within the tests
    """
    def __init__(self, jar_path):
        self.jar_path = jar_path
        self.running = False
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen(['java', '-jar', self.jar_path, "--loglevel", "ERROR"], shell=False)
        self.running = True

    def stop(self):
        if self.proc:
            self.proc.terminate()
        self.running = False

    def is_running(self):
        # We could check self.proc.poll here instead
        return self.running


SERVER_SIMULACRON = SimulacronServer(SIMULACRON_JAR)


def start_simulacron():
    """
    Starts and waits for simulacron to run
    """
    if SERVER_SIMULACRON.is_running():
        SERVER_SIMULACRON.stop()

    SERVER_SIMULACRON.start()

    #TODO improve this sleep, maybe check the logs like ccm
    time.sleep(5)

def stop_simulacron():
    SERVER_SIMULACRON.stop()


class SimulacronClient(object):
    def __init__(self, admin_addr="127.0.0.1:8187"):
        self.admin_addr = admin_addr

    def submit_request(self, query):
        opener = build_opener(HTTPHandler)
        data = json.dumps(query.fetch_json()).encode('utf8')

        request = Request("http://{}/{}{}".format(
            self.admin_addr, query.path, query.fetch_url_params()), data=data)
        request.get_method = lambda: 'POST'
        request.add_header("Content-Type", 'application/json')
        request.add_header("Content-Length", len(data))

        connection = opener.open(request)
        return connection.read().decode('utf-8')

    def prime_server_versions(self):
        """
        This information has to be primed for the test harness to run
        """
        system_local_row = {}
        system_local_row["cql_version"] = CASSANDRA_VERSION
        system_local_row["release_version"] = CASSANDRA_VERSION + "-SNAPSHOT"
        column_types = {"cql_version": "ascii", "release_version": "ascii"}
        system_local = PrimeQuery("SELECT cql_version, release_version FROM system.local",
                                  rows=[system_local_row],
                                  column_types=column_types)

        self.submit_request(system_local)

    def clear_all_queries(self, cluster_name=DEFAULT_CLUSTER):
        """
        Clear all the primed queries from a particular cluster
        :param cluster_name: cluster to clear queries from
        """
        opener = build_opener(HTTPHandler)
        request = Request("http://{0}/{1}/{2}".format(
            self.admin_addr, "prime", cluster_name))
        request.get_method = lambda: 'DELETE'
        connection = opener.open(request)
        return connection.read()


NO_THEN = object()


class SimulacronRequest(object):
    def fetch_json(self):
        raise NotImplementedError()

    def fetch_url_params(self):
        raise NotImplementedError()


class PrimeOptions(SimulacronRequest):
    """
    Class used for specifying how should simulacron respond to an OptionsMessage
    """
    def __init__(self, then=None, cluster_name=DEFAULT_CLUSTER):
        self.path = "prime/{}".format(cluster_name)
        self.then = then

    def fetch_json(self):
        json_dict = {}
        then = {}
        when = {}

        when['request'] = "options"

        if self.then is not None and self.then is not NO_THEN:
            then.update(self.then)

        json_dict['when'] = when
        if self.then is not NO_THEN:
            json_dict['then'] = then

        return json_dict

    def fetch_url_params(self):
        return ""


class PrimeQuery(SimulacronRequest):
    """
    Class used for specifying how should simulacron respond to particular query
    """
    def __init__(self, expected_query, result="success", rows=None,
                 column_types=None, when=None, then=None, cluster_name=DEFAULT_CLUSTER):
        self.expected_query = expected_query
        self.rows = rows
        self.result = result
        self.column_types = column_types
        self.path = "prime/{}".format(cluster_name)
        self.then = then
        self.when = when

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

        if self.then is not NO_THEN:
            json_dict['then'] = then

        if self.when is not None:
            when.update(self.when)

        json_dict['when'] = when

        return json_dict

    def set_node(self, cluster_id, datacenter_id, node_id):
        self.cluster_id = cluster_id
        self.datacenter_id = datacenter_id
        self.node_id = node_id

        self.path += '/'.join([component for component in
                               (self.cluster_id, self.datacenter_id, self.node_id)
                               if component is not None])

    def fetch_url_params(self):
        return ""


class ClusterQuery(SimulacronRequest):
    """
    Class used for creating a cluster
    """
    def __init__(self, cluster_name, cassandra_version, data_centers="3", json_dict=None):
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
        return "?cassandra_version={0}&data_centers={1}&name={2}".\
            format(self.cassandra_version, self.data_centers, self.cluster_name)


def prime_driver_defaults():
    """
    Function to prime the necessary queries so the test harness can run
    """
    client_simulacron = SimulacronClient()
    client_simulacron.prime_server_versions()


def prime_cluster(data_centers="3", version=None, cluster_name=DEFAULT_CLUSTER):
    """
    Creates a new cluster in the simulacron server
    :param cluster_name: name of the cluster
    :param data_centers: string describing the datacenter, e.g. 2/3 would be two
    datacenters of 2 nodes and three nodes
    :param version: C* version
    """
    version = version or CASSANDRA_VERSION
    cluster_query = ClusterQuery(cluster_name, version, data_centers)
    client_simulacron = SimulacronClient()
    response = client_simulacron.submit_request(cluster_query)
    return SimulacronCluster(response)


def start_and_prime_singledc(cluster_name=DEFAULT_CLUSTER):
    """
    Starts simulacron and creates a cluster with a single datacenter
    :param cluster_name: name of the cluster to start and prime
    :return:
    """
    return start_and_prime_cluster_defaults(number_of_dc=1, nodes_per_dc=3, cluster_name=cluster_name)


def start_and_prime_cluster_defaults(number_of_dc=1, nodes_per_dc=3, version=None, cluster_name=DEFAULT_CLUSTER):
    """
    :param number_of_dc: number of datacentes
    :param nodes_per_dc: number of nodes per datacenter
    :param version: C* version
    """
    start_simulacron()
    data_centers = ",".join([str(nodes_per_dc)] * number_of_dc)
    simulacron_cluster = prime_cluster(data_centers=data_centers, version=version, cluster_name=cluster_name)
    prime_driver_defaults()

    return simulacron_cluster


default_column_types = {
    "key": "bigint",
    "value": "ascii"
}

default_row = {"key": 2, "value": "value"}
default_rows = [default_row]


def prime_request(request):
    """
    :param request: It could be PrimeQuery class or an PrimeOptions class
    """
    return SimulacronClient().submit_request(request)


def prime_query(query, rows=default_rows, column_types=default_column_types, when=None, then=None, cluster_name=DEFAULT_CLUSTER):
    """
    Shortcut function for priming a query
    :return:
    """
    # If then is set, then rows and column_types should not
    if then:
        nose.tools.assert_equal(rows, default_rows)
        nose.tools.assert_equal(column_types, default_column_types)
        rows=None
        column_types=None

    query = PrimeQuery(query, rows=rows, column_types=column_types, when=when, then=then, cluster_name=cluster_name)
    response = prime_request(query)
    return response


def clear_queries():
    """
    Clears all the queries that have been primed to simulacron
    """
    SimulacronClient().clear_all_queries()
