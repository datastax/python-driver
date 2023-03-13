ScyllaDB Cloud Serverless
-------------------------

With ScyllaDB Cloud, you can deploy `serverless databases <https://cloud.docs.scylladb.com/stable/serverless/index.html>`_. 
The Python driver allows you to connect to a serverless database by utilizing the connection bundle you can download via the **Connect>Python** tab in the Cloud application.
The connection bundle is a YAML file with connection and credential information for your cluster.

Connecting to a ScyllaDB Cloud serverless database is very similar to a standard connection to a ScyllaDB database.

Here’s a short program that connects to a ScyllaDB Cloud serverless database and prints metadata about the cluster:

.. code-block:: python

    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

    PATH_TO_BUNDLE_YAML = '/file/downloaded/from/cloud/connect-bundle.yaml'


    def get_cluster():
        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc='us-east-1')
            )
        )

        return Cluster(
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            scylla_cloud=PATH_TO_BUNDLE_YAML,
        )


    print('Connecting to cluster')
    cluster = get_cluster()
    session = cluster.connect()

    print('Connected to cluster', cluster.metadata.cluster_name)

    print('Getting metadata')
    for host in cluster.metadata.all_hosts():
        print('Datacenter: {}; Host: {}; Rack: {}'.format(
            host.datacenter, host.address, host.rack)
        )

    cluster.shutdown()

By providing the ``scylla_cloud`` parameter to the :class:`~.Cluster` constructor,
the driver can set up the connection based on the endpoint and credential information
stored in your downloaded ScyllaDB Cloud Serverless connection bundle.