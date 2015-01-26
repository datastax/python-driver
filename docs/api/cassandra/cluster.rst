``cassandra.cluster`` - Clusters and Sessions
=============================================

.. module:: cassandra.cluster

.. autoclass:: Cluster ([contact_points=('127.0.0.1',)][, port=9042][, executor_threads=2], **attr_kwargs)

   .. autoattribute:: cql_version

   .. autoattribute:: protocol_version

   .. autoattribute:: port

   .. autoattribute:: compression

   .. autoattribute:: auth_provider

   .. autoattribute:: load_balancing_policy

   .. autoattribute:: reconnection_policy

   .. autoattribute:: default_retry_policy

   .. autoattribute:: conviction_policy_factory

   .. autoattribute:: connection_class

   .. autoattribute:: metrics_enabled

   .. autoattribute:: metrics

   .. autoattribute:: metadata

   .. autoattribute:: ssl_options

   .. autoattribute:: sockopts

   .. autoattribute:: max_schema_agreement_wait

   .. autoattribute:: control_connection_timeout

   .. autoattribute:: idle_heartbeat_interval

   .. automethod:: connect

   .. automethod:: shutdown

   .. automethod:: register_user_type

   .. automethod:: register_listener

   .. automethod:: unregister_listener

   .. automethod:: get_core_connections_per_host

   .. automethod:: set_core_connections_per_host

   .. automethod:: get_max_connections_per_host

   .. automethod:: set_max_connections_per_host

   .. automethod:: refresh_schema

.. autoclass:: Session ()

   .. autoattribute:: default_timeout

   .. autoattribute:: row_factory

   .. autoattribute:: default_fetch_size

   .. autoattribute:: use_client_timestamp

   .. autoattribute:: encoder

   .. automethod:: execute(statement[, parameters][, timeout][, trace])

   .. automethod:: execute_async(statement[, parameters][, trace])

   .. automethod:: prepare(statement)

   .. automethod:: shutdown()

   .. automethod:: set_keyspace(keyspace)

.. autoclass:: ResponseFuture ()

   .. autoattribute:: query

   .. automethod:: result([timeout])

   .. automethod:: get_query_trace()

   .. autoattribute:: has_more_pages

   .. automethod:: start_fetching_next_page()

   .. automethod:: add_callback(fn, *args, **kwargs)

   .. automethod:: add_errback(fn, *args, **kwargs)

   .. automethod:: add_callbacks(callback, errback, callback_args=(), callback_kwargs=None, errback_args=(), errback_args=None)

.. autoclass:: PagedResult ()
   :members:

.. autoexception:: QueryExhausted ()

.. autoexception:: NoHostAvailable ()
   :members:

.. autoexception:: UserTypeDoesNotExist ()
