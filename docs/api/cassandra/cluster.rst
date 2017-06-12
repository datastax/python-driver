``cassandra.cluster`` - Clusters and Sessions
=============================================

.. module:: cassandra.cluster

.. autoclass:: Cluster ([contact_points=('127.0.0.1',)][, port=9042][, executor_threads=2], **attr_kwargs)

   .. autoattribute:: contact_points

   .. autoattribute:: port

   .. autoattribute:: cql_version

   .. autoattribute:: protocol_version

   .. autoattribute:: compression

   .. autoattribute:: auth_provider

   .. autoattribute:: load_balancing_policy

   .. autoattribute:: reconnection_policy

   .. autoattribute:: default_retry_policy
      :annotation: = <cassandra.policies.RetryPolicy object>

   .. autoattribute:: conviction_policy_factory

   .. autoattribute:: address_translator

   .. autoattribute:: metrics_enabled

   .. autoattribute:: metrics

   .. autoattribute:: ssl_options

   .. autoattribute:: sockopts

   .. autoattribute:: max_schema_agreement_wait

   .. autoattribute:: metadata

   .. autoattribute:: connection_class

   .. autoattribute:: control_connection_timeout

   .. autoattribute:: idle_heartbeat_interval

   .. autoattribute:: idle_heartbeat_timeout

   .. autoattribute:: schema_event_refresh_window

   .. autoattribute:: topology_event_refresh_window

   .. autoattribute:: status_event_refresh_window

   .. autoattribute:: prepare_on_all_hosts

   .. autoattribute:: reprepare_on_up

   .. autoattribute:: connect_timeout

   .. autoattribute:: schema_metadata_enabled
      :annotation: = True

   .. autoattribute:: token_metadata_enabled
      :annotation: = True

   .. autoattribute:: timestamp_generator

   .. automethod:: connect

   .. automethod:: shutdown

   .. automethod:: register_user_type

   .. automethod:: register_listener

   .. automethod:: unregister_listener

   .. automethod:: add_execution_profile

   .. automethod:: set_max_requests_per_connection

   .. automethod:: get_max_requests_per_connection

   .. automethod:: set_min_requests_per_connection

   .. automethod:: get_min_requests_per_connection

   .. automethod:: get_core_connections_per_host

   .. automethod:: set_core_connections_per_host

   .. automethod:: get_max_connections_per_host

   .. automethod:: set_max_connections_per_host

   .. automethod:: get_control_connection_host

   .. automethod:: refresh_schema_metadata

   .. automethod:: refresh_keyspace_metadata

   .. automethod:: refresh_table_metadata

   .. automethod:: refresh_user_type_metadata

   .. automethod:: refresh_user_function_metadata

   .. automethod:: refresh_user_aggregate_metadata

   .. automethod:: refresh_nodes

   .. automethod:: set_meta_refresh_enabled

.. autoclass:: ExecutionProfile
   :members:

.. autodata:: EXEC_PROFILE_DEFAULT
   :annotation:

.. autoclass:: Session ()

   .. autoattribute:: default_timeout
      :annotation: = 10.0

   .. autoattribute:: default_consistency_level
      :annotation: = LOCAL_ONE

   .. autoattribute:: default_serial_consistency_level
      :annotation: = None

   .. autoattribute:: row_factory
      :annotation: = <function named_tuple_factory>

   .. autoattribute:: default_fetch_size

   .. autoattribute:: use_client_timestamp

   .. autoattribute:: timestamp_generator

   .. autoattribute:: encoder

   .. autoattribute:: client_protocol_handler

   .. automethod:: execute(statement[, parameters][, timeout][, trace][, custom_payload])

   .. automethod:: execute_async(statement[, parameters][, trace][, custom_payload])

   .. automethod:: prepare(statement)

   .. automethod:: shutdown()

   .. automethod:: set_keyspace(keyspace)

   .. automethod:: execution_profile_clone_update

   .. automethod:: add_request_init_listener

   .. automethod:: remove_request_init_listener

.. autoclass:: ResponseFuture ()

   .. autoattribute:: query

   .. automethod:: result()

   .. automethod:: get_query_trace()

   .. automethod:: get_all_query_traces()

   .. autoattribute:: custom_payload()

   .. autoattribute:: is_schema_agreed

   .. autoattribute:: has_more_pages

   .. autoattribute:: warnings

   .. automethod:: start_fetching_next_page()

   .. automethod:: add_callback(fn, *args, **kwargs)

   .. automethod:: add_errback(fn, *args, **kwargs)

   .. automethod:: add_callbacks(callback, errback, callback_args=(), callback_kwargs=None, errback_args=(), errback_args=None)

.. autoclass:: ResultSet ()
   :members:

.. autoexception:: QueryExhausted ()

.. autoexception:: NoHostAvailable ()
   :members:

.. autoexception:: UserTypeDoesNotExist ()
