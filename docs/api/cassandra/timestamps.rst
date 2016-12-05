``cassandra.timestamps`` - Timestamp Generation
=============================================

.. module:: cassandra.timestamps

.. autoclass:: MonotonicTimestampGenerator (warn_on_drift=True, warning_threshold=0, warning_interval=0)

    .. autoattribute:: warn_on_drift

    .. autoattribute:: warning_threshold

    .. autoattribute:: warning_interval

    .. automethod:: _next_timestamp
