Getting Started
===============

First, make sure you have the driver properly :doc:`installed <installation>`.

Connecting to Cassandra
-----------------------
Before we can start executing any queries against a Cassandra cluster we need to setup
an instance of :class:`~.Cluster`. As the name suggests, you will typically have one
instance of :class:`~.Cluster` for each Cassandra cluster you want to interact
with.

The simplest way to create a :class:`~.Cluster` is like this:

.. code-block:: python

    from cassandra.cluster import Cluster

    cluster = Cluster()

This will attempt to connection to a Cassandra instance on your
local machine (127.0.0.1).  You can also specify a list of IP
addresses for nodes in your cluster:

.. code-block:: python

    from cassandra.cluster import Cluster

    cluster = Cluster(['192.168.0.1', '192.168.0.2'])

The set of IP addresses we pass to the :class:`~.Cluster` is simply
an initial set of contact points.  After the driver connects to one
of these nodes it will *automatically discover* the rest of the
nodes in the cluster and connect to them, so you don't need to list
every node in your cluster.

If you need to use a non-standard port, use SSL, or customize the driver's
behavior in some other way, this is the place to do it:

.. code-block:: python

    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy

    cluster = Cluster(
        ['10.1.1.3', '10.1.1.4', '10.1.1.5'],
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='US_EAST'),
        port=9042)


You can find a more complete list of options in the :class:`~.Cluster` documentation.

Instantiating a :class:`~.Cluster` does not actually connect us to any nodes.
To establish connections and begin executing queries we need a
:class:`~.Session`, which is created by calling :meth:`.Cluster.connect()`:

.. code-block:: python

    cluster = Cluster()
    session = cluster.connect()

The :meth:`~.Cluster.connect()` method takes an optional ``keyspace`` argument
which sets the default keyspace for all queries made through that :class:`~.Session`:

.. code-block:: python

    cluster = Cluster()
    session = cluster.connect('mykeyspace')


You can always change a Session's keyspace using :meth:`~.Session.set_keyspace` or
by executing a ``USE <keyspace>`` query:

.. code-block:: python

    session.set_keyspace('users')
    # or you can do this instead
    session.execute('USE users')


Executing Queries
-----------------
Now that we have a :class:`.Session` we can begin to execute queries. The simplest
way to execute a query is to use :meth:`~.Session.execute()`:

.. code-block:: python

    rows = session.execute('SELECT name, age, email FROM users')
    for user_row in rows:
        print user_row.name, user_row.age, user_row.email

This will transparently pick a Cassandra node to execute the query against
and handle any retries that are necessary if the operation fails.

By default, each row in the result set will be a
`namedtuple <http://docs.python.org/2/library/collections.html#collections.namedtuple>`_.
Each row will have a matching attribute for each column defined in the schema,
such as ``name``, ``age``, and so on.  You can also treat them as normal tuples
by unpacking them or accessing fields by position.  The following three
examples are equivalent:

.. code-block:: python

    rows = session.execute('SELECT name, age, email FROM users')
    for row in rows:
        print row.name, row.age, row.email

.. code-block:: python

    rows = session.execute('SELECT name, age, email FROM users')
    for (name, age, email) in rows:
        print name, age, email

.. code-block:: python

    rows = session.execute('SELECT name, age, email FROM users')
    for row in rows:
        print row[0], row[1], row[2]

If you prefer another result format, such as a ``dict`` per row, you
can change the :attr:`~.Session.row_factory` attribute.

For queries that will be run repeatedly, you should use
`Prepared statements <#prepared-statements>`_.

Passing Parameters to CQL Queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
When executing non-prepared statements, the driver supports two forms of
parameter place-holders: positional and named.

Positional parameters are used with a ``%s`` placeholder.  For example,
when you execute:

.. code-block:: python

    session.execute(
        """
        INSERT INTO users (name, credits, user_id)
        VALUES (%s, %s, %s)
        """,
        ("John O'Reilly", 42, uuid.uuid1())
    )

It is translated to the following CQL query::

    INSERT INTO users (name, credits, user_id)
    VALUES ('John O''Reilly', 42, 2644bada-852c-11e3-89fb-e0b9a54a6d93)

Note that you should use ``%s`` for all types of arguments, not just strings.
For example, this would be **wrong**:

.. code-block:: python

    session.execute("INSERT INTO USERS (name, age) VALUES (%s, %d)", ("bob", 42))  # wrong

Instead, use ``%s`` for the age placeholder.

If you need to use a literal ``%`` character, use ``%%``.

**Note**: you must always use a sequence for the second argument, even if you are
only passing in a single variable:

.. code-block:: python

    session.execute("INSERT INTO foo (bar) VALUES (%s)", "blah")  # wrong
    session.execute("INSERT INTO foo (bar) VALUES (%s)", ("blah"))  # wrong
    session.execute("INSERT INTO foo (bar) VALUES (%s)", ("blah", ))  # right
    session.execute("INSERT INTO foo (bar) VALUES (%s)", ["blah"])  # right


Note that the second line is incorrect because in Python, single-element tuples
require a comma.

Named place-holders use the ``%(name)s`` form:

.. code-block:: python

    session.execute(
        """
        INSERT INTO users (name, credits, user_id, username)
        VALUES (%(name)s, %(credits)s, %(user_id)s, %(name)s)
        """,
        {'name': "John O'Reilly", 'credits': 42, 'user_id': uuid.uuid1()}
    )

Note that you can repeat placeholders with the same name, such as ``%(name)s``
in the above example.

Only data values should be supplied this way.  Other items, such as keyspaces,
table names, and column names should be set ahead of time (typically using
normal string formatting).

.. _type-conversions:

Type Conversions
^^^^^^^^^^^^^^^^
For non-prepared statements, Python types are cast to CQL literals in the
following way:

.. table::

    +--------------------+-------------------------+
    | Python Type        | CQL Literal Type        |
    +====================+=========================+
    | ``None``           | ``NULL``                |
    +--------------------+-------------------------+
    | ``bool``           | ``boolean``             |
    +--------------------+-------------------------+
    | ``float``          | | ``float``             |
    |                    | | ``double``            |
    +--------------------+-------------------------+
    | | ``int``          | | ``int``               |
    | | ``long``         | | ``bigint``            |
    |                    | | ``varint``            |
    |                    | | ``smallint``          |
    |                    | | ``tinyint``           |
    |                    | | ``counter``           |
    +--------------------+-------------------------+
    | ``decimal.Decimal``| ``decimal``             |
    +--------------------+-------------------------+
    | | ``str``          | | ``ascii``             |
    | | ``unicode``      | | ``varchar``           |
    |                    | | ``text``              |
    +--------------------+-------------------------+
    | | ``buffer``       | ``blob``                |
    | | ``bytearray``    |                         |
    +--------------------+-------------------------+
    | ``date``           | ``date``                |
    +--------------------+-------------------------+
    | ``datetime``       | ``timestamp``           |
    +--------------------+-------------------------+
    | ``time``           | ``time``                |
    +--------------------+-------------------------+
    | | ``list``         | ``list``                |
    | | ``tuple``        |                         |
    | | generator        |                         |
    +--------------------+-------------------------+
    | | ``set``          | ``set``                 |
    | | ``frozenset``    |                         |
    +--------------------+-------------------------+
    | | ``dict``         | ``map``                 |
    | | ``OrderedDict``  |                         |
    +--------------------+-------------------------+
    | ``uuid.UUID``      | | ``timeuuid``          |
    |                    | | ``uuid``              |
    +--------------------+-------------------------+


Asynchronous Queries
^^^^^^^^^^^^^^^^^^^^
The driver supports asynchronous query execution through
:meth:`~.Session.execute_async()`.  Instead of waiting for the query to
complete and returning rows directly, this method almost immediately
returns a :class:`~.ResponseFuture` object.  There are two ways of
getting the final result from this object.

The first is by calling :meth:`~.ResponseFuture.result()` on it. If
the query has not yet completed, this will block until it has and
then return the result or raise an Exception if an error occurred.
For example:

.. code-block:: python

    from cassandra import ReadTimeout

    query = "SELECT * FROM users WHERE user_id=%s"
    future = session.execute_async(query, [user_id])

    # ... do some other work

    try:
        rows = future.result()
        user = rows[0]
        print user.name, user.age
    except ReadTimeout:
        log.exception("Query timed out:")

This works well for executing many queries concurrently:

.. code-block:: python

    # build a list of futures
    futures = []
    query = "SELECT * FROM users WHERE user_id=%s"
    for user_id in ids_to_fetch:
        futures.append(session.execute_async(query, [user_id])

    # wait for them to complete and use the results
    for future in futures:
        rows = future.result()
        print rows[0].name

Alternatively, instead of calling :meth:`~.ResponseFuture.result()`,
you can attach callback and errback functions through the
:meth:`~.ResponseFuture.add_callback()`,
:meth:`~.ResponseFuture.add_errback()`, and
:meth:`~.ResponseFuture.add_callbacks()`, methods.  If you have used
Twisted Python before, this is designed to be a lightweight version of
that:

.. code-block:: python

    def handle_success(rows):
        user = rows[0]
        try:
            process_user(user.name, user.age, user.id)
        except Exception:
            log.error("Failed to process user %s", user.id)
            # don't re-raise errors in the callback

    def handle_error(exception):
        log.error("Failed to fetch user info: %s", exception)


    future = session.execute_async(query)
    future.add_callbacks(handle_success, handle_error)

There are a few important things to remember when working with callbacks:
 * **Exceptions that are raised inside the callback functions will be logged and then ignored.**
 * Your callback will be run on the event loop thread, so any long-running
   operations will prevent other requests from being handled


Setting a Consistency Level
---------------------------
The consistency level used for a query determines how many of the
replicas of the data you are interacting with need to respond for
the query to be considered a success.

By default, :attr:`.ConsistencyLevel.LOCAL_ONE` will be used for all queries.
You can specify a different default for the session on :attr:`.Session.default_consistency_level`.
To specify a different consistency level per request, wrap queries
in a :class:`~.SimpleStatement`:

.. code-block:: python

    from cassandra import ConsistencyLevel
    from cassandra.query import SimpleStatement

    query = SimpleStatement(
        "INSERT INTO users (name, age) VALUES (%s, %s)",
        consistency_level=ConsistencyLevel.QUORUM)
    session.execute(query, ('John', 42))

Prepared Statements
-------------------
Prepared statements are queries that are parsed by Cassandra and then saved
for later use.  When the driver uses a prepared statement, it only needs to
send the values of parameters to bind.  This lowers network traffic
and CPU utilization within Cassandra because Cassandra does not have to
re-parse the query each time.

To prepare a query, use :meth:`.Session.prepare()`:

.. code-block:: python

    user_lookup_stmt = session.prepare("SELECT * FROM users WHERE user_id=?")

    users = []
    for user_id in user_ids_to_query:
        user = session.execute(user_lookup_stmt, [user_id])
        users.append(user)

:meth:`~.Session.prepare()` returns a :class:`~.PreparedStatement` instance
which can be used in place of :class:`~.SimpleStatement` instances or literal
string queries.  It is automatically prepared against all nodes, and the driver
handles re-preparing against new nodes and restarted nodes when necessary.

Note that the placeholders for prepared statements are ``?`` characters.  This
is different than for simple, non-prepared statements (although future versions
of the driver may use the same placeholders for both).

Setting a Consistency Level with Prepared Statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To specify a consistency level for prepared statements, you have two options.

The first is to set a default consistency level for every execution of the
prepared statement:

.. code-block:: python

    from cassandra import ConsistencyLevel

    cluster = Cluster()
    session = cluster.connect("mykeyspace")
    user_lookup_stmt = session.prepare("SELECT * FROM users WHERE user_id=?")
    user_lookup_stmt.consistency_level = ConsistencyLevel.QUORUM

    # these will both use QUORUM
    user1 = session.execute(user_lookup_stmt, [user_id1])[0]
    user2 = session.execute(user_lookup_stmt, [user_id2])[0]

The second option is to create a :class:`~.BoundStatement` from the
:class:`~.PreparedStatement` and binding parameters and set a consistency
level on that:

.. code-block:: python

    # override the QUORUM default
    user3_lookup = user_lookup_stmt.bind([user_id3])
    user3_lookup.consistency_level = ConsistencyLevel.ALL
    user3 = session.execute(user3_lookup)
