.. _udts:

User Defined Types
==================
Cassandra 2.1 introduced user-defined types (UDTs).  You can create a
new type through ``CREATE TYPE`` statements in CQL::

    CREATE TYPE address (street text, zip int);

Version 2.1 of the Python driver adds support for user-defined types.

Registering a UDT
-----------------
You can tell the Python driver to return columns of a specific UDT as
instances of a class or a dict by registering them with your :class:`~.Cluster`
instance through :meth:`.Cluster.register_user_type`:


Map a Class to a UDT
++++++++++++++++++++

.. code-block:: python

    cluster = Cluster(protocol_version=3)
    session = cluster.connect()
    session.set_keyspace('mykeyspace')
    session.execute("CREATE TYPE address (street text, zipcode int)")
    session.execute("CREATE TABLE users (id int PRIMARY KEY, location frozen<address>)")

    # create a class to map to the "address" UDT
    class Address(object):

        def __init__(self, street, zipcode):
            self.street = street
            self.zipcode = zipcode

    cluster.register_user_type('mykeyspace', 'address', Address)

    # insert a row using an instance of Address
    session.execute("INSERT INTO users (id, location) VALUES (%s, %s)",
                    (0, Address("123 Main St.", 78723)))

    # results will include Address instances
    results = session.execute("SELECT * FROM users")
    row = results[0]
    print(row.id, row.location.street, row.location.zipcode)

Map a dict to a UDT
+++++++++++++++++++

.. code-block:: python

    cluster = Cluster(protocol_version=3)
    session = cluster.connect()
    session.set_keyspace('mykeyspace')
    session.execute("CREATE TYPE address (street text, zipcode int)")
    session.execute("CREATE TABLE users (id int PRIMARY KEY, location frozen<address>)")

    cluster.register_user_type('mykeyspace', 'address', dict)

    # insert a row using a prepared statement and a tuple
    insert_statement = session.prepare("INSERT INTO mykeyspace.users (id, location) VALUES (?, ?)")
    session.execute(insert_statement, [0, ("123 Main St.", 78723)])

    # results will include dict instances
    results = session.execute("SELECT * FROM users")
    row = results[0]
    print(row.id, row.location['street'], row.location['zipcode'])

Using UDTs Without Registering Them
-----------------------------------
Although it is recommended to register your types with
:meth:`.Cluster.register_user_type`, the driver gives you some options
for working with unregistered UDTS.

When you use prepared statements, the driver knows what data types to
expect for each placeholder.  This allows you to pass any object you
want for a UDT, as long as it has attributes that match the field names
for the UDT:

.. code-block:: python

    cluster = Cluster(protocol_version=3)
    session = cluster.connect()
    session.set_keyspace('mykeyspace')
    session.execute("CREATE TYPE address (street text, zipcode int)")
    session.execute("CREATE TABLE users (id int PRIMARY KEY, location frozen<address>)")

    class Foo(object):

        def __init__(self, street, zipcode, otherstuff):
            self.street = street
            self.zipcode = zipcode
            self.otherstuff = otherstuff

    insert_statement = session.prepare("INSERT INTO users (id, location) VALUES (?, ?)")

    # since we're using a prepared statement, we don't *have* to register
    # a class to map to the UDT to insert data.  The object just needs to have
    # "street" and "zipcode" attributes (which Foo does):
    session.execute(insert_statement, [0, Foo("123 Main St.", 78723, "some other stuff")])

    # when we query data, UDT columns that don't have a class registered
    # will be returned as namedtuples:
    results = session.execute("SELECT * FROM users")
    first_row = results[0]
    address = first_row.location
    print(address)  # prints "Address(street='123 Main St.', zipcode=78723)"
    street = address.street
    zipcode = address.street

As shown in the code example, inserting data for UDT columns without registering
a class works fine for prepared statements.  However, **you must register a
class to insert UDT columns with unprepared statements**.\*  You can still query
UDT columns without registered classes using unprepared statements, they will
simply return ``namedtuple`` instances (just like prepared statements do).

\* this applies to *parameterized* unprepared statements, in which the driver will be formatting parameters -- not statements with interpolated UDT literals.
