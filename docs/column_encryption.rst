Column Encryption
=================

Overview
--------
Support for client-side encryption of data was added in version 3.27.0 of the Python driver.  When using 
this feature data will be encrypted on-the-fly according to a specified :class:`~.ColumnEncryptionPolicy`
instance.  This policy is also used to decrypt data in returned rows.  If a prepared statement is used
this decryption is transparent to the user; retrieved data will be decrypted and converted into the original
type (according to definitions in the encryption policy).  Support for simple (i.e. non-prepared) queries is 
also available, although in this case values must be manually encrypted and/or decrypted.  The 
:class:`~.ColumnEncryptionPolicy` instance provides methods to assist with these operations.

Client-side encryption and decryption should work against all versions of Cassandra and DSE.  It does not
utilize any server-side functionality to do its work.

Configuration
-------------
Client-side encryption is enabled by creating an instance of a subclass of :class:`~.ColumnEncryptionPolicy`
and adding information about columns to be encrypted to it.  This policy is then supplied to :class:`~.Cluster`
when it's created.

.. code-block:: python
    import os

    from cassandra.policies import ColDesc, AES256ColumnEncryptionPolicy, AES256_KEY_SIZE_BYTES

    key = os.urandom(AES256_KEY_SIZE_BYTES)
    cl_policy = AES256ColumnEncryptionPolicy()
    col_desc = ColDesc('ks1','table1','column1')
    cql_type = "int"
    cl_policy.add_column(col_desc, key, cql_type)
    cluster = Cluster(column_encryption_policy=cl_policy)

:class:`~.AES256ColumnEncryptionPolicy` is a subclass of :class:`~.ColumnEncryptionPolicy` which provides 
encryption and decryption via AES-256.  This class is currently the only available column encryption policy 
implementation, although users can certainly implement their own by subclassing :class:`~.ColumnEncryptionPolicy`.

:class:`~.ColDesc` is a named tuple which uniquely identifies a column in a given keyspace and table.  When we
have this tuple, the encryption key and the CQL type contained by this column we can add the column to the policy
using :func:`~.ColumnEncryptionPolicy.add_column`.  Once we have added all column definitions to the policy we
pass it along to the cluster.

The CQL type for the column only has meaning at the client; it is never sent to Cassandra.  The encryption key 
is also never sent to the server; all the server ever sees are random bytes reflecting the encrypted data.  As a
result all columns containing client-side encrypted values should be declared with the CQL type "blob" at the 
Cassandra server.

Usage
-----

Encryption
^^^^^^^^^^
Client-side encryption shines most when used with prepared statements.  A prepared statement is aware of information 
about the columns in the query it was built from and we can use this information to transparently encrypt any
supplied parameters.  For example, we can create a prepared statement to insert a value into column1 (as defined above)
by executing the following code after creating a :class:`~.Cluster` in the manner described above:

.. code-block:: python
    session = cluster.connect()
    prepared = session.prepare("insert into ks1.table1 (column1) values (?)")
    session.execute(prepared, (1000,))

Our encryption policy will detect that "column1" is an encrypted column and take appropriate action.

As mentioned above client-side encryption can also be used with simple queries, although such use cases are
certainly not transparent.  :class:`~.ColumnEncryptionPolicy` provides a helper named
:func:`~.ColumnEncryptionPolicy.encode_and_encrypt` which will convert an input value into bytes using the
standard serialization methods employed by the driver.  The result is then encrypted according to the configuration
of the policy.  Using this approach the example above could be implemented along the lines of the following:

.. code-block:: python
    session = cluster.connect()
    session.execute("insert into ks1.table1 (column1) values (%s)",(cl_policy.encode_and_encrypt(col_desc, 1000),))

Decryption
^^^^^^^^^^
Decryption of values returned from the server is always transparent.  Whether we're executing a simple or prepared
statement encrypted columns will be decrypted automatically and made available via rows just like any other
result.

Limitations
-----------
:class:`~.AES256ColumnEncryptionPolicy` uses the implementation of AES-256 provided by the 
`cryptography <https://cryptography.io/en/latest/>`_ module.  Any limitations of this module should be considered
when deploying client-side encryption.  Note specifically that a Rust compiler is required for modern versions
of the cryptography package, although wheels exist for many common platforms.

Client-side encryption has been implemented for both the default Cython and pure Python row processing logic.
This functionality has not yet been ported to the NumPy Cython implementation.  We have reason to believe the
NumPy processing works reasonably well on Python 3.7 but fails for Python 3.8.  We hope to address this discrepancy
in a future release.