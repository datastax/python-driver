.. _security:

Security
========
The two main security components you will use with the
Python driver are Authentication and SSL.

Authentication
--------------
Versions 2.0 and higher of the driver support a SASL-based
authentication mechanism when :attr:`~.Cluster.protocol_version`
is set to 2 or higher.  To use this authentication, set
:attr:`~.Cluster.auth_provider` to an instance of a subclass
of :class:`~cassandra.auth.AuthProvider`.  When working
with Cassandra's ``PasswordAuthenticator``, you can use
the :class:`~cassandra.auth.PlainTextAuthProvider` class.

For example, suppose Cassandra is setup with its default
'cassandra' user with a password of 'cassandra':

.. code-block:: python

    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(auth_provider=auth_provider, protocol_version=2)



Custom Authenticators
^^^^^^^^^^^^^^^^^^^^^
If you're using something other than Cassandra's ``PasswordAuthenticator``,
:class:`~.SaslAuthProvider` is provided for generic SASL authentication mechanisms,
utilizing the ``pure-sasl`` package.
If these do not suit your needs, you may need to create your own subclasses of
:class:`~.AuthProvider` and :class:`~.Authenticator`.  You can use the Sasl classes
as example implementations.

Protocol v1 Authentication
^^^^^^^^^^^^^^^^^^^^^^^^^^
When working with Cassandra 1.2 (or a higher version with
:attr:`~.Cluster.protocol_version` set to ``1``), you will not pass in
an :class:`~.AuthProvider` instance.  Instead, you should pass in a
function that takes one argument, the IP address of a host, and returns
a dict of credentials with a ``username`` and ``password`` key:

.. code-block:: python

    from cassandra.cluster import Cluster

    def get_credentials(host_address):
        return {'username': 'joe', 'password': '1234'}

    cluster = Cluster(auth_provider=get_credentials, protocol_version=1)

SSL
---
SSL should be used when client encryption is enabled in Cassandra.

To give you as much control as possible over your SSL configuration, our SSL
API takes a user-created `SSLContext` instance from the Python standard library.
These docs will include some examples for how to achieve common configurations,
but the `ssl.SSLContext <https://docs.python.org/3/library/ssl.html#ssl.SSLContext>`_ documentation
gives a more complete description of what is possible.

To enable SSL with version 3.17.0 and higher, you will need to set :attr:`.Cluster.ssl_context` to a
``ssl.SSLContext`` instance to enable SSL. Optionally, you can also set :attr:`.Cluster.ssl_options`
to a dict of options. These will be passed as kwargs to ``ssl.SSLContext.wrap_socket()``
when new sockets are created.

The following examples assume you have generated your Cassandra certificate and
keystore files with these intructions:

* `Setup SSL Cert <https://docs.datastax.com/en/dse/6.7/dse-admin/datastax_enterprise/security/secSetUpSSLCert.html>`_

It might be also useful to learn about the different levels of identity verification to understand the examples:

* `Using SSL in DSE drivers <https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/appDevGuide/sslDrivers.html>`_

SSL with Twisted or Eventlet
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Twisted and Eventlet both use an alternative SSL implementation called pyOpenSSL, so if your `Cluster`'s connection class is
:class:`~cassandra.io.twistedreactor.TwistedConnection` or :class:`~cassandra.io.eventletreactor.EventletConnection`, you must pass a
`pyOpenSSL context <https://www.pyopenssl.org/en/stable/api/ssl.html#context-objects>`_ instead.
An example is provided in these docs, and more details can be found in the
`documentation <https://www.pyopenssl.org/en/stable/api/ssl.html#context-objects>`_.
pyOpenSSL is not installed by the driver and must be installed separately.

SSL Configuration Examples
^^^^^^^^^^^^^^^^^^^^^^^^^^
Here, we'll describe the server and driver configuration necessary to set up SSL to meet various goals, such as the client verifying the server and the server verifying the client. We'll also include Python code demonstrating how to use servers and drivers configured in these ways.

**No identity verification**

No identity verification at all. Note that this is not recommended for for production deployments.

The Cassandra configuration::

    client_encryption_options:
      enabled: true
      keystore: /path/to/127.0.0.1.keystore
      keystore_password: myStorePass
      require_client_auth: false

The driver configuration:

.. code-block:: python

    from cassandra.cluster import Cluster, Session
    from ssl import SSLContext, PROTOCOL_TLSv1

    ssl_context = SSLContext(PROTOCOL_TLSv1)

    cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context)
    session = cluster.connect()

**Client verifies server**

Ensure the python driver verifies the identity of the server.

The Cassandra configuration::

    client_encryption_options:
      enabled: true
      keystore: /path/to/127.0.0.1.keystore
      keystore_password: myStorePass
      require_client_auth: false

For the driver configuration, it's very important to set `ssl_context.verify_mode`
to `CERT_REQUIRED`. Otherwise, the loaded verify certificate will have no effect:

.. code-block:: python

    from cassandra.cluster import Cluster, Session
    from ssl import SSLContext, PROTOCOL_TLSv1, CERT_REQUIRED

    ssl_context = SSLContext(PROTOCOL_TLSv1)
    ssl_context.load_verify_locations('/path/to/rootca.crt')
    ssl_context.verify_mode = CERT_REQUIRED

    cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context)
    session = cluster.connect()

Additionally, you can also force the driver to verify the `hostname` of the server by passing additional options to `ssl_context.wrap_socket` via the `ssl_options` kwarg:

.. code-block:: python

    from cassandra.cluster import Cluster, Session
    from ssl import SSLContext, PROTOCOL_TLSv1, CERT_REQUIRED

    ssl_context = SSLContext(PROTOCOL_TLSv1)
    ssl_context.load_verify_locations('/path/to/rootca.crt')
    ssl_context.verify_mode = CERT_REQUIRED
    ssl_context.check_hostname = True
    ssl_options = {'server_hostname': '127.0.0.1'}

    cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context, ssl_options=ssl_options)
    session = cluster.connect()

**Server verifies client**

If Cassandra is configured to verify clients (``require_client_auth``), you need to generate
SSL key and certificate files.

The cassandra configuration::

    client_encryption_options:
      enabled: true
      keystore: /path/to/127.0.0.1.keystore
      keystore_password: myStorePass
      require_client_auth: true
      truststore: /path/to/dse-truststore.jks
      truststore_password: myStorePass

The Python ``ssl`` APIs require the certificate in PEM format. First, create a certificate
conf file:

.. code-block:: bash

    cat > gen_client_cert.conf <<EOF
    [ req ]
    distinguished_name = req_distinguished_name
    prompt = no
    output_password = ${ROOT_CERT_PASS}
    default_bits = 2048

    [ req_distinguished_name ]
    C = ${CERT_COUNTRY}
    O = ${CERT_ORG_NAME}
    OU = ${CERT_OU}
    CN = client
    EOF

Make sure you replaced the variables with the same values you used for the initial
root CA certificate. Then, generate the key:

.. code-block:: bash

    openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -config gen_client_cert.conf

And generate the client signed certificate:

.. code-block:: bash

    openssl x509 -req -CA ${ROOT_CA_BASE_NAME}.crt -CAkey ${ROOT_CA_BASE_NAME}.key -passin pass:${ROOT_CERT_PASS} \
        -in client.csr -out client.crt_signed -days ${CERT_VALIDITY} -CAcreateserial

Finally, you can use that configuration with the following driver code:

.. code-block:: python

    from cassandra.cluster import Cluster, Session
    from ssl import SSLContext, PROTOCOL_TLSv1

    ssl_context = SSLContext(PROTOCOL_TLSv1)
    ssl_context.load_cert_chain(
        certfile='/path/to/client.crt_signed',
        keyfile='/path/to/client.key')

    cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context)
    session = cluster.connect()


**Server verifies client and client verifies server**

See the previous section for examples of Cassandra configuration and preparing
the client certificates.

The following driver code specifies that the connection should use two-way verification:

.. code-block:: python

    from cassandra.cluster import Cluster, Session
    from ssl import SSLContext, PROTOCOL_TLSv1, CERT_REQUIRED

    ssl_context = SSLContext(PROTOCOL_TLSv1)
    ssl_context.load_verify_locations('/path/to/rootca.crt')
    ssl_context.verify_mode = CERT_REQUIRED
    ssl_context.load_cert_chain(
        certfile='/path/to/client.crt_signed',
        keyfile='/path/to/client.key')

    cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context)
    session = cluster.connect()


The driver uses ``SSLContext`` directly to give you many other options in configuring SSL. Consider reading the `Python SSL documentation <https://docs.python.org/library/ssl.html#ssl.SSLContext>`_
for more details about ``SSLContext`` configuration.

**Server verifies client and client verifies server using Twisted and pyOpenSSL**

.. code-block:: python

    from OpenSSL import SSL, crypto
    from cassandra.cluster import Cluster
    from cassandra.io.twistedreactor import TwistedConnection

    ssl_context = SSL.Context(SSL.TLSv1_METHOD)
    ssl_context.set_verify(SSL.VERIFY_PEER, callback=lambda _1, _2, _3, _4, ok: ok)
    ssl_context.use_certificate_file('/path/to/client.crt_signed')
    ssl_context.use_privatekey_file('/path/to/client.key')
    ssl_context.load_verify_locations('/path/to/rootca.crt')

    cluster = Cluster(
        contact_points=['127.0.0.1'],
        connection_class=TwistedConnection,
        ssl_context=ssl_context,
        ssl_options={'check_hostname': True}
    )
    session = cluster.connect()


Connecting using Eventlet would look similar except instead of importing and using ``TwistedConnection``, you would
import and use ``EventletConnection``, including the appropriate monkey-patching.

Versions 3.16.0 and lower
^^^^^^^^^^^^^^^^^^^^^^^^^

To enable SSL you will need to set :attr:`.Cluster.ssl_options` to a
dict of options.  These will be passed as kwargs to ``ssl.wrap_socket()``
when new sockets are created. Note that this use of ssl_options will be
deprecated in the next major release.

By default, a ``ca_certs`` value should be supplied (the value should be
a string pointing to the location of the CA certs file), and you probably
want to specify ``ssl_version`` as ``ssl.PROTOCOL_TLSv1`` to match
Cassandra's default protocol.

For example:

.. code-block:: python

    from cassandra.cluster import Cluster
    from ssl import PROTOCOL_TLSv1, CERT_REQUIRED

    ssl_opts = {
        'ca_certs': '/path/to/my/ca.certs',
        'ssl_version': PROTOCOL_TLSv1,
        'cert_reqs': CERT_REQUIRED  # Certificates are required and validated
    }
    cluster = Cluster(ssl_options=ssl_opts)

This is only an example to show how to pass the ssl parameters. Consider reading
the `python ssl documentation <https://docs.python.org/2/library/ssl.html#ssl.wrap_socket>`_ for
your configuration. For further reading, Andrew Mussey has published a thorough guide on
`Using SSL with the DataStax Python driver <http://blog.amussey.com/post/64036730812/cassandra-2-0-client-server-ssl-with-datastax-python>`_.

SSL with Twisted
++++++++++++++++

In case the twisted event loop is used pyOpenSSL must be installed or an exception will be risen. Also
to set the ``ssl_version`` and ``cert_reqs`` in ``ssl_opts`` the appropriate constants from pyOpenSSL are expected.
