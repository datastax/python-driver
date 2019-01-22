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

Versions 3.17.0 and higher
^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable SSL you will need to set :attr:`.Cluster.ssl_context` to a
``ssl.SSLContext`` instance. Optionally, you can also set :attr:`.Cluster.ssl_options`
to a dict of options. These will be passed as kwargs to ``ssl.SSLContext.wrap_socket()``
when new sockets are created.

For example:

.. code-block:: python

    from cassandra.cluster import Cluster
    from ssl import SSLContext, PROTOCOL_TLSv1, CERT_REQUIRED

    ssl_context = SSLContext(PROTOCOL_TLSv1)
    ssl_context.verify_mode = CERT_REQUIRED
    ssl_context.load_verify_locations('/path/to/my/ca.certs')
    cluster = Cluster(ssl_context=ssl_context)


This is only an example to show how to pass the ssl parameters. Consider reading
the `python ssl documentation <https://docs.python.org/3/library/ssl.html#ssl.SSLContext>`_ for
your configuration.

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
