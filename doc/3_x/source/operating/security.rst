.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

.. highlight:: none

Security
--------
There are three main components to the security features provided by Cassandra:

- TLS/SSL encryption for client and inter-node communication
- Client authentication
- Authorization

By default, these features are disabled as Cassandra is configured to easily find and be found by other members of a
cluster. In other words, an out-of-the-box Cassandra installation presents a large attack surface for a bad actor.
Enabling authentication for clients using the binary protocol is not sufficient to protect a cluster. Malicious users
able to access internode communication and JMX ports can still:

- Craft internode messages to insert users into authentication schema
- Craft internode messages to truncate or drop schema
- Use tools such as ``sstableloader`` to overwrite ``system_auth`` tables 
- Attach to the cluster directly to capture write traffic

Correct configuration of all three security components should negate theses vectors. Therefore, understanding Cassandra's
security features is crucial to configuring your cluster to meet your security needs.


TLS/SSL Encryption
^^^^^^^^^^^^^^^^^^
Cassandra provides secure communication between a client machine and a database cluster and between nodes within a
cluster. Enabling encryption ensures that data in flight is not compromised and is transferred securely. The options for
client-to-node and node-to-node encryption are managed separately and may be configured independently.

In both cases, the JVM defaults for supported protocols and cipher suites are used when encryption is enabled. These can
be overidden using the settings in ``cassandra.yaml``, but this is not recommended unless there are policies in place
which dictate certain settings or a need to disable vulnerable ciphers or protocols in cases where the JVM cannot be
updated.

FIPS compliant settings can be configured at the JVM level and should not involve changing encryption settings in
cassandra.yaml. See `the java document on FIPS <https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/FIPS.html>`__
for more details.

For information on generating the keystore and truststore files used in SSL communications, see the
`java documentation on creating keystores <http://download.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#CreateKeystore>`__

SSL Certificate Hot Reloading
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Beginning with Cassandra 4, Cassandra supports hot reloading of SSL Certificates. If SSL/TLS support is enabled in Cassandra,
the node periodically polls the Trust and Key Stores specified in cassandra.yaml. When the files are updated, Cassandra will
reload them and use them for subsequent connections. Please note that the Trust & Key Store passwords are part of the yaml so
the updated files should also use the same passwords. The default polling interval is 10 minutes.

Certificate Hot reloading may also be triggered using the ``nodetool reloadssl`` command. Use this if you want to Cassandra to
immediately notice the changed certificates.

Inter-node Encryption
~~~~~~~~~~~~~~~~~~~~~

The settings for managing inter-node encryption are found in ``cassandra.yaml`` in the ``server_encryption_options``
section. To enable inter-node encryption, change the ``internode_encryption`` setting from its default value of ``none``
to one value from: ``rack``, ``dc`` or ``all``.

Client to Node Encryption
~~~~~~~~~~~~~~~~~~~~~~~~~

The settings for managing client to node encryption are found in ``cassandra.yaml`` in the ``client_encryption_options``
section. There are two primary toggles here for enabling encryption, ``enabled`` and ``optional``.

- If neither is set to ``true``, client connections are entirely unencrypted.
- If ``enabled`` is set to ``true`` and ``optional`` is set to ``false``, all client connections must be secured.
- If both options are set to ``true``, both encrypted and unencrypted connections are supported using the same port.
  Client connections using encryption with this configuration will be automatically detected and handled by the server.

As an alternative to the ``optional`` setting, separate ports can also be configured for secure and unsecure connections
where operational requirements demand it. To do so, set ``optional`` to false and use the ``native_transport_port_ssl``
setting in ``cassandra.yaml`` to specify the port to be used for secure client communication.

.. _operation-roles:

Roles
^^^^^

Cassandra uses database roles, which may represent either a single user or a group of users, in both authentication and
permissions management. Role management is an extension point in Cassandra and may be configured using the
``role_manager`` setting in ``cassandra.yaml``. The default setting uses ``CassandraRoleManager``, an implementation
which stores role information in the tables of the ``system_auth`` keyspace.

See also the :ref:`CQL documentation on roles <cql-roles>`.

Authentication
^^^^^^^^^^^^^^

Authentication is pluggable in Cassandra and is configured using the ``authenticator`` setting in ``cassandra.yaml``.
Cassandra ships with two options included in the default distribution.

By default, Cassandra is configured with ``AllowAllAuthenticator`` which performs no authentication checks and therefore
requires no credentials. It is used to disable authentication completely. Note that authentication is a necessary
condition of Cassandra's permissions subsystem, so if authentication is disabled, effectively so are permissions.

The default distribution also includes ``PasswordAuthenticator``, which stores encrypted credentials in a system table.
This can be used to enable simple username/password authentication.

.. _password-authentication:

Enabling Password Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before enabling client authentication on the cluster, client applications should be pre-configured with their intended
credentials. When a connection is initiated, the server will only ask for credentials once authentication is
enabled, so setting up the client side config in advance is safe. In contrast, as soon as a server has authentication
enabled, any connection attempt without proper credentials will be rejected which may cause availability problems for
client applications. Once clients are setup and ready for authentication to be enabled, follow this procedure to enable
it on the cluster.

Pick a single node in the cluster on which to perform the initial configuration. Ideally, no clients should connect
to this node during the setup process, so you may want to remove it from client config, block it at the network level
or possibly add a new temporary node to the cluster for this purpose. On that node, perform the following steps:

1. Open a ``cqlsh`` session and change the replication factor of the ``system_auth`` keyspace. By default, this keyspace
   uses ``SimpleReplicationStrategy`` and a ``replication_factor`` of 1. It is recommended to change this for any
   non-trivial deployment to ensure that should nodes become unavailable, login is still possible. Best practice is to
   configure a replication factor of 3 to 5 per-DC.

::

    ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 3, 'DC2': 3};

2. Edit ``cassandra.yaml`` to change the ``authenticator`` option like so:

::

    authenticator: PasswordAuthenticator

3. Restart the node.

4. Open a new ``cqlsh`` session using the credentials of the default superuser:

::

    cqlsh -u cassandra -p cassandra

5. During login, the credentials for the default superuser are read with a consistency level of ``QUORUM``, whereas
   those for all other users (including superusers) are read at ``LOCAL_ONE``. In the interests of performance and
   availability, as well as security, operators should create another superuser and disable the default one. This step
   is optional, but highly recommended. While logged in as the default superuser, create another superuser role which
   can be used to bootstrap further configuration.

::

    # create a new superuser
    CREATE ROLE dba WITH SUPERUSER = true AND LOGIN = true AND PASSWORD = 'super';

6. Start a new cqlsh session, this time logging in as the new_superuser and disable the default superuser.

::

    ALTER ROLE cassandra WITH SUPERUSER = false AND LOGIN = false;

7. Finally, set up the roles and credentials for your application users with :ref:`CREATE ROLE <create-role-statement>`
   statements.

At the end of these steps, the one node is configured to use password authentication. To roll that out across the
cluster, repeat steps 2 and 3 on each node in the cluster. Once all nodes have been restarted, authentication will be
fully enabled throughout the cluster.

Note that using ``PasswordAuthenticator`` also requires the use of :ref:`CassandraRoleManager <operation-roles>`.

See also: :ref:`setting-credentials-for-internal-authentication`, :ref:`CREATE ROLE <create-role-statement>`,
:ref:`ALTER ROLE <alter-role-statement>`, :ref:`ALTER KEYSPACE <alter-keyspace-statement>` and :ref:`GRANT PERMISSION
<grant-permission-statement>`,

Authorization
^^^^^^^^^^^^^

Authorization is pluggable in Cassandra and is configured using the ``authorizer`` setting in ``cassandra.yaml``.
Cassandra ships with two options included in the default distribution.

By default, Cassandra is configured with ``AllowAllAuthorizer`` which performs no checking and so effectively grants all
permissions to all roles. This must be used if ``AllowAllAuthenticator`` is the configured authenticator.

The default distribution also includes ``CassandraAuthorizer``, which does implement full permissions management
functionality and stores its data in Cassandra system tables.

Enabling Internal Authorization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Permissions are modelled as a whitelist, with the default assumption that a given role has no access to any database
resources. The implication of this is that once authorization is enabled on a node, all requests will be rejected until
the required permissions have been granted. For this reason, it is strongly recommended to perform the initial setup on
a node which is not processing client requests.

The following assumes that authentication has already been enabled via the process outlined in
:ref:`password-authentication`. Perform these steps to enable internal authorization across the cluster:

1. On the selected node, edit ``cassandra.yaml`` to change the ``authorizer`` option like so:

::

    authorizer: CassandraAuthorizer

2. Restart the node.

3. Open a new ``cqlsh`` session using the credentials of a role with superuser credentials:

::

    cqlsh -u dba -p super

4. Configure the appropriate access privileges for your clients using `GRANT PERMISSION <cql.html#grant-permission>`_
   statements. On the other nodes, until configuration is updated and the node restarted, this will have no effect so
   disruption to clients is avoided.

::

    GRANT SELECT ON ks.t1 TO db_user;

5. Once all the necessary permissions have been granted, repeat steps 1 and 2 for each node in turn. As each node
   restarts and clients reconnect, the enforcement of the granted permissions will begin.

See also: :ref:`GRANT PERMISSION <grant-permission-statement>`, `GRANT ALL <grant-all>` and :ref:`REVOKE PERMISSION
<revoke-permission-statement>`

Caching
^^^^^^^

Enabling authentication and authorization places additional load on the cluster by frequently reading from the
``system_auth`` tables. Furthermore, these reads are in the critical paths of many client operations, and so has the
potential to severely impact quality of service. To mitigate this, auth data such as credentials, permissions and role
details are cached for a configurable period. The caching can be configured (and even disabled) from ``cassandra.yaml``
or using a JMX client. The JMX interface also supports invalidation of the various caches, but any changes made via JMX
are not persistent and will be re-read from ``cassandra.yaml`` when the node is restarted.

Each cache has 3 options which can be set:

Validity Period
    Controls the expiration of cache entries. After this period, entries are invalidated and removed from the cache.
Refresh Rate
    Controls the rate at which background reads are performed to pick up any changes to the underlying data. While these
    async refreshes are performed, caches will continue to serve (possibly) stale data. Typically, this will be set to a
    shorter time than the validity period.
Max Entries
    Controls the upper bound on cache size.

The naming for these options in ``cassandra.yaml`` follows the convention:

* ``<type>_validity_in_ms``
* ``<type>_update_interval_in_ms``
* ``<type>_cache_max_entries``

Where ``<type>`` is one of ``credentials``, ``permissions``, or ``roles``.

As mentioned, these are also exposed via JMX in the mbeans under the ``org.apache.cassandra.auth`` domain.

JMX access
^^^^^^^^^^

Access control for JMX clients is configured separately to that for CQL. For both authentication and authorization, two
providers are available; the first based on standard JMX security and the second which integrates more closely with
Cassandra's own auth subsystem.

The default settings for Cassandra make JMX accessible only from localhost. To enable remote JMX connections, edit
``cassandra-env.sh`` (or ``cassandra-env.ps1`` on Windows) to change the ``LOCAL_JMX`` setting to ``yes``. Under the
standard configuration, when remote JMX connections are enabled, :ref:`standard JMX authentication <standard-jmx-auth>`
is also switched on.

Note that by default, local-only connections are not subject to authentication, but this can be enabled.

If enabling remote connections, it is recommended to also use :ref:`SSL <jmx-with-ssl>` connections.

Finally, after enabling auth and/or SSL, ensure that tools which use JMX, such as :ref:`nodetool <nodetool>`, are
correctly configured and working as expected.

.. _standard-jmx-auth:

Standard JMX Auth
~~~~~~~~~~~~~~~~~

Users permitted to connect to the JMX server are specified in a simple text file. The location of this file is set in
``cassandra-env.sh`` by the line:

::

    JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.password.file=/etc/cassandra/jmxremote.password"

Edit the password file to add username/password pairs:

::

    jmx_user jmx_password

Secure the credentials file so that only the user running the Cassandra process can read it :

::

    $ chown cassandra:cassandra /etc/cassandra/jmxremote.password
    $ chmod 400 /etc/cassandra/jmxremote.password

Optionally, enable access control to limit the scope of what defined users can do via JMX. Note that this is a fairly
blunt instrument in this context as most operational tools in Cassandra require full read/write access. To configure a
simple access file, uncomment this line in ``cassandra-env.sh``:

::

    #JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.access.file=/etc/cassandra/jmxremote.access"

Then edit the access file to grant your JMX user readwrite permission:

::

    jmx_user readwrite

Cassandra must be restarted to pick up the new settings.

See also : `Using File-Based Password Authentication In JMX
<http://docs.oracle.com/javase/7/docs/technotes/guides/management/agent.html#gdenv>`__


Cassandra Integrated Auth
~~~~~~~~~~~~~~~~~~~~~~~~~

An alternative to the out-of-the-box JMX auth is to useeCassandra's own authentication and/or authorization providers
for JMX clients. This is potentially more flexible and secure but it come with one major caveat. Namely that it is not
available until `after` a node has joined the ring, because the auth subsystem is not fully configured until that point
However, it is often critical for monitoring purposes to have JMX access particularly during bootstrap. So it is
recommended, where possible, to use local only JMX auth during bootstrap and then, if remote connectivity is required,
to switch to integrated auth once the node has joined the ring and initial setup is complete.

With this option, the same database roles used for CQL authentication can be used to control access to JMX, so updates
can be managed centrally using just ``cqlsh``. Furthermore, fine grained control over exactly which operations are
permitted on particular MBeans can be acheived via :ref:`GRANT PERMISSION <grant-permission-statement>`.

To enable integrated authentication, edit ``cassandra-env.sh`` to uncomment these lines:

::

    #JVM_OPTS="$JVM_OPTS -Dcassandra.jmx.remote.login.config=CassandraLogin"
    #JVM_OPTS="$JVM_OPTS -Djava.security.auth.login.config=$CASSANDRA_HOME/conf/cassandra-jaas.config"

And disable the JMX standard auth by commenting this line:

::

    JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.password.file=/etc/cassandra/jmxremote.password"

To enable integrated authorization, uncomment this line:

::

    #JVM_OPTS="$JVM_OPTS -Dcassandra.jmx.authorizer=org.apache.cassandra.auth.jmx.AuthorizationProxy"

Check standard access control is off by ensuring this line is commented out:

::

   #JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.access.file=/etc/cassandra/jmxremote.access"

With integrated authentication and authorization enabled, operators can define specific roles and grant them access to
the particular JMX resources that they need. For example, a role with the necessary permissions to use tools such as
jconsole or jmc in read-only mode would be defined as:

::

    CREATE ROLE jmx WITH LOGIN = false;
    GRANT SELECT ON ALL MBEANS TO jmx;
    GRANT DESCRIBE ON ALL MBEANS TO jmx;
    GRANT EXECUTE ON MBEAN 'java.lang:type=Threading' TO jmx;
    GRANT EXECUTE ON MBEAN 'com.sun.management:type=HotSpotDiagnostic' TO jmx;

    # Grant the jmx role to one with login permissions so that it can access the JMX tooling
    CREATE ROLE ks_user WITH PASSWORD = 'password' AND LOGIN = true AND SUPERUSER = false;
    GRANT jmx TO ks_user;

Fine grained access control to individual MBeans is also supported:

::

    GRANT EXECUTE ON MBEAN 'org.apache.cassandra.db:type=Tables,keyspace=test_keyspace,table=t1' TO ks_user;
    GRANT EXECUTE ON MBEAN 'org.apache.cassandra.db:type=Tables,keyspace=test_keyspace,table=*' TO ks_owner;

This permits the ``ks_user`` role to invoke methods on the MBean representing a single table in ``test_keyspace``, while
granting the same permission for all table level MBeans in that keyspace to the ``ks_owner`` role.

Adding/removing roles and granting/revoking of permissions is handled dynamically once the initial setup is complete, so
no further restarts are required if permissions are altered.

See also: :ref:`Permissions <cql-permissions>`.

.. _jmx-with-ssl:

JMX With SSL
~~~~~~~~~~~~

JMX SSL configuration is controlled by a number of system properties, some of which are optional. To turn on SSL, edit
the relevant lines in ``cassandra-env.sh`` (or ``cassandra-env.ps1`` on Windows) to uncomment and set the values of these
properties as required:

``com.sun.management.jmxremote.ssl``
    set to true to enable SSL
``com.sun.management.jmxremote.ssl.need.client.auth``
    set to true to enable validation of client certificates
``com.sun.management.jmxremote.registry.ssl``
    enables SSL sockets for the RMI registry from which clients obtain the JMX connector stub
``com.sun.management.jmxremote.ssl.enabled.protocols``
    by default, the protocols supported by the JVM will be used, override with a comma-separated list. Note that this is
    not usually necessary and using the defaults is the preferred option.
``com.sun.management.jmxremote.ssl.enabled.cipher.suites``
    by default, the cipher suites supported by the JVM will be used, override with a comma-separated list. Note that
    this is not usually necessary and using the defaults is the preferred option.
``javax.net.ssl.keyStore``
    set the path on the local filesystem of the keystore containing server private keys and public certificates
``javax.net.ssl.keyStorePassword``
    set the password of the keystore file
``javax.net.ssl.trustStore``
    if validation of client certificates is required, use this property to specify the path of the truststore containing
    the public certificates of trusted clients
``javax.net.ssl.trustStorePassword``
    set the password of the truststore file

See also: `Oracle Java7 Docs <http://docs.oracle.com/javase/7/docs/technotes/guides/management/agent.html#gdemv>`__,
`Monitor Java with JMX <https://www.lullabot.com/articles/monitor-java-with-jmx>`__
