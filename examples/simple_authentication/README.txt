The files in this directory provide a (simplistic) example of how to add
authentication and resource permissions to Cassandra by implementing the
org.apache.cassandra.auth.{IAuthenticator, IAuthority} interfaces.

To try those examples, copy the two JAVA sources (in src/) into the main
cassandra sources directory and the two configuration files (in conf/) in the
main cassandra configuration directory.

You can then set the authenticator and authority properties in cassandra.yaml
to use those classes. See the two configuration files access.properties and
passwd.properties to configure the authorized users and permissions.

When starting cassandra, you need to specify the location of the passwd.properties
and access.properties files by adding JVM args similar to the following either
in cassandra-env.sh or as commandline arguments:

    -Dpasswd.properties=conf/passwd.properties
    -Daccess.properties=conf/access.properties

For example, you might invoke cassandra as follows:

    bin/cassandra -f -Dpasswd.properties=conf/passwd.properties -Daccess.properties=conf/access.properties

Please note that the code in this directory is for demonstration purposes. In
particular, it does not provide a high level of security.
