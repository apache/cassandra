PropertyFileEndPointSnitch
==========================

Cassandra's RackAwareStrategy can be used to have replication take
data-center and/or rack location into account when placing replicas. The
ProperyFileEndPointSnitch allows you to make use of RackAwareStrategy by
specifying node locations in a standard key/value properties file.


Properties File
---------------
The EndPointSnitch expects to find a file on the classapth named
cassandra-rack.properties in the following format:

  <node IP>\:<port>=<data center name>:<rack name>

There is also a special directive used to define which information to
return for unconfigured nodes:

  default=<data center name>:<rack name>

See conf/cassandra-rack.properties for an annotated example config.


Installing
----------
 * Run the ant jar target 
 * Copy build/cassandra-propsnitch.jar to your Cassandra lib/
   directory, or otherwise add it to the CLASSPATH
   (see http://wiki.apache.org/cassandra/RunningCassandra)
 * Edit the EndPointSnitch element of storage-conf.xml to use
   org.apache.cassandra.locator.PropertyFileEndPointSnitch
 * Create the cassandra-rack.properties in your classpath
   (e.g. in your Cassandra conf/ directory)
 * Optionally set ReplicaPlacementStrategy in cassandra.xml to
   org.apache.cassandra.locator.RackAwareStrategy


Running/Managing
----------------
This endpointsnitch also registers itself as an MBean which can be
used to reload the configuration file in the case the properties file
has changed.  Additionally, the current rack information can be
retrieved.
