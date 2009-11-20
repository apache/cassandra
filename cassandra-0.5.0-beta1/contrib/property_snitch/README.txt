PropertyFileEndPointSnitch
==========================

Cassandra's RackAwareStrategy can be used to have replication take
data-center and/or rack location into account when placing replicas. The
ProperyFileEndPointSnitch allows you to make use of RackAwareStrategy by
specifying node locations in a standard key/value properties file.


Properties File
---------------
The EndPointSnitch expects to find a standard properties file at
/etc/cassandra/rack.properties in the following format:

  <node IP>\:<port>=<data center name>:<rack name>

There is also a special directive used to define which information to
return for unconfigured nodes:

  default=<data center name>:<rack name>

See conf/rack.properties for an annotated example config.


Installing
----------
 * Run the ant jar target 
 * Add build/cassandra-propsnitch.jar to the CLASSPATH
 * Edit storage-conf.xml and set ReplicaPlacementStrategy to
   org.apache.cassandra.locator.RackAwareStrategy
 * Edit the EndPointSnitch element of storage-conf.xml to use
   org.apache.cassandra.locator.PropertyFileEndPointSnitch
 * Create the file /etc/cassandra/rack.properties


Running/Managing
----------------
This endpointsnitch also registers itself as an MBean which can be used to
reload the configuration file in the case the rack.properties file has
changed.  Additionally, the current rack information can be retrieved as
well.
