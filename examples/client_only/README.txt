The client_only example uses the fat client to insert data into and read
data from Cassandra.

-- Warning --
The method used in this example (the fat client) should generally
not be used instead of the thrift interface because of possible
instability of the internal Cassandra API.

-- Prerequisite --
Build the Cassandra source in the current source tree. Also, if
running the client_only code against a local Cassandra node, start
the local node prior to running the client_only script. See the
configuration below for more info.

-- Build --
To build, run ant from the contrib/client_only directory. It will build
the source, then jar up the compiled class, the conf/cassandra.yaml, and
dependencies into build/client_only.jar.

-- Run --
To run, from the contrib/client_only directory run:
bin/client_only write
or
bin/client_only read

'write' will create keyspace Keyspace1 and column family Standard1. If
it is already there, it will error out. It will then write a bunch of
data to the cluster it connects to.

'read' will read the data that was written in the write step.

-- Configuration --
The conf/cassandra.yaml is to start up the fat client. The fat client
joins the gossip network but does not participate in storage. It
needs to have the same configuration as the rest of the cluster except
listen address and rpc address. If you are running your cluster just
on your local machine, you'll need to use another address for this node.
Therefore, your local full Cassandra node can be 127.0.0.1 and the fat 
client can be 127.0.0.2. Such aliasing is enabled by default on linux.
On Mac OS X, use the following command to use the second IP address:
sudo ifconfig lo0 alias 127.0.0.2 up

cassandra.yaml can be on the classpath as is done here, can be specified
(by modifying the script) in a location within the classpath like this:
java -Xmx1G -Dcassandra.config=/path/in/classpath/to/cassandra.yaml ...
or can be retrieved from a location outside the classpath like this:
... -Dcassandra.config=file:///path/to/cassandra.yaml ...
or
... -Dcassandra.config=http://awesomesauce.com/cassandra.yaml ...