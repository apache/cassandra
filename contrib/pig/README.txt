A Pig storage class that reads all columns from a given ColumnFamily, or writes
properly formatted results into a ColumnFamily.

Setup:

First build and start a Cassandra server with the default
configuration and set the PIG_HOME and JAVA_HOME environment
variables to the location of a Pig >= 0.7.0 install and your Java
install. 

NOTE: if you intend to _output_ to Cassandra, until there is a Pig release that
uses jackson > 1.0.1 (see https://issues.apache.org/jira/browse/PIG-1863) you
will need to build Pig yourself with jackson 1.4.  To do this, edit Pig's
ivy/libraries.properties, and run ant.

If you would like to run using the Hadoop backend, you should
also set PIG_CONF_DIR to the location of your Hadoop config.

Finally, set the following as environment variables (uppercase,
underscored), or as Hadoop configuration variables (lowercase, dotted):
* PIG_RPC_PORT or cassandra.thrift.port : the port thrift is listening on 
* PIG_INITIAL_ADDRESS or cassandra.thrift.address : initial address to connect to
* PIG_PARTITIONER or cassandra.partitioner.class : cluster partitioner

Run:

contrib/pig$ ant
contrib/pig$ bin/pig_cassandra -x local example-script.pig

This will run the test script against your Cassandra instance
and will assume that there is a Keyspace1/Standard1 with some
data in it. It will run in local mode (see pig docs for more info).

If you'd like to get to a 'grunt>' shell prompt, run:

contrib/pig$ bin/pig_cassandra -x local

Once the 'grunt>' shell has loaded, try a simple program like the
following, which will determine the top 50 column names:

grunt> rows = LOAD 'cassandra://Keyspace1/Standard1' USING CassandraStorage();
grunt> cols = FOREACH rows GENERATE flatten($1);
grunt> colnames = FOREACH cols GENERATE $0;
grunt> namegroups = GROUP colnames BY $0;
grunt> namecounts = FOREACH namegroups GENERATE COUNT($1), group;
grunt> orderednames = ORDER namecounts BY $0;
grunt> topnames = LIMIT orderednames 50;
grunt> dump topnames;

Outputting to Cassandra requires the same format from input, so the simplest example is:

grunt> rows = LOAD 'cassandra://Keyspace1/Standard1' USING CassandraStorage();
grunt> STORE rows into 'cassandra://Keyspace1/Standard2' USING CassandraStorage();

Which will copy the ColumnFamily.  Note that the destination ColumnFamily must
already exist for this to work.
