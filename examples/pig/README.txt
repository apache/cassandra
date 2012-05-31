A Pig storage class that reads all columns from a given ColumnFamily, or writes
properly formatted results into a ColumnFamily.

Getting Started
===============

First build and start a Cassandra server with the default
configuration and set the PIG_HOME and JAVA_HOME environment
variables to the location of a Pig >= 0.7.0 install and your Java
install. 

If you would like to run using the Hadoop backend, you should
also set PIG_CONF_DIR to the location of your Hadoop config.

Finally, set the following as environment variables (uppercase,
underscored), or as Hadoop configuration variables (lowercase, dotted):
* PIG_INITIAL_ADDRESS or cassandra.thrift.address : initial address to connect to
* PIG_RPC_PORT or cassandra.thrift.port : the port thrift is listening on
* PIG_PARTITIONER or cassandra.partitioner.class : cluster partitioner

For example, against a local node with the default settings, you'd use:
export PIG_INITIAL_ADDRESS=localhost
export PIG_RPC_PORT=9160
export PIG_PARTITIONER=org.apache.cassandra.dht.RandomPartitioner

These properties can be overridden with the following if you use different clusters
for input and output:
* PIG_INPUT_INITIAL_ADDRESS : initial address to connect to for reading
* PIG_INPUT_RPC_PORT : the port thrift is listening on for reading
* PIG_INPUT_PARTITIONER : cluster partitioner for reading
* PIG_OUTPUT_INITIAL_ADDRESS : initial address to connect to for writing
* PIG_OUTPUT_RPC_PORT : the port thrift is listening on for writing
* PIG_OUTPUT_PARTITIONER : cluster partitioner for writing

Then you can run it like this:

examples/pig$ bin/pig_cassandra -x local example-script.pig

This will run the test script against your Cassandra instance
and will assume that there is a MyKeyspace/MyColumnFamily with some
data in it. It will run in local mode (see pig docs for more info).

If you'd like to get to a 'grunt>' shell prompt, run:

examples/pig$ bin/pig_cassandra -x local

Once the 'grunt>' shell has loaded, try a simple program like the
following, which will determine the top 50 column names:

grunt> rows = LOAD 'cassandra://MyKeyspace/MyColumnFamily' USING CassandraStorage();
grunt> cols = FOREACH rows GENERATE flatten(columns);
grunt> colnames = FOREACH cols GENERATE $0;
grunt> namegroups = GROUP colnames BY (chararray) $0;
grunt> namecounts = FOREACH namegroups GENERATE COUNT($1), group;
grunt> orderednames = ORDER namecounts BY $0 DESC;
grunt> topnames = LIMIT orderednames 50;
grunt> dump topnames;

Slices on columns can also be specified:
grunt> rows = LOAD 'cassandra://MyKeyspace/MyColumnFamily?slice_start=C2&slice_end=C4&limit=1&reversed=true' USING CassandraStorage();

Binary values for slice_start and slice_end can be escaped such as '\u0255'

Outputting to Cassandra requires the same format from input, so the simplest example is:

grunt> rows = LOAD 'cassandra://MyKeyspace/MyColumnFamily' USING CassandraStorage();
grunt> STORE rows into 'cassandra://MyKeyspace/MyColumnFamily' USING CassandraStorage();

Which will copy the ColumnFamily.  Note that the destination ColumnFamily must
already exist for this to work.

See the example in test/ to see how schema is inferred.

Advanced Options
================

The following environment variables default to false but can be set to true to enable them:

PIG_WIDEROW_INPUT:  this enables loading of rows with many columns without
                    incurring memory pressure.  All columns will be in a bag and indexes are not
                    supported.

PIG_USE_SECONDARY:  this allows easy use of secondary indexes within your
                    script, by appending every index to the schema as 'index_$name', allowing
                    filtering of loaded rows with a statement like "FILTER rows BY index_color eq
                    'blue'" if you have an index called 'color' defined.

