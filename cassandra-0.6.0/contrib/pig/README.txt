A Pig LoadFunc that reads all columns from a given ColumnFamily.

Setup:

First build and start a Cassandra server with the default
configuration* and set the PIG_HOME and JAVA_HOME environment
variables to the location of a Pig >= 0.7.0-dev install and your Java
install. If you would like to run using the Hadoop backend, you should
also set PIG_CONF_DIR to the location of your Hadoop config.

Run:

contrib/pig$ ant
contrib/pig$ bin/pig_cassandra

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

*If you want to point Pig at a real cluster, modify the seed
address in storage-conf.xml and re-run the build step.
