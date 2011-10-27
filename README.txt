Cassandra is a highly scalable, eventually consistent, distributed, structured 
key-value store. 


Project description
-------------------

Cassandra brings together the distributed systems technologies from Dynamo 
and the data model from Google's BigTable. Like Dynamo, Cassandra is 
eventually consistent. Like BigTable, Cassandra provides a ColumnFamily-based
data model richer than typical key/value systems.

For more information see http://cassandra.apache.org/

Requirements
------------
  * Java >= 1.6 (OpenJDK and Sun have been tested)

Getting started
---------------

This short guide will walk you through getting a basic one node cluster up
and running, and demonstrate some simple reads and writes.

  * tar -zxvf apache-cassandra-$VERSION.tar.gz
  * cd apache-cassandra-$VERSION
  * sudo mkdir -p /var/log/cassandra
  * sudo chown -R `whoami` /var/log/cassandra
  * sudo mkdir -p /var/lib/cassandra
  * sudo chown -R `whoami` /var/lib/cassandra

Note: The sample configuration files in conf/ determine the file-system 
locations Cassandra uses for logging and data storage. You are free to
change these to suit your own environment and adjust the path names
used here accordingly.

Now that we're ready, let's start it up!

  * bin/cassandra -f

Unix: Running the startup script with the -f argument will cause
Cassandra to remain in the foreground and log to standard out.

Windows: bin\cassandra.bat runs in the foreground by default.  To
install Cassandra as a Windows service, download Procrun from
http://commons.apache.org/daemon/procrun.html, set the PRUNSRV
environment variable to the full path of prunsrv (e.g.,
C:\procrun\prunsrv.exe), and run "bin\cassandra.bat install".
Similarly, "uninstall" will remove the service.

Now let's try to read and write some data using the command line client.

  * bin/cassandra-cli --host localhost

The command line client is interactive so if everything worked you should
be sitting in front of a prompt...

  Connected to: "Test Cluster" on localhost/9160
  Welcome to cassandra CLI.

  Type 'help;' or '?' for help. Type 'quit;' or 'exit;' to quit.
  [default@unknown] 

As the banner says, you can use 'help;' or '?' to see what the CLI has to
offer, and 'quit;' or 'exit;' when you've had enough fun. But lets try
something slightly more interesting...

  [default@unknown] create keyspace Keyspace1;
  ece86bde-dc55-11df-8240-e700f669bcfc
  [default@unknown] use Keyspace1;
  Authenticated to keyspace: Keyspace1
  [default@Keyspace1] create column family Users with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type;
  737c7a71-dc56-11df-8240-e700f669bcfc

  [default@KS1] set Users[jsmith][first] = 'John';
  Value inserted.
  [default@KS1] set Users[jsmith][last] = 'Smith';
  Value inserted.
  [default@KS1] set Users[jsmith][age] = long(42);
  Value inserted.
  [default@KS1] get Users[jsmith];
  => (column=last, value=Smith, timestamp=1287604215498000)
  => (column=first, value=John, timestamp=1287604214111000)
  => (column=age, value=42, timestamp=1287604216661000)
  Returned 3 results.

If your session looks similar to what's above, congrats, your single node
cluster is operational! But what exactly was all of that? Let's break it
down into pieces and see.

  set Users[jsmith][first] = 'John';
        \      \        \          \
         \      \_ key   \          \_ value
          \               \_ column
           \_ column family

Data stored in Cassandra is associated with a column family (Users),
which in turn is associated with a keyspace (Keyspace1). In the example
above, we set the value 'John' in the 'first' column for key 'jsmith'.

For more information on the Cassandra data model be sure to checkout 
http://wiki.apache.org/cassandra/DataModel

Wondering where to go from here? 

  * The wiki (http://wiki.apache.org/cassandra/) is the 
    best source for additional information.
  * Join us in #cassandra on irc.freenode.net and ask questions.
  * Subscribe to the Users mailing list by sending a mail to
    user-subscribe@cassandra.apache.org




