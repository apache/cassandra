

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
  * Java >= 1.7 (OpenJDK and Sun have been tested)

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

Now let's try to read and write some data using the Cassandra Query Language:

  * bin/cqlsh

The command line client is interactive so if everything worked you should
be sitting in front of a prompt...

  Connected to Test Cluster at localhost:9160.
  [cqlsh 2.2.0 | Cassandra 1.2.0 | CQL spec 3.0.0 | Thrift protocol 19.35.0]
  Use HELP for help.
  cqlsh> 


As the banner says, you can use 'help;' or '?' to see what CQL has to
offer, and 'quit;' or 'exit;' when you've had enough fun. But lets try
something slightly more interesting:

  cqlsh> CREATE SCHEMA schema1 
         WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
  cqlsh> USE schema1;
  cqlsh:Schema1> CREATE TABLE users (
                   user_id varchar PRIMARY KEY,
                   first varchar,
                   last varchar,
                   age int
                 );
  cqlsh:Schema1> INSERT INTO users (user_id, first, last, age) 
                 VALUES ('jsmith', 'John', 'Smith', 42);
  cqlsh:Schema1> SELECT * FROM users;
   user_id | age | first | last
  ---------+-----+-------+-------
    jsmith |  42 |  john | smith

  cqlsh:Schema1> 

If your session looks similar to what's above, congrats, your single node
cluster is operational! 

For more on what commands are supported by CQL, see
https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile.  A
reasonable way to think of it is as, "SQL minus joins and subqueries."

Wondering where to go from here? 

  * Getting started: http://wiki.apache.org/cassandra/GettingStarted
  * Join us in #cassandra on irc.freenode.net and ask questions
  * Subscribe to the Users mailing list by sending a mail to
    user-subscribe@cassandra.apache.org
  * Planet Cassandra aggregates Cassandra articles and news:
    http://planetcassandra.org/
