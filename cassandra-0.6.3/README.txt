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

Running the startup script with the -f argument will cause Cassandra to
remain in the foreground and log to standard out.

Now let's try to read and write some data using the command line client.

  * bin/cassandra-cli --host localhost --port 9160

The command line client is interactive so if everything worked you should
be sitting in front of a prompt...

  Connected to localhost/9160
  Welcome to cassandra CLI.
  
  Type 'help' or '?' for help. Type 'quit' or 'exit' to quit.
  cassandra>

As the banner says, you can use 'help' or '?' to see what the CLI has to
offer, and 'quit' or 'exit' when you've had enough fun. But lets try
something slightly more interesting...

  cassandra> set Keyspace1.Standard2['jsmith']['first'] = 'John'
  Value inserted.
  cassandra> set Keyspace1.Standard2['jsmith']['last'] = 'Smith'
  Value inserted.
  cassandra> set Keyspace1.Standard2['jsmith']['age'] = '42'
  Value inserted.
  cassandra> get Keyspace1.Standard2['jsmith']
    (column=age, value=42; timestamp=1249930062801)
    (column=first, value=John; timestamp=1249930053103)
    (column=last, value=Smith; timestamp=1249930058345)
  Returned 3 rows.
  cassandra>

If your session looks similar to what's above, congrats, your single node
cluster is operational! But what exactly was all of that? Let's break it
down into pieces and see.

  set Keyspace1.Standard2['jsmith']['first'] = 'John'
       \            \         \        \          \
        \            \         \_ key   \          \_ value
         \            \                  \_ column
          \_ keyspace  \_ column family

Data stored in Cassandra is associated with a column family (Standard2),
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



