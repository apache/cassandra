cassandra-stress
======

Setup
-----
Run `ant` from the Cassandra source directory, then cassandra-stress can be invoked from tools/bin/cassandra-stress.

Usage & Examples
----------------

cassandra-stress write n=2 -mode user=cassandra password=cassandra

cassandra-stress read n=2 -mode user=cassandra password=cassandra


See: https://cassandra.apache.org/doc/latest/tools/cassandra_stress.html
