cassandra-stress
======

Description
-----------
cassandra-stress is a tool for benchmarking and load testing a Cassandra
cluster. cassandra-stress supports testing arbitrary CQL tables and queries
to allow users to benchmark their data model.

Setup
-----
Run `ant` from the Cassandra source directory, then cassandra-stress can be invoked from tools/bin/cassandra-stress.
cassandra-stress supports benchmarking any Cassandra cluster of version 2.0+.

Usage
-----
There are several operation types:

    * write-only, read-only, and mixed workloads of standard data
    * write-only and read-only workloads for counter columns
    * user configured workloads, running custom queries on custom schemas
    * support for legacy cassandra-stress operations

The syntax is `cassandra-stress <command> [options]`. If you want more information on a given command
or options, just run `cassandra-stress help <command|option>`.

Commands:
    read:
        Multiple concurrent reads - the cluster must first be populated by a write test
    write:
        Multiple concurrent writes against the cluster
    mixed:
        Interleaving of any basic commands, with configurable ratio and distribution - the cluster must first be populated by a write test
    counter_write:
        Multiple concurrent updates of counters.
    counter_read:
        Multiple concurrent reads of counters. The cluster must first be populated by a counterwrite test.
    user:
        Interleaving of user provided queries, with configurable ratio and distribution.
        See http://www.datastax.com/dev/blog/improved-cassandra-2-1-stress-tool-benchmark-any-schema
    help:
        Print help for a command or option
    print:
        Inspect the output of a distribution definition
    legacy:
        Legacy support mode

Primary Options:
    -pop:
        Population distribution and intra-partition visit order
    -insert:
        Insert specific options relating to various methods for batching and splitting partition updates
    -col:
        Column details such as size and count distribution, data generator, names, comparator and if super columns should be used
    -rate:
        Thread count, rate limit or automatic mode (default is auto)
    -mode:
        Thrift or CQL with options
    -errors:
        How to handle errors when encountered during stress
    -sample:
        Specify the number of samples to collect for measuring latency
    -schema:
        Replication settings, compression, compaction, etc.
    -node:
        Nodes to connect to
    -log:
        Where to log progress to, and the interval at which to do it
    -transport:
        Custom transport factories
    -port:
        The port to connect to cassandra nodes on
    -sendto:
        Specify a stress server to send this command to
    -graph:
        Graph recorded metrics
    -tokenrange:
        Token range settings


Suboptions:
    Every command and primary option has its own collection of suboptions. These are too numerous to list here.
    For information on the suboptions for each command or option, please use the help command,
    `cassandra-stress help <command|option>`.

Examples
--------

    * tools/bin/cassandra-stress write n=1000000 -node 192.168.1.101 # 1M inserts to given host
    * tools/bin/cassandra-stress read n=10000000 -node 192.168.1.101 -o read # 1M reads
    * tools/bin/cassandra-stress write -node 192.168.1.101,192.168.1.102 n=10000000 # 10M inserts spread across two nodes
    * tools/bin/cassandra-stress help -pop # Print help for population distribution option
