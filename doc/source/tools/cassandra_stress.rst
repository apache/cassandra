.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

.. highlight:: yaml

.. _cassandra_stress:

Cassandra Stress
----------------

cassandra-stress is a tool for benchmarking and load testing a Cassandra
cluster. cassandra-stress supports testing arbitrary CQL tables and queries
to allow users to benchmark their data model.

This documentation focuses on user mode as this allows the testing of your
actual schema. 

Usage
^^^^^
There are several operation types:

    * write-only, read-only, and mixed workloads of standard data
    * write-only and read-only workloads for counter columns
    * user configured workloads, running custom queries on custom schemas

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

User mode
^^^^^^^^^

User mode allows you to use your stress your own schemas. This can save time in
the long run rather than building an application and then realising your schema
doesn't scale.

Profile
+++++++

User mode requires a profile defined in YAML.
Multiple YAML files may be specified in which case operations in the ops argument are referenced as specname.opname.

An identifier for the profile::

  specname: staff_activities

The keyspace for the test::

  keyspace: staff

CQL for the keyspace. Optional if the keyspace already exists::

  keyspace_definition: |
   CREATE KEYSPACE stresscql WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

The table to be stressed::
  
  table: staff_activities

CQL for the table. Optional if the table already exists::

  table_definition: |
    CREATE TABLE staff_activities (
        name text,
        when timeuuid,
        what text,
        PRIMARY KEY(name, when, what)
    ) 


Optional meta information on the generated columns in the above table.
The min and max only apply to text and blob types.
The distribution field represents the total unique population
distribution of that column across rows::

    columnspec:
      - name: name
        size: uniform(5..10) # The names of the staff members are between 5-10 characters
        population: uniform(1..10) # 10 possible staff members to pick from
      - name: when
        cluster: uniform(20..500) # Staff members do between 20 and 500 events
      - name: what
        size: normal(10..100,50)

Supported types are:

An exponential distribution over the range [min..max]::

    EXP(min..max)

An extreme value (Weibull) distribution over the range [min..max]::

    EXTREME(min..max,shape)

A gaussian/normal distribution, where mean=(min+max)/2, and stdev is (mean-min)/stdvrng::

    GAUSSIAN(min..max,stdvrng)

A gaussian/normal distribution, with explicitly defined mean and stdev::

    GAUSSIAN(min..max,mean,stdev)

A uniform distribution over the range [min, max]::

    UNIFORM(min..max)

A fixed distribution, always returning the same value::

    FIXED(val)
      
If preceded by ~, the distribution is inverted

Defaults for all columns are size: uniform(4..8), population: uniform(1..100B), cluster: fixed(1)

Insert distributions::

    insert:
      # How many partition to insert per batch
      partitions: fixed(1)
      # How many rows to update per partition
      select: fixed(1)/500
      # UNLOGGED or LOGGED batch for insert
      batchtype: UNLOGGED


Currently all inserts are done inside batches.

Read statements to use during the test::

    queries:
       events:
          cql: select *  from staff_activities where name = ?
          fields: samerow
       latest_event:
          cql: select * from staff_activities where name = ?  LIMIT 1
          fields: samerow

Running a user mode test::

    cassandra-stress user profile=./example.yaml duration=1m "ops(insert=1,latest_event=1,events=1)" truncate=once

This will create the schema then run tests for 1 minute with an equal number of inserts, latest_event queries and events
queries. Additionally the table will be truncated once before the test.

The full example can be found here :download:`yaml <./stress-example.yaml>`

Running a user mode test with multiple yaml files::
    cassandra-stress user profile=./example.yaml,./example2.yaml duration=1m "ops(ex1.insert=1,ex1.latest_event=1,ex2.insert=2)" truncate=once

This will run operations as specified in both the example.yaml and example2.yaml files. example.yaml and example2.yaml can reference the same table
 although care must be taken that the table definition is identical (data generation specs can be different).

Lightweight transaction support
+++++++++++++++++++++++++++++++

cassandra-stress supports lightweight transactions. In this it will first read current data from Cassandra and then uses read value(s)
to fulfill lightweight transaction condition(s).

Lightweight transaction update query::

    queries:
      regularupdate:
          cql: update blogposts set author = ? where domain = ? and published_date = ?
          fields: samerow
      updatewithlwt:
          cql: update blogposts set author = ? where domain = ? and published_date = ? IF body = ? AND url = ?
          fields: samerow

The full example can be found here :download:`yaml <./stress-lwt-example.yaml>`

Graphing
^^^^^^^^

Graphs can be generated for each run of stress.

.. image:: example-stress-graph.png

To create a new graph::

    cassandra-stress user profile=./stress-example.yaml "ops(insert=1,latest_event=1,events=1)" -graph file=graph.html title="Awesome graph"

To add a new run to an existing graph point to an existing file and add a revision name::

    cassandra-stress user profile=./stress-example.yaml duration=1m "ops(insert=1,latest_event=1,events=1)" -graph file=graph.html title="Awesome graph" revision="Second run"

FAQ
^^^^

**How do you use NetworkTopologyStrategy for the keyspace?**

Use the schema option making sure to either escape the parenthesis or enclose in quotes::

    cassandra-stress write -schema "replication(strategy=NetworkTopologyStrategy,datacenter1=3)"

**How do you use SSL?**

Use the transport option::

    cassandra-stress "write n=100k cl=ONE no-warmup" -transport "truststore=$HOME/jks/truststore.jks truststore-password=cassandra"

**Is Cassandra Stress a secured tool?**

Cassandra stress is not a secured tool. Serialization and other aspects of the tool offer no security guarantees.
