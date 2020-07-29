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

Full Query Logging
------------------

Apache Cassandra 4.0 adds a new feature to support a means of logging all queries as they are invoked (`CASSANDRA-13983
<https://issues.apache.org/jira/browse/CASSANDRA-13983>`_). To test for processing correctness, it's useful to capture production traffic and replay both the previous and the current versions to compare the results.

Cassandra 4.0 implements a full query logging (FQL) capability that uses a chronicle-queue to rotate a log of queries. Some of the features of FQL are:

- Single thread asynchronously writes log entries to disk, reducing the impact on query latency
- Heap memory usage bounded by a weighted queue, with configurable maximum weight sitting in front of logging thread
- If the weighted queue is full, producers can be blocked or samples can be dropped
- Disk utilization is bounded by deleting old log segments once a configurable size is reached
- On-disk serialization uses a flexible schema binary format (chronicle-wire) that can skip unrecognized fields, add new ones, and omit old ones.
- Can be enabled, disabled, or reset (to delete on-disk data) using JMX 
- Can configure the logging path using either JMX and YAML
- Introduces new ``fqltool`` that currently can ``Dump`` full query logs (FQ logs) to a readable format FQ logs or follow active FQ logs. Other options are ``Replay`` and ``Compare``.

Cassandra 4.0 has a binary full query log based on chronicle-queue controlled with ``nodetool enablefullquerylog``, ``disablefullquerylog``, and ``resetfullquerylog``. The log contains:
- all queries invoked 
- approximate time they were invoked 
- any parameters necessary to bind wildcard values 
- all query options 

A readable version of the log can be dumped or checked with `tail` using ``fqltool`` utility. The FQ log is designed for safe use in production. The capability limits heap memory and disk space use that are specified when enabling the log.

Objective
^^^^^^^^^^
FQ logs all requests to the CQL interface. The FQ logs is useful for debugging, performance benchmarking, testing and auditing CQL queries. While the audit logs also include CQL requests, FQL only logs CQL requests and can replay or compare those logs, which audit logging cannot. 

Full Query Logger
^^^^^^^^^^^^^^^^^^
The Full Query Logger is a logger that includes the entire query contents after the query finishes. FQL only logs the queries that successfully complete. Other queries, such as timed out, failed queries, are not logged. Queries are logged as either a single query or a batch of queries. 

The log for single CQL query includes the following attributes:

::

 query - CQL query text
 queryOptions - Options associated with the query invocation
 queryState - Timestamp state associated with the query invocation
 queryTimeMillis - Approximate time in milliseconds since the epoch since the batch was invoked

A batch of queries is logged with the following attributes:

::

 type - The type of the batch
 queries - CQL text of the queries
 values - Values to bind to as parameters for the queries
 queryOptions - Options associated with the query invocation
 queryState - Timestamp state associated with the query invocation
 batchTimeMillis - Approximate time in milliseconds since the epoch since the batch was invoked

FQL is backed by ``BinLog``. BinLog is a quick and dirty binary log. Its goal is good enough performance, predictable footprint, simplicity in terms of implementation and configuration and most importantly minimal impact on producers of log records. Performance safety is accomplished by feeding items to the binary log using a weighted queue and dropping records if the binary log falls sufficiently far behind. Simplicity and good enough performance is achieved by using a single log writing thread as well as chronicle-queue to handle writing the log, making it available for readers, as well as log rolling.

Weighted queue is a wrapper around any blocking queue that turns it into a blocking weighted queue. The queue will weigh each element being added and removed. Adding to the queue is blocked if adding would violate the weight bound. If an element weighs in at larger than the capacity of the queue then exactly one such element will be allowed into the queue at a time. If the weight of an object changes after it is added it could create issues. Checking weight should be cheap so memorize expensive to compute weights. If weight throws that can also result in leaked permits so it's always a good idea to memorize weight so it doesn't throw. In the interests of not writing unit tests for methods no one uses there is a lot of ``UnsupportedOperationException``. If you need them then add them and add proper unit tests to ``WeightedQueueTest``. "Good" tests. 100% coverage including exception paths and resource leaks.


The FQL tracks information about store files:

- Store files as they are added and their storage impact. Delete them if over storage limit.
- The files in the chronicle queue that have already rolled
- The number of bytes in store files that have already rolled

FQL logger sequence is:

1. Start the consumer thread that writes log records. Can only be done once.
2. Offer a record to the log. If the in memory queue is full the record will be dropped and offer will return false.
3. Put a record into the log. If the in memory queue is full the putting thread will be blocked until there is space or it is interrupted.
4. Clean up the buffers on thread exit, finalization will check again once this is no longer reachable ensuring there are no stragglers in the queue.
5. Stop the consumer thread that writes log records. Can be called multiple times.


Configuring Full Query Logging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Full Query Logger default options are configured on a per-node basis in the ``cassandra.yaml`` file.

::

# default options for full query logging - these can be overridden from command line when executing
# nodetool enablefullquerylog
#full_query_logging_options:
    # log_dir:
    # roll_cycle: HOURLY
    # block: true
    # max_queue_weight: 268435456 # 256 MiB
    # max_log_size: 17179869184 # 16 GiB
    ## archive command is "/path/to/script.sh %path" where %path is replaced with the file being rolled:
    # archive_command:
    # max_archive_retries: 10

An existing directory must be set in ``log_dir`` to write FQ logs if FQL is enabled. The directory must have appropriate permissions set. The full query log will recursively delete the contents of this path as needed. Do not place links in this directory to other sections of the filesystem. 

::

log_dir: /tmp/cassandrafullquerylog

Make the directory if required:

::

 sudo mkdir -p /tmp/cassandrafullquerylog
 sudo chmod -R 777 /tmp/cassandrafullquerylog

::

The ``roll_cycle`` defines the frequency with which the log segments are rolled. Supported values are ``HOURLY`` (default), ``MINUTELY``, and ``DAILY``.

::

roll_cycle: DAILY

The ``block`` option specifies whether the FQL should block if the FQL falls behind or should drop log records. Supported boolean values are ``true`` (default) or ``false``.

The ``max_queue_weight`` option sets the maximum weight of in-memory queue for records waiting to be written to the file before blocking or dropping.  The option must be set to a positive value. The default value is 268435456, or 256 MiB.

:: 

max_queue_weight: 134217728 # 128 MiB

The ``max_log_size`` option sets the maximum size of the rolled files to retain on disk before deleting the oldest file.  The option must be set to a positive value. The default is 17179869184, or 16 GiB.

::

max_log_size: 34359738368 # 32 GiB

The ``archive_command`` option sets the user-defined archive script to execute on rolled log files. 

:: 

archive_command: /usr/local/bin/archiveit.sh %path # %path is the file being rolled

The ``max_archive_retries`` option sets the max number of retries of failed archive commands. The default is 10.

::

max_archive_retries: 10

Enabling Full Query Logging
***************************

Full Query Logging is enabled on a per-node basis. .  The ``nodetool enablefullquerylog`` command can be used to enable full query logging. Defaults for the options are configured in the ``cassandra.yaml`` file and can be overridden from the command-line.

The syntax of the ``nodetool enablefullquerylog`` command has all the same options that can be set in the ``cassandra.yaml`` file, in addition to options for which host and port to run the command on, and username and password if the command requires authentication. At a minimum, the path to the logging directory must be defined. 

::

  nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
 [(-pp | --print-port)] [(-pw <password> | --password <password>)]
 [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
 [(-u <username> | --username <username>)] enablefullquerylog
 [--archive-command <archive_command>] [--blocking]
 [--max-archive-retries <archive_retries>]
 [--max-log-size <max_log_size>] [--max-queue-weight <max_queue_weight>]
 [--path <path>] [--roll-cycle <roll_cycle>]

 OPTIONS
   --archive-command <archive_command>
  Command that will handle archiving rolled full query log files.
  Format is "/path/to/script.sh %path" where %path will be replaced
  with the file to archive

   --blocking
  If the queue is full whether to block producers or drop samples.

   -h <host>, --host <host>
  Node hostname or ip address

   --max-archive-retries <archive_retries>
  Max number of archive retries.

   --max-log-size <max_log_size>
  How many bytes of log data to store before dropping segments. Might
  not be respected if a log file hasn't rolled so it can be deleted.

   --max-queue-weight <max_queue_weight>
  Maximum number of bytes of query data to queue to disk before
  blocking or dropping samples.

   -p <port>, --port <port>
  Remote jmx agent port number

   --path <path>
  Path to store the full query log at. Will have it's contents
  recursively deleted.

   -pp, --print-port
  Operate in 4.0 mode with hosts disambiguated by port number

   -pw <password>, --password <password>
  Remote jmx agent password

   -pwf <passwordFilePath>, --password-file <passwordFilePath>
  Path to the JMX password file

   --roll-cycle <roll_cycle>
  How often to roll the log file (MINUTELY, HOURLY, DAILY).

   -u <username>, --username <username>
  Remote jmx agent username

Run the following command on each node in the cluster that you with to enable logging:

::

 nodetool enablefullquerylog --path /tmp/cassandrafullquerylog

Example
^^^^^^^

To demonstrate FQL, create a keyspace and table and insert some data using ``cqlsh``:

::

 cqlsh> CREATE KEYSPACE auditlogkeyspace
   ... WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
 cqlsh> USE auditlogkeyspace;
 cqlsh:auditlogkeyspace> CREATE TABLE t (
 ...id int,
 ...k int,
 ...v text,
 ...PRIMARY KEY (id)
 ... );
 cqlsh:auditlogkeyspace> INSERT INTO t (id, k, v) VALUES (0, 0, 'val0');
 cqlsh:auditlogkeyspace> INSERT INTO t (id, k, v) VALUES (0, 1, 'val1');
 cqlsh:auditlogkeyspace> SELECT * FROM t;

 id | k | v
 ----+---+------
  0 | 1 | val1

 (1 rows)
 cqlsh:auditlogkeyspace>

Use the ``fqltool`` command to view the FQ logs.  The ``fqltool`` has the following options: compare, dump, replay, help.

The ``fqltool dump`` command is used to dump (list) the contents of a FQ log. 

::

$ fqltool dump /tmp/cassandrafullquerylog

A partial sample of the CQL queries run is:

::

      WARN  [main] 2019-08-02 03:07:53,635 Slf4jExceptionHandler.java:42 - Using Pauser.sleepy() as not enough processors, have 2, needs 8+
      Type: single-query
      Query start time: 1564708322030
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708322
      Query: SELECT * FROM system.peers
      Values:

      Type: single-query
      Query start time: 1564708322054
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708322
      Query: SELECT * FROM system.local WHERE key='local'
      Values:

      Type: single-query
      Query start time: 1564708322109
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708322
      Query: SELECT * FROM system_schema.keyspaces
      Values:

      Type: single-query
      Query start time: 1564708322116
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708322
      Query: SELECT * FROM system_schema.tables
      Values:

      Type: single-query
      Query start time: 1564708322139
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708322
      Query: SELECT * FROM system_schema.columns
      Values:

      Type: single-query
      Query start time: 1564708322142
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708322
      Query: SELECT * FROM system_schema.functions
      Values:

      Type: single-query
      Query start time: 1564708322141
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708322
      Query: SELECT * FROM system_schema.aggregates
      Values:

      Type: single-query
      Query start time: 1564708322143
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708322
      Query: SELECT * FROM system_schema.types
      Values:

      Type: single-query
      Query start time: 1564708322144
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708322
      Query: SELECT * FROM system_schema.indexes
      Values:

      Type: single-query
      Query start time: 1564708322145
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708322
      Query: SELECT * FROM system_schema.views
      Values:

      Type: single-query
      Query start time: 1564708345408
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:-2147483648
      Query: CREATE KEYSPACE AuditLogKeyspace
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
      Values:

      Type: single-query
      Query start time: 1564708360873
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:-2147483648
      Query: USE auditlogkeyspace;
      Values:

      Type: single-query
      Query start time: 1564708360874
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:-2147483648
      Query: USE "auditlogkeyspace"
      Values:

      Type: single-query
      Query start time: 1564708378837
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:-2147483648
      Query: CREATE TABLE t (
          id int,
          k int,
          v text,
          PRIMARY KEY (id)
      );
      Values:

      Type: single-query
      Query start time: 1564708379247
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708379
      Query: SELECT * FROM system_schema.tables WHERE keyspace_name = 'auditlogkeyspace' AND table_name = 't'
      Values:

      Type: single-query
      Query start time: 1564708397144
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708397
      Query: INSERT INTO t (id, k, v) VALUES (0, 0, 'val0');
      Values:

      Type: single-query
      Query start time: 1564708434782
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708434
      Query: SELECT * FROM t;
      Values:

Full query logs are generated on each enabled node, so logs on each node will have that node's queries.  

The ``nodetool resetfullquerylog`` stops the FQL and cleans files in the configured FQ log directory from ``cassandra.yaml`` as well as JMX.

The ``fqltool`` provides the ``replay`` command (`CASSANDRA-14618 <https://issues.apache.org/jira/browse/CASSANDRA-14618>`_) to replay the FQ logs. The command can run from a different machine or cluster for testing, debugging, or performance benchmarking. The command, run on the same node on which the FQ logs are generated can recreate a dropped database object.

The main objectives of ``fqltool replay`` are:

- Record and compare different runs of production traffic against different versions/configurations of Cassandra or different clusters.
- Gather FQL logs from several machines and replay them in "order" by the timestamps recorded.

The syntax of ``fqltool replay`` is:

::

  fqltool replay [--keyspace <keyspace>] [--results <results>]
 [--store-queries <store_queries>] --target <target>... [--] <path1>
 [<path2>...<pathN>]

 OPTIONS
   --keyspace <keyspace>
  Only replay queries against this keyspace and queries without
  keyspace set.

   --results <results>
  Where to store the results of the queries, this should be a
  directory. Leave this option out to avoid storing results.

   --store-queries <store_queries>
  Path to store the queries executed. Stores queries in the same order
  as the result sets are in the result files. Requires --results

   --target <target>
  Hosts to replay the logs to, can be repeated to replay to more
  hosts.

   --
  This option can be used to separate command-line options from the
  list of argument, (useful when arguments might be mistaken for
  command-line options

   <path1> [<path2>...<pathN>]
  Paths containing the FQ logs to replay.

To demonstrate ``fqltool replay``, first drop a keyspace.

::

 cqlsh:auditlogkeyspace> DROP KEYSPACE auditlogkeyspace;

Now run ``fqltool replay``:

::

$ fqltool replay 
--keyspace auditlogkeyspace --results /cassandra/fql/logs/results/replay 
--store-queries /cassandra/fql/logs/queries/replay 
-- target 3.91.56.164 
/tmp/cassandrafullquerylog

Specify existing directories to store both the results of queries and queries run are specified. 
The ``--results`` and ``--store-queries`` directories are optional,
 but if ``--store-queries`` is to be set the ``--results`` must also be set.

Check that the keyspace was replayed and exists again using the ``DESCRIBE KEYSPACES`` command:

::

 cqlsh:auditlogkeyspace> DESC KEYSPACES;

 system_schema  system  system_distributed  system_virtual_schema
 system_auth    auditlogkeyspace  system_traces  system_views

The ``fqltool compare`` command (`CASSANDRA-14619 <https://issues.apache.org/jira/browse/CASSANDRA-14619>`_) compares result files generated by ``fqltool replay``. 
The command uses recorded runs from ``fqltool replay`` and compares, outputting any differences, and potentially all queries, against the mismatched partition up to the mismatch.
The ``fqltool compare`` stores each row as a separate chronicle document to avoid reading the entire result from in-memory when comparing.

The syntax is:

::

$ fqltool help compare
 NAME
   fqltool compare - Compare result files generated by fqltool replay

 SYNOPSIS
   fqltool compare --queries <queries> [--] <path1> [<path2>...<pathN>]

 OPTIONS
   --queries <queries>
  Directory to read the queries from. It is produced by the fqltool
  replay --store-queries option.

   --
  This option can be used to separate command-line options from the
  list of argument, (useful when arguments might be mistaken for
  command-line options

   <path1> [<path2>...<pathN>]
  Directories containing result files to compare.

The comparison sets the following marks:

Mark the beginning of a query set:

::

  -------------------
  version: int16
  type: column_definitions
  column_count: int32;
  column_definition: text, text
  column_definition: text, text
  ....
  --------------------


Mark a failed query set:

::

  ---------------------
  version: int16
  type: query_failed
  message: text
  ---------------------

Mark a row set:

::

  --------------------
  version: int16
  type: row
  row_column_count: int32
  column: bytes
  ---------------------

Mark the end of a result set:

::

  -------------------
  version: int16
  type: end_resultset
  -------------------


Performance Overhead of FQL
^^^^^^^^^^^^^^^^^^^^^^^^^^^
In performance testing, FQL appears to have little or no overhead in ``WRITE`` only workloads, and a minor overhead in ``MIXED`` workload.
