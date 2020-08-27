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

Full Query Logging (FQL)
========================

Apache Cassandra 4.0 adds a new highly performant feature that supports live query logging (`CASSANDRA-13983 <https://issues.apache.org/jira/browse/CASSANDRA-13983>`_). 
FQL is safe for production use, with configurable limits to heap memory and disk space to prevent out-of-memory errors.
This feature is useful for live traffic capture, as well as traffic replay. The tool provided can be used for both debugging query traffic and migration.
New ``nodetool`` options are also added to enable, disable or reset FQL, as well as a new tool to read and replay the binary logs.
The full query logging (FQL) capability uses `Chronicle-Queue <http://github.com/OpenHFT/Chronicle-Queue>`_ to rotate a log of queries. 
Full query logs will be referred to as *logs* for the remainder of the page.

Some of the features of FQL are:

- The impact on query latency is reduced by asynchronous single-thread log entry writes to disk.
- Heap memory usage is bounded by a weighted queue, with configurable maximum weight sitting in front of logging thread.
- If the weighted queue is full, producers can be blocked or samples can be dropped.
- Disk utilization is bounded by a configurable size, deleting old log segments once the limit is reached.
- A flexible schema binary format, `Chronicle-Wire <http://github.com/OpenHFT/Chronicle-Wire>`_, for on-disk serialization that can skip unrecognized fields, add new ones, and omit old ones.
- Can be enabled, disabled, or reset (to delete on-disk data) using the JMX tool, ``nodetool``.
- Can configure the settings in either the `cassandra.yaml` file or by using ``nodetool``.
- Introduces new ``fqltool`` that currently can ``Dump`` the binary logs to a readable format. Other options are ``Replay`` and ``Compare``.

FQL logs all successful Cassandra Query Language (CQL) requests, both events that modify the data and those that query. 
While audit logs also include CQL requests, FQL logs only the CQL request. This difference means that FQL can be used to replay or compare logs, which audit logging cannot. FQL is useful for debugging, performance benchmarking, testing and auditing CQL queries, while audit logs are useful for compliance.

In performance testing, FQL appears to have little or no overhead in ``WRITE`` only workloads, and a minor overhead in ``MIXED`` workload.

Query information logged
------------------------

The query log contains:

- all queries invoked 
- approximate time they were invoked 
- any parameters necessary to bind wildcard values 
- all query options 

The logger writes single or batched CQL queries after they finish, so only successfully completed queries are logged. Failed or timed-out queries are not logged. Different data is logged, depending on the type of query. 

A single CQL query log entry contains:

- query - CQL query text
- queryOptions - Options associated with the query invocation
- queryState - Timestamp state associated with the query invocation
- queryTimeMillis - Approximate time in milliseconds since the epoch since the query was invoked

A batch CQL query log entry contains:

- queries - CQL text of the queries
- queryOptions - Options associated with the query invocation
- queryState - Timestamp state associated with the query invocation
- batchTimeMillis - Approximate time in milliseconds since the epoch since the batch was invoked
- type - The type of the batch
- values - Values to bind to as parameters for the queries

Because FQL is backed by `Binlog`, the performance and footprint are predictable, with minimal impact on log record producers. 
Performance safety prevents the producers from overloading the log, using a weighted queue to drop records if the logging falls behind.
Single-thread asynchronous writing produces the logs. Chronicle-Queue provides an easy method of  rolling the logs.

Logging information logged
--------------------------

FQL also tracks information about the stored log files:

- Stored log files that are added and their storage impact. Deletes them if over storage limit.
- The log files in Chronicle-Queue that have already rolled
- The number of bytes in the log files that have already rolled

Logging sequence
----------------

The logger follows a well-defined sequence of events:

1. The consumer thread that writes log records is started. This action can occur only once.
2. The consumer thread offers a record to the log. If the in-memory queue is full, the record will be dropped and offer returns a `false` value.
3. If accepted, the record is entered into the log. If the in-memory queue is full, the putting thread will be blocked until there is space or it is interrupted.
4. The buffers are cleaned up at thread exit. Finalization will check again, to ensure there are no stragglers in the queue.
5. The consumer thread is stopped. It can be called multiple times.

Using FQL
---------

To use FQL, two actions must be completed. FQL must be configured using either the `cassandra.yaml` file or ``nodetool``, and logging must be enabled using ``nodetool enablefullquerylog``. 
Both actions are completed on a per-node basis.
With either method, at a minimum, the path to the log directory must be specified.
Full query logs are generated on each enabled node, so logs on each node will have that node's queries.

Configuring FQL in cassandra.yaml
---------------------------------

The `cassandra.yaml` file can be used to configure FQL before enabling the feature with ``nodetool``. 

The file includes the following options that can be uncommented for use:

:: 

 # default options for full query logging - these can be overridden from command line
 # when executing nodetool enablefullquerylog
 #full_query_logging_options:
    # log_dir:
    # roll_cycle: HOURLY
    # block: true
    # max_queue_weight: 268435456 # 256 MiB
    # max_log_size: 17179869184 # 16 GiB
    ## archive command is "/path/to/script.sh %path" where %path is replaced with the file being rolled:
    # archive_command:
    # max_archive_retries: 10

log_dir
^^^^^^^

To write logs, an existing directory must be set in ``log_dir``. 

The directory must have appropriate permissions set to allow reading, writing, and executing. 
Logging will recursively delete the directory contents as needed. 
Do not place links in this directory to other sections of the filesystem. 
For example, ``log_dir: /tmp/cassandrafullquerylog``.

roll_cycle
^^^^^^^^^^

The ``roll_cycle`` defines the frequency with which the log segments are rolled. 
Supported values are ``HOURLY`` (default), ``MINUTELY``, and ``DAILY``.
For example: ``roll_cycle: DAILY``

block
^^^^^

The ``block`` option specifies whether FQL should block writing or drop log records if FQL falls behind. Supported boolean values are ``true`` (default) or ``false``.
For example: ``block: false`` to drop records

max_queue_weight
^^^^^^^^^^^^^^^^

The ``max_queue_weight`` option sets the maximum weight of in-memory queue for records waiting to be written to the file before blocking or dropping.  The option must be set to a positive value. The default value is 268435456, or 256 MiB.
For example, to change the default: ``max_queue_weight: 134217728 # 128 MiB``

max_log_size
^^^^^^^^^^^^

The ``max_log_size`` option sets the maximum size of the rolled files to retain on disk before deleting the oldest file.  The option must be set to a positive value. The default is 17179869184, or 16 GiB.
For example, to change the default: ``max_log_size: 34359738368 # 32 GiB``

archive_command
^^^^^^^^^^^^^^^

The ``archive_command`` option sets the user-defined archive script to execute on rolled log files. 
When not defined, files are deleted, with a default of ``""`` which then maps to `org.apache.cassandra.utils.binlog.DeletingArchiver`.
For example: ``archive_command: /usr/local/bin/archiveit.sh %path # %path is the file being rolled``

max_archive_retries
^^^^^^^^^^^^^^^^^^^

The ``max_archive_retries`` option sets the max number of retries of failed archive commands. The default is 10.
For example: ``max_archive_retries: 10``

FQL can also be configured using ``nodetool`` when enabling the feature, and will override any values set in the `cassandra.yaml` file, as discussed in the next section.

Enabling FQL
------------

FQL is enabled on a per-node basis using the ``nodetool enablefullquerylog`` command. At a minimum, the path to the logging directory must be defined, if ``log_dir`` is not set in the `cassandra.yaml` file. 

The syntax of the ``nodetool enablefullquerylog`` command has all the same options that can be set in the ``cassandra.yaml`` file.
In addition, ``nodetool`` has options to set which host and port to run the command on, and username and password if the command requires authentication. 

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

To enable FQL, run the following command on each node in the cluster on which you want to enable logging:

::

 nodetool enablefullquerylog --path /tmp/cassandrafullquerylog

Disabling or resetting FQL
-------------

Use the ``nodetool disablefullquerylog`` to disable logging. 
Use ``nodetool resetfullquerylog`` to stop FQL and clear the log files in the configured directory.
**IMPORTANT:** Using ``nodetool resetfullquerylog`` will delete the log files! Do not use this command unless you need to delete all log files.

fqltool
-------

The ``fqltool`` command is used to view (dump), replay, or compare logs.
``fqltool dump`` converts the binary log files into human-readable format; only the log directory must be supplied as a command-line option.

``fqltool replay`` (`CASSANDRA-14618 <https://issues.apache.org/jira/browse/CASSANDRA-14618>`_) enables replay of logs. 
The command can run from a different machine or cluster for testing, debugging, or performance benchmarking. 
The command can also be used to recreate a dropped database object (keyspace, table), usually in a different cluster.
The ``fqltool replay`` command does not replay DDL statements automatically; explicitly enable it with the ``--replay-ddl-statements`` flag.
Use ``fqltool replay`` to record and compare different runs of production traffic against different versions/configurations of Cassandra or different clusters.
Another use is to gather logs from several machines and replay them in “order” by the timestamps recorded.

The syntax of ``fqltool replay`` is:

::

  fqltool replay [--keyspace <keyspace>] [--replay-ddl-statements]
  [--results <results>] [--store-queries <store_queries>] 
  --target <target>... [--] <path1> [<path2>...<pathN>]

 OPTIONS
   --keyspace <keyspace>
  Only replay queries against this keyspace and queries without
  keyspace set.

   --replay-ddl-statements
   If specified, replays DDL statements as well, they are excluded from
   replaying by default.

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

``fqltool compare`` (`CASSANDRA-14619 <https://issues.apache.org/jira/browse/CASSANDRA-14619>`_) compares result files generated by ``fqltool replay``.
The command uses recorded runs from ``fqltool replay`` and compareslog, outputting any differences (potentially all queries).
It also stores each row as a separate chronicle document to avoid reading the entire result from in-memory when comparing.

The syntax of ``fqltool compare`` is:

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

- Mark the beginning of a query set:

::

  -------------------
  version: int16
  type: column_definitions
  column_count: int32;
  column_definition: text, text
  column_definition: text, text
  ....
  --------------------


- Mark a failed query set:

::

  ---------------------
  version: int16
  type: query_failed
  message: text
  ---------------------

- Mark a row set:

::

  --------------------
  version: int16
  type: row
  row_column_count: int32
  column: bytes
  ---------------------

- Mark the end of a result set:

::

  -------------------
  version: int16
  type: end_resultset
  -------------------

Example
-------

1. To demonstrate FQL, first configure and enable FQL on a node in your cluster:

::
 
 nodetool enablefullquerylog --path /tmp/cassandrafullquerylog


2. Now create a demo keyspace and table and insert some data using ``cqlsh``:

::

 cqlsh> CREATE KEYSPACE querylogkeyspace
   ... WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
 cqlsh> USE querylogkeyspace;
 cqlsh:querylogkeyspace> CREATE TABLE t (
 ...id int,
 ...k int,
 ...v text,
 ...PRIMARY KEY (id)
 ... );
 cqlsh:querylogkeyspace> INSERT INTO t (id, k, v) VALUES (0, 0, 'val0');
 cqlsh:querylogkeyspace> INSERT INTO t (id, k, v) VALUES (0, 1, 'val1');

3. Then check that the data is inserted:

:: 

 cqlsh:querylogkeyspace> SELECT * FROM t;

 id | k | v
 ----+---+------
  0 | 1 | val1

 (1 rows)

4. Use the ``fqltool dump`` command to view the logs.

::

$ fqltool dump /tmp/cassandrafullquerylog

This command will return a readable version of the log. Here is a partial sample of the log for the commands in this demo:

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
      Query: CREATE KEYSPACE querylogkeyspace
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
      Values:

      Type: single-query
      Query start time: 1564708360873
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:-2147483648
      Query: USE querylogkeyspace;
      Values:

      Type: single-query
      Query start time: 1564708360874
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:-2147483648
      Query: USE "querylogkeyspace"
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
      Query: SELECT * FROM system_schema.tables WHERE keyspace_name = 'querylogkeyspace' AND table_name = 't'
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

5. This example will demonstrate ``fqltool replay`` in a single cluster. However, the most common method of using ``replay`` is between clusters. 
To demonstrate in the same cluster, first drop the keyspace.

::

 cqlsh:querylogkeyspace> DROP KEYSPACE querylogkeyspace;

6. Now run ``fqltool replay`` specifying the directories in which to store the results of the queries and 
the list of queries run, respectively, in `--results` and `--store-queries`, and specifiying that the DDL statements to create the keyspace and tables will be executed:

::

 $ fqltool replay \
 --keyspace querylogkeyspace --replay-ddl-statements --results /cassandra/fql/logs/results/replay \
 --store-queries /cassandra/fql/logs/queries/replay \
 --target 3.91.56.164 \
 /tmp/cassandrafullquerylog

The ``--results`` and ``--store-queries`` directories are optional, but if ``--store-queries`` is set, then ``--results`` must also be set.
The ``--target`` specifies the node on which to replay to logs.
If ``--replay-ddl-statements`` is not specified, the keyspace and any tables must be created prior to the ``replay``.

7. Check that the keyspace was replayed and exists again using the ``DESCRIBE KEYSPACES`` command:

::

 cqlsh:querylogkeyspace> DESC KEYSPACES;

 system_schema  system  system_distributed  system_virtual_schema
 system_auth    querylogkeyspace  system_traces  system_views
