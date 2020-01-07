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

Apache Cassandra 4.0 adds a new feature to support a means of logging all queries as they were invoked (`CASSANDRA-13983
<https://issues.apache.org/jira/browse/CASSANDRA-13983>`_). For correctness testing it's useful to be able to capture production traffic so that it can be replayed against both the old and new versions of Cassandra while comparing the results.

Cassandra 4.0 includes an implementation of a full query logging (FQL) that uses chronicle-queue to implement a rotating log of queries. Some of the features of FQL are:

- Single thread asynchronously writes log entries to disk to reduce impact on query latency
- Heap memory usage bounded by a weighted queue with configurable maximum weight sitting in front of logging thread
- If the weighted queue is full producers can be blocked or samples can be dropped
- Disk utilization is bounded by deleting old log segments once a configurable size is reached
- The on disk serialization uses a flexible schema binary format (chronicle-wire) making it easy to skip unrecognized fields, add new ones, and omit old ones.
- Can be enabled and configured via JMX, disabled, and reset (delete on disk data), logging path is configurable via both JMX and YAML
- Introduce new ``fqltool`` in ``/bin`` that currently implements ``Dump`` which can dump in a readable format full query logs as well as follow active full query logs. FQL ``Replay`` and ``Compare`` are also available.

Cassandra 4.0 has a binary full query log based on Chronicle Queue that can be controlled using ``nodetool enablefullquerylog``, ``disablefullquerylog``, and ``resetfullquerylog``. The log contains all queries invoked, approximate time they were invoked, any parameters necessary to bind wildcard values, and all query options. A readable version of the log can be dumped or tailed using the new ``bin/fqltool`` utility. The full query log is designed to be safe to use in production and limits utilization of heap memory and disk space with limits you can specify when enabling the log.

Objective
^^^^^^^^^^
Full Query Logging logs all requests to the CQL interface. The full query logs could be used for debugging, performance benchmarking, testing and auditing CQL queries. The audit logs also include CQL requests but full query logging is dedicated to CQL requests only with features such as FQL Replay and FQL Compare that are not available in audit logging.

Full Query Logger
^^^^^^^^^^^^^^^^^^
The Full Query Logger is a logger that logs entire query contents after the query finishes. FQL only logs the queries that successfully complete. The other queries (e.g. timed out, failed) are not to be logged. Queries are logged in one of two modes: single query or batch of queries. The log for an invocation of a batch of queries includes the following attributes:

::

 type - The type of the batch
 queries - CQL text of the queries
 values - Values to bind to as parameters for the queries
 queryOptions - Options associated with the query invocation
 queryState - Timestamp state associated with the query invocation
 batchTimeMillis - Approximate time in milliseconds since the epoch since the batch was invoked

The log for single CQL query includes the following attributes:

::

 query - CQL query text
 queryOptions - Options associated with the query invocation
 queryState - Timestamp state associated with the query invocation
 queryTimeMillis - Approximate time in milliseconds since the epoch since the batch was invoked

Full query logging is backed up by ``BinLog``. BinLog is a quick and dirty binary log. Its goal is good enough performance, predictable footprint, simplicity in terms of implementation and configuration and most importantly minimal impact on producers of log records. Performance safety is accomplished by feeding items to the binary log using a weighted queue and dropping records if the binary log falls sufficiently far behind. Simplicity and good enough performance is achieved by using a single log writing thread as well as Chronicle Queue to handle writing the log, making it available for readers, as well as log rolling.

Weighted queue is a wrapper around any blocking queue that turns it into a blocking weighted queue. The queue will weigh each element being added and removed. Adding to the queue is blocked if adding would violate the weight bound. If an element weighs in at larger than the capacity of the queue then exactly one such element will be allowed into the queue at a time. If the weight of an object changes after it is added it could create issues. Checking weight should be cheap so memorize expensive to compute weights. If weight throws that can also result in leaked permits so it's always a good idea to memorize weight so it doesn't throw. In the interests of not writing unit tests for methods no one uses there is a lot of ``UnsupportedOperationException``. If you need them then add them and add proper unit tests to ``WeightedQueueTest``. "Good" tests. 100% coverage including exception paths and resource leaks.


The FQL tracks information about store files:

- Store files as they are added and their storage impact. Delete them if over storage limit.
- The files in the chronicle queue that have already rolled
- The number of bytes in store files that have already rolled

FQL logger sequence is as follows:

1. Start the consumer thread that writes log records. Can only be done once.
2. Offer a record to the log. If the in memory queue is full the record will be dropped and offer will return false.
3. Put a record into the log. If the in memory queue is full the putting thread will be blocked until there is space or it is interrupted.
4. Clean up the buffers on thread exit, finalization will check again once this is no longer reachable ensuring there are no stragglers in the queue.
5. Stop the consumer thread that writes log records. Can be called multiple times.

Next, we shall demonstrate full query logging with an example.


Configuring Full Query Logging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Full Query Logger default options are configured on a per node basis in ``cassandra.yaml`` with following configuration property.

::

 full_query_logging_options:

As an example setup create a three node Cassandra 4.0 cluster.  The ``nodetool status`` command lists the nodes in the cluster.

::

 [ec2-user@ip-10-0-2-238 ~]$ nodetool status
 Datacenter: us-east-1
 =====================
 Status=Up/Down
 |/ State=Normal/Leaving/Joining/Moving
 --  AddressLoad   Tokens  Owns (effective)  Host ID Rack
 UN  10.0.1.115  442.42 KiB  25632.6%   b64cb32a-b32a-46b4-9eeb-e123fa8fc287  us-east-1b
 UN  10.0.3.206  559.52 KiB  25631.9%   74863177-684b-45f4-99f7-d1006625dc9e  us-east-1d
 UN  10.0.2.238  587.87 KiB  25635.5%   4dcdadd2-41f9-4f34-9892-1f20868b27c7  us-east-1c


In subsequent sub-sections we shall discuss enabling and configuring full query logging.

Setting the FQL Directory
*************************

A dedicated directory path must be provided to write full query log data to when the full query log is enabled. The directory for FQL must exist, and have permissions set. The full query log will recursively delete the contents of this path at times. It is recommended not to place links in this directory to other sections of the filesystem. The ``full_query_log_dir`` property in ``cassandra.yaml`` is pre-configured.

::

 full_query_log_dir: /tmp/cassandrafullquerylog

The ``log_dir`` option may be used to configure the FQL directory if the ``full_query_log_dir``  is not set.

::

 full_query_logging_options:
    # log_dir:

Create the FQL directory if  it does not exist and set its permissions.

::

 sudo mkdir -p /tmp/cassandrafullquerylog
 sudo chmod -R 777 /tmp/cassandrafullquerylog

Setting the Roll Cycle
**********************

The ``roll_cycle`` option sets how often to roll FQL log segments so they can potentially be reclaimed. Supported values are ``MINUTELY``, ``HOURLY`` and ``DAILY``. Default setting is ``HOURLY``.

::

 roll_cycle: HOURLY

Setting Other Options
*********************

The ``block`` option specifies whether the FQL should block if the FQL falls behind or should drop log records. Default value of ``block`` is ``true``. The ``max_queue_weight`` option sets the maximum weight of in memory queue for records waiting to be written to the file before blocking or dropping. The ``max_log_size`` option sets the maximum size of the rolled files to retain on disk before deleting the oldest file. The ``archive_command`` option sets the archive command to execute on rolled log files. The ``max_archive_retries`` option sets the max number of retries of failed archive commands.

::

 # block: true
    # max_queue_weight: 268435456 # 256 MiB
    # max_log_size: 17179869184 # 16 GiB
    ## archive command is "/path/to/script.sh %path" where %path is replaced with the file
 being rolled:
    # archive_command:
    # max_archive_retries: 10

The ``max_queue_weight`` must be > 0. Similarly ``max_log_size`` must be > 0. An example full query logging options is as follows.

::

 full_query_log_dir: /tmp/cassandrafullquerylog

 # default options for full query logging - these can be overridden from command line when
 executing
 # nodetool enablefullquerylog
 # nodetool enablefullquerylog
 #full_query_logging_options:
    # log_dir:
    roll_cycle: HOURLY
    # block: true
    # max_queue_weight: 268435456 # 256 MiB
    # max_log_size: 17179869184 # 16 GiB
    ## archive command is "/path/to/script.sh %path" where %path is replaced with the file
 being rolled:
    # archive_command:
    # max_archive_retries: 10

The ``full_query_log_dir`` setting is not within the ``full_query_logging_options`` but still is for full query logging.

Enabling Full Query Logging
***************************

Full Query Logging is enabled on a per-node basis. .  The ``nodetool enablefullquerylog`` command is used to enable full query logging. Defaults for the options are configured in ``cassandra.yaml`` and these can be overridden from command line.

The syntax of the nodetool enablefullquerylog command is as follows:

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

Run the following command on each node in the cluster.

::

 nodetool enablefullquerylog --path /tmp/cassandrafullquerylog

After the full query logging has been  enabled run some CQL statements to generate full query logs.

Running CQL Statements
^^^^^^^^^^^^^^^^^^^^^^^

Start CQL interface  with ``cqlsh`` command.

::

 [ec2-user@ip-10-0-2-238 ~]$ cqlsh
 Connected to Cassandra Cluster at 127.0.0.1:9042.
 [cqlsh 5.0.1 | Cassandra 4.0-SNAPSHOT | CQL spec 3.4.5 | Native protocol v4]
 Use HELP for help.
 cqlsh>

Run some CQL statements. Create a keyspace.  Create a table and add some data. Query the table.

::

 cqlsh> CREATE KEYSPACE AuditLogKeyspace
   ... WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
 cqlsh> USE AuditLogKeyspace;
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

Viewing the Full Query Logs
^^^^^^^^^^^^^^^^^^^^^^^^^^^
The ``fqltool`` is used to view the full query logs.  The ``fqltool`` has the following usage syntax.

::

 fqltool <command> [<args>]

 The most commonly used fqltool commands are:
    compare   Compare result files generated by fqltool replay
    dump Dump the contents of a full query log
    help Display help information
    replay    Replay full query logs

 See 'fqltool help <command>' for more information on a specific command.

The ``fqltool dump`` command is used to dump (list) the contents of a full query log. Run the ``fqltool dump`` command after some CQL statements have been run.

The full query logs get listed. Truncated output is as follows:

::

      [ec2-user@ip-10-0-2-238 cassandrafullquerylog]$ fqltool dump ./
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
      Query start time: 1564708322142
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708322
      Query: SELECT * FROM system_schema.triggers
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
      Query start time: 1564708345675
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708345
      Query: SELECT peer, rpc_address, schema_version FROM system.peers
      Values:

      Type: single-query
      Query start time: 1564708345676
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708345
      Query: SELECT schema_version FROM system.local WHERE key='local'
      Values:

      Type: single-query
      Query start time: 1564708346323
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708346
      Query: SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'auditlogkeyspace'
      Values:

      Type: single-query
      Query start time: 1564708360873
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:-2147483648
      Query: USE AuditLogKeyspace;
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
      Query start time: 1564708379255
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708379
      Query: SELECT * FROM system_schema.views WHERE keyspace_name = 'auditlogkeyspace' AND view_name = 't'
      Values:

      Type: single-query
      Query start time: 1564708397144
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708397
      Query: INSERT INTO t (id, k, v) VALUES (0, 0, 'val0');
      Values:

      Type: single-query
      Query start time: 1564708397167
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708397
      Query: INSERT INTO t (id, k, v) VALUES (0, 1, 'val1');
      Values:

      Type: single-query
      Query start time: 1564708434782
      Protocol version: 4
      Generated timestamp:-9223372036854775808
      Generated nowInSeconds:1564708434
      Query: SELECT * FROM t;
      Values:

      [ec2-user@ip-10-0-2-238 cassandrafullquerylog]$



Full query logs are generated on each node.  Enabling of full query logging on one node and the log files generated on the node are as follows:

::

 [root@localhost ~]# ssh -i cassandra.pem ec2-user@52.1.243.83
 Last login: Fri Aug  2 00:14:53 2019 from 75.155.255.51
 [ec2-user@ip-10-0-3-206 ~]$ sudo mkdir /tmp/cassandrafullquerylog
 [ec2-user@ip-10-0-3-206 ~]$ sudo chmod -R 777 /tmp/cassandrafullquerylog
 [ec2-user@ip-10-0-3-206 ~]$ nodetool enablefullquerylog --path /tmp/cassandrafullquerylog
 [ec2-user@ip-10-0-3-206 ~]$ cd /tmp/cassandrafullquerylog
 [ec2-user@ip-10-0-3-206 cassandrafullquerylog]$ ls -l
 total 44
 -rw-rw-r--. 1 ec2-user ec2-user 83886080 Aug  2 01:24 20190802-01.cq4
 -rw-rw-r--. 1 ec2-user ec2-user    65536 Aug  2 01:23 directory-listing.cq4t
 [ec2-user@ip-10-0-3-206 cassandrafullquerylog]$

Enabling of full query logging on another node and the log files generated on the node are as follows:

::

 [root@localhost ~]# ssh -i cassandra.pem ec2-user@3.86.103.229
 Last login: Fri Aug  2 00:13:04 2019 from 75.155.255.51
 [ec2-user@ip-10-0-1-115 ~]$ sudo mkdir /tmp/cassandrafullquerylog
 [ec2-user@ip-10-0-1-115 ~]$ sudo chmod -R 777 /tmp/cassandrafullquerylog
 [ec2-user@ip-10-0-1-115 ~]$ nodetool enablefullquerylog --path /tmp/cassandrafullquerylog
 [ec2-user@ip-10-0-1-115 ~]$ cd /tmp/cassandrafullquerylog
 [ec2-user@ip-10-0-1-115 cassandrafullquerylog]$ ls -l
 total 44
 -rw-rw-r--. 1 ec2-user ec2-user 83886080 Aug  2 01:24 20190802-01.cq4
 -rw-rw-r--. 1 ec2-user ec2-user    65536 Aug  2 01:23 directory-listing.cq4t
 [ec2-user@ip-10-0-1-115 cassandrafullquerylog]$

The ``nodetool resetfullquerylog`` resets the full query logger if it is enabled. Also deletes any generated files in the last used full query log path as well as the one configured in ``cassandra.yaml``. It stops the full query log and cleans files in the configured full query log directory from ``cassandra.yaml`` as well as JMX.

Full Query Replay
^^^^^^^^^^^^^^^^^
The ``fqltool`` provides the ``replay`` command (`CASSANDRA-14618
<https://issues.apache.org/jira/browse/CASSANDRA-14618>`_) to replay the full query logs. The FQL replay could be run on a different machine or even a different cluster  for testing, debugging and performance benchmarking.

The main objectives of ``fqltool replay`` are:

- To be able to compare different runs of production traffic against different versions/configurations of Cassandra.
- Take FQL logs from several machines and replay them in "order" by the timestamps recorded.
- Record the results from each run to be able to compare different runs (against different clusters/versions/etc).
- If fqltool replay is run against 2 or more clusters, the results could be compared.

The FQL replay could also be used on the same node on which the full query log are generated to recreate a dropped database object.

 The syntax of ``fqltool replay`` is as follows:

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
  Paths containing the full query logs to replay.

As an example of using ``fqltool replay``, drop a keyspace.

::

 cqlsh:auditlogkeyspace> DROP KEYSPACE AuditLogKeyspace;

Subsequently run ``fqltool replay``.   The directory to store results of queries and the directory to store the queries run are specified and these directories must be created and permissions set before running ``fqltool replay``. The ``--results`` and ``--store-queries`` directories are optional but if ``--store-queries`` is to be set the ``--results`` must also be set.

::

 [ec2-user@ip-10-0-2-238 cassandra]$ fqltool replay --keyspace AuditLogKeyspace --results
 /cassandra/fql/logs/results/replay --store-queries /cassandra/fql/logs/queries/replay --
 target 3.91.56.164 -- /tmp/cassandrafullquerylog

Describe the keyspaces after running ``fqltool replay`` and the keyspace that was dropped gets listed again.

::

 cqlsh:auditlogkeyspace> DESC KEYSPACES;

 system_schema  system  system_distributed  system_virtual_schema
 system_auth    auditlogkeyspace  system_traces  system_views

 cqlsh:auditlogkeyspace>

Full Query Compare
^^^^^^^^^^^^^^^^^^
The ``fqltool compare`` command (`CASSANDRA-14619
<https://issues.apache.org/jira/browse/CASSANDRA-14619>`_) is used to compare result files generated by ``fqltool replay``. The ``fqltool compare`` command that can take the recorded runs from ``fqltool replay`` and compares them, it should output any differences and potentially all queries against the mismatching partition up until the mismatch.

The ``fqltool compare``  could be used for comparing result files generated by different versions of Cassandra or different Cassandra configurations as an example. The command usage is as follows:

::

 [ec2-user@ip-10-0-2-238 ~]$ fqltool help compare
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

The ``fqltool compare`` stores each row as a separate chronicle document to be able to avoid reading up the entire result set in memory when comparing document formats:

To mark the start of a new result set:

::

  -------------------
  version: int16
  type: column_definitions
  column_count: int32;
  column_definition: text, text
  column_definition: text, text
  ....
  --------------------


To mark a failed query set:

::

  ---------------------
  version: int16
  type: query_failed
  message: text
  ---------------------

To mark a row set:

::

  --------------------
  version: int16
  type: row
  row_column_count: int32
  column: bytes
  ---------------------

To mark the end of a result set:

::

  -------------------
  version: int16
  type: end_resultset
  -------------------


Performance Overhead of FQL
^^^^^^^^^^^^^^^^^^^^^^^^^^^
In performance testing FQL appears to have little or no overhead in ``WRITE`` only workloads, and a minor overhead in ``MIXED`` workload.
