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
.. highlight:: none

.. _bulk-loading:

Bulk Loading
==============

Bulk loading of data in Apache Cassandra is supported by different tools. The data to be bulk loaded must be in the form of SSTables. Cassandra does not support loading data in any other format such as CSV, JSON, and XML directly. Bulk loading could be used to:

- Restore incremental backups and snapshots. Backups and snapshots are already in the form of SSTables.
- Load existing SSTables into another cluster, which could have a different number of nodes or replication strategy.
- Load external data into a cluster

**Note*: CSV Data can be loaded via the cqlsh COPY command but we do not recommend this for bulk loading, which typically requires many GB or TB of data.

Tools for Bulk Loading
^^^^^^^^^^^^^^^^^^^^^^

Cassandra provides two commands or tools for bulk loading data. These are:

- Cassandra Bulk loader, also called ``sstableloader``
- The ``nodetool import`` command

The ``sstableloader`` and ``nodetool import`` are accessible if the Cassandra installation ``bin`` directory is in the ``PATH`` environment variable.  Or these may be accessed directly from the ``bin`` directory. We shall discuss each of these next. We shall use the example or sample keyspaces and tables created in the Backups section.

Using sstableloader
^^^^^^^^^^^^^^^^^^^

The ``sstableloader`` is the main tool for bulk uploading data. The ``sstableloader`` streams SSTable data files to a running cluster. The ``sstableloader`` loads data conforming to the replication strategy and replication factor. The table to upload data to does need not to be empty.

The only requirements to run ``sstableloader`` are:

1. One or more comma separated initial hosts to connect to and get ring information.
2. A directory path for the SSTables to load.

Its usage is as follows.

::

 sstableloader [options] <dir_path>

Sstableloader bulk loads the SSTables found in the directory ``<dir_path>`` to the configured cluster. The   ``<dir_path>`` is used as the target *keyspace/table* name. As an example, to load an SSTable named
``Standard1-g-1-Data.db`` into ``Keyspace1/Standard1``, you will need to have the
files ``Standard1-g-1-Data.db`` and ``Standard1-g-1-Index.db`` in a directory ``/path/to/Keyspace1/Standard1/``.

Sstableloader Option to accept Target keyspace name
****************************************************
Often as part of a backup strategy some Cassandra DBAs store an entire data directory. When corruption in data is found then they would like to restore data in the same cluster (for large clusters 200 nodes) but with different keyspace name.

Currently ``sstableloader`` derives keyspace name from the folder structure. As  an option to specify target keyspace name as part of ``sstableloader``, version 4.0 adds support for the ``--target-keyspace``  option (`CASSANDRA-13884
<https://issues.apache.org/jira/browse/CASSANDRA-13884>`_).

The supported options are as follows from which only ``-d,--nodes <initial hosts>``  is required.

::

 -alg,--ssl-alg <ALGORITHM>                                   Client SSL: algorithm

 -ap,--auth-provider <auth provider>                          Custom
                                                              AuthProvider class name for
                                                              cassandra authentication
 -ciphers,--ssl-ciphers <CIPHER-SUITES>                       Client SSL:
                                                              comma-separated list of
                                                              encryption suites to use
 -cph,--connections-per-host <connectionsPerHost>             Number of
                                                              concurrent connections-per-host.
 -d,--nodes <initial hosts>                                   Required.
                                                              Try to connect to these hosts (comma separated) initially for ring information

 -f,--conf-path <path to config file>                         cassandra.yaml file path for streaming throughput and client/server SSL.

 -h,--help                                                    Display this help message

 -i,--ignore <NODES>                                          Don't stream to this (comma separated) list of nodes

 -idct,--inter-dc-throttle <inter-dc-throttle>                Inter-datacenter throttle speed in Mbits (default unlimited)

 -k,--target-keyspace <target keyspace name>                  Target
                                                              keyspace name
 -ks,--keystore <KEYSTORE>                                    Client SSL:
                                                              full path to keystore
 -kspw,--keystore-password <KEYSTORE-PASSWORD>                Client SSL:
                                                              password of the keystore
 --no-progress                                                Don't
                                                              display progress
 -p,--port <native transport port>                            Port used
                                                              for native connection (default 9042)
 -prtcl,--ssl-protocol <PROTOCOL>                             Client SSL:
                                                              connections protocol to use (default: TLS)
 -pw,--password <password>                                    Password for
                                                              cassandra authentication
 -sp,--storage-port <storage port>                            Port used
                                                              for internode communication (default 7000)
 -spd,--server-port-discovery <allow server port discovery>   Use ports
                                                              published by server to decide how to connect. With SSL requires StartTLS
                                                              to be used.
 -ssp,--ssl-storage-port <ssl storage port>                   Port used
                                                              for TLS internode communication (default 7001)
 -st,--store-type <STORE-TYPE>                                Client SSL:
                                                              type of store
 -t,--throttle <throttle>                                     Throttle
                                                              speed in Mbits (default unlimited)
 -ts,--truststore <TRUSTSTORE>                                Client SSL:
                                                              full path to truststore
 -tspw,--truststore-password <TRUSTSTORE-PASSWORD>            Client SSL:
                                                              Password of the truststore
 -u,--username <username>                                     Username for
                                                              cassandra authentication
 -v,--verbose                                                 verbose
                                                              output

The ``cassandra.yaml`` file could be provided  on the command-line with ``-f`` option to set up streaming throughput, client and server encryption options. Only ``stream_throughput_outbound_megabits_per_sec``, ``server_encryption_options`` and ``client_encryption_options`` are read from yaml. You can override options read from ``cassandra.yaml`` with corresponding command line options.

A sstableloader Demo
********************
We shall demonstrate using ``sstableloader`` by uploading incremental backup data for table ``catalogkeyspace.magazine``.  We shall also use a snapshot of the same table to bulk upload in a different run of  ``sstableloader``.  The backups and snapshots for the ``catalogkeyspace.magazine`` table are listed as follows.

::

 [ec2-user@ip-10-0-2-238 ~]$ cd ./cassandra/data/data/catalogkeyspace/magazine-
 446eae30c22a11e9b1350d927649052c
 [ec2-user@ip-10-0-2-238 magazine-446eae30c22a11e9b1350d927649052c]$ ls -l
 total 0
 drwxrwxr-x. 2 ec2-user ec2-user 226 Aug 19 02:38 backups
 drwxrwxr-x. 4 ec2-user ec2-user  40 Aug 19 02:45 snapshots

The directory path structure of SSTables to be uploaded using ``sstableloader`` is used as the  target keyspace/table.

We could have directly uploaded from the ``backups`` and ``snapshots`` directories respectively if the directory structure were in the format used by ``sstableloader``. But the directory path of backups and snapshots for SSTables  is ``/catalogkeyspace/magazine-446eae30c22a11e9b1350d927649052c/backups`` and ``/catalogkeyspace/magazine-446eae30c22a11e9b1350d927649052c/snapshots`` respectively, which cannot be used to upload SSTables to ``catalogkeyspace.magazine`` table. The directory path structure must be ``/catalogkeyspace/magazine/`` to use ``sstableloader``. We need to create a new directory structure to upload SSTables with ``sstableloader`` which is typical when using ``sstableloader``. Create a directory structure ``/catalogkeyspace/magazine`` and set its permissions.

::

 [ec2-user@ip-10-0-2-238 ~]$ sudo mkdir -p /catalogkeyspace/magazine
 [ec2-user@ip-10-0-2-238 ~]$ sudo chmod -R 777 /catalogkeyspace/magazine

Bulk Loading from an Incremental Backup
+++++++++++++++++++++++++++++++++++++++
An incremental backup does not include the DDL for a table. The table must already exist. If the table was dropped it may be created using the ``schema.cql`` generated with every snapshot of a table. As we shall be using ``sstableloader`` to load SSTables to the ``magazine`` table, the table must exist prior to running ``sstableloader``. The table does not need to be empty but we have used an empty table as indicated by a CQL query:

::

 cqlsh:catalogkeyspace> SELECT * FROM magazine;

 id | name | publisher
 ----+------+-----------

 (0 rows)

After the table to upload has been created copy the SSTable files from the ``backups`` directory to the ``/catalogkeyspace/magazine/`` directory that we created.

::

 [ec2-user@ip-10-0-2-238 ~]$ sudo cp ./cassandra/data/data/catalogkeyspace/magazine-
 446eae30c22a11e9b1350d927649052c/backups/* /catalogkeyspace/magazine/

Run the ``sstableloader`` to upload SSTables from the ``/catalogkeyspace/magazine/`` directory.

::

 sstableloader --nodes 10.0.2.238  /catalogkeyspace/magazine/

The output from the ``sstableloader`` command should be similar to the listed:

::

 [ec2-user@ip-10-0-2-238 ~]$ sstableloader --nodes 10.0.2.238  /catalogkeyspace/magazine/
 Opening SSTables and calculating sections to stream
 Streaming relevant part of /catalogkeyspace/magazine/na-1-big-Data.db
 /catalogkeyspace/magazine/na-2-big-Data.db  to [35.173.233.153:7000, 10.0.2.238:7000,
 54.158.45.75:7000]
 progress: [35.173.233.153:7000]0:1/2 88 % total: 88% 0.018KiB/s (avg: 0.018KiB/s)
 progress: [35.173.233.153:7000]0:2/2 176% total: 176% 33.807KiB/s (avg: 0.036KiB/s)
 progress: [35.173.233.153:7000]0:2/2 176% total: 176% 0.000KiB/s (avg: 0.029KiB/s)
 progress: [35.173.233.153:7000]0:2/2 176% [10.0.2.238:7000]0:1/2 39 % total: 81% 0.115KiB/s
 (avg: 0.024KiB/s)
 progress: [35.173.233.153:7000]0:2/2 176% [10.0.2.238:7000]0:2/2 78 % total: 108%
 97.683KiB/s (avg: 0.033KiB/s)
 progress: [35.173.233.153:7000]0:2/2 176% [10.0.2.238:7000]0:2/2 78 %
 [54.158.45.75:7000]0:1/2 39 % total: 80% 0.233KiB/s (avg: 0.040KiB/s)
 progress: [35.173.233.153:7000]0:2/2 176% [10.0.2.238:7000]0:2/2 78 %
 [54.158.45.75:7000]0:2/2 78 % total: 96% 88.522KiB/s (avg: 0.049KiB/s)
 progress: [35.173.233.153:7000]0:2/2 176% [10.0.2.238:7000]0:2/2 78 %
 [54.158.45.75:7000]0:2/2 78 % total: 96% 0.000KiB/s (avg: 0.045KiB/s)
 progress: [35.173.233.153:7000]0:2/2 176% [10.0.2.238:7000]0:2/2 78 %
 [54.158.45.75:7000]0:2/2 78 % total: 96% 0.000KiB/s (avg: 0.044KiB/s)

After the ``sstableloader`` has run query the ``magazine`` table and the loaded table should get listed when a query is run.

::

 cqlsh:catalogkeyspace> SELECT * FROM magazine;

 id | name                      | publisher
 ----+---------------------------+------------------
  1 |        Couchbase Magazine |        Couchbase
  0 | Apache Cassandra Magazine | Apache Cassandra

 (2 rows)
 cqlsh:catalogkeyspace>

Bulk Loading from a Snapshot
+++++++++++++++++++++++++++++
In this section we shall demonstrate restoring a snapshot of the ``magazine`` table to the ``magazine`` table.  As we used the same table to restore data from a backup the directory structure required by ``sstableloader`` should already exist.  If the directory structure needed to load SSTables to ``catalogkeyspace.magazine`` does not exist create the directories and set their permissions.

::

 [ec2-user@ip-10-0-2-238 ~]$ sudo mkdir -p /catalogkeyspace/magazine
 [ec2-user@ip-10-0-2-238 ~]$ sudo chmod -R 777 /catalogkeyspace/magazine

As we shall be copying the snapshot  files to the directory remove any files that may be in the directory.

::

 [ec2-user@ip-10-0-2-238 ~]$ sudo rm /catalogkeyspace/magazine/*
 [ec2-user@ip-10-0-2-238 ~]$ cd /catalogkeyspace/magazine/
 [ec2-user@ip-10-0-2-238 magazine]$ ls -l
 total 0


Copy the snapshot files to the ``/catalogkeyspace/magazine`` directory.

::

 [ec2-user@ip-10-0-2-238 ~]$ sudo cp ./cassandra/data/data/catalogkeyspace/magazine-
 446eae30c22a11e9b1350d927649052c/snapshots/magazine/* /catalogkeyspace/magazine

List the files in the ``/catalogkeyspace/magazine`` directory and a ``schema.cql`` should also get listed.

::

 [ec2-user@ip-10-0-2-238 ~]$ cd /catalogkeyspace/magazine
 [ec2-user@ip-10-0-2-238 magazine]$ ls -l
 total 44
 -rw-r--r--. 1 root root   31 Aug 19 04:13 manifest.json
 -rw-r--r--. 1 root root   47 Aug 19 04:13 na-1-big-CompressionInfo.db
 -rw-r--r--. 1 root root   97 Aug 19 04:13 na-1-big-Data.db
 -rw-r--r--. 1 root root   10 Aug 19 04:13 na-1-big-Digest.crc32
 -rw-r--r--. 1 root root   16 Aug 19 04:13 na-1-big-Filter.db
 -rw-r--r--. 1 root root   16 Aug 19 04:13 na-1-big-Index.db
 -rw-r--r--. 1 root root 4687 Aug 19 04:13 na-1-big-Statistics.db
 -rw-r--r--. 1 root root   56 Aug 19 04:13 na-1-big-Summary.db
 -rw-r--r--. 1 root root   92 Aug 19 04:13 na-1-big-TOC.txt
 -rw-r--r--. 1 root root  815 Aug 19 04:13 schema.cql

Alternatively create symlinks to the snapshot folder instead of copying the data, something like:

::

  mkdir keyspace_name
  ln -s _path_to_snapshot_folder keyspace_name/table_name

If the ``magazine`` table was dropped run the DDL in the ``schema.cql`` to create the table.  Run the ``sstableloader`` with the following command.

::

 sstableloader --nodes 10.0.2.238  /catalogkeyspace/magazine/

As the output from the command indicates SSTables get streamed to the cluster.

::

 [ec2-user@ip-10-0-2-238 ~]$ sstableloader --nodes 10.0.2.238  /catalogkeyspace/magazine/

 Established connection to initial hosts
 Opening SSTables and calculating sections to stream
 Streaming relevant part of /catalogkeyspace/magazine/na-1-big-Data.db  to
 [35.173.233.153:7000, 10.0.2.238:7000, 54.158.45.75:7000]
 progress: [35.173.233.153:7000]0:1/1 176% total: 176% 0.017KiB/s (avg: 0.017KiB/s)
 progress: [35.173.233.153:7000]0:1/1 176% total: 176% 0.000KiB/s (avg: 0.014KiB/s)
 progress: [35.173.233.153:7000]0:1/1 176% [10.0.2.238:7000]0:1/1 78 % total: 108% 0.115KiB/s
 (avg: 0.017KiB/s)
 progress: [35.173.233.153:7000]0:1/1 176% [10.0.2.238:7000]0:1/1 78 %
 [54.158.45.75:7000]0:1/1 78 % total: 96% 0.232KiB/s (avg: 0.024KiB/s)
 progress: [35.173.233.153:7000]0:1/1 176% [10.0.2.238:7000]0:1/1 78 %
 [54.158.45.75:7000]0:1/1 78 % total: 96% 0.000KiB/s (avg: 0.022KiB/s)
 progress: [35.173.233.153:7000]0:1/1 176% [10.0.2.238:7000]0:1/1 78 %
 [54.158.45.75:7000]0:1/1 78 % total: 96% 0.000KiB/s (avg: 0.021KiB/s)

Some other requirements of ``sstableloader`` that should be kept into consideration are:

- The SSTables to be loaded must be compatible with  the Cassandra version being loaded into.
- Repairing tables that have been loaded into a different cluster does not repair the source tables.
- Sstableloader makes use of port 7000 for internode communication.
- Before restoring incremental backups run ``nodetool flush`` to backup any data in memtables

Using nodetool import
^^^^^^^^^^^^^^^^^^^^^
In this section we shall import SSTables into a table using the ``nodetool import`` command. The ``nodetool refresh`` command is deprecated, and it is recommended to use ``nodetool import`` instead. The ``nodetool refresh`` does not have an option to load new SSTables from a separate directory which the ``nodetool import`` does.

The command usage is as follows.

::

         nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
                [(-pp | --print-port)] [(-pw <password> | --password <password>)]
                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
                [(-u <username> | --username <username>)] import
                [(-c | --no-invalidate-caches)] [(-e | --extended-verify)]
                [(-l | --keep-level)] [(-q | --quick)] [(-r | --keep-repaired)]
                [(-t | --no-tokens)] [(-v | --no-verify)] [--] <keyspace> <table>
                <directory> ...

The arguments ``keyspace``, ``table`` name and ``directory`` to import SSTables from are required.

The supported options are as follows.

::

        -c, --no-invalidate-caches
            Don't invalidate the row cache when importing

        -e, --extended-verify
            Run an extended verify, verifying all values in the new SSTables

        -h <host>, --host <host>
            Node hostname or ip address

        -l, --keep-level
            Keep the level on the new SSTables

        -p <port>, --port <port>
            Remote jmx agent port number

        -pp, --print-port
            Operate in 4.0 mode with hosts disambiguated by port number

        -pw <password>, --password <password>
            Remote jmx agent password

        -pwf <passwordFilePath>, --password-file <passwordFilePath>
            Path to the JMX password file

        -q, --quick
            Do a quick import without verifying SSTables, clearing row cache or
            checking in which data directory to put the file

        -r, --keep-repaired
            Keep any repaired information from the SSTables

        -t, --no-tokens
            Don't verify that all tokens in the new SSTable are owned by the
            current node

        -u <username>, --username <username>
            Remote jmx agent username

        -v, --no-verify
            Don't verify new SSTables

        --
            This option can be used to separate command-line options from the
            list of argument, (useful when arguments might be mistaken for
            command-line options

As the keyspace and table are specified on the command line  ``nodetool import`` does not have the same requirement that ``sstableloader`` does, which is to have the SSTables in a specific directory path. When importing snapshots or incremental backups with ``nodetool import`` the SSTables don’t need to be copied to another directory.

Importing Data from an Incremental Backup
*****************************************

In this section we shall demonstrate using ``nodetool import`` to import SSTables from an incremental backup.  We shall use the example table ``cqlkeyspace.t``. Drop table ``t`` as we are demonstrating to   restore the table.

::

 cqlsh:cqlkeyspace> DROP table t;

An incremental backup for a table does not include the schema definition for the table. If the schema definition is not kept as a separate backup,  the ``schema.cql`` from a backup of the table may be used to create the table as follows.

::

 cqlsh:cqlkeyspace> CREATE TABLE IF NOT EXISTS cqlkeyspace.t (
               ...         id int PRIMARY KEY,
               ...         k int,
               ...         v text)
               ...         WITH ID = d132e240-c217-11e9-bbee-19821dcea330
               ...         AND bloom_filter_fp_chance = 0.01
               ...         AND crc_check_chance = 1.0
               ...         AND default_time_to_live = 0
               ...         AND gc_grace_seconds = 864000
               ...         AND min_index_interval = 128
               ...         AND max_index_interval = 2048
               ...         AND memtable_flush_period_in_ms = 0
               ...         AND speculative_retry = '99p'
               ...         AND additional_write_policy = '99p'
               ...         AND comment = ''
               ...         AND caching = { 'keys': 'ALL', 'rows_per_partition': 'NONE' }
               ...         AND compaction = { 'max_threshold': '32', 'min_threshold': '4',
 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy' }
               ...         AND compression = { 'chunk_length_in_kb': '16', 'class':
 'org.apache.cassandra.io.compress.LZ4Compressor' }
               ...         AND cdc = false
               ...         AND extensions = {  };

Initially the table could be empty, but does not have to be.

::

 cqlsh:cqlkeyspace> SELECT * FROM t;

 id | k | v
 ----+---+---

 (0 rows)

Run the ``nodetool import`` command by providing the keyspace, table and the backups directory. We don’t need to copy the table backups to another directory to run  ``nodetool import`` as we had to when using ``sstableloader``.

::

 [ec2-user@ip-10-0-2-238 ~]$ nodetool import -- cqlkeyspace t
 ./cassandra/data/data/cqlkeyspace/t-d132e240c21711e9bbee19821dcea330/backups
 [ec2-user@ip-10-0-2-238 ~]$

The SSTables get imported into the table. Run a query in cqlsh to list the data imported.

::

 cqlsh:cqlkeyspace> SELECT * FROM t;

 id | k | v
 ----+---+------
  1 | 1 | val1
  0 | 0 | val0


Importing Data from a Snapshot
********************************
Importing SSTables from a snapshot with the ``nodetool import`` command is similar to importing SSTables from an incremental backup. To demonstrate we shall import a snapshot for table ``catalogkeyspace.journal``.  Drop the table as we are demonstrating to restore the table from a snapshot.

::

 cqlsh:cqlkeyspace> use CATALOGKEYSPACE;
 cqlsh:catalogkeyspace> DROP TABLE journal;

We shall use the ``catalog-ks`` snapshot for the ``journal`` table. List the files in the snapshot. The snapshot includes a ``schema.cql``, which is the schema definition for the ``journal`` table.

::

 [ec2-user@ip-10-0-2-238 catalog-ks]$ ls -l
 total 44
 -rw-rw-r--. 1 ec2-user ec2-user   31 Aug 19 02:44 manifest.json
 -rw-rw-r--. 3 ec2-user ec2-user   47 Aug 19 02:38 na-1-big-CompressionInfo.db
 -rw-rw-r--. 3 ec2-user ec2-user   97 Aug 19 02:38 na-1-big-Data.db
 -rw-rw-r--. 3 ec2-user ec2-user   10 Aug 19 02:38 na-1-big-Digest.crc32
 -rw-rw-r--. 3 ec2-user ec2-user   16 Aug 19 02:38 na-1-big-Filter.db
 -rw-rw-r--. 3 ec2-user ec2-user   16 Aug 19 02:38 na-1-big-Index.db
 -rw-rw-r--. 3 ec2-user ec2-user 4687 Aug 19 02:38 na-1-big-Statistics.db
 -rw-rw-r--. 3 ec2-user ec2-user   56 Aug 19 02:38 na-1-big-Summary.db
 -rw-rw-r--. 3 ec2-user ec2-user   92 Aug 19 02:38 na-1-big-TOC.txt
 -rw-rw-r--. 1 ec2-user ec2-user  814 Aug 19 02:44 schema.cql

Copy the DDL from the ``schema.cql`` and run in cqlsh to create the ``catalogkeyspace.journal`` table.

::

 cqlsh:catalogkeyspace> CREATE TABLE IF NOT EXISTS catalogkeyspace.journal (
                   ...         id int PRIMARY KEY,
                   ...         name text,
                   ...         publisher text)
                   ...         WITH ID = 296a2d30-c22a-11e9-b135-0d927649052c
                   ...         AND bloom_filter_fp_chance = 0.01
                   ...         AND crc_check_chance = 1.0
                   ...         AND default_time_to_live = 0
                   ...         AND gc_grace_seconds = 864000
                   ...         AND min_index_interval = 128
                   ...         AND max_index_interval = 2048
                   ...         AND memtable_flush_period_in_ms = 0
                   ...         AND speculative_retry = '99p'
                   ...         AND additional_write_policy = '99p'
                   ...         AND comment = ''
                   ...         AND caching = { 'keys': 'ALL', 'rows_per_partition': 'NONE' }
                   ...         AND compaction = { 'min_threshold': '4', 'max_threshold':
 '32', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy' }
                   ...         AND compression = { 'chunk_length_in_kb': '16', 'class':
 'org.apache.cassandra.io.compress.LZ4Compressor' }
                   ...         AND cdc = false
                   ...         AND extensions = {  };


Run the ``nodetool import`` command to import the SSTables for the snapshot.

::

 [ec2-user@ip-10-0-2-238 ~]$ nodetool import -- catalogkeyspace journal
 ./cassandra/data/data/catalogkeyspace/journal-
 296a2d30c22a11e9b1350d927649052c/snapshots/catalog-ks/
 [ec2-user@ip-10-0-2-238 ~]$

Subsequently run a CQL query on the ``journal`` table and the data imported gets listed.

::

 cqlsh:catalogkeyspace>
 cqlsh:catalogkeyspace> SELECT * FROM journal;

 id | name                      | publisher
 ----+---------------------------+------------------
  1 |        Couchbase Magazine |        Couchbase
  0 | Apache Cassandra Magazine | Apache Cassandra

 (2 rows)
 cqlsh:catalogkeyspace>


Bulk Loading External Data
^^^^^^^^^^^^^^^^^^^^^^^^^^

Bulk loading external data directly is not supported by any of the tools we have discussed which include ``sstableloader`` and ``nodetool import``.  The ``sstableloader`` and ``nodetool import`` require data to be in the form of SSTables.  Apache Cassandra supports a Java API for generating SSTables from input data. Subsequently the ``sstableloader`` or ``nodetool import`` could be used to bulk load the SSTables. Next, we shall discuss the ``org.apache.cassandra.io.sstable.CQLSSTableWriter`` Java class for generating SSTables.

Generating SSTables with CQLSSTableWriter Java API
***************************************************
To generate SSTables using the ``CQLSSTableWriter`` class the following need to be supplied at the least.

- An output directory to generate the SSTable in
- The schema for the SSTable
- A prepared insert statement
- A partitioner

The output directory must already have been created. Create a directory (``/sstables`` as an example) and set its permissions.

::

 sudo mkdir /sstables
 sudo chmod  777 -R /sstables

Next, we shall discuss To use ``CQLSSTableWriter`` could be used in a Java application. Create a Java constant for the output directory.

::

 public static final String OUTPUT_DIR = "./sstables";

``CQLSSTableWriter`` Java API has the provision to create a user defined type. Create a new type to store ``int`` data:

::

 String type = "CREATE TYPE CQLKeyspace.intType (a int, b int)";
 // Define a String variable for the SSTable schema.
 String schema = "CREATE TABLE CQLKeyspace.t ("
                  + "  id int PRIMARY KEY,"
                  + "  k int,"
                  + "  v1 text,"
                  + "  v2 intType,"
                  + ")";

Define a ``String`` variable for the prepared insert statement to use:

::

 String insertStmt = "INSERT INTO CQLKeyspace.t (id, k, v1, v2) VALUES (?, ?, ?, ?)";

The partitioner to use does not need to be set as the default partitioner ``Murmur3Partitioner`` is used.

All these variables or settings are used by the builder class ``CQLSSTableWriter.Builder`` to create a ``CQLSSTableWriter`` object.

Create a File object for the output directory.

::

 File outputDir = new File(OUTPUT_DIR + File.separator + "CQLKeyspace" + File.separator + "t");

Next, obtain a ``CQLSSTableWriter.Builder`` object using ``static`` method ``CQLSSTableWriter.builder()``. Set the output
directory ``File`` object, user defined type, SSTable schema, buffer size,  prepared insert statement, and optionally any of the other builder options, and invoke the ``build()`` method to create a ``CQLSSTableWriter`` object:

::

 CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                              .inDirectory(outputDir)
                                              .withType(type)
                                              .forTable(schema)
                                              .withBufferSizeInMB(256)
                                              .using(insertStmt).build();

Next, set the SSTable data. If any user define types are used obtain a ``UserType`` object for these:

::

 UserType userType = writer.getUDType("intType");

Add data rows for the resulting SSTable.

::

 writer.addRow(0, 0, "val0", userType.newValue().setInt("a", 0).setInt("b", 0));
    writer.addRow(1, 1, "val1", userType.newValue().setInt("a", 1).setInt("b", 1));
    writer.addRow(2, 2, "val2", userType.newValue().setInt("a", 2).setInt("b", 2));

Close the writer, finalizing the SSTable.

::

    writer.close();

All the public methods the ``CQLSSTableWriter`` class provides including some other methods that are not discussed in the preceding example are as follows.

=====================================================================   ============
Method                                                                  Description
=====================================================================   ============
addRow(java.util.List<java.lang.Object> values)                         Adds a new row to the writer. Returns a CQLSSTableWriter object. Each provided value type should correspond to the types of the CQL column the value is for. The correspondence between java type and CQL type is the same one than the one documented at www.datastax.com/drivers/java/2.0/apidocs/com/datastax/driver/core/DataType.Name.html#asJavaC lass().
addRow(java.util.Map<java.lang.String,java.lang.Object> values)         Adds a new row to the writer. Returns a CQLSSTableWriter object. This is equivalent to the other addRow methods, but takes a map whose keys are the names of the columns to add instead of taking a list of the values in the order of the insert statement used during construction of this SSTable writer. The column names in the map keys must be in lowercase unless the declared column name is a case-sensitive quoted identifier in which case the map key must use the exact case of the column. The values parameter is a map of column name to column values representing the new row to add. If a column is not included in the map, it's value will be null. If the map contains keys that do not correspond to one of the columns of the insert statement used when creating this SSTable writer, the corresponding value is ignored.
addRow(java.lang.Object... values)                                      Adds a new row to the writer. Returns a CQLSSTableWriter object.
CQLSSTableWriter.builder()                                              Returns a new builder for a CQLSSTableWriter.
close()                                                                 Closes the writer.
rawAddRow(java.nio.ByteBuffer... values)                                Adds a new row to the writer given already serialized binary values.  Returns a CQLSSTableWriter object. The row values must correspond  to the bind variables of the insertion statement used when creating by this SSTable writer.
rawAddRow(java.util.List<java.nio.ByteBuffer> values)                   Adds a new row to the writer given already serialized binary values.  Returns a CQLSSTableWriter object. The row values must correspond  to the bind variables of the insertion statement used when creating by this SSTable writer. |
rawAddRow(java.util.Map<java.lang.String, java.nio.ByteBuffer> values)  Adds a new row to the writer given already serialized binary values.  Returns a CQLSSTableWriter object. The row values must correspond  to the bind variables of the insertion statement used when creating by this SSTable  writer. |
getUDType(String dataType)                                              Returns the User Defined type used in this SSTable Writer that can be used to create UDTValue instances.
=====================================================================   ============


All the public  methods the  ``CQLSSTableWriter.Builder`` class provides including some other methods that are not discussed in the preceding example are as follows.

============================================   ============
Method                                         Description
============================================   ============
inDirectory(String directory)                  The directory where to write the SSTables.  This is a mandatory option.  The directory to use should already exist and be writable.
inDirectory(File directory)                    The directory where to write the SSTables.  This is a mandatory option.  The directory to use should already exist and be writable.
forTable(String schema)                        The schema (CREATE TABLE statement) for the table for which SSTable is to be created.  The
                                               provided CREATE TABLE statement must use a fully-qualified table name, one that includes the
                                               keyspace name. This is a mandatory option.

withPartitioner(IPartitioner partitioner)      The partitioner to use. By default,  Murmur3Partitioner will be used. If this is not the
                                               partitioner used by the cluster for which the SSTables are created, the correct partitioner
                                               needs to be provided.

using(String insert)                           The INSERT or UPDATE statement defining the order of the values to add for a given CQL row.
                                               The provided INSERT statement must use a fully-qualified table name, one that includes the
                                               keyspace name. Moreover, said statement must use bind variables since these variables will
                                               be bound to values by the resulting SSTable writer. This is a mandatory option.

withBufferSizeInMB(int size)                   The size of the buffer to use. This defines how much data will be buffered before being
                                               written as a new SSTable. This corresponds roughly to the data size that will have the
                                               created SSTable. The default is 128MB, which should be reasonable for a 1GB heap. If
                                               OutOfMemory exception gets generated while using the SSTable writer, should lower this
                                               value.

sorted()                                       Creates a CQLSSTableWriter that expects sorted inputs. If this option is used, the resulting
                                               SSTable writer will expect rows to be added in SSTable sorted order (and an exception will
                                               be thrown if that is not the case during row insertion). The SSTable sorted order means that
                                               rows are added such that their partition keys respect the partitioner order. This option
                                               should only be used if the rows can be provided in order, which is rarely the case. If the
                                               rows can be provided in order however, using this sorted might be more efficient. If this
                                               option is used, some option like withBufferSizeInMB will be ignored.

build()                                        Builds a CQLSSTableWriter object.

============================================   ============

