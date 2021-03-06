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

Configuring Cassandra
---------------------

The :term:`Cassandra` configuration files location varies, depending on the type of installation:

- tarball: ``conf`` directory within the tarball install location
- package: ``/etc/cassandra`` directory

Cassandra's default configuration file, ``cassandra.yaml``, is sufficient to explore a simple single-node :term:`cluster`.
However, anything beyond running a single-node cluster locally requires additional configuration to various Cassandra configuration files.
Some examples that require non-default configuration are deploying a multi-node cluster or using clients that are not running on a cluster node.

- ``cassandra.yaml``: the main configuration file for Cassandra
- ``cassandra-env.sh``:  environment variables can be set
- ``cassandra-rackdc.properties`` OR ``cassandra-topology.properties``: set rack and datacenter information for a cluster
- ``logback.xml``: logging configuration including logging levels
- ``jvm-*``: a number of JVM configuration files for both the server and clients
- ``commitlog_archiving.properties``: set archiving parameters for the :term:`commitlog`

Two sample configuration files can also be found in ``./conf``:

- ``metrics-reporter-config-sample.yaml``: configuring what the metrics-report will collect
- ``cqlshrc.sample``: how the CQL shell, cqlsh, can be configured

Main runtime properties
^^^^^^^^^^^^^^^^^^^^^^^

Configuring Cassandra is done by setting yaml properties in the ``cassandra.yaml`` file. At a minimum you
should consider setting the following properties:

- ``cluster_name``: Set the name of your cluster.
- ``seeds``: A comma separated list of the IP addresses of your cluster :term:`seed nodes`.
- ``storage_port``: Check that you don't have the default port of 7000 blocked by a firewall.
- ``listen_address``: The :term:`listen address` is the IP address of a node that allows it to communicate with other nodes in the cluster. Set to `localhost` by default. Alternatively, you can set ``listen_interface`` to tell Cassandra which interface to use, and consecutively which address to use. Set one property, not both.
- ``native_transport_port``: Check that you don't have the default port of 9042 blocked by a firewall, so that clients like cqlsh can communicate with Cassandra on this port.

Changing the location of directories
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following yaml properties control the location of directories:

- ``data_file_directories``: One or more directories where data files, like :term:`SSTables` are located.
- ``commitlog_directory``: The directory where commitlog files are located.
- ``saved_caches_directory``: The directory where saved caches are located.
- ``hints_directory``: The directory where :term:`hints` are located.

For performance reasons, if you have multiple disks, consider putting commitlog and data files on different disks.

Environment variables
^^^^^^^^^^^^^^^^^^^^^

JVM-level settings such as heap size can be set in ``cassandra-env.sh``.  You can add any additional JVM command line
argument to the ``JVM_OPTS`` environment variable; when Cassandra starts, these arguments will be passed to the JVM.

Logging
^^^^^^^

The default logger is `logback`. By default it will log:

- **INFO** level in ``system.log`` 
- **DEBUG** level in ``debug.log``

When running in the foreground, it will also log at INFO level to the console. You can change logging properties by editing ``logback.xml`` or by running the `nodetool setlogginglevel` command.

