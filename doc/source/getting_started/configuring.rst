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

For running Cassandra on a single node, the steps above are enough, you don't really need to change any configuration.
However, when you deploy a cluster of nodes, or use clients that are not on the same host, then there are some
parameters that must be changed.

The Cassandra configuration files can be found in the ``conf`` directory of tarballs. For packages, the configuration
files will be located in ``/etc/cassandra``.

Main runtime properties
^^^^^^^^^^^^^^^^^^^^^^^

Most of configuration in Cassandra is done via yaml properties that can be set in ``cassandra.yaml``. At a minimum you
should consider setting the following properties:

- ``cluster_name``: the name of your cluster.
- ``seeds``: a comma separated list of the IP addresses of your cluster seeds.
- ``storage_port``: you don't necessarily need to change this but make sure that there are no firewalls blocking this
  port.
- ``listen_address``: the IP address of your node, this is what allows other nodes to communicate with this node so it
  is important that you change it. Alternatively, you can set ``listen_interface`` to tell Cassandra which interface to
  use, and consecutively which address to use. Set only one, not both.
- ``native_transport_port``: as for storage\_port, make sure this port is not blocked by firewalls as clients will
  communicate with Cassandra on this port.

Changing the location of directories
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following yaml properties control the location of directories:

- ``data_file_directories``: one or more directories where data files are located.
- ``commitlog_directory``: the directory where commitlog files are located.
- ``saved_caches_directory``: the directory where saved caches are located.
- ``hints_directory``: the directory where hints are located.

For performance reasons, if you have multiple disks, consider putting commitlog and data files on different disks.

Environment variables
^^^^^^^^^^^^^^^^^^^^^

JVM-level settings such as heap size can be set in ``cassandra-env.sh``.  You can add any additional JVM command line
argument to the ``JVM_OPTS`` environment variable; when Cassandra starts these arguments will be passed to the JVM.

Logging
^^^^^^^

The logger in use is logback. You can change logging properties by editing ``logback.xml``. By default it will log at
INFO level into a file called ``system.log`` and at debug level into a file called ``debug.log``. When running in the
foreground, it will also log at INFO level to the console.

