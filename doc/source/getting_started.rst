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

Getting Started
===============

Installing Cassandra
--------------------

Prerequisites
^^^^^^^^^^^^^

- The latest version of Java 8, either the `Oracle Java Standard Edition 8
  <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__ or `OpenJDK 8 <http://openjdk.java.net/>`__. To
  verify that you have the correct version of java installed, type ``java -version``.

- For using cqlsh, the latest version of `Python 2.7 <https://www.python.org/downloads/>`__. To verify that you have
  the correct version of Python installed, type ``python --version``.

Installation from binary tarball files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Download the latest stable release from the `Apache Cassandra downloads website <http://cassandra.apache.org/download/>`__.

- Untar the file somewhere, for example:

::

    tar -xvf apache-cassandra-3.6-bin.tar.gz cassandra

The files will be extracted into ``apache-cassandra-3.6``, you need to substitute 3.6 with the release number that you
have downloaded.

- Optionally add ``apache-cassandra-3.6\bin`` to your path.
- Start Cassandra in the foreground by invoking ``bin/cassandra -f`` from the command line. Press "Control-C" to stop
  Cassandra. Start Cassandra in the background by invoking ``bin/cassandra`` from the command line. Invoke ``kill pid``
  or ``pkill -f CassandraDaemon`` to stop Cassandra, where pid is the Cassandra process id, which you can find for
  example by invoking ``pgrep -f CassandraDaemon``.
- Verify that Cassandra is running by invoking ``bin/nodetool status`` from the command line.
- Configuration files are located in the ``conf`` sub-directory.
- Since Cassandra 2.1, log and data directories are located in the ``logs`` and ``data`` sub-directories respectively.
  Older versions defaulted to ``/var/log/cassandra`` and ``/var/lib/cassandra``. Due to this, it is necessary to either
  start Cassandra with root privileges or change ``conf/cassandra.yaml`` to use directories owned by the current user,
  as explained below in the section on changing the location of directories.

Installation from Debian packages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Add the Apache repository of Cassandra to ``/etc/apt/sources.list.d/cassandra.sources.list``, for example for version
  3.6:

::

    echo "deb http://www.apache.org/dist/cassandra/debian 36x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list

- Update the repositories:

::

    sudo apt-get update

- If you encounter this error:

::

    GPG error: http://www.apache.org 36x InRelease: The following signatures couldn't be verified because the public key is not available: NO_PUBKEY 749D6EEC0353B12C

Then add the public key 749D6EEC0353B12C as follows:

::

    gpg --keyserver pgp.mit.edu --recv-keys 749D6EEC0353B12C
    gpg --export --armor 749D6EEC0353B12C | sudo apt-key add -

and repeat ``sudo apt-get update``. The actual key may be different, you get it from the error message itself. For a
full list of Apache contributors public keys, you can refer to `this link <https://www.apache.org/dist/cassandra/KEYS>`__.

- Install Cassandra:

::

    sudo apt-get install cassandra

- You can start Cassandra with ``sudo service cassandra start`` and stop it with ``sudo service cassandra stop``.
  However, normally the service will start automatically. For this reason be sure to stop it if you need to make any
  configuration changes.
- Verify that Cassandra is running by invoking ``nodetool status`` from the command line.
- The default location of configuration files is ``/etc/cassandra``.
- The default location of log and data directories is ``/var/log/cassandra/`` and ``/var/lib/cassandra``.

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


cqlsh
-----

cqlsh is a command line shell for interacting with Cassandra through CQL (the Cassandra Query Language). It is shipped
with every Cassandra package, and can be found in the bin/ directory alongside the cassandra executable. cqlsh utilizes
the Python native protocol driver, and connects to the single node specified on the command line. For example::

    $ bin/cqlsh localhost
    Connected to Test Cluster at localhost:9042.
    [cqlsh 5.0.1 | Cassandra 3.8 | CQL spec 3.4.2 | Native protocol v4]
    Use HELP for help.
    cqlsh> SELECT cluster_name, listen_address FROM system.local;

     cluster_name | listen_address
    --------------+----------------
     Test Cluster |      127.0.0.1

    (1 rows)
    cqlsh>


See the :ref:`cqlsh section <cqlsh>` for full documentation.

Cassandra client drivers
------------------------

Here are known Cassandra client drivers organized by language. Before choosing a driver, you should verify the Cassandra
version and functionality supported by a specific driver.

Java
^^^^

- `Achilles <http://achilles.archinnov.info/>`__
- `Astyanax <https://github.com/Netflix/astyanax/wiki/Getting-Started>`__
- `Casser <https://github.com/noorq/casser>`__
- `Datastax Java driver <https://github.com/datastax/java-driver>`__
- `Kundera <https://github.com/impetus-opensource/Kundera>`__
- `PlayORM <https://github.com/deanhiller/playorm>`__

Python
^^^^^^

- `Datastax Python driver <https://github.com/datastax/python-driver>`__

Ruby
^^^^

- `Datastax Ruby driver <https://github.com/datastax/ruby-driver>`__

C# / .NET
^^^^^^^^^

- `Cassandra Sharp <https://github.com/pchalamet/cassandra-sharp>`__
- `Datastax C# driver <https://github.com/datastax/csharp-driver>`__
- `Fluent Cassandra <https://github.com/managedfusion/fluentcassandra>`__

Nodejs
^^^^^^

- `Datastax Nodejs driver <https://github.com/datastax/nodejs-driver>`__
- `Node-Cassandra-CQL <https://github.com/jorgebay/node-cassandra-cql>`__

PHP
^^^

- `CQL \| PHP <http://code.google.com/a/apache-extras.org/p/cassandra-pdo>`__
- `Datastax PHP driver <https://github.com/datastax/php-driver/>`__
- `PHP-Cassandra <https://github.com/aparkhomenko/php-cassandra>`__
- `PHP Library for Cassandra <http://evseevnn.github.io/php-cassandra-binary/>`__

C++
^^^

- `Datastax C++ driver <https://github.com/datastax/cpp-driver>`__
- `libQTCassandra <http://sourceforge.net/projects/libqtcassandra>`__

Scala
^^^^^

- `Datastax Spark connector <https://github.com/datastax/spark-cassandra-connector>`__
- `Phantom <https://github.com/newzly/phantom>`__
- `Quill <https://github.com/getquill/quill>`__

Clojure
^^^^^^^

- `Alia <https://github.com/mpenet/alia>`__
- `Cassaforte <https://github.com/clojurewerkz/cassaforte>`__
- `Hayt <https://github.com/mpenet/hayt>`__

Erlang
^^^^^^

- `CQerl <https://github.com/matehat/cqerl>`__
- `Erlcass <https://github.com/silviucpp/erlcass>`__

Go
^^

- `CQLc <http://relops.com/cqlc/>`__
- `Gocassa <https://github.com/hailocab/gocassa>`__
- `GoCQL <https://github.com/gocql/gocql>`__

Haskell
^^^^^^^

- `Cassy <https://github.com/ozataman/cassy>`__

Rust
^^^^

- `Rust CQL <https://github.com/neich/rust-cql>`__
