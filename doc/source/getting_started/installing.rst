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

- Add the Apache Cassandra repository keys:

::

    curl https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -

- Update the repositories:

::

    sudo apt-get update

- If you encounter this error:

::

    GPG error: http://www.apache.org 36x InRelease: The following signatures couldn't be verified because the public key is not available: NO_PUBKEY A278B781FE4B2BDA

Then add the public key A278B781FE4B2BDA as follows:

::

    sudo apt-key adv --keyserver pool.sks-keyservers.net --recv-key A278B781FE4B2BDA

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
