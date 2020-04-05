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

These are the instructions for deploying the supported releases of Apache Cassandra on Linux servers.

Cassandra runs on a wide array of Linux distributions including (but not limited to):

- Ubuntu, most notably LTS releases 16.04 to 18.04
- CentOS & RedHat Enterprise Linux (RHEL) including 6.6 to 7.7
- Amazon Linux AMIs including 2016.09 through to Linux 2
- Debian versions 8 & 9
- SUSE Enterprise Linux 12

This is not an exhaustive list of operating system platforms, nor is it prescriptive. However users will be
well-advised to conduct exhaustive tests of their own particularly for less-popular distributions of Linux.
Deploying on older versions is not recommended unless you have previous experience with the older distribution
in a production environment.

Prerequisites
^^^^^^^^^^^^^

- Install the latest version of Java 8, either the `Oracle Java Standard Edition 8
  <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__ or `OpenJDK 8 <http://openjdk.java.net/>`__. To
  verify that you have the correct version of java installed, type ``java -version``.
- **NOTE**: *Experimental* support for Java 11 was added in Cassandra 4.0 (`CASSANDRA-9608 <https://issues.apache.org/jira/browse/CASSANDRA-9608>`__).
  Running Cassandra on Java 11 is *experimental*. Do so at your own risk. For more information, see
  `NEWS.txt <https://github.com/apache/cassandra/blob/trunk/NEWS.txt>`__.
- For using cqlsh, the latest version of `Python 2.7 <https://www.python.org/downloads/>`__ or Python 3.6+. To verify that you have
  the correct version of Python installed, type ``python --version``.

Choosing an installation method
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For most users, installing the binary tarball is the simplest choice. The tarball unpacks all its contents
into a single location with binaries and configuration files located in their own subdirectories. The most
obvious attribute of the tarball installation is it does not require ``root`` permissions and can be
installed on any Linux distribution.

Packaged installations require ``root`` permissions. Install the RPM build on CentOS and RHEL-based
distributions if you want to install Cassandra using YUM. Install the Debian build on Ubuntu and other
Debian-based distributions if you want to install Cassandra using APT. Note that both the YUM and APT
methods required ``root`` permissions and will install the binaries and configuration files as the
``cassandra`` OS user.

Installing the binary tarball
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Verify the version of Java installed. For example:

::

   $ java -version
   openjdk version "1.8.0_222"
   OpenJDK Runtime Environment (build 1.8.0_222-8u222-b10-1ubuntu1~16.04.1-b10)
   OpenJDK 64-Bit Server VM (build 25.222-b10, mixed mode)

2. Download the binary tarball from one of the mirrors on the `Apache Cassandra Download <http://cassandra.apache.org/download/>`__
   site. For example, to download 4.0:

::

   $ curl -OL http://apache.mirror.digitalpacific.com.au/cassandra/4.0.0/apache-cassandra-4.0.0-bin.tar.gz

NOTE: The mirrors only host the latest versions of each major supported release. To download an earlier
version of Cassandra, visit the `Apache Archives <http://archive.apache.org/dist/cassandra/>`__.

3. OPTIONAL: Verify the integrity of the downloaded tarball using one of the methods `here <https://www.apache.org/dyn/closer.cgi#verify>`__.
   For example, to verify the hash of the downloaded file using GPG:

::

   $ gpg --print-md SHA256 apache-cassandra-4.0.0-bin.tar.gz 
   apache-cassandra-4.0.0-bin.tar.gz: 28757DDE 589F7041 0F9A6A95 C39EE7E6
                                      CDE63440 E2B06B91 AE6B2006 14FA364D

Compare the signature with the SHA256 file from the Downloads site:

::

   $ curl -L https://downloads.apache.org/cassandra/4.0.0/apache-cassandra-4.0.0-bin.tar.gz.sha256
   28757dde589f70410f9a6a95c39ee7e6cde63440e2b06b91ae6b200614fa364d

4. Unpack the tarball:

::

   $ tar xzvf apache-cassandra-4.0.0-bin.tar.gz

The files will be extracted to the ``apache-cassandra-4.0.0/`` directory. This is the tarball installation
location.

5. Located in the tarball installation location are the directories for the scripts, binaries, utilities, configuration, data and log files:

::

   <tarball_installation>/
       bin/
       conf/
       data/
       doc/
       interface/
       javadoc/
       lib/
       logs/
       pylib/
       tools/
       
For information on how to configure your installation, see
`Configuring Cassandra <http://cassandra.apache.org/doc/latest/getting_started/configuring.html>`__.

6. Start Cassandra:

::

   $ cd apache-cassandra-4.0.0/
   $ bin/cassandra

NOTE: This will run Cassandra as the authenticated Linux user.

You can monitor the progress of the startup with:

::

   $ tail -f logs/system.log

Cassandra is ready when you see an entry like this in the ``system.log``:

::

   INFO  [main] 2019-12-17 03:03:37,526 Server.java:156 - Starting listening for CQL clients on localhost/127.0.0.1:9042 (unencrypted)...

7. Check the status of Cassandra:

::

   $ bin/nodetool status

The status column in the output should report UN which stands for "Up/Normal".

Alternatively, connect to the database with:

::

   $ bin/cqlsh

Installing the Debian packages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Verify the version of Java installed. For example:

::

   $ java -version
   openjdk version "1.8.0_222"
   OpenJDK Runtime Environment (build 1.8.0_222-8u222-b10-1ubuntu1~16.04.1-b10)
   OpenJDK 64-Bit Server VM (build 25.222-b10, mixed mode)

2. Add the Apache repository of Cassandra to the file ``cassandra.sources.list``. The latest major version
   is 4.0 and the corresponding distribution name is ``40x`` (with an "x" as the suffix).
   For older releases use ``311x`` for C* 3.11 series, ``30x`` for 3.0, ``22x`` for 2.2 and ``21x`` for 2.1.
   For example, to add the repository for version 4.0 (``40x``):

::

   $ echo "deb http://www.apache.org/dist/cassandra/debian 40x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
   deb http://www.apache.org/dist/cassandra/debian 40x main

3. Add the Apache Cassandra repository keys to the list of trusted keys on the server:

::

   $ curl https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -
     % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                    Dload  Upload   Total   Spent    Left  Speed
   100  266k  100  266k    0     0   320k      0 --:--:-- --:--:-- --:--:--  320k
   OK

4. Update the package index from sources:

::

   $ sudo apt-get update

5. Install Cassandra with APT:

::

   $ sudo apt-get install cassandra


NOTE: A new Linux user ``cassandra`` will get created as part of the installation. The Cassandra service
will also be run as this user.

6. The Cassandra service gets started automatically after installation. Monitor the progress of
   the startup with:

::

   $ tail -f /var/log/cassandra/system.log

Cassandra is ready when you see an entry like this in the ``system.log``:

::

   INFO  [main] 2019-12-17 03:03:37,526 Server.java:156 - Starting listening for CQL clients on localhost/127.0.0.1:9042 (unencrypted)...

NOTE: For information on how to configure your installation, see
`Configuring Cassandra <http://cassandra.apache.org/doc/latest/getting_started/configuring.html>`__.

7. Check the status of Cassandra:

::

   $ nodetool status

The status column in the output should report ``UN`` which stands for "Up/Normal".

Alternatively, connect to the database with:

::

   $ cqlsh
   
Installing the RPM packages
^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Verify the version of Java installed. For example:

::

   $ java -version
   openjdk version "1.8.0_222"
   OpenJDK Runtime Environment (build 1.8.0_232-b09)
   OpenJDK 64-Bit Server VM (build 25.232-b09, mixed mode)

2. Add the Apache repository of Cassandra to the file ``/etc/yum.repos.d/cassandra.repo`` (as the ``root``
   user). The latest major version is 4.0 and the corresponding distribution name is ``40x`` (with an "x" as the suffix).
   For older releases use ``311x`` for C* 3.11 series, ``30x`` for 3.0, ``22x`` for 2.2 and ``21x`` for 2.1.
   For example, to add the repository for version 4.0 (``40x``):

::

   [cassandra]
   name=Apache Cassandra
   baseurl=https://downloads.apache.org/cassandra/redhat/40x/
   gpgcheck=1
   repo_gpgcheck=1
   gpgkey=https://downloads.apache.org/cassandra/KEYS

3. Update the package index from sources:

::

   $ sudo yum update

4. Install Cassandra with YUM:

::

   $ sudo yum install cassandra


NOTE: A new Linux user ``cassandra`` will get created as part of the installation. The Cassandra service
will also be run as this user.

5. Start the Cassandra service:

::

   $ sudo service cassandra start

6. Monitor the progress of the startup with:

::

   $ tail -f /var/log/cassandra/system.log

Cassandra is ready when you see an entry like this in the ``system.log``:

::

   INFO  [main] 2019-12-17 03:03:37,526 Server.java:156 - Starting listening for CQL clients on localhost/127.0.0.1:9042 (unencrypted)...

NOTE: For information on how to configure your installation, see
`Configuring Cassandra <http://cassandra.apache.org/doc/latest/getting_started/configuring.html>`__.

7. Check the status of Cassandra:

::

   $ nodetool status

The status column in the output should report ``UN`` which stands for "Up/Normal".

Alternatively, connect to the database with:

::

   $ cqlsh

Further installation info
^^^^^^^^^^^^^^^^^^^^^^^^^

For help with installation issues, see the `Troubleshooting <http://cassandra.apache.org/doc/latest/troubleshooting/index.html>`__ section.


