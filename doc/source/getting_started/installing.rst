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

- Ubuntu, most notably LTS releases 14.04 to 18.04
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
- For using cqlsh, the latest version of `Python 2.7 <https://www.python.org/downloads/>`__. To verify that you have
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

1. Download the binary tarball from one of the mirrors on the `Apache Cassandra Downloadi <http://cassandra.apache.org/download/>`__
   site. For example, to download 3.11.6:

::

   $ curl -OL http://apache.mirror.digitalpacific.com.au/cassandra/3.11.6/apache-cassandra-3.11.6-bin.tar.gz

NOTE: The mirrors only host the latest versions of each major supported release. To download an earlier
version of Cassandra, visit the `Apache Archives <http://archive.apache.org/dist/cassandra/>`__.

2. OPTIONAL: Verify the integrity of the downloaded tarball using one of the methods `here <https://www.apache.org/dyn/closer.cgi#verify>`__.
   For example, to verify the hash of the downloaded file using GPG:

::

   $ gpg --print-md SHA256 apache-cassandra-3.11.6-bin.tar.gz 
   apache-cassandra-3.11.6-bin.tar.gz: CE34EDEB D1B6BB35 216AE97B D06D3EFC 338C05B2
                                       73B78267 556A99F8 5D30E45B

Compare the signature with the SHA256 file from the Downloads site:

::

   $ curl -L https://downloads.apache.org/cassandra/3.11.6/apache-cassandra-3.11.6-bin.tar.gz.sha256
   ce34edebd1b6bb35216ae97bd06d3efc338c05b273b78267556a99f85d30e45b

3. Unpack the tarball:

::

   $ tar xzvf apache-cassandra-3.11.6-bin.tar.gz

The files will be extracted to the ``apache-cassandra-3.11.6/`` directory. This is the tarball installation
location.

4. Configure Cassandra by updating the file ``conf/cassandra.yaml``.

If this is your first time installing Cassandra, the following are the recommended properties to set
to get you started:

::

   cluster_name: 'My First Cluster'
   num_tokens: 16
   listen_address: private_IP
   native_transport_port: public_IP

NOTE: If the server only has 1 IP address, use the same IP for both listen_address and rpc_address.

Also set the seeds list. See `the FAQ section <http://cassandra.apache.org/doc/latest/faq/index.html?highlight=seeds#what-are-seeds>`__
for more details. For now, just use the private IP of the node:

::

   - seeds: "private_IP"

For more information, see `Configuring Cassandra <http://cassandra.apache.org/doc/latest/getting_started/configuring.html>`__.

5. The tarball instllation default ``data``, ``commitlog``, ``saved_caches`` and ``hints`` directories
   are in ``installation_location/data``.

OPTIONAL: To change the location, set the following properties in ``conf/cassandra.yaml``:

::

   data_file_directories:
       - /path/to/data
   commitlog_directory: /path/to/commitlog
   saved_caches_directory: /path/to/saved_caches
   hints_directory: /path/to/hints

6. The tarball installation default logs directory is ``installation_location/logs``.

OPTIONAL: To change the location of the logs, update the ``CASSANDRA_LOG_DIR`` variable in this section of
the script to the path to the logs directory:

::

   if [ -z "$CASSANDRA_LOG_DIR" ]; then
     CASSANDRA_LOG_DIR=$CASSANDRA_HOME/logs
   fi

7. Start Cassandra:

::

   $ bin/cassandra

You can monitor the progress of the startup with:

::

   $ tail -f logs/system.log

Cassandra is ready when you see an entry like this in the system.log:

::

   INFO  [main] 2019-12-17 03:03:37,526 Server.java:156 - Starting listening for CQL clients on /x.x.x.x:9042 (unencrypted)...

8. Check the status of Cassandra:

::

   $ bin/nodetool status

The status column in the output should report UN which stands for "Up/Normal".

Alternatively, connect to the database with:

::

   $ bin/cqlsh <private_IP>

Installation from Debian packages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Add the Apache repository of Cassandra to ``/etc/apt/sources.list.d/cassandra.sources.list``, for example for version
  3.6:

::

    echo "deb https://downloads.apache.org/cassandra/debian 36x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list

- Add the Apache Cassandra repository keys:

::

    curl https://downloads.apache.org/cassandra/KEYS | sudo apt-key add -

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
full list of Apache contributors public keys, you can refer to `this link <https://downloads.apache.org/cassandra/KEYS>`__.

- Install Cassandra:

::

    sudo apt-get install cassandra

- You can start Cassandra with ``sudo service cassandra start`` and stop it with ``sudo service cassandra stop``.
  However, normally the service will start automatically. For this reason be sure to stop it if you need to make any
  configuration changes.
- Verify that Cassandra is running by invoking ``nodetool status`` from the command line.
- The default location of configuration files is ``/etc/cassandra``.
- The default location of log and data directories is ``/var/log/cassandra/`` and ``/var/lib/cassandra``.
