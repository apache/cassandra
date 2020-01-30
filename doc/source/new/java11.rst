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

Support for Java 11
-------------------

In the new Java release cadence a new Java version is made available every six months. The more frequent release cycle 
is favored as it brings new Java features to the developers as and when they are developed without the wait that the 
earlier 3 year release model incurred.  Not every Java version is a Long Term Support (LTS) version. After Java 8 the 
next LTS version is Java 11. Java 9, 10, 12 and 13 are all non-LTS versions. 

One of the objectives of the Apache Cassandra 4.0 version is to support the recent LTS Java versions 8 and 11 (`CASSANDRA-9608
<https://issues.apache.org/jira/browse/CASSANDRA-9608>`_). Java 8 and 
Java 11 may be used to build and run Apache Cassandra 4.0. 

**Note**: Support for JDK 11 in Apache Cassandra 4.0 is an experimental feature, and not recommended for production use.

Support Matrix
^^^^^^^^^^^^^^

The support matrix for the Java versions for compiling and running Apache Cassandra 4.0 is detailed in Table 1. The 
build version is along the vertical axis and the run version is along the horizontal axis.

Table 1 : Support Matrix for Java 

+---------------+--------------+-----------------+
|               | Java 8 (Run) | Java 11 (Run)   | 
+---------------+--------------+-----------------+
| Java 8 (Build)|Supported     |Supported        |           
+---------------+--------------+-----------------+
| Java 11(Build)| Not Supported|Supported        |         
+---------------+--------------+-----------------+  

Essentially Apache 4.0 source code built with Java 11 cannot be run with Java 8. Next, we shall discuss using each of Java 8 and 11 to build and run Apache Cassandra 4.0.

Using Java 8 to Build
^^^^^^^^^^^^^^^^^^^^^

To start with, install Java 8. As an example, for installing Java 8 on RedHat Linux the command is as follows:

::

$ sudo yum install java-1.8.0-openjdk-devel
    
Set ``JAVA_HOME`` and ``JRE_HOME`` environment variables in the shell bash script. First, open the bash script:

::

$ sudo vi ~/.bashrc

Set the environment variables including the ``PATH``.

::

  $ export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
  $ export JRE_HOME=/usr/lib/jvm/java-1.8.0-openjdk/jre
  $ export PATH=$PATH:$JAVA_HOME/bin:$JRE_HOME/bin

Download and install Apache Cassandra 4.0 source code from the Git along with the dependencies.

::

 $ git clone https://github.com/apache/cassandra.git

If Cassandra is already running stop Cassandra with the following command.

::

 [ec2-user@ip-172-30-3-146 bin]$ ./nodetool stopdaemon

Build the source code from the ``cassandra`` directory, which has the ``build.xml`` build script. The Apache Ant uses the Java version set in the ``JAVA_HOME`` environment variable.

::

 $ cd ~/cassandra
 $ ant

Apache Cassandra 4.0 gets built with Java 8.  Set the environment variable for ``CASSANDRA_HOME`` in the bash script. Also add the ``CASSANDRA_HOME/bin`` to the ``PATH`` variable.

::

 $ export CASSANDRA_HOME=~/cassandra
 $ export PATH=$PATH:$JAVA_HOME/bin:$JRE_HOME/bin:$CASSANDRA_HOME/bin

To run Apache Cassandra 4.0 with either of Java 8 or Java 11 run the Cassandra application in the ``CASSANDRA_HOME/bin`` directory, which is in the ``PATH`` env variable.

::

 $ cassandra

The Java version used to run Cassandra gets output as Cassandra is getting started. As an example if Java 11 is used, the run output should include similar to the following output snippet:

::

 INFO  [main] 2019-07-31 21:18:16,862 CassandraDaemon.java:480 - Hostname: ip-172-30-3- 
 146.ec2.internal:7000:7001
 INFO  [main] 2019-07-31 21:18:16,862 CassandraDaemon.java:487 - JVM vendor/version: OpenJDK 
 64-Bit Server VM/11.0.3
 INFO  [main] 2019-07-31 21:18:16,863 CassandraDaemon.java:488 - Heap size: 
 1004.000MiB/1004.000MiB

The following output indicates a single node Cassandra 4.0 cluster has started.

::

 INFO  [main] 2019-07-31 21:18:19,687 InboundConnectionInitiator.java:130 - Listening on 
 address: (127.0.0.1:7000), nic: lo, encryption: enabled (openssl)
 ...
 ...
 INFO  [main] 2019-07-31 21:18:19,850 StorageService.java:512 - Unable to gossip with any 
 peers but continuing anyway since node is in its own seed list
 INFO  [main] 2019-07-31 21:18:19,864 StorageService.java:695 - Loading persisted ring state
 INFO  [main] 2019-07-31 21:18:19,865 StorageService.java:814 - Starting up server gossip
 INFO  [main] 2019-07-31 21:18:20,088 BufferPool.java:216 - Global buffer pool is enabled,  
 when pool is exhausted (max is 251.000MiB) it will allocate on heap
 INFO  [main] 2019-07-31 21:18:20,110 StorageService.java:875 - This node will not auto 
 bootstrap because it is configured to be a seed node.
 ...
 ...
 INFO  [main] 2019-07-31 21:18:20,809 StorageService.java:1507 - JOINING: Finish joining ring
 INFO  [main] 2019-07-31 21:18:20,921 StorageService.java:2508 - Node 127.0.0.1:7000 state 
 jump to NORMAL

Using Java 11 to Build
^^^^^^^^^^^^^^^^^^^^^^
If Java 11 is used to build Apache Cassandra 4.0, first Java 11 must be installed and the environment variables set. As an example, to download and install Java 11 on RedHat Linux run the following command.

::

 $ yum install java-11-openjdk-devel

Set the environment variables in the bash script for Java 11. The first command is to open the bash script.

::

 $ sudo vi ~/.bashrc 
 $ export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
 $ export JRE_HOME=/usr/lib/jvm/java-11-openjdk/jre
 $ export PATH=$PATH:$JAVA_HOME/bin:$JRE_HOME/bin

To build source code with Java 11 one of the following two options must be used.

 1. Include Apache Ant command-line option ``-Duse.jdk=11`` as follows: 
     ::

      $ ant -Duse.jdk=11

 2. Set environment variable ``CASSANDRA_USE_JDK11`` to ``true``: 
     ::

      $ export CASSANDRA_USE_JDK11=true

As an example, set the environment variable ``CASSANDRA_USE_JDK11`` to ``true``.

::

 [ec2-user@ip-172-30-3-146 cassandra]$ export CASSANDRA_USE_JDK11=true
 [ec2-user@ip-172-30-3-146 cassandra]$ ant
 Buildfile: /home/ec2-user/cassandra/build.xml

Or, set the command-line option.

::

 [ec2-user@ip-172-30-3-146 cassandra]$ ant -Duse.jdk11=true

The build output should include the following.

::

 _build_java:
     [echo] Compiling for Java 11
 ...
 ...
 build:

 _main-jar:
          [copy] Copying 1 file to /home/ec2-user/cassandra/build/classes/main/META-INF
      [jar] Building jar: /home/ec2-user/cassandra/build/apache-cassandra-4.0-SNAPSHOT.jar
 ...
 ...
 _build-test:
    [javac] Compiling 739 source files to /home/ec2-user/cassandra/build/test/classes
     [copy] Copying 25 files to /home/ec2-user/cassandra/build/test/classes
 ...
 ...
 jar:
    [mkdir] Created dir: /home/ec2-user/cassandra/build/classes/stress/META-INF
    [mkdir] Created dir: /home/ec2-user/cassandra/build/tools/lib
      [jar] Building jar: /home/ec2-user/cassandra/build/tools/lib/stress.jar
    [mkdir] Created dir: /home/ec2-user/cassandra/build/classes/fqltool/META-INF
      [jar] Building jar: /home/ec2-user/cassandra/build/tools/lib/fqltool.jar

 BUILD SUCCESSFUL
 Total time: 1 minute 3 seconds
 [ec2-user@ip-172-30-3-146 cassandra]$ 

Common Issues
^^^^^^^^^^^^^^
One of the two options mentioned must be used to compile with JDK 11 or the build fails and the following error message is output.

::

 [ec2-user@ip-172-30-3-146 cassandra]$ ant
 Buildfile: /home/ec2-user/cassandra/build.xml
 validate-build-conf:

 BUILD FAILED
 /home/ec2-user/cassandra/build.xml:293: -Duse.jdk11=true or $CASSANDRA_USE_JDK11=true must 
 be set when building from java 11
 Total time: 1 second
 [ec2-user@ip-172-30-3-146 cassandra]$ 

The Java 11 built Apache Cassandra 4.0 source code may be run with Java 11 only. If a Java 11 built code is run with Java 8 the following error message gets output.

::

 [root@localhost ~]# ssh -i cassandra.pem ec2-user@ec2-3-85-85-75.compute-1.amazonaws.com
 Last login: Wed Jul 31 20:47:26 2019 from 75.155.255.51
 [ec2-user@ip-172-30-3-146 ~]$ echo $JAVA_HOME
 /usr/lib/jvm/java-1.8.0-openjdk
 [ec2-user@ip-172-30-3-146 ~]$ cassandra 
 ...
 ...
 Error: A JNI error has occurred, please check your installation and try again
 Exception in thread "main" java.lang.UnsupportedClassVersionError: 
 org/apache/cassandra/service/CassandraDaemon has been compiled by a more recent version of 
 the Java Runtime (class file version 55.0), this version of the Java Runtime only recognizes 
 class file versions up to 52.0
   at java.lang.ClassLoader.defineClass1(Native Method)
   at java.lang.ClassLoader.defineClass(ClassLoader.java:763)
   at ...
 ...

The ``CASSANDRA_USE_JDK11`` variable or the command-line option ``-Duse.jdk11`` cannot be used to build with Java 8. To demonstrate set ``JAVA_HOME`` to version 8.

::

 [root@localhost ~]# ssh -i cassandra.pem ec2-user@ec2-3-85-85-75.compute-1.amazonaws.com
 Last login: Wed Jul 31 21:41:50 2019 from 75.155.255.51
 [ec2-user@ip-172-30-3-146 ~]$ echo $JAVA_HOME
 /usr/lib/jvm/java-1.8.0-openjdk

Set the ``CASSANDRA_USE_JDK11=true`` or command-line option ``-Duse.jdk11=true``. Subsequently, run Apache Ant to start the build. The build fails with error message listed.

::

 [ec2-user@ip-172-30-3-146 ~]$ cd 
 cassandra
 [ec2-user@ip-172-30-3-146 cassandra]$ export CASSANDRA_USE_JDK11=true
 [ec2-user@ip-172-30-3-146 cassandra]$ ant 
 Buildfile: /home/ec2-user/cassandra/build.xml

 validate-build-conf:

 BUILD FAILED
 /home/ec2-user/cassandra/build.xml:285: -Duse.jdk11=true or $CASSANDRA_USE_JDK11=true cannot 
 be set when building from java 8

 Total time: 0 seconds
   
