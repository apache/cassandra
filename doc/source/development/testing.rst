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

Testing
*******

Creating tests is one of the most important and also most difficult parts of developing Cassandra. There are different ways to test your code depending on what you're working on.


Unit Testing
============

The most simple way to test code in Cassandra is probably by writing a unit test. Cassandra uses JUnit as a testing framework and test cases can be found in the ``test/unit`` directory. Ideally you’d be able to create a unit test for your implementation that would exclusively cover the class you created (the unit under test). Unfortunately this is not always possible and Cassandra doesn’t have a very mock friendly code base. Often you’ll find yourself in a situation where you have to make use of an embedded Cassandra instance that you’ll be able to interact with in your test. If you want to make use of CQL in your test, you can simply extend CQLTester and use some of the convenient helper methods such as in the following example.

.. code-block:: java

  @Test
  public void testBatchAndList() throws Throwable
  {
     createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<int>)");
     execute("BEGIN BATCH " +
             "UPDATE %1$s SET l = l +[ 1 ] WHERE k = 0; " +
             "UPDATE %1$s SET l = l + [ 2 ] WHERE k = 0; " +
             "UPDATE %1$s SET l = l + [ 3 ] WHERE k = 0; " +
             "APPLY BATCH");

     assertRows(execute("SELECT l FROM %s WHERE k = 0"),
                row(list(1, 2, 3)));
  }

Unit tests can be run from the command line using the ``ant test`` command, ``ant test -Dtest.name=<simple_classname>`` to execute a test suite or ``ant testsome -Dtest.name=<FQCN> -Dtest.methods=<testmethod1>[,testmethod2]`` for individual tests.  For example, to run all test methods in the ``org.apache.cassandra.cql3.SimpleQueryTest`` class, you would run::

    ant test -Dtest.name=SimpleQueryTest

To run only the ``testStaticCompactTables()`` test method from that class, you would run::

    ant testsome -Dtest.name=org.apache.cassandra.cql3.SimpleQueryTest -Dtest.methods=testStaticCompactTables

Long running tests
------------------

Test that consume a significant amount of time during execution can be found in the ``test/long`` directory and executed as a regular JUnit test or standalone program. Except for the execution time, there’s nothing really special about them. However, ant will execute tests under ``test/long`` only when using the ``ant long-test`` target.

DTests
======

One way of doing integration or system testing at larger scale is by using `dtest <https://github.com/riptano/cassandra-dtest>`_, which stands for “Cassandra Distributed Tests”. The idea is to automatically setup Cassandra clusters using various configurations and simulate certain use cases you want to test. This is done using Python scripts and ``ccmlib`` from the `ccm <https://github.com/pcmanus/ccm>`_ project. Dtests will setup clusters using this library just as you do running ad-hoc ``ccm`` commands on your local machine. Afterwards dtests will use the `Python driver <http://datastax.github.io/python-driver/installation.html>`_ to interact with the nodes, manipulate the file system, analyze logs or mess with individual nodes.

Using dtests helps us to prevent regression bugs by continually executing tests on the `CI server <http://cassci.datastax.com/>`_ against new patches. For frequent contributors, this Jenkins is set up to build branches from their GitHub repositories. It is likely that your reviewer will use this Jenkins instance to run tests for your patch. Read more on the motivation behind the CI server `here <http://www.datastax.com/dev/blog/cassandra-testing-improvements-for-developer-convenience-and-confidence>`_.

The best way to learn how to write dtests is probably by reading the introduction "`How to Write a Dtest <http://www.datastax.com/dev/blog/how-to-write-a-dtest>`_" and by looking at existing, recently updated tests in the project. New tests must follow certain `style conventions <https://github.com/riptano/cassandra-dtest/blob/master/CONTRIBUTING.md>`_ that are being checked before accepting contributions. In contrast to Cassandra, dtest issues and pull-requests are managed on github, therefor you should make sure to link any created dtests in your Cassandra ticket and also refer to the ticket number in your dtest PR.

Creating a good dtest can be tough, but it should not prevent you from submitting patches! Please ask in the corresponding JIRA ticket how to write a good dtest for the patch. In most cases a reviewer or committer will able to support you, and in some cases they may offer to write a dtest for you.

Performance Testing
===================

Performance tests for Cassandra are a special breed of tests that are not part of the usual patch contribution process. In fact you can contribute tons of patches to Cassandra without ever running performance tests. They are important however when working on performance improvements, as such improvements must be measurable.

Cassandra Stress Tool
---------------------

TODO: `CASSANDRA-12365 <https://issues.apache.org/jira/browse/CASSANDRA-12365>`_

cstar_perf
----------

Another tool available on github is `cstar_perf <https://github.com/datastax/cstar_perf>`_ that can be used for intensive performance testing in large clusters or locally. Please refer to the project page on how to set it up and how to use it.



