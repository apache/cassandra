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

Dependency Management
*********************

Managing libraries for Cassandra is a bit less straight forward compared to other projects, as the build process is based on ant, maven and manually managed jars. Make sure to follow the steps below carefully and pay attention to any emerging issues in the :doc:`ci` and reported related issues on Jira/ML, in case of any project dependency changes.

As Cassandra is an Apache product, all included libraries must follow Apache's `software license requirements <https://www.apache.org/legal/resolved.html>`_.

Required steps to add or update libraries
=========================================

* Add or replace jar file in ``lib`` directory
* Add or update ``lib/license`` files
* Update dependencies in ``build.xml``

  * Add to ``parent-pom`` with correct version
  * Add to ``all-pom`` if simple Cassandra dependency (see below)


POM file types
==============

* **parent-pom** - contains all dependencies with the respective version. All other poms will refer to the artifacts with specified versions listed here.
* **build-deps-pom(-sources)** + **coverage-deps-pom** - used by ``ant build`` compile target. Listed dependenices will be resolved and copied to ``build/lib/{jar,sources}`` by executing the ``maven-ant-tasks-retrieve-build`` target. This should contain libraries that are required for build tools (grammar, docs, instrumentation), but are not shipped as part of the Cassandra distribution.
* **test-deps-pom** - refered by ``maven-ant-tasks-retrieve-test`` to retrieve and save dependencies to ``build/test/lib``. Exclusively used during JUnit test execution.
* **all-pom** - pom for `cassandra-all.jar <https://mvnrepository.com/artifact/org.apache.cassandra/cassandra-all>`_ that can be installed or deployed to public maven repos via ``ant publish``


Troubleshooting and conflict resolution
=======================================

Here are some useful commands that may help you out resolving conflicts.

* ``ant realclean`` - gets rid of the build directory, including build artifacts.
* ``mvn dependency:tree -f build/apache-cassandra-*-SNAPSHOT.pom -Dverbose -Dincludes=org.slf4j`` - shows transitive dependency tree for artifacts, e.g. org.slf4j. In case the command above fails due to a missing parent pom file, try running ``ant mvn-install``.
* ``rm ~/.m2/repository/org/apache/cassandra/apache-cassandra/`` - removes cached local Cassandra maven artifacts


