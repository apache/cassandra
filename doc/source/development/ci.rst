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

Jenkins CI Environment
**********************

About CI testing and Apache Cassandra
=====================================

Cassandra can be automatically tested using various test suites, that are either implemented based on JUnit or the `dtest <https://github.com/riptano/cassandra-dtest>`_ scripts written in Python. As outlined in :doc:`testing`, each kind of test suite addresses a different way how to test Cassandra. But in the end, all of them will be executed together on our CI platform at `builds.apache.org <https://builds.apache.org>`_, running `Jenkins <http://jenkins-ci.org>`_.



Setting up your own Jenkins server
==================================

Jenkins is an open source solution that can be installed on a large number of platforms. Setting up a custom Jenkins instance for Cassandra may be desirable for users who have hardware to spare, or organizations that want to run Cassandra tests for custom patches before contribution.

Please refer to the Jenkins download and documentation pages for details on how to get Jenkins running, possibly also including slave build executor instances. The rest of the document will focus on how to setup Cassandra jobs in your Jenkins environment.

Required plugins
----------------

The following plugins need to be installed additionally to the standard plugins (git, ant, ..).

You can install any missing plugins through the install manager.

Go to ``Manage Jenkins -> Manage Plugins -> Available`` and install the following plugins and respective dependencies:

* Job DSL
* Javadoc Plugin
* description setter plugin
* Throttle Concurrent Builds Plug-in
* Test stability history
* Hudson Post build task


Configure Throttle Category
---------------------------

Builds that are not containerized (e.g. cqlshlib tests and in-jvm dtests) use local resources for Cassandra (ccm). To prevent these builds running concurrently the ``Cassandra`` throttle category needs to be created.

This is done under ``Manage Jenkins -> System Configuration -> Throttle Concurrent Builds``. Enter "Cassandra" for the ``Category Name`` and "1" for ``Maximum Concurrent Builds Per Node``.

Setup seed job
--------------

Config ``New Item``

* Name it ``Cassandra-Job-DSL``
* Select ``Freestyle project``

Under ``Source Code Management`` select Git using the repository: ``https://github.com/apache/cassandra-builds``

Under ``Build``, confirm ``Add build step`` -> ``Process Job DSLs`` and enter at ``Look on Filesystem``: ``jenkins-dsl/cassandra_job_dsl_seed.groovy``

Generated jobs will be created based on the Groovy script's default settings. You may want to override settings by checking ``This project is parameterized`` and add ``String Parameter`` for on the variables that can be found in the top of the script. This will allow you to setup jobs for your own repository and branches (e.g. working branches).

**When done, confirm "Save"**

You should now find a new entry with the given name in your project list. However, building the project will still fail and abort with an error message `"Processing DSL script cassandra_job_dsl_seed.groovy ERROR: script not yet approved for use"`. Goto ``Manage Jenkins`` -> ``In-process Script Approval`` to fix this issue. Afterwards you should be able to run the script and have it generate numerous new jobs based on the found branches and configured templates.

Jobs are triggered by either changes in Git or are scheduled to execute periodically, e.g. on daily basis. Jenkins will use any available executor with the label "cassandra", once the job is to be run. Please make sure to make any executors available by selecting ``Build Executor Status`` -> ``Configure`` -> Add "``cassandra``" as label and save.

Executors need to have "JDK 1.8 (latest)" installed. This is done under ``Manage Jenkins -> Global Tool Configuration -> JDK Installations…``. Executors also need to have the virtualenv package installed on their system.

