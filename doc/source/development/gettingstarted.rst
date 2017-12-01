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
..  _gettingstarted:

Getting Started
*************************

Initial Contributions
========================

Writing a new feature is just one way to contribute to the Cassandra project.  In fact, making sure that supporting tasks, such as QA, documentation and helping users, keep up with the development of new features is an ongoing challenge for the project (and most open source projects). So, before firing up your IDE to create that new feature, we'd suggest you consider some of the following activities as a way of introducing yourself to the project and getting to know how things work.
 * Add to or update the documentation
 * Answer questions on the user list
 * Review and test a submitted patch
 * Investigate and fix a reported bug
 * Create unit tests and d-tests

Updating documentation
========================

The Cassandra documentation is maintained in the Cassandra source repository along with the Cassandra code base. To submit changes to the documentation, follow the standard process for submitting a patch (:ref:`patches`).

Answering questions on the user list
====================================

Subscribe to the user list, look out for some questions you know the answer to and reply with an answer. Simple as that!
See the `community <http://cassandra.apache.org/community/>`_ page for details on how to subscribe to the mailing list.

Reviewing and testing a submitted patch
=======================================

Reviewing patches is not the sole domain of committers, if others have reviewed a patch it can reduce the load on the committers allowing them to write more great features or review more patches. Follow the instructions in :ref:`_development_how_to_review` or create a build with the patch and test it with your own workload. Add a comment to the JIRA ticket to let others know what you have done and the results of your work. (For example, "I tested this performance enhacement on our application's standard production load test and found a 3% improvement.")

Investigate and/or fix a reported bug
=====================================

Often, the hardest work in fixing a bug is reproducing it. Even if you don't have the knowledge to produce a fix, figuring out a way to reliable reproduce an issues can be a massive contribution to getting a bug fixed. Document your method of reproduction in a JIRA comment or, better yet, produce an automated test that reproduces the issue and attach it to the ticket. If you go as far as producing a fix, follow the process for submitting a patch (:ref:`patches`).

Create unit tests and Dtests
============================

Test coverage in Cassandra is improving but, as with most code bases, it could benefit from more automated test coverage. Before starting work in an area, consider reviewing and enhancing the existing test coverage. This will both improve your knowledge of the code before you start on an enhancement and reduce the chances of your change in introducing new issues. See :ref:`testing` and :ref:`patches` for more detail.



