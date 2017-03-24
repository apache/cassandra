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

How-to Commit
=============

If you are a committer, feel free to pick any process that works for you - so long as you are planning to commit the work yourself.

Here is how committing and merging will usually look for merging and pushing for tickets that follow the convention (if patch-based):

Hypothetical CASSANDRA-12345 ticket is a cassandra-3.0 based bug fix that requires different code for cassandra-3.3, and trunk. Contributor Jackie supplied a patch for the root branch (12345-3.0.patch), and patches for the remaining branches (12345-3.3.patch, 12345-trunk.patch).

On cassandra-3.0:
   #. ``git am -3 12345-3.0.patch`` (if we have a problem b/c of CHANGES.txt not merging anymore, we fix  it ourselves, in place)

On cassandra-3.3:
   #. ``git merge cassandra-3.0 -s ours``
   #. ``git apply -3 12345-3.3.patch`` (likely to have an issue with CHANGES.txt here: fix it ourselves, then git add CHANGES.txt)
   #. ``git commit -amend``

On trunk:
   #. ``git merge cassandra-3.3 -s ours``
   #. ``git apply -3 12345-trunk.patch`` (likely to have an issue with CHANGES.txt here: fix it ourselves, then git add CHANGES.txt)
   #. ``git commit -amend``

On any branch:
   #. ``git push origin cassandra-3.0 cassandra-3.3 trunk -atomic``

Same scenario, but a branch-based contribution:

On cassandra-3.0:
   #. ``git cherry-pick <sha-of-3.0-commit>`` (if we have a problem b/c of CHANGES.txt not merging anymore, we fix it ourselves, in place)

On cassandra-3.3:
   #. ``git merge cassandra-3.0 -s ours``
   #. ``git format-patch -1 <sha-of-3.3-commit>``
   #. ``git apply -3 <sha-of-3.3-commit>.patch`` (likely to have an issue with CHANGES.txt here: fix it ourselves, then git add CHANGES.txt)
   #. ``git commit -amend``

On trunk:
   #. ``git merge cassandra-3.3 -s ours``
   #. ``git format-patch -1 <sha-of-trunk-commit>``
   #. ``git apply -3 <sha-of-trunk-commit>.patch`` (likely to have an issue with CHANGES.txt here: fix it ourselves, then git add CHANGES.txt)
   #. ``git commit -amend``

On any branch:
   #. ``git push origin cassandra-3.0 cassandra-3.3 trunk -atomic``

.. tip::

   Notes on git flags:
   ``-3`` flag to am and apply will instruct git to perform a 3-way merge for you. If a conflict is detected, you can either resolve it manually or invoke git mergetool - for both am and apply.

   ``-atomic`` flag to git push does the obvious thing: pushes all or nothing. Without the flag, the command is equivalent to running git push once per each branch. This is nifty in case a race condition happens - you won’t push half the branches, blocking other committers’ progress while you are resolving the issue.

.. tip::

   The fastest way to get a patch from someone’s commit in a branch on GH - if you don’t have their repo in remotes -  is to append .patch to the commit url, e.g.
   curl -O https://github.com/apache/cassandra/commit/7374e9b5ab08c1f1e612bf72293ea14c959b0c3c.patch

