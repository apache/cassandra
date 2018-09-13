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

SSTable Tools
=============

This section describes the functionality of the various sstable tools.

Cassandra must be stopped before these tools are executed, or unexpected results will occur. Note: the scripts do not verify that Cassandra is stopped.

.. toctree::
   :maxdepth: 2

   sstabledump
   sstableexpiredblockers
   sstablelevelreset
   sstableloader
   sstablemetadata
   sstableofflinerelevel
   sstablerepairedset
   sstablescrub
   sstablesplit
   sstableupgrade
   sstableutil
   sstableverify

