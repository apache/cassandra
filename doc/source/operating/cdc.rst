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

Change Data Capture
-------------------

Overview
^^^^^^^^

Change data capture (CDC) provides a mechanism to flag specific tables for archival as well as rejecting writes to those
tables once a configurable size-on-disk for the combined flushed and unflushed CDC-log is reached. An operator can
enable CDC on a table by setting the table property ``cdc=true`` (either when :ref:`creating the table
<create-table-statement>` or :ref:`altering it <alter-table-statement>`), after which any CommitLogSegments containing
data for a CDC-enabled table are moved to the directory specified in ``cassandra.yaml`` on segment discard. A threshold
of total disk space allowed is specified in the yaml at which time newly allocated CommitLogSegments will not allow CDC
data until a consumer parses and removes data from the destination archival directory.

Configuration
^^^^^^^^^^^^^

Enabling or disable CDC on a table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CDC is enable or disable through the `cdc` table property, for instance::

    CREATE TABLE foo (a int, b text, PRIMARY KEY(a)) WITH cdc=true;

    ALTER TABLE foo WITH cdc=true;

    ALTER TABLE foo WITH cdc=false;

cassandra.yaml parameters
~~~~~~~~~~~~~~~~~~~~~~~~~

The following `cassandra.yaml` are available for CDC:

``cdc_enabled`` (default: false)
   Enable or disable CDC operations node-wide.
``cdc_raw_directory`` (default: ``$CASSANDRA_HOME/data/cdc_raw``)
   Destination for CommitLogSegments to be moved after all corresponding memtables are flushed.
``cdc_free_space_in_mb``: (default: min of 4096 and 1/8th volume space)
   Calculated as sum of all active CommitLogSegments that permit CDC + all flushed CDC segments in
   ``cdc_raw_directory``.
``cdc_free_space_check_interval_ms`` (default: 250)
   When at capacity, we limit the frequency with which we re-calculate the space taken up by ``cdc_raw_directory`` to
   prevent burning CPU cycles unnecessarily. Default is to check 4 times per second.

.. _reading-commitlogsegments:

Reading CommitLogSegments
^^^^^^^^^^^^^^^^^^^^^^^^^
This implementation included a refactor of CommitLogReplayer into `CommitLogReader.java
<https://github.com/apache/cassandra/blob/e31e216234c6b57a531cae607e0355666007deb2/src/java/org/apache/cassandra/db/commitlog/CommitLogReader.java>`__.
Usage is `fairly straightforward
<https://github.com/apache/cassandra/blob/e31e216234c6b57a531cae607e0355666007deb2/src/java/org/apache/cassandra/db/commitlog/CommitLogReplayer.java#L132-L140>`__
with a `variety of signatures
<https://github.com/apache/cassandra/blob/e31e216234c6b57a531cae607e0355666007deb2/src/java/org/apache/cassandra/db/commitlog/CommitLogReader.java#L71-L103>`__
available for use. In order to handle mutations read from disk, implement `CommitLogReadHandler
<https://github.com/apache/cassandra/blob/e31e216234c6b57a531cae607e0355666007deb2/src/java/org/apache/cassandra/db/commitlog/CommitLogReadHandler.java>`__.

Warnings
^^^^^^^^

**Do not enable CDC without some kind of consumption process in-place.**

The initial implementation of Change Data Capture does not include a parser (see :ref:`reading-commitlogsegments` above)
so, if CDC is enabled on a node and then on a table, the ``cdc_free_space_in_mb`` will fill up and then writes to
CDC-enabled tables will be rejected unless some consumption process is in place.

Further Reading
^^^^^^^^^^^^^^^

- `Design doc <https://docs.google.com/document/d/1ZxCWYkeZTquxsvf5hdPc0fiUnUHna8POvgt6TIzML4Y/edit>`__
- `JIRA ticket <https://issues.apache.org/jira/browse/CASSANDRA-8844>`__
