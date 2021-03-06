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

sstablescrub
------------

Fix a broken sstable. The scrub process rewrites the sstable, skipping any corrupted rows. Because these rows are lost, follow this process with a repair.

ref: https://issues.apache.org/jira/browse/CASSANDRA-4321

Cassandra must be stopped before this tool is executed, or unexpected results will occur. Note: the script does not verify that Cassandra is stopped.

Usage
^^^^^
sstablescrub <options> <keyspace> <table>

===================================     ================================================================================
--debug                                 display stack traces
-e,--header-fix <arg>                   Option whether and how to perform a check of the sstable serialization-headers and fix , fixable issues.
                                        Possible argument values:
                                         - validate-only: validate the serialization-headers, but do not fix those. Do not continue with scrub - i.e. only validate the header (dry-run of fix-only).
                                         - validate: (default) validate the serialization-headers, but do not fix those and only continue with scrub if no error were detected.
                                         - fix-only: validate and fix the serialization-headers, don't continue with scrub.
                                         - fix: validate and fix the serialization-headers, do not fix and do not continue with scrub if the serialization-header check encountered errors.
                                         - off: don't perform the serialization-header checks.
-h,--help                               display this help message
-m,--manifest-check                     only check and repair the leveled manifest, without actually scrubbing the sstables
-n,--no-validate                        do not validate columns using column validator
-r,--reinsert-overflowed-ttl            Rewrites rows with overflowed expiration date affected by CASSANDRA-14092 
                                        with the maximum supported expiration date of 2038-01-19T03:14:06+00:00. The rows are rewritten with the original timestamp incremented by one millisecond to override/supersede any potential tombstone that may have been generated during compaction of the affected rows.
-s,--skip-corrupted                     skip corrupt rows in counter tables
-v,--verbose                            verbose output
===================================     ================================================================================

Basic Scrub
^^^^^^^^^^^

The scrub without options will do a snapshot first, then write all non-corrupted files to a new sstable.

Example::

    sstablescrub keyspace1 standard1
    Pre-scrub sstables snapshotted into snapshot pre-scrub-1534424070883
    Scrubbing BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-6365332094dd11e88f324f9c503e4753/mc-5-big-Data.db') (17.142MiB)
    Scrub of BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-6365332094dd11e88f324f9c503e4753/mc-5-big-Data.db') complete: 73367 rows in new sstable and 0 empty (tombstoned) rows dropped
    Checking leveled manifest

Scrub without Validation
^^^^^^^^^^^^^^^^^^^^^^^^
ref: https://issues.apache.org/jira/browse/CASSANDRA-9406

Use the --no-validate option to retain data that may be misrepresented (e.g., an integer stored in a long field) but not corrupt. This data usually doesn not present any errors to the client.

Example::

    sstablescrub --no-validate keyspace1 standard1
    Pre-scrub sstables snapshotted into snapshot pre-scrub-1536243158517
    Scrubbing BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-bc9cf530b1da11e886c66d2c86545d91/mc-2-big-Data.db') (4.482MiB)
    Scrub of BigTableReader(path='/var/lib/cassandra/data/keyspace1/standard1-bc9cf530b1da11e886c66d2c86545d91/mc-2-big-Data.db') complete; looks like all 0 rows were tombstoned

Skip Corrupted Counter Tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

ref: https://issues.apache.org/jira/browse/CASSANDRA-5930

If counter tables are corrupted in a way that prevents sstablescrub from completing, you can use the --skip-corrupted option to skip scrubbing those counter tables. This workaround is not necessary in versions 2.0+.

Example::

    sstablescrub --skip-corrupted keyspace1 counter1

Dealing with Overflow Dates
^^^^^^^^^^^^^^^^^^^^^^^^^^^

ref: https://issues.apache.org/jira/browse/CASSANDRA-14092

Using the option --reinsert-overflowed-ttl allows a rewriting of rows that had a max TTL going over the maximum (causing an overflow).

Example::

    sstablescrub --reinsert-overflowed-ttl keyspace1 counter1

Manifest Check
^^^^^^^^^^^^^^

As of Cassandra version 2.0, this option is no longer relevant, since level data was moved from a separate manifest into the sstable metadata.

