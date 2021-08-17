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

.. _reading-logs:

Cassandra Logs
==============
Cassandra has rich support for logging and attempts to give operators maximum
insight into the database while at the same time limiting noise to the logs.

Common Log Files
----------------
Cassandra has three main logs, the ``system.log``, ``debug.log`` and
``gc.log`` which hold general logging messages, debugging logging messages, and
java garbage collection logs respectively.

These logs by default live in ``${CASSANDRA_HOME}/logs``, but most Linux
distributions relocate logs to ``/var/log/cassandra``. Operators can tune
this location as well as what levels are logged using the provided
``logback.xml`` file.

``system.log``
^^^^^^^^^^^^^^
This log is the default Cassandra log and is a good place to start any
investigation. Some examples of activities logged to this log:

* Uncaught exceptions. These can be very useful for debugging errors.
* ``GCInspector`` messages indicating long garbage collector pauses. When long
  pauses happen Cassandra will print how long and also what was the state of
  the system (thread state) at the time of that pause. This can help narrow
  down a capacity issue (either not enough heap or not enough spare CPU).
* Information about nodes joining and leaving the cluster as well as token
  metadata (data ownersip) changes. This is useful for debugging network
  partitions, data movements, and more.
* Keyspace/Table creation, modification, deletion.
* ``StartupChecks`` that ensure optimal configuration of the operating system
  to run Cassandra
* Information about some background operational tasks (e.g. Index
  Redistribution).

As with any application, looking for ``ERROR`` or ``WARN`` lines can be a
great first step::

    $ # Search for warnings or errors in the latest system.log
    $ grep 'WARN\|ERROR' system.log | tail
    ...

    $ # Search for warnings or errors in all rotated system.log
    $ zgrep 'WARN\|ERROR' system.log.* | less
    ...

``debug.log``
^^^^^^^^^^^^^^
This log contains additional debugging information that may be useful when
troubleshooting but may be much noiser than the normal ``system.log``. Some
examples of activities logged to this log:

* Information about compactions, including when they start, which sstables
  they contain, and when they finish.
* Information about memtable flushes to disk, including when they happened,
  how large the flushes were, and which commitlog segments the flush impacted.

This log can be *very* noisy, so it is highly recommended to use ``grep`` and
other log analysis tools to dive deep. For example::

    $ # Search for messages involving a CompactionTask with 5 lines of context
    $ grep CompactionTask debug.log -C 5
    ...

    $ # Look at the distribution of flush tasks per keyspace
    $ grep "Enqueuing flush" debug.log | cut -f 10 -d ' ' | sort | uniq -c
        6 compaction_history:
        1 test_keyspace:
        2 local:
        17 size_estimates:
        17 sstable_activity:


``gc.log``
^^^^^^^^^^^^^^
The gc log is a standard Java GC log. With the default ``jvm.options``
settings you get a lot of valuable information in this log such as
application pause times, and why pauses happened. This may help narrow
down throughput or latency issues to a mistuned JVM. For example you can
view the last few pauses::

    $ grep stopped gc.log.0.current | tail
    2018-08-29T00:19:39.522+0000: 3022663.591: Total time for which application threads were stopped: 0.0332813 seconds, Stopping threads took: 0.0008189 seconds
    2018-08-29T00:19:44.369+0000: 3022668.438: Total time for which application threads were stopped: 0.0312507 seconds, Stopping threads took: 0.0007025 seconds
    2018-08-29T00:19:49.796+0000: 3022673.865: Total time for which application threads were stopped: 0.0307071 seconds, Stopping threads took: 0.0006662 seconds
    2018-08-29T00:19:55.452+0000: 3022679.521: Total time for which application threads were stopped: 0.0309578 seconds, Stopping threads took: 0.0006832 seconds
    2018-08-29T00:20:00.127+0000: 3022684.197: Total time for which application threads were stopped: 0.0310082 seconds, Stopping threads took: 0.0007090 seconds
    2018-08-29T00:20:06.583+0000: 3022690.653: Total time for which application threads were stopped: 0.0317346 seconds, Stopping threads took: 0.0007106 seconds
    2018-08-29T00:20:10.079+0000: 3022694.148: Total time for which application threads were stopped: 0.0299036 seconds, Stopping threads took: 0.0006889 seconds
    2018-08-29T00:20:15.739+0000: 3022699.809: Total time for which application threads were stopped: 0.0078283 seconds, Stopping threads took: 0.0006012 seconds
    2018-08-29T00:20:15.770+0000: 3022699.839: Total time for which application threads were stopped: 0.0301285 seconds, Stopping threads took: 0.0003789 seconds
    2018-08-29T00:20:15.798+0000: 3022699.867: Total time for which application threads were stopped: 0.0279407 seconds, Stopping threads took: 0.0003627 seconds


This shows a lot of valuable information including how long the application
was paused (meaning zero user queries were being serviced during the e.g. 33ms
JVM pause) as well as how long it took to enter the safepoint. You can use this
raw data to e.g. get the longest pauses::

    $ grep stopped gc.log.0.current | cut -f 11 -d ' ' | sort -n  | tail | xargs -IX grep X gc.log.0.current | sort -k 1
    2018-08-28T17:13:40.520-0700: 1.193: Total time for which application threads were stopped: 0.0157914 seconds, Stopping threads took: 0.0000355 seconds
    2018-08-28T17:13:41.206-0700: 1.879: Total time for which application threads were stopped: 0.0249811 seconds, Stopping threads took: 0.0000318 seconds
    2018-08-28T17:13:41.638-0700: 2.311: Total time for which application threads were stopped: 0.0561130 seconds, Stopping threads took: 0.0000328 seconds
    2018-08-28T17:13:41.677-0700: 2.350: Total time for which application threads were stopped: 0.0362129 seconds, Stopping threads took: 0.0000597 seconds
    2018-08-28T17:13:41.781-0700: 2.454: Total time for which application threads were stopped: 0.0442846 seconds, Stopping threads took: 0.0000238 seconds
    2018-08-28T17:13:41.976-0700: 2.649: Total time for which application threads were stopped: 0.0377115 seconds, Stopping threads took: 0.0000250 seconds
    2018-08-28T17:13:42.172-0700: 2.845: Total time for which application threads were stopped: 0.0475415 seconds, Stopping threads took: 0.0001018 seconds
    2018-08-28T17:13:42.825-0700: 3.498: Total time for which application threads were stopped: 0.0379155 seconds, Stopping threads took: 0.0000571 seconds
    2018-08-28T17:13:43.574-0700: 4.247: Total time for which application threads were stopped: 0.0323812 seconds, Stopping threads took: 0.0000574 seconds
    2018-08-28T17:13:44.602-0700: 5.275: Total time for which application threads were stopped: 0.0238975 seconds, Stopping threads took: 0.0000788 seconds

In this case any client waiting on a query would have experienced a `56ms`
latency at 17:13:41.

Note that GC pauses are not _only_ garbage collection, although
generally speaking high pauses with fast safepoints indicate a lack of JVM heap
or mistuned JVM GC algorithm. High pauses with slow safepoints typically
indicate that the JVM is having trouble entering a safepoint which usually
indicates slow disk drives (Cassandra makes heavy use of memory mapped reads
which the JVM doesn't know could have disk latency, so the JVM safepoint logic
doesn't handle a blocking memory mapped read particularly well).

Using these logs you can even get a pause distribution with something like
`histogram.py <https://github.com/bitly/data_hacks/blob/master/data_hacks/histogram.py>`_::

    $ grep stopped gc.log.0.current | cut -f 11 -d ' ' | sort -n | histogram.py
    # NumSamples = 410293; Min = 0.00; Max = 11.49
    # Mean = 0.035346; Variance = 0.002216; SD = 0.047078; Median 0.036498
    # each ∎ represents a count of 5470
        0.0001 -     1.1496 [410255]: ∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
        1.1496 -     2.2991 [    15]:
        2.2991 -     3.4486 [     5]:
        3.4486 -     4.5981 [     1]:
        4.5981 -     5.7475 [     5]:
        5.7475 -     6.8970 [     9]:
        6.8970 -     8.0465 [     1]:
        8.0465 -     9.1960 [     0]:
        9.1960 -    10.3455 [     0]:
       10.3455 -    11.4949 [     2]:

We can see in this case while we have very good average performance something
is causing multi second JVM pauses ... In this case it was mostly safepoint
pauses caused by slow disks::

    $ grep stopped gc.log.0.current | cut -f 11 -d ' ' | sort -n | tail | xargs -IX grep X  gc.log.0.current| sort -k 1
    2018-07-27T04:52:27.413+0000: 187831.482: Total time for which application threads were stopped: 6.5037022 seconds, Stopping threads took: 0.0005212 seconds
    2018-07-30T23:38:18.354+0000: 514582.423: Total time for which application threads were stopped: 6.3262938 seconds, Stopping threads took: 0.0004882 seconds
    2018-08-01T02:37:48.380+0000: 611752.450: Total time for which application threads were stopped: 10.3879659 seconds, Stopping threads took: 0.0004475 seconds
    2018-08-06T22:04:14.990+0000: 1113739.059: Total time for which application threads were stopped: 6.0917409 seconds, Stopping threads took: 0.0005553 seconds
    2018-08-14T00:04:06.091+0000: 1725730.160: Total time for which application threads were stopped: 6.0141054 seconds, Stopping threads took: 0.0004976 seconds
    2018-08-17T06:23:06.755+0000: 2007670.824: Total time for which application threads were stopped: 6.0133694 seconds, Stopping threads took: 0.0006011 seconds
    2018-08-23T06:35:46.068+0000: 2526830.137: Total time for which application threads were stopped: 6.4767751 seconds, Stopping threads took: 6.4426849 seconds
    2018-08-23T06:36:29.018+0000: 2526873.087: Total time for which application threads were stopped: 11.4949489 seconds, Stopping threads took: 11.4638297 seconds
    2018-08-23T06:37:12.671+0000: 2526916.741: Total time for which application threads were stopped: 6.3867003 seconds, Stopping threads took: 6.3507166 seconds
    2018-08-23T06:37:47.156+0000: 2526951.225: Total time for which application threads were stopped: 7.9528200 seconds, Stopping threads took: 7.9197756 seconds

Sometimes reading and understanding java GC logs is hard, but you can take the
raw GC files and visualize them using tools such as `GCViewer
<https://github.com/chewiebug/GCViewer>`_ which take the Cassandra GC log as
input and show you detailed visual information on your garbage collection
performance. This includes pause analysis as well as throughput information.
For a stable Cassandra JVM you probably want to aim for pauses less than
`200ms` and GC throughput greater than `99%` (ymmv).

Java GC pauses are one of the leading causes of tail latency in Cassandra
(along with drive latency) so sometimes this information can be crucial
while debugging tail latency issues.


Getting More Information
------------------------

If the default logging levels are insuficient, ``nodetool`` can set higher
or lower logging levels for various packages and classes using the
``nodetool setlogginglevel`` command. Start by viewing the current levels::

    $ nodetool getlogginglevels

    Logger Name                                        Log Level
    ROOT                                                    INFO
    org.apache.cassandra                                   DEBUG

Perhaps the ``Gossiper`` is acting up and we wish to enable it at ``TRACE``
level for even more insight::


    $ nodetool setlogginglevel org.apache.cassandra.gms.Gossiper TRACE

    $ nodetool getlogginglevels

    Logger Name                                        Log Level
    ROOT                                                    INFO
    org.apache.cassandra                                   DEBUG
    org.apache.cassandra.gms.Gossiper                      TRACE

    $ grep TRACE debug.log | tail -2
    TRACE [GossipStage:1] 2018-07-04 17:07:47,879 Gossiper.java:1234 - Updating
    heartbeat state version to 2344 from 2343 for 127.0.0.2:7000 ...
    TRACE [GossipStage:1] 2018-07-04 17:07:47,879 Gossiper.java:923 - local
    heartbeat version 2341 greater than 2340 for 127.0.0.1:7000


Note that any changes made this way are reverted on next Cassandra process
restart. To make the changes permanent add the appropriate rule to
``logback.xml``.

.. code-block:: diff

	diff --git a/conf/logback.xml b/conf/logback.xml
	index b2c5b10..71b0a49 100644
	--- a/conf/logback.xml
	+++ b/conf/logback.xml
	@@ -98,4 +98,5 @@ appender reference in the root level section below.
	   </root>

	   <logger name="org.apache.cassandra" level="DEBUG"/>
	+  <logger name="org.apache.cassandra.gms.Gossiper" level="TRACE"/>
	 </configuration>

Full Query Logger
^^^^^^^^^^^^^^^^^

Cassandra 4.0 additionally ships with support for full query logging. This
is a highly performant binary logging tool which captures Cassandra queries
in real time, writes them (if possible) to a log file, and ensures the total
size of the capture does not exceed a particular limit. FQL is enabled with
``nodetool`` and the logs are read with the provided ``bin/fqltool`` utility::

    $ mkdir /var/tmp/fql_logs
    $ nodetool enablefullquerylog --path /var/tmp/fql_logs

    # ... do some querying

    $ bin/fqltool dump /var/tmp/fql_logs/20180705-00.cq4 | tail
    Query time: 1530750927224
    Query: SELECT * FROM system_virtual_schema.columns WHERE keyspace_name =
    'system_views' AND table_name = 'sstable_tasks';
    Values:

    Type: single
    Protocol version: 4
    Query time: 1530750934072
    Query: select * from keyspace1.standard1 ;
    Values:

    $ nodetool disablefullquerylog

Note that if you want more information than this tool provides, there are other
live capture options available such as :ref:`packet capture <packet-capture>`.
