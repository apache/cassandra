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

sstabledump
-----------

Dump contents of a given SSTable to standard output in JSON format.

You must supply exactly one sstable. 

Cassandra must be stopped before this tool is executed, or unexpected results will occur. Note: the script does not verify that Cassandra is stopped.

Usage
^^^^^
sstabledump <options> <sstable file path> 

===================================     ================================================================================
-d                                      CQL row per line internal representation
-e                                      Enumerate partition keys only
-k <arg>                                Included partition key(s)
-x <arg>                                Excluded partition key(s)
-t                                      Print raw timestamps instead of iso8601 date strings
-l                                      Output each row as a separate JSON object
===================================     ================================================================================

If necessary, use sstableutil first to find out the sstables used by a table.

Dump entire table
^^^^^^^^^^^^^^^^^

Dump the entire table without any options.

Example::

    sstabledump /var/lib/cassandra/data/keyspace/eventlog-65c429e08c5a11e8939edf4f403979ef/mc-1-big-Data.db > eventlog_dump_2018Jul26

    cat eventlog_dump_2018Jul26
    [
      {
        "partition" : {
          "key" : [ "3578d7de-c60d-4599-aefb-3f22a07b2bc6" ],
          "position" : 0
        },
        "rows" : [
          {
            "type" : "row",
            "position" : 61,
            "liveness_info" : { "tstamp" : "2018-07-20T20:23:08.378711Z" },
            "cells" : [
              { "name" : "event", "value" : "party" },
              { "name" : "insertedtimestamp", "value" : "2018-07-20 20:23:08.384Z" },
              { "name" : "source", "value" : "asdf" }
            ]
          }
        ]
      },
      {
        "partition" : {
          "key" : [ "d18250c0-84fc-4d40-b957-4248dc9d790e" ],
          "position" : 62
        },
        "rows" : [
          {
            "type" : "row",
            "position" : 123,
            "liveness_info" : { "tstamp" : "2018-07-20T20:23:07.783522Z" },
            "cells" : [
              { "name" : "event", "value" : "party" },
              { "name" : "insertedtimestamp", "value" : "2018-07-20 20:23:07.789Z" },
              { "name" : "source", "value" : "asdf" }
            ]
          }
        ]
      },
      {
        "partition" : {
          "key" : [ "cf188983-d85b-48d6-9365-25005289beb2" ],
          "position" : 124
        },
        "rows" : [
          {
            "type" : "row",
            "position" : 182,
            "liveness_info" : { "tstamp" : "2018-07-20T20:22:27.028809Z" },
            "cells" : [
              { "name" : "event", "value" : "party" },
              { "name" : "insertedtimestamp", "value" : "2018-07-20 20:22:27.055Z" },
              { "name" : "source", "value" : "asdf" }
            ]
          }
        ]
      }
    ]

Dump table in a more manageable format
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the -l option to dump each row as a separate JSON object. This will make the output easier to manipulate for large data sets. ref: https://issues.apache.org/jira/browse/CASSANDRA-13848

Example::

    sstabledump /var/lib/cassandra/data/keyspace/eventlog-65c429e08c5a11e8939edf4f403979ef/mc-1-big-Data.db -l > eventlog_dump_2018Jul26_justlines

    cat eventlog_dump_2018Jul26_justlines
    [
      {
        "partition" : {
          "key" : [ "3578d7de-c60d-4599-aefb-3f22a07b2bc6" ],
          "position" : 0
        },
        "rows" : [
          {
            "type" : "row",
            "position" : 61,
            "liveness_info" : { "tstamp" : "2018-07-20T20:23:08.378711Z" },
            "cells" : [
              { "name" : "event", "value" : "party" },
              { "name" : "insertedtimestamp", "value" : "2018-07-20 20:23:08.384Z" },
              { "name" : "source", "value" : "asdf" }
            ]
          }
        ]
      },
      {
        "partition" : {
          "key" : [ "d18250c0-84fc-4d40-b957-4248dc9d790e" ],
          "position" : 62
        },
        "rows" : [
          {
            "type" : "row",
            "position" : 123,
            "liveness_info" : { "tstamp" : "2018-07-20T20:23:07.783522Z" },
            "cells" : [
              { "name" : "event", "value" : "party" },
              { "name" : "insertedtimestamp", "value" : "2018-07-20 20:23:07.789Z" },
              { "name" : "source", "value" : "asdf" }
            ]
          }
        ]
      },
      {
        "partition" : {
          "key" : [ "cf188983-d85b-48d6-9365-25005289beb2" ],
          "position" : 124
        },
        "rows" : [
          {
            "type" : "row",
            "position" : 182,
            "liveness_info" : { "tstamp" : "2018-07-20T20:22:27.028809Z" },
            "cells" : [
              { "name" : "event", "value" : "party" },
              { "name" : "insertedtimestamp", "value" : "2018-07-20 20:22:27.055Z" },
              { "name" : "source", "value" : "asdf" }
            ]
          }
        ]
      }

Dump only keys
^^^^^^^^^^^^^^

Dump only the partition keys by using the -e option.

Example::

    sstabledump /var/lib/cassandra/data/keyspace/eventlog-65c429e08c5a11e8939edf4f403979ef/mc-1-big-Data.db -e > eventlog_dump_2018Jul26_justkeys

    cat eventlog_dump_2018Jul26b
    [ [ "3578d7de-c60d-4599-aefb-3f22a07b2bc6" ], [ "d18250c0-84fc-4d40-b957-4248dc9d790e" ], [ "cf188983-d85b-48d6-9365-25005289beb2" ]

Dump rows with some partition key or keys
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Dump a set of rows identified by their partition keys using the -k option. Multiple keys can be used.

Please note that the -k option should be after the sstable path.

Example::

    sstabledump /var/lib/cassandra/data/keyspace/eventlog-65c429e08c5a11e8939edf4f403979ef/mc-1-big-Data.db -k 3578d7de-c60d-4599-aefb-3f22a07b2bc6 d18250c0-84fc-4d40-b957-4248dc9d790e > eventlog_dump_2018Jul26_singlekey

    cat eventlog_dump_2018Jul26_singlekey
    [
      {
        "partition" : {
          "key" : [ "3578d7de-c60d-4599-aefb-3f22a07b2bc6" ],
          "position" : 0
        },
        "rows" : [
          {
            "type" : "row",
            "position" : 61,
            "liveness_info" : { "tstamp" : "2018-07-20T20:23:08.378711Z" },
            "cells" : [
              { "name" : "event", "value" : "party" },
              { "name" : "insertedtimestamp", "value" : "2018-07-20 20:23:08.384Z" },
              { "name" : "source", "value" : "asdf" }
            ]
          }
        ]
      },
      {
        "partition" : {
          "key" : [ "d18250c0-84fc-4d40-b957-4248dc9d790e" ],
          "position" : 62
        },
        "rows" : [
          {
            "type" : "row",
            "position" : 123,
            "liveness_info" : { "tstamp" : "2018-07-20T20:23:07.783522Z" },
            "cells" : [
              { "name" : "event", "value" : "party" },
              { "name" : "insertedtimestamp", "value" : "2018-07-20 20:23:07.789Z" },
              { "name" : "source", "value" : "asdf" }
            ]
          }
        ]
      }

Exclude a partition key or keys in dump of rows
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Dump a table except for the rows excluded with the -x option. Multiple partition keys can be used.

Please note that the -x option should be after the sstable path.

Example::

    sstabledump /var/lib/cassandra/data/keyspace/eventlog-65c429e08c5a11e8939edf4f403979ef/mc-1-big-Data.db -x 3578d7de-c60d-4599-aefb-3f22a07b2bc6 d18250c0-84fc-4d40-b957-4248dc9d790e  > eventlog_dump_2018Jul26_excludekeys

    cat eventlog_dump_2018Jul26_excludekeys
    [
      {
        "partition" : {
          "key" : [ "cf188983-d85b-48d6-9365-25005289beb2" ],
          "position" : 0
        },
        "rows" : [
          {
            "type" : "row",
            "position" : 182,
            "liveness_info" : { "tstamp" : "2018-07-20T20:22:27.028809Z" },
            "cells" : [
              { "name" : "event", "value" : "party" },
              { "name" : "insertedtimestamp", "value" : "2018-07-20 20:22:27.055Z" },
              { "name" : "source", "value" : "asdf" }
            ]
          }
        ]
      }

Display raw timestamps
^^^^^^^^^^^^^^^^^^^^^^

By default, dates are displayed in iso8601 date format. Using the -t option will dump the data with the raw timestamp.

Example::

    sstabledump /var/lib/cassandra/data/keyspace/eventlog-65c429e08c5a11e8939edf4f403979ef/mc-1-big-Data.db -t -k cf188983-d85b-48d6-9365-25005289beb2 > eventlog_dump_2018Jul26_times

    cat eventlog_dump_2018Jul26_times
    [
      {
        "partition" : {
          "key" : [ "cf188983-d85b-48d6-9365-25005289beb2" ],
          "position" : 124
        },
        "rows" : [
          {
            "type" : "row",
            "position" : 182,
            "liveness_info" : { "tstamp" : "1532118147028809" },
            "cells" : [
              { "name" : "event", "value" : "party" },
              { "name" : "insertedtimestamp", "value" : "2018-07-20 20:22:27.055Z" },
              { "name" : "source", "value" : "asdf" }
            ]
          }
        ]
      }


Display internal structure in output
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Dump the table in a format that reflects the internal structure.

Example::

    sstabledump /var/lib/cassandra/data/keyspace/eventlog-65c429e08c5a11e8939edf4f403979ef/mc-1-big-Data.db -d > eventlog_dump_2018Jul26_d

    cat eventlog_dump_2018Jul26_d
    [3578d7de-c60d-4599-aefb-3f22a07b2bc6]@0 Row[info=[ts=1532118188378711] ]:  | [event=party ts=1532118188378711], [insertedtimestamp=2018-07-20 20:23Z ts=1532118188378711], [source=asdf ts=1532118188378711]
    [d18250c0-84fc-4d40-b957-4248dc9d790e]@62 Row[info=[ts=1532118187783522] ]:  | [event=party ts=1532118187783522], [insertedtimestamp=2018-07-20 20:23Z ts=1532118187783522], [source=asdf ts=1532118187783522]
    [cf188983-d85b-48d6-9365-25005289beb2]@124 Row[info=[ts=1532118147028809] ]:  | [event=party ts=1532118147028809], [insertedtimestamp=2018-07-20 20:22Z ts=1532118147028809], [source=asdf ts=1532118147028809]





