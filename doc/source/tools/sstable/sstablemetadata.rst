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

sstablemetadata
---------------

Print information about an sstable from the related Statistics.db and Summary.db files to standard output.

ref: https://issues.apache.org/jira/browse/CASSANDRA-7159 and https://issues.apache.org/jira/browse/CASSANDRA-10838

Cassandra must be stopped before this tool is executed, or unexpected results will occur. Note: the script does not verify that Cassandra is stopped.

Usage
^^^^^

sstablemetadata <options> <sstable filename(s)>

=========================        ================================================================================
-c,--colors                      Use ANSI color sequences
-g,--gc_grace_seconds <arg>      Time to use when calculating droppable tombstones
-s,--scan                        Full sstable scan for additional details. Only available in 3.0+ sstables. Defaults: false
-t,--timestamp_unit <arg>        Time unit that cell timestamps are written with
-u,--unicode                     Use unicode to draw histograms and progress bars
=========================        ================================================================================

Print all the metadata
^^^^^^^^^^^^^^^^^^^^^^

Run sstablemetadata against the *Data.db file(s) related to a table. If necessary, find the *Data.db file(s) using sstableutil.

Example::

    sstableutil keyspace1 standard1 | grep Data
    /var/lib/cassandra/data/keyspace1/standard1-f6845640a6cb11e8b6836d2c86545d91/mc-1-big-Data.db

    sstablemetadata /var/lib/cassandra/data/keyspace1/standard1-f6845640a6cb11e8b6836d2c86545d91/mc-1-big-Data.db

    SSTable: /var/lib/cassandra/data/keyspace1/standard1-f6845640a6cb11e8b6836d2c86545d91/mc-1-big
    Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
    Bloom Filter FP chance: 0.010000
    Minimum timestamp: 1535025576141000
    Maximum timestamp: 1535025604309000
    SSTable min local deletion time: 2147483647
    SSTable max local deletion time: 2147483647
    Compressor: org.apache.cassandra.io.compress.LZ4Compressor
    TTL min: 86400
    TTL max: 86400
    First token: -9223004712949498654 (key=39373333373831303130)
    Last token: 9222554117157811897 (key=4f3438394e39374d3730)
    Estimated droppable tombstones: 0.9188263888888889
    SSTable Level: 0
    Repaired at: 0
    Replay positions covered: {CommitLogPosition(segmentId=1535025390651, position=226400)=CommitLogPosition(segmentId=1535025390651, position=6849139)}
    totalColumnsSet: 100000
    totalRows: 20000
    Estimated tombstone drop times:
    1535039100:     80390
    1535039160:      5645
    1535039220:     13965
    Count               Row Size        Cell Count
    1                          0                 0
    2                          0                 0
    3                          0                 0
    4                          0                 0
    5                          0             20000
    6                          0                 0
    7                          0                 0
    8                          0                 0
    10                         0                 0
    12                         0                 0
    14                         0                 0
    17                         0                 0
    20                         0                 0
    24                         0                 0
    29                         0                 0
    35                         0                 0
    42                         0                 0
    50                         0                 0
    60                         0                 0
    72                         0                 0
    86                         0                 0
    103                        0                 0
    124                        0                 0
    149                        0                 0
    179                        0                 0
    215                        0                 0
    258                    20000                 0
    310                        0                 0
    372                        0                 0
    446                        0                 0
    535                        0                 0
    642                        0                 0
    770                        0                 0
    924                        0                 0
    1109                       0                 0
    1331                       0                 0
    1597                       0                 0
    1916                       0                 0
    2299                       0                 0
    2759                       0                 0
    3311                       0                 0
    3973                       0                 0
    4768                       0                 0
    5722                       0                 0
    6866                       0                 0
    8239                       0                 0
    9887                       0                 0
    11864                      0                 0
    14237                      0                 0
    17084                      0                 0
    20501                      0                 0
    24601                      0                 0
    29521                      0                 0
    35425                      0                 0
    42510                      0                 0
    51012                      0                 0
    61214                      0                 0
    73457                      0                 0
    88148                      0                 0
    105778                     0                 0
    126934                     0                 0
    152321                     0                 0
    182785                     0                 0
    219342                     0                 0
    263210                     0                 0
    315852                     0                 0
    379022                     0                 0
    454826                     0                 0
    545791                     0                 0
    654949                     0                 0
    785939                     0                 0
    943127                     0                 0
    1131752                    0                 0
    1358102                    0                 0
    1629722                    0                 0
    1955666                    0                 0
    2346799                    0                 0
    2816159                    0                 0
    3379391                    0                 0
    4055269                    0                 0
    4866323                    0                 0
    5839588                    0                 0
    7007506                    0                 0
    8409007                    0                 0
    10090808                   0                 0
    12108970                   0                 0
    14530764                   0                 0
    17436917                   0                 0
    20924300                   0                 0
    25109160                   0                 0
    30130992                   0                 0
    36157190                   0                 0
    43388628                   0                 0
    52066354                   0                 0
    62479625                   0                 0
    74975550                   0                 0
    89970660                   0                 0
    107964792                  0                 0
    129557750                  0                 0
    155469300                  0                 0
    186563160                  0                 0
    223875792                  0                 0
    268650950                  0                 0
    322381140                  0                 0
    386857368                  0                 0
    464228842                  0                 0
    557074610                  0                 0
    668489532                  0                 0
    802187438                  0                 0
    962624926                  0                 0
    1155149911                 0                 0
    1386179893                 0                 0
    1663415872                 0                 0
    1996099046                 0                 0
    2395318855                 0                 0
    2874382626                 0
    3449259151                 0
    4139110981                 0
    4966933177                 0
    5960319812                 0
    7152383774                 0
    8582860529                 0
    10299432635                 0
    12359319162                 0
    14831182994                 0
    17797419593                 0
    21356903512                 0
    25628284214                 0
    30753941057                 0
    36904729268                 0
    44285675122                 0
    53142810146                 0
    63771372175                 0
    76525646610                 0
    91830775932                 0
    110196931118                 0
    132236317342                 0
    158683580810                 0
    190420296972                 0
    228504356366                 0
    274205227639                 0
    329046273167                 0
    394855527800                 0
    473826633360                 0
    568591960032                 0
    682310352038                 0
    818772422446                 0
    982526906935                 0
    1179032288322                 0
    1414838745986                 0
    Estimated cardinality: 20196
    EncodingStats minTTL: 0
    EncodingStats minLocalDeletionTime: 1442880000
    EncodingStats minTimestamp: 1535025565275000
    KeyType: org.apache.cassandra.db.marshal.BytesType
    ClusteringTypes: [org.apache.cassandra.db.marshal.UTF8Type]
    StaticColumns: {C3:org.apache.cassandra.db.marshal.BytesType, C4:org.apache.cassandra.db.marshal.BytesType, C0:org.apache.cassandra.db.marshal.BytesType, C1:org.apache.cassandra.db.marshal.BytesType, C2:org.apache.cassandra.db.marshal.BytesType}
    RegularColumns: {}

Specify gc grace seconds
^^^^^^^^^^^^^^^^^^^^^^^^

To see the ratio of droppable tombstones given a configured gc grace seconds, use the gc_grace_seconds option. Because the sstablemetadata tool doesn't access the schema directly, this is a way to more accurately estimate droppable tombstones -- for example, if you pass in gc_grace_seconds matching what is configured in the schema. The gc_grace_seconds value provided is subtracted from the curent machine time (in seconds). 

ref: https://issues.apache.org/jira/browse/CASSANDRA-12208

Example::

    sstablemetadata /var/lib/cassandra/data/keyspace1/standard1-41b52700b4ed11e896476d2c86545d91/mc-12-big-Data.db | grep "Estimated tombstone drop times" -A4
    Estimated tombstone drop times:
    1536599100:         1
    1536599640:         1
    1536599700:         2

    echo $(date +%s)
    1536602005

    # if gc_grace_seconds was configured at 100, all of the tombstones would be currently droppable 
    sstablemetadata --gc_grace_seconds 100 /var/lib/cassandra/data/keyspace1/standard1-41b52700b4ed11e896476d2c86545d91/mc-12-big-Data.db | grep "Estimated droppable tombstones"
    Estimated droppable tombstones: 4.0E-5

    # if gc_grace_seconds was configured at 4700, some of the tombstones would be currently droppable 
    sstablemetadata --gc_grace_seconds 4700 /var/lib/cassandra/data/keyspace1/standard1-41b52700b4ed11e896476d2c86545d91/mc-12-big-Data.db | grep "Estimated droppable tombstones"
    Estimated droppable tombstones: 9.61111111111111E-6

    # if gc_grace_seconds was configured at 5000, none of the tombstones would be currently droppable 
    sstablemetadata --gc_grace_seconds 5000 /var/lib/cassandra/data/keyspace1/standard1-41b52700b4ed11e896476d2c86545d91/mc-12-big-Data.db | grep "Estimated droppable tombstones"
    Estimated droppable tombstones: 0.0

Explanation of each value printed above
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

===================================  ================================================================================
   Value                             Explanation
===================================  ================================================================================
SSTable                              prefix of the sstable filenames related to this sstable
Partitioner                          partitioner type used to distribute data across nodes; defined in cassandra.yaml  
Bloom Filter FP                      precision of Bloom filter used in reads; defined in the table definition   
Minimum timestamp                    minimum timestamp of any entry in this sstable, in epoch microseconds  
Maximum timestamp                    maximum timestamp of any entry in this sstable, in epoch microseconds
SSTable min local deletion time      minimum timestamp of deletion date, based on TTL, in epoch seconds
SSTable max local deletion time      maximum timestamp of deletion date, based on TTL, in epoch seconds
Compressor                           blank (-) by default; if not blank, indicates type of compression enabled on the table
TTL min                              time-to-live in seconds; default 0 unless defined in the table definition
TTL max                              time-to-live in seconds; default 0 unless defined in the table definition
First token                          lowest token and related key found in the sstable summary
Last token                           highest token and related key found in the sstable summary
Estimated droppable tombstones       ratio of tombstones to columns, using configured gc grace seconds if relevant
SSTable level                        compaction level of this sstable, if leveled compaction (LCS) is used
Repaired at                          the timestamp this sstable was marked as repaired via sstablerepairedset, in epoch milliseconds
Replay positions covered             the interval of time and commitlog positions related to this sstable
totalColumnsSet                      number of cells in the table
totalRows                            number of rows in the table
Estimated tombstone drop times       approximate number of rows that will expire, ordered by epoch seconds
Count  Row Size  Cell Count          two histograms in two columns; one represents distribution of Row Size 
                                     and the other represents distribution of Cell Count
Estimated cardinality                an estimate of unique values, used for compaction
EncodingStats* minTTL                in epoch milliseconds
EncodingStats* minLocalDeletionTime  in epoch seconds
EncodingStats* minTimestamp          in epoch microseconds
KeyType                              the type of partition key, useful in reading and writing data 
                                     from/to storage; defined in the table definition
ClusteringTypes                      the type of clustering key, useful in reading and writing data 
                                     from/to storage; defined in the table definition
StaticColumns                        a list of the shared columns in the table
RegularColumns                       a list of non-static, non-key columns in the table
===================================  ================================================================================
* For the encoding stats values, the delta of this and the current epoch time is used when encoding and storing data in the most optimal way.



