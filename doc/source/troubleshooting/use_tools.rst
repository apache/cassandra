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

.. _use-os-tools:

Diving Deep, Use External Tools
===============================

Machine access allows operators to dive even deeper than logs and ``nodetool``
allow. While every Cassandra operator may have their personal favorite
toolsets for troubleshooting issues, this page contains some of the most common
operator techniques and examples of those tools. Many of these commands work
only on Linux, but if you are deploying on a different operating system you may
have access to other substantially similar tools that assess similar OS level
metrics and processes.

JVM Tooling
-----------
The JVM ships with a number of useful tools. Some of them are useful for
debugging Cassandra issues, especially related to heap and execution stacks.

**NOTE**: There are two common gotchas with JVM tooling and Cassandra:

1. By default Cassandra ships with ``-XX:+PerfDisableSharedMem`` set to prevent
   long pauses (see ``CASSANDRA-9242`` and ``CASSANDRA-9483`` for details). If
   you want to use JVM tooling you can instead have ``/tmp`` mounted on an in
   memory ``tmpfs`` which also effectively works around ``CASSANDRA-9242``.
2. Make sure you run the tools as the same user as Cassandra is running as,
   e.g. if the database is running as ``cassandra`` the tool also has to be
   run as ``cassandra``, e.g. via ``sudo -u cassandra <cmd>``.

Garbage Collection State (jstat)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If you suspect heap pressure you can use ``jstat`` to dive deep into the
garbage collection state of a Cassandra process. This command is always
safe to run and yields detailed heap information including eden heap usage (E),
old generation heap usage (O), count of eden collections (YGC), time spend in
eden collections (YGCT), old/mixed generation collections (FGC) and time spent
in old/mixed generation collections (FGCT)::


    jstat -gcutil <cassandra pid> 500ms
     S0     S1     E      O      M     CCS    YGC     YGCT    FGC    FGCT     GCT
     0.00   0.00  81.53  31.16  93.07  88.20     12    0.151     3    0.257    0.408
     0.00   0.00  82.36  31.16  93.07  88.20     12    0.151     3    0.257    0.408
     0.00   0.00  82.36  31.16  93.07  88.20     12    0.151     3    0.257    0.408
     0.00   0.00  83.19  31.16  93.07  88.20     12    0.151     3    0.257    0.408
     0.00   0.00  83.19  31.16  93.07  88.20     12    0.151     3    0.257    0.408
     0.00   0.00  84.19  31.16  93.07  88.20     12    0.151     3    0.257    0.408
     0.00   0.00  84.19  31.16  93.07  88.20     12    0.151     3    0.257    0.408
     0.00   0.00  85.03  31.16  93.07  88.20     12    0.151     3    0.257    0.408
     0.00   0.00  85.03  31.16  93.07  88.20     12    0.151     3    0.257    0.408
     0.00   0.00  85.94  31.16  93.07  88.20     12    0.151     3    0.257    0.408

In this case we see we have a relatively healthy heap profile, with 31.16%
old generation heap usage and 83% eden. If the old generation routinely is
above 75% then you probably need more heap (assuming CMS with a 75% occupancy
threshold). If you do have such persistently high old gen that often means you
either have under-provisioned the old generation heap, or that there is too
much live data on heap for Cassandra to collect (e.g. because of memtables).
Another thing to watch for is time between young garbage collections (YGC),
which indicate how frequently the eden heap is collected. Each young gc pause
is about 20-50ms, so if you have a lot of them your clients will notice in
their high percentile latencies.

Thread Information (jstack)
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To get a point in time snapshot of exactly what Cassandra is doing, run
``jstack`` against the Cassandra PID. **Note** that this does pause the JVM for
a very brief period (<20ms).::

    $ jstack <cassandra pid> > threaddump

    # display the threaddump
    $ cat threaddump
    ...

    # look at runnable threads
    $grep RUNNABLE threaddump -B 1
    "Attach Listener" #15 daemon prio=9 os_prio=0 tid=0x00007f829c001000 nid=0x3a74 waiting on condition [0x0000000000000000]
       java.lang.Thread.State: RUNNABLE
    --
    "DestroyJavaVM" #13 prio=5 os_prio=0 tid=0x00007f82e800e000 nid=0x2a19 waiting on condition [0x0000000000000000]
       java.lang.Thread.State: RUNNABLE
    --
    "JPS thread pool" #10 prio=5 os_prio=0 tid=0x00007f82e84d0800 nid=0x2a2c runnable [0x00007f82d0856000]
       java.lang.Thread.State: RUNNABLE
    --
    "Service Thread" #9 daemon prio=9 os_prio=0 tid=0x00007f82e80d7000 nid=0x2a2a runnable [0x0000000000000000]
       java.lang.Thread.State: RUNNABLE
    --
    "C1 CompilerThread3" #8 daemon prio=9 os_prio=0 tid=0x00007f82e80cc000 nid=0x2a29 waiting on condition [0x0000000000000000]
       java.lang.Thread.State: RUNNABLE
    --
    ...

    # Note that the nid is the Linux thread id

Some of the most important information in the threaddumps are waiting/blocking
threads, including what locks or monitors the thread is blocking/waiting on.

Basic OS Tooling
----------------
A great place to start when debugging a Cassandra issue is understanding how
Cassandra is interacting with system resources. The following are all
resources that Cassandra makes heavy uses of:

* CPU cores. For executing concurrent user queries
* CPU processing time. For query activity (data decompression, row merging,
  etc...)
* CPU processing time (low priority). For background tasks (compaction,
  streaming, etc ...)
* RAM for Java Heap. Used to hold internal data-structures and by default the
  Cassandra memtables. Heap space is a crucial component of write performance
  as well as generally.
* RAM for OS disk cache. Used to cache frequently accessed SSTable blocks. OS
  disk cache is a crucial component of read performance.
* Disks. Cassandra cares a lot about disk read latency, disk write throughput,
  and of course disk space.
* Network latency. Cassandra makes many internode requests, so network latency
  between nodes can directly impact performance.
* Network throughput. Cassandra (as other databases) frequently have the
  so called "incast" problem where a small request (e.g. ``SELECT * from
  foo.bar``) returns a massively large result set (e.g. the entire dataset).
  In such situations outgoing bandwidth is crucial.

Often troubleshooting Cassandra comes down to troubleshooting what resource
the machine or cluster is running out of. Then you create more of that resource
or change the query pattern to make less use of that resource.

High Level Resource Usage (top/htop)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Cassandra makes signifiant use of system resources, and often the very first
useful action is to run ``top`` or ``htop`` (`website
<https://hisham.hm/htop/>`_)to see the state of the machine.

Useful things to look at:

* System load levels. While these numbers can be confusing, generally speaking
  if the load average is greater than the number of CPU cores, Cassandra
  probably won't have very good (sub 100 millisecond) latencies. See
  `Linux Load Averages <http://www.brendangregg.com/blog/2017-08-08/linux-load-averages.html>`_
  for more information.
* CPU utilization. ``htop`` in particular can help break down CPU utilization
  into ``user`` (low and normal priority), ``system`` (kernel), and ``io-wait``
  . Cassandra query threads execute as normal priority ``user`` threads, while
  compaction threads execute as low priority ``user`` threads. High ``system``
  time could indicate problems like thread contention, and high ``io-wait``
  may indicate slow disk drives. This can help you understand what Cassandra
  is spending processing resources doing.
* Memory usage. Look for which programs have the most resident memory, it is
  probably Cassandra. The number for Cassandra is likely inaccurately high due
  to how Linux (as of 2018) accounts for memory mapped file memory.

.. _os-iostat:

IO Usage (iostat)
^^^^^^^^^^^^^^^^^
Use iostat to determine how data drives are faring, including latency
distributions, throughput, and utilization::

    $ sudo iostat -xdm 2
    Linux 4.13.0-13-generic (hostname)     07/03/2018     _x86_64_    (8 CPU)

    Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
    sda               0.00     0.28    0.32    5.42     0.01     0.13    48.55     0.01    2.21    0.26    2.32   0.64   0.37
    sdb               0.00     0.00    0.00    0.00     0.00     0.00    79.34     0.00    0.20    0.20    0.00   0.16   0.00
    sdc               0.34     0.27    0.76    0.36     0.01     0.02    47.56     0.03   26.90    2.98   77.73   9.21   1.03

    Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
    sda               0.00     0.00    2.00   32.00     0.01     4.04   244.24     0.54   16.00    0.00   17.00   1.06   3.60
    sdb               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00
    sdc               0.00    24.50    0.00  114.00     0.00    11.62   208.70     5.56   48.79    0.00   48.79   1.12  12.80


In this case we can see that ``/dev/sdc1`` is a very slow drive, having an
``await`` close to 50 milliseconds and an ``avgqu-sz`` close to 5 ios. The
drive is not particularly saturated (utilization is only 12.8%), but we should
still be concerned about how this would affect our p99 latency since 50ms is
quite long for typical Cassandra operations. That being said, in this case
most of the latency is present in writes (typically writes are more latent
than reads), which due to the LSM nature of Cassandra is often hidden from
the user.

Important metrics to assess using iostat:

* Reads and writes per second. These numbers will change with the workload,
  but generally speaking the more reads Cassandra has to do from disk the
  slower Cassandra read latencies are. Large numbers of reads per second
  can be a dead giveaway that the cluster has insufficient memory for OS
  page caching.
* Write throughput. Cassandra's LSM model defers user writes and batches them
  together, which means that throughput to the underlying medium is the most
  important write metric for Cassandra.
* Read latency (``r_await``). When Cassandra missed the OS page cache and reads
  from SSTables, the read latency directly determines how fast Cassandra can
  respond with the data.
* Write latency. Cassandra is less sensitive to write latency except when it
  syncs the commit log. This typically enters into the very high percentiles of
  write latency.

Note that to get detailed latency breakdowns you will need a more advanced
tool such as :ref:`bcc-tools <use-bcc-tools>`.

OS page Cache Usage
^^^^^^^^^^^^^^^^^^^
As Cassandra makes heavy use of memory mapped files, the health of the
operating system's `Page Cache <https://en.wikipedia.org/wiki/Page_cache>`_ is
crucial to performance. Start by finding how much available cache is in the
system::

    $ free -g
                  total        used        free      shared  buff/cache   available
    Mem:             15           9           2           0           3           5
    Swap:             0           0           0

In this case 9GB of memory is used by user processes (Cassandra heap) and 8GB
is available for OS page cache. Of that, 3GB is actually used to cache files.
If most memory is used and unavailable to the page cache, Cassandra performance
can suffer significantly. This is why Cassandra starts with a reasonably small
amount of memory reserved for the heap.

If you suspect that you are missing the OS page cache frequently you can use
advanced tools like :ref:`cachestat <use-bcc-tools>` or
:ref:`vmtouch <use-vmtouch>` to dive deeper.

Network Latency and Reliability
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Whenever Cassandra does writes or reads that involve other replicas,
``LOCAL_QUORUM`` reads for example, one of the dominant effects on latency is
network latency. When trying to debug issues with multi machine operations,
the network can be an important resource to investigate. You can determine
internode latency using tools like ``ping`` and ``traceroute`` or most
effectively ``mtr``::

    $ mtr -nr www.google.com
    Start: Sun Jul 22 13:10:28 2018
    HOST: hostname                     Loss%   Snt   Last   Avg  Best  Wrst StDev
      1.|-- 192.168.1.1                0.0%    10    2.0   1.9   1.1   3.7   0.7
      2.|-- 96.123.29.15               0.0%    10   11.4  11.0   9.0  16.4   1.9
      3.|-- 68.86.249.21               0.0%    10   10.6  10.7   9.0  13.7   1.1
      4.|-- 162.141.78.129             0.0%    10   11.5  10.6   9.6  12.4   0.7
      5.|-- 162.151.78.253             0.0%    10   10.9  12.1  10.4  20.2   2.8
      6.|-- 68.86.143.93               0.0%    10   12.4  12.6   9.9  23.1   3.8
      7.|-- 96.112.146.18              0.0%    10   11.9  12.4  10.6  15.5   1.6
      9.|-- 209.85.252.250             0.0%    10   13.7  13.2  12.5  13.9   0.0
     10.|-- 108.170.242.238            0.0%    10   12.7  12.4  11.1  13.0   0.5
     11.|-- 74.125.253.149             0.0%    10   13.4  13.7  11.8  19.2   2.1
     12.|-- 216.239.62.40              0.0%    10   13.4  14.7  11.5  26.9   4.6
     13.|-- 108.170.242.81             0.0%    10   14.4  13.2  10.9  16.0   1.7
     14.|-- 72.14.239.43               0.0%    10   12.2  16.1  11.0  32.8   7.1
     15.|-- 216.58.195.68              0.0%    10   25.1  15.3  11.1  25.1   4.8

In this example of ``mtr``, we can rapidly assess the path that your packets
are taking, as well as what their typical loss and latency are. Packet loss
typically leads to between ``200ms`` and ``3s`` of additional latency, so that
can be a common cause of latency issues.

Network Throughput
^^^^^^^^^^^^^^^^^^
As Cassandra is sensitive to outgoing bandwidth limitations, sometimes it is
useful to determine if network throughput is limited. One handy tool to do
this is `iftop <https://www.systutorials.com/docs/linux/man/8-iftop/>`_ which
shows both bandwidth usage as well as connection information at a glance. An
example showing traffic during a stress run against a local ``ccm`` cluster::

    $ # remove the -t for ncurses instead of pure text
    $ sudo iftop -nNtP -i lo
    interface: lo
    IP address is: 127.0.0.1
    MAC address is: 00:00:00:00:00:00
    Listening on lo
       # Host name (port/service if enabled)            last 2s   last 10s   last 40s cumulative
    --------------------------------------------------------------------------------------------
       1 127.0.0.1:58946                          =>      869Kb      869Kb      869Kb      217KB
         127.0.0.3:9042                           <=         0b         0b         0b         0B
       2 127.0.0.1:54654                          =>      736Kb      736Kb      736Kb      184KB
         127.0.0.1:9042                           <=         0b         0b         0b         0B
       3 127.0.0.1:51186                          =>      669Kb      669Kb      669Kb      167KB
         127.0.0.2:9042                           <=         0b         0b         0b         0B
       4 127.0.0.3:9042                           =>     3.30Kb     3.30Kb     3.30Kb       845B
         127.0.0.1:58946                          <=         0b         0b         0b         0B
       5 127.0.0.1:9042                           =>     2.79Kb     2.79Kb     2.79Kb       715B
         127.0.0.1:54654                          <=         0b         0b         0b         0B
       6 127.0.0.2:9042                           =>     2.54Kb     2.54Kb     2.54Kb       650B
         127.0.0.1:51186                          <=         0b         0b         0b         0B
       7 127.0.0.1:36894                          =>     1.65Kb     1.65Kb     1.65Kb       423B
         127.0.0.5:7000                           <=         0b         0b         0b         0B
       8 127.0.0.1:38034                          =>     1.50Kb     1.50Kb     1.50Kb       385B
         127.0.0.2:7000                           <=         0b         0b         0b         0B
       9 127.0.0.1:56324                          =>     1.50Kb     1.50Kb     1.50Kb       383B
         127.0.0.1:7000                           <=         0b         0b         0b         0B
      10 127.0.0.1:53044                          =>     1.43Kb     1.43Kb     1.43Kb       366B
         127.0.0.4:7000                           <=         0b         0b         0b         0B
    --------------------------------------------------------------------------------------------
    Total send rate:                                     2.25Mb     2.25Mb     2.25Mb
    Total receive rate:                                      0b         0b         0b
    Total send and receive rate:                         2.25Mb     2.25Mb     2.25Mb
    --------------------------------------------------------------------------------------------
    Peak rate (sent/received/total):                     2.25Mb         0b     2.25Mb
    Cumulative (sent/received/total):                     576KB         0B      576KB
    ============================================================================================

In this case we can see that bandwidth is fairly shared between many peers,
but if the total was getting close to the rated capacity of the NIC or was focussed
on a single client, that may indicate a clue as to what issue is occurring.

Advanced tools
--------------
Sometimes as an operator you may need to really dive deep. This is where
advanced OS tooling can come in handy.

.. _use-bcc-tools:

bcc-tools
^^^^^^^^^
Most modern Linux distributions (kernels newer than ``4.1``) support `bcc-tools
<https://github.com/iovisor/bcc>`_ for diving deep into performance problems.
First install ``bcc-tools``, e.g.  via ``apt`` on Debian::

    $ apt install bcc-tools

Then you can use all the tools that ``bcc-tools`` contains. One of the most
useful tools is ``cachestat``
(`cachestat examples <https://github.com/iovisor/bcc/blob/master/tools/cachestat_example.txt>`_)
which allows you to determine exactly how many OS page cache hits and misses
are happening::

    $ sudo /usr/share/bcc/tools/cachestat -T 1
    TIME        TOTAL   MISSES     HITS  DIRTIES   BUFFERS_MB  CACHED_MB
    18:44:08       66       66        0       64           88       4427
    18:44:09       40       40        0       75           88       4427
    18:44:10     4353       45     4308      203           88       4427
    18:44:11       84       77        7       13           88       4428
    18:44:12     2511       14     2497       14           88       4428
    18:44:13      101       98        3       18           88       4428
    18:44:14    16741        0    16741       58           88       4428
    18:44:15     1935       36     1899       18           88       4428
    18:44:16       89       34       55       18           88       4428

In this case there are not too many page cache ``MISSES`` which indicates a
reasonably sized cache. These metrics are the most direct measurement of your
Cassandra node's "hot" dataset. If you don't have enough cache, ``MISSES`` will
be high and performance will be slow. If you have enough cache, ``MISSES`` will
be low and performance will be fast (as almost all reads are being served out
of memory).

You can also measure disk latency distributions using ``biolatency``
(`biolatency examples <https://github.com/iovisor/bcc/blob/master/tools/biolatency_example.txt>`_)
to get an idea of how slow Cassandra will be when reads miss the OS page Cache
and have to hit disks::

    $ sudo /usr/share/bcc/tools/biolatency -D 10
    Tracing block device I/O... Hit Ctrl-C to end.


    disk = 'sda'
         usecs               : count     distribution
             0 -> 1          : 0        |                                        |
             2 -> 3          : 0        |                                        |
             4 -> 7          : 0        |                                        |
             8 -> 15         : 0        |                                        |
            16 -> 31         : 12       |****************************************|
            32 -> 63         : 9        |******************************          |
            64 -> 127        : 1        |***                                     |
           128 -> 255        : 3        |**********                              |
           256 -> 511        : 7        |***********************                 |
           512 -> 1023       : 2        |******                                  |

    disk = 'sdc'
         usecs               : count     distribution
             0 -> 1          : 0        |                                        |
             2 -> 3          : 0        |                                        |
             4 -> 7          : 0        |                                        |
             8 -> 15         : 0        |                                        |
            16 -> 31         : 0        |                                        |
            32 -> 63         : 0        |                                        |
            64 -> 127        : 41       |************                            |
           128 -> 255        : 17       |*****                                   |
           256 -> 511        : 13       |***                                     |
           512 -> 1023       : 2        |                                        |
          1024 -> 2047       : 0        |                                        |
          2048 -> 4095       : 0        |                                        |
          4096 -> 8191       : 56       |*****************                       |
          8192 -> 16383      : 131      |****************************************|
         16384 -> 32767      : 9        |**                                      |

In this case most ios on the data drive (``sdc``) are fast, but many take
between 8 and 16 milliseconds.

Finally ``biosnoop`` (`examples <https://github.com/iovisor/bcc/blob/master/tools/biosnoop_example.txt>`_)
can be used to dive even deeper and see per IO latencies::

    $ sudo /usr/share/bcc/tools/biosnoop | grep java | head
    0.000000000    java           17427  sdc     R  3972458600 4096      13.58
    0.000818000    java           17427  sdc     R  3972459408 4096       0.35
    0.007098000    java           17416  sdc     R  3972401824 4096       5.81
    0.007896000    java           17416  sdc     R  3972489960 4096       0.34
    0.008920000    java           17416  sdc     R  3972489896 4096       0.34
    0.009487000    java           17427  sdc     R  3972401880 4096       0.32
    0.010238000    java           17416  sdc     R  3972488368 4096       0.37
    0.010596000    java           17427  sdc     R  3972488376 4096       0.34
    0.011236000    java           17410  sdc     R  3972488424 4096       0.32
    0.011825000    java           17427  sdc     R  3972488576 16384      0.65
    ... time passes
    8.032687000    java           18279  sdc     R  10899712  122880     3.01
    8.033175000    java           18279  sdc     R  10899952  8192       0.46
    8.073295000    java           18279  sdc     R  23384320  122880     3.01
    8.073768000    java           18279  sdc     R  23384560  8192       0.46


With ``biosnoop`` you see every single IO and how long they take. This data
can be used to construct the latency distributions in ``biolatency`` but can
also be used to better understand how disk latency affects performance. For
example this particular drive takes ~3ms to service a memory mapped read due to
the large default value (``128kb``) of ``read_ahead_kb``. To improve point read
performance you may may want to decrease ``read_ahead_kb`` on fast data volumes
such as SSDs while keeping the a higher value like ``128kb`` value is probably
right for HDs. There are tradeoffs involved, see `queue-sysfs
<https://www.kernel.org/doc/Documentation/block/queue-sysfs.txt>`_ docs for more
information, but regardless ``biosnoop`` is useful for understanding *how*
Cassandra uses drives.

.. _use-vmtouch:

vmtouch
^^^^^^^
Sometimes it's useful to know how much of the Cassandra data files are being
cached by the OS. A great tool for answering this question is
`vmtouch <https://github.com/hoytech/vmtouch>`_.

First install it::

    $ git clone https://github.com/hoytech/vmtouch.git
    $ cd vmtouch
    $ make

Then run it on the Cassandra data directory::

    $ ./vmtouch /var/lib/cassandra/data/
               Files: 312
         Directories: 92
      Resident Pages: 62503/64308  244M/251M  97.2%
             Elapsed: 0.005657 seconds

In this case almost the entire dataset is hot in OS page Cache. Generally
speaking the percentage doesn't really matter unless reads are missing the
cache (per e.g. :ref:`cachestat <use-bcc-tools>`), in which case having
additional memory may help read performance.

CPU Flamegraphs
^^^^^^^^^^^^^^^
Cassandra often uses a lot of CPU, but telling *what* it is doing can prove
difficult. One of the best ways to analyze Cassandra on CPU time is to use
`CPU Flamegraphs <http://www.brendangregg.com/FlameGraphs/cpuflamegraphs.html>`_
which display in a useful way which areas of Cassandra code are using CPU. This
may help narrow down a compaction problem to a "compaction problem dropping
tombstones" or just generally help you narrow down what Cassandra is doing
while it is having an issue. To get CPU flamegraphs follow the instructions for
`Java Flamegraphs
<http://www.brendangregg.com/FlameGraphs/cpuflamegraphs.html#Java>`_.

Generally:

1. Enable the ``-XX:+PreserveFramePointer`` option in Cassandra's
   ``jvm.options`` configuation file. This has a negligible performance impact
   but allows you actually see what Cassandra is doing.
2. Run ``perf`` to get some data.
3. Send that data through the relevant scripts in the FlameGraph toolset and
   convert the data into a pretty flamegraph. View the resulting SVG image in
   a browser or other image browser.

For example just cloning straight off github we first install the
``perf-map-agent`` to the location of our JVMs (assumed to be
``/usr/lib/jvm``)::

    $ sudo bash
    $ export JAVA_HOME=/usr/lib/jvm/java-8-oracle/
    $ cd /usr/lib/jvm
    $ git clone --depth=1 https://github.com/jvm-profiling-tools/perf-map-agent
    $ cd perf-map-agent
    $ cmake .
    $ make

Now to get a flamegraph::

    $ git clone --depth=1 https://github.com/brendangregg/FlameGraph
    $ sudo bash
    $ cd FlameGraph
    $ # Record traces of Cassandra and map symbols for all java processes
    $ perf record -F 49 -a -g -p <CASSANDRA PID> -- sleep 30; ./jmaps
    $ # Translate the data
    $ perf script > cassandra_stacks
    $ cat cassandra_stacks | ./stackcollapse-perf.pl | grep -v cpu_idle | \
        ./flamegraph.pl --color=java --hash > cassandra_flames.svg


The resulting SVG is searchable, zoomable, and generally easy to introspect
using a browser.

.. _packet-capture:

Packet Capture
^^^^^^^^^^^^^^
Sometimes you have to understand what queries a Cassandra node is performing
*right now* to troubleshoot an issue. For these times trusty packet capture
tools like ``tcpdump`` and `Wireshark
<https://www.wireshark.org/>`_ can be very helpful to dissect packet captures.
Wireshark even has native `CQL support
<https://www.wireshark.org/docs/dfref/c/cql.html>`_ although it sometimes has
compatibility issues with newer Cassandra protocol releases.

To get a packet capture first capture some packets::

    $ sudo tcpdump -U -s0 -i <INTERFACE> -w cassandra.pcap -n "tcp port 9042"

Now open it up with wireshark::

    $ wireshark cassandra.pcap

If you don't see CQL like statements try telling to decode as CQL by right
clicking on a packet going to 9042 -> ``Decode as`` -> select CQL from the
dropdown for port 9042.

If you don't want to do this manually or use a GUI, you can also use something
like `cqltrace <https://github.com/jolynch/cqltrace>`_ to ease obtaining and
parsing CQL packet captures.
