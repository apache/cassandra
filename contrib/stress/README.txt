stress
======

Description
-----------
stress is a tool for benchmarking and load testing a Cassandra
cluster.  It is significantly faster than the older py_stress tool.

Setup
-----
Run `ant` from the Cassandra source directory, then Run `ant` from the
contrib/stress directory.

Usage
-----
There are three different modes of operation:

    * inserting (loading test data)
    * reading
    * range slicing (only works with the OrderPreservingPartioner)
    * indexed range slicing (works with RandomParitioner on indexed ColumnFamilies)

Important options:
    -o or --operation:
        Sets the operation mode, one of 'insert', 'read', 'rangeslice', or 'indexedrangeslice'
    -n or --num-keys:
        the number of rows to insert/read/slice; defaults to one million
    -d or --nodes:
        the node(s) to perform the test against.  For multiple nodes, supply a
        comma-separated list without spaces, ex: cassandra1,cassandra2,cassandra3
    -y or --family-type:
        Sets the ColumnFamily type.  One of 'Standard' or 'Super'.  If using super,
        you probably want to set the -u option also.
    -c or --columns:
        the number of columns per row, defaults to 5
    -u or --supercolumns:
        use the number of supercolumns specified NOTE: you must set the -y
        option appropriately, or this option has no effect.
    -g or --get-range-slice-count:
        This is only used for the rangeslice operation and will *NOT* work with
        the RandomPartioner.  You must set the OrderPreservingPartioner in your
        storage-conf.xml (note that you will need to wipe all existing data
        when switching partioners.)  This option sets the number of rows to
        slice at a time and defaults to 1000.
    -r or --random:
        Only used for reads.  By default, stress.py will perform reads on rows
        with a guassian distribution, which will cause some repeats.  Setting
        this option makes the reads completely random instead.
    -i or --progress-interval:
        The interval, in seconds, at which progress will be output.

Remember that you must perform inserts before performing reads or range slices.

Examples
--------

    * contrib/stress/bin/stress -d 192.168.1.101 # 1M inserts to given host
    * contrib/stress/bin/stress -d 192.168.1.101 -o read # 1M reads
    * contrib/stress/bin/stress -d 192.168.1.101,192.168.1.102 -n 10000000 # 10M inserts spread across two nodes
