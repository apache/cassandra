.. _cassandra-envsh:

cassandra-env.sh file 
=====================

The ``cassandra-env.sh`` bash script file can be used to pass additional options to the Java virtual machine (JVM), such as maximum and minimum heap size, rather than setting them in the environment. If the JVM settings are static and do not need to be computed from the node characteristics, the :ref:`cassandra-jvm-options` files should be used instead. For example, commonly computed values are the heap sizes, using the system values.

For example, add ``JVM_OPTS="$JVM_OPTS -Dcassandra.load_ring_state=false"`` to the ``cassandra_env.sh`` file
and run the command-line ``cassandra`` to start. The option is set from the ``cassandra-env.sh`` file, and is equivalent to starting Cassandra with the command-line option ``cassandra -Dcassandra.load_ring_state=false``.

The ``-D`` option specifies the start-up parameters in both the command line and ``cassandra-env.sh`` file. The following options are available:

``cassandra.auto_bootstrap=false``
----------------------------------
Facilitates setting auto_bootstrap to false on initial set-up of the cluster. The next time you start the cluster, you do not need to change the ``cassandra.yaml`` file on each node to revert to true, the default value.

``cassandra.available_processors=<number_of_processors>``
---------------------------------------------------------
In a multi-instance deployment, multiple Cassandra instances will independently assume that all CPU processors are available to it. This setting allows you to specify a smaller set of processors.

``cassandra.boot_without_jna=true``
-----------------------------------
If JNA fails to initialize, Cassandra fails to boot. Use this command to boot Cassandra without JNA.

``cassandra.config=<directory>``
--------------------------------
The directory location of the ``cassandra.yaml file``. The default location depends on the type of installation.

``cassandra.ignore_dynamic_snitch_severity=true|false`` 
-------------------------------------------------------
Setting this property to true causes the dynamic snitch to ignore the severity indicator from gossip when scoring nodes.  Explore failure detection and recovery and dynamic snitching for more information.

**Default:** false

``cassandra.initial_token=<token>``
-----------------------------------
Use when virtual nodes (vnodes) are not used. Sets the initial partitioner token for a node the first time the node is started. 
Note: Vnodes are highly recommended as they automatically select tokens.

**Default:** disabled

``cassandra.join_ring=true|false``
----------------------------------
Set to false to start Cassandra on a node but not have the node join the cluster. 
You can use ``nodetool join`` and a JMX call to join the ring afterwards.

**Default:** true

``cassandra.load_ring_state=true|false``
----------------------------------------
Set to false to clear all gossip state for the node on restart. 

**Default:** true

``cassandra.metricsReporterConfigFile=<filename>``
--------------------------------------------------
Enable pluggable metrics reporter. Explore pluggable metrics reporting for more information.

``cassandra.partitioner=<partitioner>``
---------------------------------------
Set the partitioner. 

**Default:** org.apache.cassandra.dht.Murmur3Partitioner

``cassandra.prepared_statements_cache_size_in_bytes=<cache_size>``
------------------------------------------------------------------
Set the cache size for prepared statements.

``cassandra.replace_address=<listen_address of dead node>|<broadcast_address of dead node>``
--------------------------------------------------------------------------------------------
To replace a node that has died, restart a new node in its place specifying the ``listen_address`` or ``broadcast_address`` that the new node is assuming. The new node must not have any data in its data directory, the same state as before bootstrapping.
Note: The ``broadcast_address`` defaults to the ``listen_address`` except when using the ``Ec2MultiRegionSnitch``.

``cassandra.replayList=<table>``
--------------------------------
Allow restoring specific tables from an archived commit log.

``cassandra.ring_delay_ms=<number_of_ms>``
------------------------------------------
Defines the amount of time a node waits to hear from other nodes before formally joining the ring. 

**Default:** 1000ms

``cassandra.native_transport_port=<port>``
------------------------------------------
Set the port on which the CQL native transport listens for clients. 

**Default:** 9042

``cassandra.rpc_port=<port>``
-----------------------------
Set the port for the Thrift RPC service, which is used for client connections. 

**Default:** 9160

``cassandra.storage_port=<port>``
---------------------------------
Set the port for inter-node communication. 

**Default:** 7000

``cassandra.ssl_storage_port=<port>``
-------------------------------------
Set the SSL port for encrypted communication. 

**Default:** 7001

``cassandra.start_native_transport=true|false``
-----------------------------------------------
Enable or disable the native transport server. See ``start_native_transport`` in ``cassandra.yaml``. 

**Default:** true

``cassandra.start_rpc=true|false``
----------------------------------
Enable or disable the Thrift RPC server. 

**Default:** true

``cassandra.triggers_dir=<directory>``
--------------------------------------
Set the default location for the trigger JARs. 

**Default:** conf/triggers

``cassandra.write_survey=true``
-------------------------------
For testing new compaction and compression strategies. It allows you to experiment with different strategies and benchmark write performance differences without affecting the production workload.

``consistent.rangemovement=true|false``
---------------------------------------
Set to true makes Cassandra perform bootstrap safely without violating consistency. False disables this.
