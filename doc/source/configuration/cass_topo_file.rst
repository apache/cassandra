.. _cassandra-topology:

cassandra-topologies.properties file 
================================

The ``PropertyFileSnitch`` :term:`snitch` option uses the ``cassandra-topologies.properties`` configuration file to determine which :term:`datacenters` and racks cluster nodes belong to. If other snitches are used, the 
:ref:cassandra_rackdc must be used. The snitch determines network topology (proximity by rack and datacenter) so that requests are routed efficiently and allows the database to distribute replicas evenly.

Include every node in the cluster in the properties file, defining your datacenter names as in the keyspace definition. The datacenter and rack names are case-sensitive.

The ``cassandra-topologies.properties`` file must be copied identically to every node in the cluster.


===========================
Example
===========================
This example uses three datacenters:

.. code-block:: bash

   # datacenter One

   175.56.12.105=DC1:RAC1
   175.50.13.200=DC1:RAC1
   175.54.35.197=DC1:RAC1

   120.53.24.101=DC1:RAC2
   120.55.16.200=DC1:RAC2
   120.57.102.103=DC1:RAC2

   # datacenter Two

   110.56.12.120=DC2:RAC1
   110.50.13.201=DC2:RAC1
   110.54.35.184=DC2:RAC1

   50.33.23.120=DC2:RAC2
   50.45.14.220=DC2:RAC2
   50.17.10.203=DC2:RAC2

   # datacenter Three

   172.106.12.120=DC3:RAC1
   172.106.12.121=DC3:RAC1
   172.106.12.122=DC3:RAC1

   # default for unknown nodes 
   default =DC3:RAC1
