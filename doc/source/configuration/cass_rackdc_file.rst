.. _cassandra-rackdc:

cassandra-rackdc.properties file 
================================

Several :term:`snitch` options use the ``cassandra-rackdc.properties`` configuration file to determine which :term:`datacenters` and racks cluster nodes belong to. Information about the 
network topology allows requests to be routed efficiently and to distribute replicas evenly. The following snitches can be configured here:

- GossipingPropertyFileSnitch
- AWS EC2 single-region snitch
- AWS EC2 multi-region snitch

The GossipingPropertyFileSnitch is recommended for production. This snitch uses the datacenter and rack information configured in a local node's ``cassandra-rackdc.properties``
file and propagates the information to other nodes using :term:`gossip`. It is the default snitch and the settings in this properties file are enabled.

The AWS EC2 snitches are configured for clusters in AWS. This snitch uses the ``cassandra-rackdc.properties`` options to designate one of two AWS EC2 datacenter and rack naming conventions:

- legacy: Datacenter name is the part of the availability zone name preceding the last "-" when the zone ends in -1 and includes the number if not -1. Rack name is the portion of the availability zone name following  the last "-".

          Examples: us-west-1a => dc: us-west, rack: 1a; us-west-2b => dc: us-west-2, rack: 2b;

- standard: Datacenter name is the standard AWS region name, including the number. Rack name is the region plus the availability zone letter.

          Examples: us-west-1a => dc: us-west-1, rack: us-west-1a; us-west-2b => dc: us-west-2, rack: us-west-2b;

Either snitch can set to use the local or internal IP address when multiple datacenters are not communicating.

===========================
GossipingPropertyFileSnitch
===========================

``dc``
------
Name of the datacenter. The value is case-sensitive.

**Default value:** DC1

``rack``
--------
Rack designation. The value is case-sensitive.

**Default value:** RAC1 

===========================
AWS EC2 snitch
===========================

``ec2_naming_scheme``
---------------------
Datacenter and rack naming convention. Options are ``legacy`` or ``standard`` (default). **This option is commented out by default.** 

**Default value:** standard


.. NOTE::
          YOU MUST USE THE ``legacy`` VALUE IF YOU ARE UPGRADING A PRE-4.0 CLUSTER.

===========================
Either snitch
===========================

``prefer_local``
----------------
Option to use the local or internal IP address when communication is not across different datacenters. **This option is commented out by default.**

**Default value:** true

