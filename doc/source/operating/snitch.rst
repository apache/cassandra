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

Snitch
------

In cassandra, the snitch has two functions:

- it teaches Cassandra enough about your network topology to route requests efficiently.
- it allows Cassandra to spread replicas around your cluster to avoid correlated failures. It does this by grouping
  machines into "datacenters" and "racks."  Cassandra will do its best not to have more than one replica on the same
  "rack" (which may not actually be a physical location).

Dynamic snitching
^^^^^^^^^^^^^^^^^

The dynamic snitch monitor read latencies to avoid reading from hosts that have slowed down. The dynamic snitch is
configured with the following properties on ``cassandra.yaml``:

- ``dynamic_snitch``: whether the dynamic snitch should be enabled or disabled.
- ``dynamic_snitch_update_interval_in_ms``: controls how often to perform the more expensive part of host score
  calculation.
- ``dynamic_snitch_reset_interval_in_ms``: if set greater than zero and read_repair_chance is < 1.0, this will allow
  'pinning' of replicas to hosts in order to increase cache capacity.
- ``dynamic_snitch_badness_threshold:``: The badness threshold will control how much worse the pinned host has to be
  before the dynamic snitch will prefer other replicas over it.  This is expressed as a double which represents a
  percentage.  Thus, a value of 0.2 means Cassandra would continue to prefer the static snitch values until the pinned
  host was 20% worse than the fastest.

Snitch classes
^^^^^^^^^^^^^^

The ``endpoint_snitch`` parameter in ``cassandra.yaml`` should be set to the class the class that implements
``IEndPointSnitch`` which will be wrapped by the dynamic snitch and decide if two endpoints are in the same data center
or on the same rack. Out of the box, Cassandra provides the snitch implementations:

GossipingPropertyFileSnitch
    This should be your go-to snitch for production use. The rack and datacenter for the local node are defined in
    cassandra-rackdc.properties and propagated to other nodes via gossip. If ``cassandra-topology.properties`` exists,
    it is used as a fallback, allowing migration from the PropertyFileSnitch.

SimpleSnitch
    Treats Strategy order as proximity. This can improve cache locality when disabling read repair. Only appropriate for
    single-datacenter deployments.

PropertyFileSnitch
    Proximity is determined by rack and data center, which are explicitly configured in
    ``cassandra-topology.properties``.

Ec2Snitch
    Appropriate for EC2 deployments in a single Region. Loads Region and Availability Zone information from the EC2 API.
    The Region is treated as the datacenter, and the Availability Zone as the rack. Only private IPs are used, so this
    will not work across multiple regions.

Ec2MultiRegionSnitch
    Uses public IPs as broadcast_address to allow cross-region connectivity (thus, you should set seed addresses to the
    public IP as well). You will need to open the ``storage_port`` or ``ssl_storage_port`` on the public IP firewall
    (For intra-Region traffic, Cassandra will switch to the private IP after establishing a connection).

RackInferringSnitch
    Proximity is determined by rack and data center, which are assumed to correspond to the 3rd and 2nd octet of each
    node's IP address, respectively.  Unless this happens to match your deployment conventions, this is best used as an
    example of writing a custom Snitch class and is provided in that spirit.
