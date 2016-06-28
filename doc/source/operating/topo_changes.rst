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

.. _topology-changes:

Adding, replacing, moving and removing nodes
--------------------------------------------

Bootstrap
^^^^^^^^^

Adding new nodes is called "bootstrapping". The ``num_tokens`` parameter will define the amount of virtual nodes
(tokens) the joining node will be assigned during bootstrap. The tokens define the sections of the ring (token ranges)
the node will become responsible for.

Token allocation
~~~~~~~~~~~~~~~~

With the default token allocation algorithm the new node will pick ``num_tokens`` random tokens to become responsible
for. Since tokens are distributed randomly, load distribution improves with a higher amount of virtual nodes, but it
also increases token management overhead. The default of 256 virtual nodes should provide a reasonable load balance with
acceptable overhead.

On 3.0+ a new token allocation algorithm was introduced to allocate tokens based on the load of existing virtual nodes
for a given keyspace, and thus yield an improved load distribution with a lower number of tokens. To use this approach,
the new node must be started with the JVM option ``-Dcassandra.allocate_tokens_for_keyspace=<keyspace>``, where
``<keyspace>`` is the keyspace from which the algorithm can find the load information to optimize token assignment for.

Manual token assignment
"""""""""""""""""""""""

You may specify a comma-separated list of tokens manually with the ``initial_token`` ``cassandra.yaml`` parameter, and
if that is specified Cassandra will skip the token allocation process. This may be useful when doing token assignment
with an external tool or when restoring a node with its previous tokens.

Range streaming
~~~~~~~~~~~~~~~~

After the tokens are allocated, the joining node will pick current replicas of the token ranges it will become
responsible for to stream data from. By default it will stream from the primary replica of each token range in order to
guarantee data in the new node will be consistent with the current state.

In the case of any unavailable replica, the consistent bootstrap process will fail. To override this behavior and
potentially miss data from an unavailable replica, set the JVM flag ``-Dcassandra.consistent.rangemovement=false``.

Resuming failed/hanged bootstrap
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On 2.2+, if the bootstrap process fails, it's possible to resume bootstrap from the previous saved state by calling
``nodetool bootstrap resume``. If for some reason the bootstrap hangs or stalls, it may also be resumed by simply
restarting the node. In order to cleanup bootstrap state and start fresh, you may set the JVM startup flag
``-Dcassandra.reset_bootstrap_progress=true``.

On lower versions, when the bootstrap proces fails it is recommended to wipe the node (remove all the data), and restart
the bootstrap process again.

Manual bootstrapping
~~~~~~~~~~~~~~~~~~~~

It's possible to skip the bootstrapping process entirely and join the ring straight away by setting the hidden parameter
``auto_bootstrap: false``. This may be useful when restoring a node from a backup or creating a new data-center.

Removing nodes
^^^^^^^^^^^^^^

You can take a node out of the cluster with ``nodetool decommission`` to a live node, or ``nodetool removenode`` (to any
other machine) to remove a dead one. This will assign the ranges the old node was responsible for to other nodes, and
replicate the appropriate data there. If decommission is used, the data will stream from the decommissioned node. If
removenode is used, the data will stream from the remaining replicas.

No data is removed automatically from the node being decommissioned, so if you want to put the node back into service at
a different token on the ring, it should be removed manually.

Moving nodes
^^^^^^^^^^^^

When ``num_tokens: 1`` it's possible to move the node position in the ring with ``nodetool move``. Moving is both a
convenience over and more efficient than decommission + bootstrap. After moving a node, ``nodetool cleanup`` should be
run to remove any unnecessary data.

Replacing a dead node
^^^^^^^^^^^^^^^^^^^^^

In order to replace a dead node, start cassandra with the JVM startup flag
``-Dcassandra.replace_address_first_boot=<dead_node_ip>``. Once this property is enabled the node starts in a hibernate
state, during which all the other nodes will see this node to be down.

The replacing node will now start to bootstrap the data from the rest of the nodes in the cluster. The main difference
between normal bootstrapping of a new node is that this new node will not accept any writes during this phase.

Once the bootstrapping is complete the node will be marked "UP", we rely on the hinted handoff's for making this node
consistent (since we don't accept writes since the start of the bootstrap).

.. Note:: If the replacement process takes longer than ``max_hint_window_in_ms`` you **MUST** run repair to make the
   replaced node consistent again, since it missed ongoing writes during bootstrapping.

Monitoring progress
^^^^^^^^^^^^^^^^^^^

Bootstrap, replace, move and remove progress can be monitored using ``nodetool netstats`` which will show the progress
of the streaming operations.

Cleanup data after range movements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As a safety measure, Cassandra does not automatically remove data from nodes that "lose" part of their token range due
to a range movement operation (bootstrap, move, replace). Run ``nodetool cleanup`` on the nodes that lost ranges to the
joining node when you are satisfied the new node is up and working. If you do not do this the old data will still be
counted against the load on that node.
