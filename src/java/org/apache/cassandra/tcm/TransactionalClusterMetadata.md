<!--
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->

# Transactional Cluster Metadata (TCM)

### More Detail

Further detail on the motivation, design, constraints and guarantees of TCM can be found in the Cassandra Enhancement Proposal document at https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-21%3A+Transactional+Cluster+Metadata

Lower level implementation details regarding certain areas of the codebase can be found in-tree in TCM_implementation.md

## ClusterMetadata

`ClusterMetadata` is the immutable data object representing metadata about the state of the cluster.

## Cluster Metadata Service
A subset of nodes in the cluster act as members of the Cluster Metadata Service (CMS) to maintain a log of events which modify cluster wide metadata. Membership of the CMS is flexible and dynamic and is updated when any of the existing members are decommissioned or replaced.

Members of the CMS are responsible for linearizing insertion of entries into the metadata change log. An entry contains a transformation to be applied to ClusterMetadata and a monotonically increasing counter, or epoch. Each event committed to the log by the CMS implies a new epoch and as such, an epoch represents a specific point in the linearized history of cluster metadata.

 Clients of the CMS submit metadata change events and the CMS node receiving it determines whether that event should be executed or rejected based on the current metadata state. The CMS enforces a serial order (initially using Paxos LWTs to insert into a distributed log table) for accepted events and proactively disseminates them to the rest of the cluster.

Independently, each node in the cluster applies the replicated transformations in strict order to produce a consistent view of metadata. After doing so, the transformed `ClusterMetadata` is atomically published on the node so that the most current metadata is always available. Code consuming metadata never witnesses partial updates to `ClusterMetadata`, and each version is uniquely identifiable by its epoch.

## Schema

`DistributedSchema` is a component of `ClusterMetadata` and all DDL updates are applied by the CMS inserting a log entry containing a schema transformation. As the log entries are disseminated around the cluster, each peer applies the transformation to its local `ClusterMetadata`, enacting the schema change. This entails some changes to the way the database objects represented in schema are intialised locally (db objects refers to classes like `Keyspace`, `ColumnFamilyStore`, etc).

## Cluster Membership

The `Directory` component of `ClusterMetadata` manages member identity, state, location and addressing. This duplicates many of the functions previously performed by `TokenMetadata`, `Topology` et al but with updates performed in deterministic order via the global log.

## Data Placement

`DataPlacements` provide a mapping from (keyspace, token range) tuples to sets of replicas. There are several such mappings in a cluster, one for each distinct set of `ReplicationParams` belonging to keyspaces in the schema. Where previously these mappings would be calculated on demand as topology modifications were discovered via gossip, these are now statically constructed for/from a given `ClusterMetadata` version.

## Operations

Performing operations such as shrink, expand, node replacement etc, do not simply modify metadata so atomic updates to cluster metadata are not in themselves sufficient for performing these safely. Streaming transfer of data between nodes must also occur and this must happen concurrently with reads and writes. Replication factors for writes must be temporarily expanded during range movements, what was previously provided by the concept of `PendingRanges`.

With TCM, these operations are performed in coordinated phases, planned in advance and properly linearized using the global log. For instance, to join an new node, the full set of phased range movements required is calculated to generate an actionable plan which is then stored in `InProgressSequences`, another component of `ClusterMetadata`.

Concurrent operations are permitted as long as they only affect disjoint token ranges, ensuring that concurrent range movements remain safe and cluster invariants are preserved at all times, including in the case of the failure to complete any operation (i.e. failed or aborted bootstrap).

## Read/Write Consistency

During a read or write, the coordinator constructs a replica plan based on the cluster metadata that is known to it at the time. The `Mutation` or`ReadCommand` messages that the coordinator sends to those replicas include the epochs which identify the most recent changes to either `DataPlacements` or `TableMetadata` which may have a material effect on the execution of the message payload. This allows replicas and coordinators to detect divergence between their respective metadata and take action accordingly. Coordinators also re-check `DataPlacements` after collecting replica responses, before returning to the client. If these have changed while the request was in flight, the coordinator can then verify whether received responses meet the desired consistency given the new topology.

## Recovering Missing Log Entries

A node may detect that it is missing some entries from the metadata log, either by receiving an update from the CMS which is non-consecutive with its current metadata epoch or during an exchange of messages with a peer. Catching up can be done simply by requesting the sequence of log entries with a greater epoch than any previously seen by the node. As the log is immutable and totally ordered, this request can be made to any peer as the results must be consistent, but not exhaustive. Any peers themselves may be lagging behind the "true" tail of the log, but this is perfectly acceptable, as it is impossible to propagate changes to all participants simultaneously, and we achieve correctness by means other than synchrony.

Nodes provide each other with missing log entries in the form of a `LogState`, which comprises an optional snapshot of `ClusterMetadata` at a given epoch, plus a list of individual log entries (a `Replication` object) to apply on top of the snapshot (if present). When receiving a `LogState`, the node can use the snapshot to effectively skip ahead to a precise point in the linearized history without requiring every intermediate transformation individually. To do this, it constructs a synthetic log entry containing a `ForceSnapshot` transformation which it inserts at the head of its local buffer of pending log entries. The log consumer then applies this transformation, producing a new `ClusterMetadata` (the exact one contained in the `LogState`) and publishing it as it would the result of any other transformation.

## Upgrading and Transitioning to TCM
Following an upgrade, nodes in an existing cluster will enter a minimal modification mode. In this state, the set of allowed cluster metadata modifications is constrained to include only the addition, removal and replacement of nodes, to allow failed hosts to be replaced during the
upgrade. In this mode the CMS has no members and each peer maintains its own `ClusterMetadata` instance independently. This metadata is intitialised at startup from system tables and gossip is used to propagate the permitted subset of metadata changes.

When the operator is ready, one node is chosen for promotion to the initial CMS, which is done manually via nodetool `cms initialize`. At this point, the candidate node will propose itself as the initial CMS and attempt to gain consensus from the rest of the cluster. If successful, it verifies that all peers have an identical view of cluster metadata and initialises the distributed log with a snapshot of that metadata.

Once this process is complete all future cluster metadata updates are performed via the CMS using the global log and reverting to the previous method of metadata management is not supported. Further members can and should be added to the CMS using nodetool's `cms reconfigure` command.

