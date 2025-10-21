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

# TCM Implementation Details

This document will walk you through the core classes involved in Transactional Cluster Metadata. It describes a process of a node bringup into the existing TCM cluster. Each section will be prefixed by the header holding key classes that are used/described in the setion.

## Startup: Discovery, Vote

Boot process in TCM is very similar to the previously existing one, but is now split into several different classes rather than being mostly in `StorageService`. At first, `ClusterMetadataService` is initialized using `Startup#initialize`. Node determines its startup mode, which will be `Vote` in a usual case, which means that the node will initialize itself as a non-CMS node and will attempt to discover an existing CMS service or, failing that, participate in a vote to establish a new one with other discovered participants. If the seeds are configured correctly, the node is going to learn from the seed about existing CMS nodes, and will try contacting them to fetch the initial log.

Node then continues startup, and eventually gets to `StorageService#initServer`, where it, among other things, gossips with CMS nodes to get a fresh view of the cluster for FD purposes, and then waits for Gossip to settle (again, for FD purposes).

## Registration: ClusterMetadata, Transformation, RemoteProcessor

Before joining the ring, the node has to register in order to obtain `NodeId`, which happens in `Register#maybeRegister`. Registration happens by committing a `Register` transformation using `ClusterMetadataService#commit` method. `Register` and other transformations are side-effect free functions mapping an instance of immutable `ClusterMetadata` to next `ClusterMetadata`. `ClusterMetadata` holds all information about cluster: directory of registered nodes, schema, node states and data ownership.

Since the node executing register is not a CMS node, it is going to use a `RemoteProcessor` in order to perform this commit. `RemoteProcessor` is a simple RPC tool that serializes transformation and attempts to execute it by contacting CMS nodes and sending them `TCM_COMMIT_REQ`.

## Commit Request: PaxosBackedProcessor, DistributedMetadataLogKeyspace, Retry

When a CMS node receives a commit request, it deserializes and attempts to execute the transformation using `PaxosBackedProcessor`. Paxos backed processor stores an entire cluster metadata log in the `system_cluster_metadata.``distributed_metadata_log` table. It performs a simple CAS LWT that attempts to append a new entry to the log with an `Epoch` that is strictly consecutive to the last one. `Epoch` is a monotonically incrementing counter of `ClusterMetadata` versions.

Both remote and paxos-backed processors are using `Retry` class for managing retries. Remote processor sets a deadline for its retries using `tcm_await_timeout`. CMS-local processor permits itself to use at most `tcm_rpc_timeout` for its attempts to retry.

`PaxosBackedProcessor` then attempts to execute `Transformation`. Result of the execution can be either `Success` or `Reject`. `Reject`s are not persisted in the log, and are linearized using a read that confirms that transformation was executed against the highest epoch. Examples of `Reject`s are validation errors, exceptions encountered while attempting to execute transformation, etc. For example, `Register` would return a rejection if a node with the same IP address already exists in the registry.

## Commit Response: Entry, LocalLog

After `PaxosBackedProcessor` suceeds with committing the entry to the distributed log, it broadcasts the commit result that contains `Entry` holding newly appended transformation to the rest of the cluster using `Replicator` (which simply iterates all nodes in the directory, informing them about the new epoch). This operation does not need to be reliable and has no retries. In other words, if a node was down during CMS attempt to replicate entries to it, it will inevitably learn about the new epoch later when it comes back alive.

Along with committed `Entry`, the response from CMS to the peer which submitted it also contains all entries that will allow the node that has initiated the commit to fully catch up to the epoch enacted by the committed transformation.

When `RemoteProcessor` receives a response from CMS node, it appends all received entries to the `LocalLog`. `LocalLog` processes the backlog of pending entries and enacts a new epoch by constructing new `ClusterMetadata`.

## Bootstrap: InProgressSequence, PrepareJoin, BootstrapAndJoin

At that point, the node is ready to start the process of joining the ring. It begins in `Startup#startup`. `Startup#getInitialTransformation` determines that the node should start regular bootstrap process (as opposed to replace), and the node proceeds with commit of `PrepareJoin` transformation. During `PrepareJoin`, `ClusterMetadata` is changed in the following ways:

* Ranges that will be affected by the bootstrap of the node are locked (see `LockedRanges`)
    * If computed locked ranges intersect with ranges that were locked before this transformation got executed, `PrepareJoin` is rejected.
* `InProgressSequence`, holding the three transformations (`StartJoin`, `MidJoin` and `FinishJoin`), is computed and added to `InProgressSequences` map.
    * If any in-progress sequences associated with the current node are present, `PrepareJoin` is rejected.
* `AffectedRanges`, ranges whose placements are going to be changed while executing this sequence, are computed and returned as a part of commit success message.

`InProgressSequence` is then executed step-by-step. All local operations that the node has to perform between executing these steps are implemented as a part of the in-progress sequence (see `BootstrapAndJoin#executeNext`). We make *no assumptions* about liveness of the node between execution of in-progress sequence steps. For example, the node may crash after executing `PrepareJoin` but before it updates tokens in the local keyspace. So the only assumption we make is that `SystemKeyspace.updateLocalTokens` has to be called *before* `StartJoin` is committed. Similarly, owned data has to be streamed towards the node *before* it becomes a part of a read quorum, so even if the node crashes or is restarted an arbitrary number of times during streaming.

In order to ensure quorum consistency, before executing each next step, the node has to await on the `ProgressBarrier`. CEP-21 contains a detailed explanation about why progress barriers are necessary. For the purpose of this document, it suffices to say that majority of owners of the `AffectedRanges` have to learn about the epoch enacting the previous step before each next step can be executed. This is done in order to preserve replication factor for eventually consistent queries.

Upon executing all steps in the progress sequence, ranges are unlocked, and sequence itself is removed from `ClusterMetadata`.


## Querying: CoordinatorBehindException, FetchPeerLog, FetchCMSLog

As the node starts participating in reads and writes, it may happen that its view of the ring or schema becomes divergent from other nodes. TCM makes best effort to minimize the time window of this happening, but in a distributed system at least some delay is inevitable. TCM solves this problem by including the highest `Epoch` known by the node in every request that the node coordinates, and in every response to the coordinator when serving as a replica.

Replicas can check the schema and ring consistency of the *current* request by comparing the `Epoch` that coordinator has with the epoch when schema was last modified, and when the placements for the given range were last modified. If it happens that the replica knows that coordinator couldn’t have known about either schema, or the ring, it will throw `CoordinatorBehindException`. In all other cases (i.e. when either coordinator, or the replica are aware of the higher `Epoch`, but existence of this epoch does not influence consistency or outcome of the given query), lagging participant will issue an asynchonous `TCM_FETCH_PEER_LOG_REQ` and attempt to catch up from the peer. Failing that, it will attempt to catch up from the CMS node using `TCM_FETCH_CMS_LOG_REQ`.

After coordinator has collected enough responses, it compares its `Epoch` with the `Epoch` that was used to construct the `ReplicaPlan` for the query it is coordinating. If epochs are different, it checks if collected replica responses still correspond to the consistency level query was executed at.

