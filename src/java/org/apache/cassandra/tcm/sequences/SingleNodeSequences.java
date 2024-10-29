/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tcm.sequences;

import java.util.Collections;
import java.util.EnumSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.transformations.CancelInProgressSequence;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareMove;

import static org.apache.cassandra.service.StorageService.Mode.LEAVING;
import static org.apache.cassandra.service.StorageService.Mode.NORMAL;
import static org.apache.cassandra.service.StorageService.Mode.DECOMMISSION_FAILED;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

/**
 * This exists simply to group the static entrypoints to sequences that modify a single node
 * e.g. decommission, remove, move
 */
public interface SingleNodeSequences
{
    Logger logger = LoggerFactory.getLogger(SingleNodeSequences.class);

    /**
     * Entrypoint to begin node decommission process.
     *
     * @param shutdownNetworking if set to true, will also shut down networking on completion
     * @param force if set to true, will decommission the node even if this would mean there will be not enough nodes
     *              to satisfy replication factor
     */
    static void decommission(boolean shutdownNetworking, boolean force)
    {
        if (ClusterMetadataService.instance().isMigrating() || ClusterMetadataService.state() == ClusterMetadataService.State.GOSSIP)
            throw new IllegalStateException("This cluster is migrating to cluster metadata, can't decommission until that is done.");

        ClusterMetadata metadata = ClusterMetadata.current();

        StorageService.Mode mode = StorageService.instance.operationMode();
        if (!EnumSet.of(LEAVING, NORMAL, DECOMMISSION_FAILED).contains(mode))
            throw new UnsupportedOperationException("Node in " + mode + " state; wait for status to become normal");
        logger.debug("DECOMMISSIONING");

        NodeId self = metadata.myNodeId();

        ReconfigureCMS.maybeReconfigureCMS(metadata, getBroadcastAddressAndPort());
        MultiStepOperation<?> inProgress = metadata.inProgressSequences.get(self);

        if (inProgress == null)
        {
            logger.info("starting decommission with {} {}", metadata.epoch, self);
            ClusterMetadataService.instance().commit(new PrepareLeave(self,
                                                                      force,
                                                                      ClusterMetadataService.instance().placementProvider(),
                                                                      LeaveStreams.Kind.UNBOOTSTRAP));
        }
        else if (InProgressSequences.isLeave(inProgress))
        {
            logger.info("Resuming decommission @ {} (current epoch = {}): {}", inProgress.latestModification, metadata.epoch, inProgress.status());
        }
        else
        {
            throw new IllegalArgumentException("Can not decommission a node that has an in-progress sequence");
        }

        InProgressSequences.finishInProgressSequences(self);
        if (shutdownNetworking)
            StorageService.instance.shutdownNetworking();
    }

    /**
     * Entrypoint to begin node removal process
     *
     * @param toRemove id of the node to remove
     * @param force if set to true, will remove the node even if this would mean there will be not enough nodes
     *              to satisfy replication factor
     */
    static void removeNode(NodeId toRemove, boolean force)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        if (toRemove.equals(metadata.myNodeId()))
            throw new UnsupportedOperationException("Cannot remove self");
        InetAddressAndPort endpoint = metadata.directory.endpoint(toRemove);
        if (endpoint == null)
            throw new UnsupportedOperationException("Host ID not found.");
        if (Gossiper.instance.getLiveMembers().contains(endpoint))
            throw new UnsupportedOperationException("Node " + endpoint + " is alive and owns this ID. Use decommission command to remove it from the ring");

        NodeState removeState = metadata.directory.peerState(toRemove);
        if (removeState == null)
            throw new UnsupportedOperationException("Node to be removed is not a member of the token ring");
        if (removeState == NodeState.LEAVING)
            logger.warn("Node {} is already leaving or being removed, continuing removal anyway", endpoint);

        if (metadata.inProgressSequences.contains(toRemove))
            throw new UnsupportedOperationException("Can not remove a node that has an in-progress sequence");

        ReconfigureCMS.maybeReconfigureCMS(metadata, endpoint);

        logger.info("starting removenode with {} {}", metadata.epoch, toRemove);

        ClusterMetadataService.instance().commit(new PrepareLeave(toRemove,
                                                                  force,
                                                                  ClusterMetadataService.instance().placementProvider(),
                                                                  LeaveStreams.Kind.REMOVENODE));
        InProgressSequences.finishInProgressSequences(toRemove);
    }

    /**
     * move the node to new token or find a new token to boot to according to load
     *
     * @param newToken new token to boot to, or if null, find balanced token to boot to
     */
    static void move(Token newToken)
    {
        if (ClusterMetadataService.instance().isMigrating() || ClusterMetadataService.state() == ClusterMetadataService.State.GOSSIP)
            throw new IllegalStateException("This cluster is migrating to cluster metadata, can't move until that is done.");

        if (newToken == null)
            throw new IllegalArgumentException("Can't move to the undefined (null) token.");

        if (ClusterMetadata.current().tokenMap.tokens().contains(newToken))
            throw new IllegalArgumentException(String.format("target token %s is already owned by another node.", newToken));

        // address of the current node
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId self = metadata.myNodeId();
        // This doesn't make any sense in a vnodes environment.
        if (metadata.tokenMap.tokens(self).size() > 1)
        {
            logger.error("Invalid request to move(Token); This node has more than one token and cannot be moved thusly.");
            throw new UnsupportedOperationException("This node has more than one token and cannot be moved thusly.");
        }

        ClusterMetadataService.instance().commit(new PrepareMove(self,
                                                                 Collections.singleton(newToken),
                                                                 ClusterMetadataService.instance().placementProvider(),
                                                                 true));
        InProgressSequences.finishInProgressSequences(self);

        if (logger.isDebugEnabled())
            logger.debug("Successfully moved to new token {}", StorageService.instance.getLocalTokens().iterator().next());
    }

    static void resumeMove()
    {
        if (ClusterMetadataService.instance().isMigrating() || ClusterMetadataService.state() == ClusterMetadataService.State.GOSSIP)
            throw new IllegalStateException("This cluster is migrating to cluster metadata, can't move until that is done.");

        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId self = metadata.myNodeId();
        MultiStepOperation<?> sequence = metadata.inProgressSequences.get(self);
        if (sequence == null || sequence.kind() != MultiStepOperation.Kind.MOVE)
        {
            String msg = "No move operation in progress, can't resume";
            logger.info(msg);
            throw new IllegalStateException(msg);
        }
        if (StorageService.instance.operationMode() != StorageService.Mode.MOVE_FAILED)
        {
            String msg = "Can't resume a move operation unless it has failed";
            logger.info(msg);
            throw new IllegalStateException(msg);
        }
        StorageService.instance.clearTransientMode();
        InProgressSequences.finishInProgressSequences(self);
    }

    static void abortMove()
    {
        if (ClusterMetadataService.instance().isMigrating() || ClusterMetadataService.state() == ClusterMetadataService.State.GOSSIP)
            throw new IllegalStateException("This cluster is migrating to cluster metadata, can't move until that is done.");

        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId self = metadata.myNodeId();
        MultiStepOperation<?> sequence = metadata.inProgressSequences.get(self);
        if (sequence == null || sequence.kind() != MultiStepOperation.Kind.MOVE)
        {
            String msg = "No move operation in progress, can't abort";
            logger.info(msg);
            throw new IllegalStateException(msg);
        }
        if (StorageService.instance.operationMode() != StorageService.Mode.MOVE_FAILED)
        {
            String msg = "Can't abort a move operation unless it has failed";
            logger.info(msg);
            throw new IllegalStateException(msg);
        }
        StorageService.instance.clearTransientMode();
        ClusterMetadataService.instance().commit(new CancelInProgressSequence(self));
    }

}
