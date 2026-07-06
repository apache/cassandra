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

package org.apache.cassandra.tcm.listeners;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.StreamSupport;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.virtual.PeersTable;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.compatibility.GossipHelper;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.BootstrapAndReplace;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.gms.ApplicationState.SCHEMA;
import static org.apache.cassandra.tcm.membership.NodeState.BOOTSTRAPPING;
import static org.apache.cassandra.tcm.membership.NodeState.BOOT_REPLACING;
import static org.apache.cassandra.tcm.membership.NodeState.LEFT;
import static org.apache.cassandra.tcm.membership.NodeState.MOVING;
import static org.apache.cassandra.tcm.membership.NodeState.REGISTERED;

public class LegacyStateListener implements ChangeListener
{
    private static final Logger logger = LoggerFactory.getLogger(LegacyStateListener.class);

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        if (!fromSnapshot &&
            next.directory.lastModified().equals(prev.directory.lastModified()) &&
            next.tokenMap.lastModified().equals(prev.tokenMap.lastModified()))
            return;

        Set<InetAddressAndPort> removedAddr = Sets.difference(prev.directory.allAddresses(), next.directory.allAddresses());

        Set<NodeId> changed = new HashSet<>();
        for (NodeId node : next.directory.peerIds())
        {
            if (prev.epoch.isEqualOrBefore(Epoch.FIRST)
                || directoryEntryChangedFor(node, prev.directory, next.directory)
                || !prev.tokenMap.tokens(node).equals(next.tokenMap.tokens(node)))
            {
                changed.add(node);
            }
        }

        // next.myNodeId() can be UNREGISTERED during replay (before we have registered) but if not and
        // there is a relevant change to the state of the local node, process that synchronously.
        if (next.myNodeId() != NodeId.UNREGISTERED && changed.contains(next.myNodeId()))
        {
            // Default is to process updates for the local node synchronously, overridable via config/hotprop
            if (DatabaseDescriptor.getLegacyStateListenerSyncLocalUpdates())
                processChangesToLocalState(prev, next, next.myNodeId());
            else
                ScheduledExecutors.optionalTasks.submit(() -> processChangesToLocalState(prev, next, next.myNodeId()));

            changed.remove(next.myNodeId());
        }

        // Schedule async processing of changes to peers and removing unregistered nodes (potentially including the
        // local node).
        ScheduledExecutors.optionalTasks.submit(() -> {
            processRemovedNodes(removedAddr);
            processChangesToRemotePeers(prev, next, changed);
        });
    }

    private void processChangesToLocalState(ClusterMetadata prev, ClusterMetadata next, NodeId localId)
    {
        logger.info("Processing changes to local node state {} for epoch {}->{}", localId, prev.epoch.getEpoch(), next.epoch.getEpoch());
        Collection<Token> tokensForGossip = next.tokenMap.tokens(localId);
        NodeState state = next.directory.peerState(localId);
        switch (state)
        {
            case BOOTSTRAPPING:
            case BOOT_REPLACING:
                // For compatibility with clients, ensure we set TOKENS for bootstrapping nodes in gossip.
                // As these are not yet added to the token map they must be extracted from the in progress sequence.
                tokensForGossip = GossipHelper.getTokensFromOperation(localId, next);
                if (state == BOOTSTRAPPING && prev.directory.peerState(localId) != BOOTSTRAPPING)
                {
                    // legacy log messages for tests
                    logger.info("JOINING: Starting to bootstrap");
                    logger.info("JOINING: calculation complete, ready to bootstrap");
                }
                break;
            case JOINED:
                tokensForGossip = next.tokenMap.tokens(localId);
                SystemKeyspace.updateTokens(next.directory.endpoint(localId), tokensForGossip);
                Set<String> userKeyspaces = Schema.instance.getUserKeyspaces().names();
                StreamSupport.stream(ColumnFamilyStore.all().spliterator(), false)
                             .filter(cfs -> userKeyspaces.contains(cfs.keyspace.getName()))
                             .forEach(cfs -> cfs.indexManager.executePreJoinTasksBlocking(true));
                NodeState previousState = prev.directory.peerState(localId);
                if (previousState == MOVING)
                {
                    logger.info("Node {} state jump to NORMAL", next.directory.endpoint(localId));
                }
                else if (previousState == BOOT_REPLACING)
                {
                    // legacy log message for compatibility (& tests)
                    MultiStepOperation<?> sequence = prev.inProgressSequences.get(localId);
                    if (sequence != null && sequence.kind() == MultiStepOperation.Kind.REPLACE)
                    {
                        logCompletedReplacement(prev.directory, (BootstrapAndReplace) sequence);
                        tokensForGossip = GossipHelper.getTokensFromOperation(sequence);
                    }
                }
                break;
            case MOVING:
                logger.debug("Node {} state MOVING, tokens {}", next.directory.endpoint(localId), prev.tokenMap.tokens(localId));
                tokensForGossip = next.tokenMap.tokens(localId);
                break;
            case LEFT:
                tokensForGossip = prev.tokenMap.tokens(localId);
                break;
        }

        // Maybe initialise local epstate whatever the node state because we could be processing after a
        // replay and so may have not seen any previous local states, making this the first mutation of gossip
        // state for the local node.
        Gossiper.instance.maybeInitializeLocalState(SystemKeyspace.incrementAndGetGeneration());
        Gossiper.instance.addLocalApplicationState(SCHEMA, StorageService.instance.valueFactory.schema(next.schema.getVersion()));
        // Pull node properties from cluster metadata into gossip, except if the node is only in the REGISTERED state
        // as that has no equivalent gossip STATUS
        if (state != REGISTERED)
            Gossiper.instance.mergeNodeToGossip(localId, next, tokensForGossip);
        // if the local node's location has changed, update system.local.
        if (!next.directory.location(localId).equals(prev.directory.location(localId)))
            SystemKeyspace.updateLocation(next.directory.location(localId));
    }

    private void processChangesToRemotePeers(ClusterMetadata prev, ClusterMetadata next, Set<NodeId> changed)
    {
        for (NodeId change : changed)
        {
            logger.info("Processing changes to peer {} for epoch {}->{}", change, prev.epoch.getEpoch(), next.epoch.getEpoch());
            if (next.directory.peerState(change) == REGISTERED)
            {
                // Re-establish any connections made prior to this node registering
                InetAddressAndPort endpoint = next.directory.endpoint(change);
                logger.info("Peer with address {} has registered, interrupting any previously established connections", endpoint);
                MessagingService.instance().interruptOutbound(endpoint);
            }
            else if (next.directory.peerState(change) == LEFT)
            {
                Gossiper.instance.mergeNodeToGossip(change, next, prev.tokenMap.tokens(change));
                InetAddressAndPort endpoint = prev.directory.endpoint(change);
                if (endpoint != null)
                {
                    PeersTable.updateLegacyPeerTable(change, prev, next);
                    if (!endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
                        GossipHelper.removeFromGossip(endpoint);
                }
            }
            else if(next.directory.peerState(change) == MOVING)
            {
                // legacy log messages for tests
                logger.debug("Node {} state MOVING, tokens {}", next.directory.endpoint(change), prev.tokenMap.tokens(change));
                Gossiper.instance.mergeNodeToGossip(change, next);
                PeersTable.updateLegacyPeerTable(change, prev, next);
            }
            else if (NodeState.isBootstrap(next.directory.peerState(change)))
            {
                // For compatibility with clients, ensure we set TOKENS for bootstrapping nodes in gossip.
                // As these are not yet added to the token map they must be extracted from the in progress sequence.
                Collection<Token> tokens = GossipHelper.getTokensFromOperation(change, next);
                Gossiper.instance.mergeNodeToGossip(change, next, tokens);
            }
            else if (prev.directory.peerState(change) == BOOT_REPLACING)
            {
                MultiStepOperation<?> sequence = prev.inProgressSequences.get(change);
                if (sequence != null && sequence.kind() == MultiStepOperation.Kind.REPLACE)
                {
                    logCompletedReplacement(prev.directory, (BootstrapAndReplace) sequence);
                    Gossiper.instance.mergeNodeToGossip(change, next, GossipHelper.getTokensFromOperation(sequence));
                    PeersTable.updateLegacyPeerTable(change, prev, next);
                }
            }
            else
            {
                Gossiper.instance.mergeNodeToGossip(change, next);
                PeersTable.updateLegacyPeerTable(change, prev, next);
            }
        }
    }

    private void processRemovedNodes(Set<InetAddressAndPort> removed)
    {
        for (InetAddressAndPort remove : removed)
        {
            GossipHelper.removeAndEvict(remove);
            PeersTable.removeFromSystemPeersTables(remove);
        }
    }

    private void logCompletedReplacement(Directory directory, BootstrapAndReplace sequence)
    {
        // legacy log message for compatibility (& tests)
        InetAddressAndPort replaced = directory.endpoint(sequence.startReplace.replaced());
        InetAddressAndPort replacement = directory.endpoint(sequence.startReplace.replacement());
        Collection<Token> tokens = GossipHelper.getTokensFromOperation(sequence);
        logger.info("Node {} will complete replacement of {} for tokens {}", replacement, replaced, tokens);
        if (!replacement.equals(replaced))
        {
            for (Token token : tokens)
                logger.warn("Token {} changing ownership from {} to {}", token, replaced, replacement);
        }
    }

    private boolean directoryEntryChangedFor(NodeId nodeId, Directory prev, Directory next)
    {
        return prev.peerState(nodeId) != next.peerState(nodeId) ||
               !Objects.equals(prev.getNodeAddresses(nodeId), next.getNodeAddresses(nodeId)) ||
               !Objects.equals(prev.version(nodeId), next.version(nodeId)) ||
               !Objects.equals(prev.location(nodeId), next.location(nodeId));

    }
}
