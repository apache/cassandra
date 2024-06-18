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

public class LegacyStateListener implements ChangeListener.Async
{
    private static final Logger logger = LoggerFactory.getLogger(LegacyStateListener.class);

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        if (!fromSnapshot &&
            next.directory.lastModified().equals(prev.directory.lastModified()) &&
            next.tokenMap.lastModified().equals(prev.tokenMap.lastModified()))
            return;

        Set<InetAddressAndPort> removedAddr = Sets.difference(new HashSet<>(prev.directory.allAddresses()),
                                                              new HashSet<>(next.directory.allAddresses()));

        Set<NodeId> changed = new HashSet<>();
        for (NodeId node : next.directory.peerIds())
        {
            if (directoryEntryChangedFor(node, prev.directory, next.directory) || !prev.tokenMap.tokens(node).equals(next.tokenMap.tokens(node)))
                changed.add(node);
        }

        for (InetAddressAndPort remove : removedAddr)
        {
            GossipHelper.evictFromMembership(remove);
            PeersTable.removeFromSystemPeersTables(remove);
        }

        for (NodeId change : changed)
        {
            // next.myNodeId() can be null during replay (before we have registered)
            if (next.myNodeId() != null && next.myNodeId().equals(change))
            {
                switch (next.directory.peerState(change))
                {
                    case BOOTSTRAPPING:
                        if (prev.directory.peerState(change) != BOOTSTRAPPING)
                        {
                            // legacy log messages for tests
                            logger.info("JOINING: Starting to bootstrap");
                            logger.info("JOINING: calculation complete, ready to bootstrap");
                        }
                        break;
                    case BOOT_REPLACING:
                    case REGISTERED:
                        break;
                    case JOINED:
                        SystemKeyspace.updateTokens(next.directory.endpoint(change), next.tokenMap.tokens(change));
                        // needed if we miss the REGISTERED above; Does nothing if we are already in epStateMap:
                        Gossiper.instance.maybeInitializeLocalState(SystemKeyspace.incrementAndGetGeneration());
                        StreamSupport.stream(ColumnFamilyStore.all().spliterator(), false)
                                     .filter(cfs -> Schema.instance.getUserKeyspaces().names().contains(cfs.keyspace.getName()))
                                     .forEach(cfs -> cfs.indexManager.executePreJoinTasksBlocking(true));
                        if (prev.directory.peerState(change) == MOVING)
                            logger.info("Node {} state jump to NORMAL", next.directory.endpoint(change));
                        break;
                }
                // Maybe intitialise local epstate whatever the node state because we could be processing after a
                // replay and so may have not seen any previous local states, making this the first mutation of gossip
                // state for the local node.
                Gossiper.instance.maybeInitializeLocalState(SystemKeyspace.incrementAndGetGeneration());
                Gossiper.instance.addLocalApplicationState(SCHEMA, StorageService.instance.valueFactory.schema(next.schema.getVersion()));
            }

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
                // legacy log message for compatibility (& tests)
                MultiStepOperation<?> sequence = prev.inProgressSequences.get(change);
                if (sequence != null && sequence.kind() == MultiStepOperation.Kind.REPLACE)
                {
                    BootstrapAndReplace replace = (BootstrapAndReplace) sequence;
                    InetAddressAndPort replaced = prev.directory.endpoint(replace.startReplace.replaced());
                    InetAddressAndPort replacement = prev.directory.endpoint(change);
                    Collection<Token> tokens = GossipHelper.getTokensFromOperation(replace);
                    logger.info("Node {} will complete replacement of {} for tokens {}", replacement, replaced, tokens);
                    if (!replacement.equals(replaced))
                    {
                        for (Token token : tokens)
                            logger.warn("Token {} changing ownership from {} to {}", token, replaced, replacement);
                    }
                    Gossiper.instance.mergeNodeToGossip(change, next, tokens);
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

    private boolean directoryEntryChangedFor(NodeId nodeId, Directory prev, Directory next)
    {
        return prev.peerState(nodeId) != next.peerState(nodeId) ||
               !Objects.equals(prev.getNodeAddresses(nodeId), next.getNodeAddresses(nodeId)) ||
               !Objects.equals(prev.version(nodeId), next.version(nodeId));
    }
}
