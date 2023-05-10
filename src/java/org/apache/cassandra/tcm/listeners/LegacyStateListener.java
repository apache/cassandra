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
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.compatibility.GossipHelper;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.BootstrapAndReplace;
import org.apache.cassandra.tcm.sequences.InProgressSequences;

import static org.apache.cassandra.tcm.membership.NodeState.BOOTSTRAPPING;
import static org.apache.cassandra.tcm.membership.NodeState.BOOT_REPLACING;
import static org.apache.cassandra.tcm.membership.NodeState.LEFT;
import static org.apache.cassandra.tcm.membership.NodeState.MOVING;

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

        Set<NodeId> removed = Sets.difference(prev.directory.peerIds(), next.directory.peerIds());
        Set<NodeId> changed = new HashSet<>();
        for (NodeId node : next.directory.peerIds())
        {
            if (directoryEntryChangedFor(node, prev.directory, next.directory) || !prev.tokenMap.tokens(node).equals(next.tokenMap.tokens(node)))
                changed.add(node);
        }

        for (NodeId remove : removed)
        {
            GossipHelper.removeFromGossip(prev.directory.endpoint(remove));
            PeersTable.updateLegacyPeerTable(remove, prev, next);
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
                    case REGISTERED:
                        Gossiper.instance.maybeInitializeLocalState(SystemKeyspace.incrementAndGetGeneration());
                        break;
                    case JOINED:
                        SystemKeyspace.updateTokens(next.tokenMap.tokens());
                        // needed if we miss the REGISTERED above; Does nothing if we are already in epStateMap:
                        Gossiper.instance.maybeInitializeLocalState(SystemKeyspace.incrementAndGetGeneration());
                        SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
                        StreamSupport.stream(ColumnFamilyStore.all().spliterator(), false)
                                     .filter(cfs -> Schema.instance.getUserKeyspaces().names().contains(cfs.keyspace.getName()))
                                     .forEach(cfs -> cfs.indexManager.executePreJoinTasksBlocking(true));
                        if (prev.directory.peerState(change) == MOVING)
                            logger.info("Node {} state jump to NORMAL", next.directory.endpoint(change));
                        break;
                }
                if (next.directory.peerState(change) == LEFT)
                {
                    GossipHelper.mergeNodeToGossip(change, next, prev.tokenMap.tokens(change));
                    InetAddressAndPort endpoint = prev.directory.endpoint(change);
                    if (endpoint != null)
                        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.removeEndpoint(endpoint));
                }
                else if (NodeState.isBootstrap(next.directory.peerState(change)))
                {
                    // For compatibility with clients, ensure we set TOKENS for bootstrapping nodes in gossip.
                    // As these are not yet added to the token map they must be extracted from the in progress sequence.
                    Collection<Token> tokens = GossipHelper.getTokensFromSequence(change, next);
                    GossipHelper.mergeNodeToGossip(change, next, tokens);
                }
                else if (prev.directory.peerState(change) == BOOT_REPLACING)
                {
                    // legacy log message for compatibility (& tests)
                    InProgressSequence<?> sequence = prev.inProgressSequences.get(change);
                    if (sequence != null && sequence.kind() == InProgressSequences.Kind.REPLACE)
                    {
                        BootstrapAndReplace replace = (BootstrapAndReplace) sequence;
                        InetAddressAndPort replaced = prev.directory.endpoint(replace.startReplace.replaced());
                        InetAddressAndPort replacement = prev.directory.endpoint(change);
                        Collection<Token> tokens = GossipHelper.getTokensFromSequence(replace);
                        logger.info("Node {} will complete replacement of {} for tokens {}", replacement, replaced, tokens);
                    }
                }
                else
                {
                    GossipHelper.mergeNodeToGossip(change, next);
                    PeersTable.updateLegacyPeerTable(change, prev, next);
                }
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
