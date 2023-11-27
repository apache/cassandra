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

import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.MovementMap;

public class ReplaceSameAddress
{
    private static final Logger logger = LoggerFactory.getLogger(ReplaceSameAddress.class);

    public static MovementMap movementMap(NodeId nodeId, ClusterMetadata metadata)
    {
        MovementMap.Builder builder = MovementMap.builder();
        InetAddressAndPort addr = metadata.directory.endpoint(nodeId);
        metadata.placements.forEach((params, placement) -> {
            EndpointsByReplica.Builder sources = new EndpointsByReplica.Builder();
            placement.reads.byEndpoint().get(addr).forEach(destination -> {
                placement.reads.forRange(destination.range()).forEach(potentialSource -> {
                    if (!potentialSource.endpoint().equals(addr))
                        sources.put(destination, potentialSource);
                });
            });
            builder.put(params, sources.build());
        });
        return builder.build();
    }

    public static void streamData(NodeId nodeId, ClusterMetadata metadata, boolean shouldBootstrap, boolean finishJoiningRing)
    {
        BootstrapAndReplace.checkUnsafeReplace(shouldBootstrap);

        //only go into hibernate state if replacing the same address (CASSANDRA-8523)
        logger.warn("Writes will not be forwarded to this node during replacement because it has the same address as " +
                    "the node to be replaced ({}). If the previous node has been down for longer than max_hint_window, " +
                    "repair must be run after the replacement process in order to make this node consistent.",
                    DatabaseDescriptor.getReplaceAddress());

        BootstrapAndReplace.gossipStateToHibernate(metadata, nodeId);

        SystemKeyspace.updateLocalTokens(metadata.tokenMap.tokens(nodeId));

        if (shouldBootstrap)
        {
            boolean dataAvailable = BootstrapAndJoin.bootstrap(metadata.tokenMap.tokens(nodeId),
                                                               StorageService.INDEFINITE,
                                                               metadata,
                                                               metadata.directory.endpoint(nodeId),
                                                               movementMap(nodeId, metadata),
                                                               null);

            if (!dataAvailable)
            {
                logger.warn("Some data streaming failed. Use nodetool to check bootstrap state and resume. " +
                            "For more, see `nodetool help bootstrap`. {}", SystemKeyspace.getBootstrapState());
                throw new IllegalStateException("Could not finish join for during replacement");
            }
        }

        if (finishJoiningRing)
        {
            SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
            StreamSupport.stream(ColumnFamilyStore.all().spliterator(), false)
                         .filter(cfs -> Schema.instance.getUserKeyspaces().names().contains(cfs.keyspace.getName()))
                         .forEach(cfs -> cfs.indexManager.executePreJoinTasksBlocking(true));
            Gossiper.instance.mergeNodeToGossip(metadata.myNodeId(), metadata);
        }
    }
}
