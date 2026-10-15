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

package org.apache.cassandra.db.compression;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Handles compression dictionary events including training completion and cluster notifications.
 * <p>
 * This class handles:
 * - Notifying other nodes about a dictionary trained, imported or promoted to be used by all nodes, and the node
 *   a discarded dictionary was local to
 * - Refreshing the current dictionary when notified, or when this node promoted a dictionary
 */
public class CompressionDictionaryEventHandler implements ICompressionDictionaryEventHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CompressionDictionaryEventHandler.class);

    private final ColumnFamilyStore cfs;
    private final String keyspaceName;
    private final String tableName;
    // makes the newest dictionary used by all nodes or local to this node the current one
    private final Runnable refresh;

    public CompressionDictionaryEventHandler(ColumnFamilyStore cfs, Runnable refresh)
    {
        this.cfs = cfs;
        this.keyspaceName = cfs.keyspace.getName();
        this.tableName = cfs.getTableName();
        this.refresh = refresh;
    }

    @Override
    public void onNewDictionaryTrained(CompressionDictionary.DictId dictionaryId, boolean global)
    {
        if (!global)
            return;

        logger.info("Notifying cluster about dictionary update for {}.{} with {}",
                    keyspaceName, tableName, dictionaryId);

        CompressionDictionaryUpdateMessage message = new CompressionDictionaryUpdateMessage(cfs.metadata().id, dictionaryId);
        Collection<InetAddressAndPort> allNodes = ClusterMetadata.current().directory.allJoinedEndpoints();
        // Broadcast notification using the fire-and-forget fashion
        for (InetAddressAndPort node : allNodes)
        {
            if (node.equals(FBUtilities.getBroadcastAddressAndPort())) // skip ourself
                continue;
            sendNotification(node, message);
        }
    }

    @Override
    public void onDictionaryDiscarded(CompressionDictionary.DictId dictionaryId, NodeId owner)
    {
        // a node which left the cluster neither uses the dictionary nor receives messages anymore
        Directory directory = ClusterMetadata.current().directory;
        NodeState state = directory.peerState(owner);
        InetAddressAndPort endpoint = directory.endpoint(owner);
        if (state == null || state == NodeState.LEFT || endpoint == null)
            return;

        logger.info("Notifying {} about discarded dictionary {} for {}.{}", endpoint, dictionaryId, keyspaceName, tableName);
        sendNotification(endpoint, new CompressionDictionaryUpdateMessage(cfs.metadata().id, dictionaryId));
    }

    @Override
    public void onNewDictionaryAvailable(CompressionDictionary.DictId dictionaryId)
    {
        // Whether a dictionary was trained, imported, promoted or discarded, this node refreshes its current
        // dictionary. Best effort; otherwise, the periodic task refreshes it later.
        ScheduledExecutors.nonPeriodicTasks.submit(() -> {
            try
            {
                if (!cfs.metadata().params.compression.isDictionaryCompressionEnabled())
                {
                    return;
                }

                refresh.run();
            }
            catch (Exception e)
            {
                logger.warn("Failed to refresh compression dictionary for {}.{} on the announcement of {}; " +
                            "this node refreshes it on the next periodic refresh.",
                            keyspaceName, tableName, dictionaryId, e);
            }
        });
    }

    // Best effort to notify the peer regarding the new dictionary being available to pull.
    // If the request fails, each peer has periodic task scheduled to pull.
    private void sendNotification(InetAddressAndPort target, CompressionDictionaryUpdateMessage message)
    {
        logger.debug("Sending dictionary update notification for {} to {}", message.dictionaryId, target);

        Message<CompressionDictionaryUpdateMessage> msg = Message.out(Verb.DICTIONARY_UPDATE_REQ, message);
        MessagingService.instance()
                        .sendWithResponse(target, msg)
                        .addListener(future -> {
                            if (future.isSuccess())
                            {
                                logger.debug("Successfully sent dictionary update notification to {}", target);
                            }
                            else
                            {
                                logger.warn("Failed to send dictionary update notification to {}",
                                            target, future.cause());
                            }
                        });
    }
}
