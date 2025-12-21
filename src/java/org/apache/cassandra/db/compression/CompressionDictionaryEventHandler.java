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
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Handles compression dictionary events including training completion and cluster notifications.
 * <p>
 * This class handles:
 * - Broadcasting dictionary updates to cluster nodes
 * - Retrieving new dictionaries when notified by other nodes
 * - Managing dictionary cache updates
 */
public class CompressionDictionaryEventHandler implements ICompressionDictionaryEventHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CompressionDictionaryEventHandler.class);

    private final ColumnFamilyStore cfs;
    private final String keyspaceName;
    private final String tableName;
    private final ICompressionDictionaryCache cache;

    public CompressionDictionaryEventHandler(ColumnFamilyStore cfs, ICompressionDictionaryCache cache)
    {
        this.cfs = cfs;
        this.keyspaceName = cfs.keyspace.getName();
        this.tableName = cfs.getTableName();
        this.cache = cache;
    }

    @Override
    public void onNewDictionaryTrained(CompressionDictionary.DictId dictionaryId)
    {
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
    public void onNewDictionaryAvailable(CompressionDictionary.DictId dictionaryId)
    {
        // Best effort to retrieve the dictionary; otherwise, the periodic task should retrieve the dictionary later
        ScheduledExecutors.nonPeriodicTasks.submit(() -> {
            try
            {
                if (!cfs.metadata().params.compression.isDictionaryCompressionEnabled())
                {
                    return;
                }

                CompressionDictionary dictionary = SystemDistributedKeyspace.retrieveCompressionDictionary(keyspaceName, tableName, dictionaryId.id);
                cache.add(dictionary);
            }
            catch (Exception e)
            {
                logger.warn("Failed to retrieve compression dictionary for {}.{}. {}",
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
