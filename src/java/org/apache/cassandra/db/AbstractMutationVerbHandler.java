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

package org.apache.cassandra.db;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.CoordinatorBehindException;
import org.apache.cassandra.exceptions.InvalidRoutingException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.utils.NoSpamLogger;

public abstract class AbstractMutationVerbHandler<T extends IMutation> implements IVerbHandler<T>
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractMutationVerbHandler.class);
    private static final String logMessageTemplate = "Received mutation from {} for token {} outside valid range for keyspace {}";

    public void doVerb(Message<T> message) throws IOException
    {
        processMessage(message, message.respondTo());
    }

    protected void processMessage(Message<T> message, InetAddressAndPort respondTo)
    {
        if (message.epoch().isAfter(Epoch.EMPTY))
        {
            ClusterMetadata metadata = ClusterMetadata.current();
            metadata = checkTokenOwnership(metadata, message);
            metadata = checkSchemaVersion(metadata, message);
        }
        applyMutation(message, respondTo);
    }

    abstract void applyMutation(Message<T> message, InetAddressAndPort respondToAddress);

    private ClusterMetadata checkTokenOwnership(ClusterMetadata metadata, Message<T> message)
    {
        String keyspace = message.payload.getKeyspaceName();
        DecoratedKey key = message.payload.key();

        VersionedEndpoints.ForToken forToken = writePlacements(metadata, keyspace, key);

        if (message.epoch().isAfter(metadata.epoch))
        {
            // If replica detects that coordinator has made an out-of-range request, it has to catch up blockingly,
            // since coordinator's routing may be more recent.
            if (!forToken.get().containsSelf())
            {
                metadata = ClusterMetadataService.instance().fetchLogFromPeerOrCMS(metadata, message.from(), message.epoch());
                forToken = writePlacements(metadata, keyspace, key);
            }
            // Otherwise, coordinator and the replica agree about the placement of the givent token, so catch-up can be async
            else
            {
                ClusterMetadataService.instance().fetchLogFromPeerOrCMSAsync(metadata, message.from(), message.epoch());
            }
        }

        if (!forToken.get().containsSelf())
        {
            StorageService.instance.incOutOfRangeOperationCount();
            Keyspace.open(message.payload.getKeyspaceName()).metric.outOfRangeTokenWrites.inc();
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS, logMessageTemplate, message.from(), key.getToken(), message.payload.getKeyspaceName());
            throw InvalidRoutingException.forWrite(message.from(), key.getToken(), metadata.epoch, message.payload);
        }

        if (forToken.lastModified().isAfter(message.epoch()))
        {
            TCMMetrics.instance.coordinatorBehindPlacements.mark();
            throw new CoordinatorBehindException(String.format("Routing is correct, but coordinator needs to catch-up at least to epoch %s to maintain consistency. Current coordinator epoch is %s",
                                                               forToken.lastModified(), message.epoch()));
        }

        return metadata;
    }

    private ClusterMetadata checkSchemaVersion(ClusterMetadata metadata, Message<T> message)
    {
        if (SchemaConstants.isSystemKeyspace(message.payload.getKeyspaceName()) || message.epoch().is(metadata.epoch))
            return metadata;
        String keyspace = message.payload.getKeyspaceName();
        Keyspace ks = metadata.schema.getKeyspace(keyspace);
        if (ks != null)
        {
            if (message.epoch().isAfter(metadata.epoch))
            {
                // coordinator is ahead - check each partition update if the schema is ahead of the schema we have for the table
                for (PartitionUpdate pu : message.payload.getPartitionUpdates())
                {
                    Epoch remoteSchemaEpoch = pu.serializedAtEpoch;
                    if (remoteSchemaEpoch != null && remoteSchemaEpoch.isAfter(metadata.epoch))
                    {
                        // the partition update was serialized after the epoch we currently know, catch up and
                        // make sure we've seen the epoch it has seen, otherwise fail request.
                        metadata = ClusterMetadataService.instance().fetchLogFromPeerOrCMS(metadata, message.from(), message.epoch());
                        if (pu.serializedAtEpoch.isAfter(metadata.epoch))
                            throw new IllegalStateException(String.format("Coordinator %s is still ahead after fetching log, our epoch = %s, their epoch = %s",
                                                                          message.from(),
                                                                          metadata.epoch, message.epoch()));
                    }
                }
            }
            else if (message.epoch().isBefore(metadata.schema.lastModified()))
            {
                // coordinator might not have seen the latest schema change - check each modified table individually
                for (PartitionUpdate pu : message.payload.getPartitionUpdates())
                {
                    // coordinator could be behind, check local tables
                    ColumnFamilyStore cfs = ks.getColumnFamilyStore(pu.metadata().id);
                    if (cfs != null)
                    {
                        Epoch remoteSchemaEpoch = pu.serializedAtEpoch;
                        if (remoteSchemaEpoch != null && remoteSchemaEpoch.isBefore(cfs.metadata().epoch))
                        {
                            TCMMetrics.instance.coordinatorBehindSchema.mark();
                            throw new CoordinatorBehindException(String.format("Coordinator %s is behind, our epoch = %s, their epoch = %s",
                                                                               message.from(),
                                                                               metadata.epoch, message.epoch()));
                        }
                    }
                    else
                    {
                        TCMMetrics.instance.coordinatorBehindSchema.mark();
                        throw new CoordinatorBehindException(String.format("Schema mismatch, coordinator %s is behind, we're missing table %s.%s, our epoch = %s, their epoch = %s",
                                                                           message.from(),
                                                                           pu.metadata().keyspace,
                                                                           pu.metadata().name,
                                                                           metadata.epoch, message.epoch()));
                    }
                }
            }
        }
        else
        {
            if (message.epoch().isBefore(metadata.schema.lastModified()))
            {
                TCMMetrics.instance.coordinatorBehindSchema.mark();
                throw new CoordinatorBehindException(String.format("Schema mismatch, coordinator %s is behind, we're missing keyspace %s, our epoch = %s, their epoch = %s",
                                                                   message.from(),
                                                                   keyspace,
                                                                   metadata.epoch, message.epoch()));
            }
            else
            {
                metadata = ClusterMetadataService.instance().fetchLogFromPeerOrCMS(metadata, message.from(), message.epoch());
            }
        }

        return metadata;
    }

    private static VersionedEndpoints.ForToken writePlacements(ClusterMetadata metadata, String keyspace, DecoratedKey key)
    {
        return metadata.placements.get(metadata.schema.getKeyspace(keyspace).getMetadata().params.replication).writes.forToken(key.getToken());
    }
}
