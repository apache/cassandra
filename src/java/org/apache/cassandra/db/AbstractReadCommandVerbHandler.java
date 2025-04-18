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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.CoordinatorBehindException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.InvalidRoutingException;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public abstract class AbstractReadCommandVerbHandler<T> implements IVerbHandler<T>
{
    protected abstract void performRead(Message<T> message, ClusterMetadata metadata);

    protected abstract ReadCommand getCommand(T payload);

    public final void doVerb(Message<T> message)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        if (message.epoch().isAfter(Epoch.EMPTY))
        {
            metadata = checkTokenOwnership(metadata, message);
            metadata = checkSchemaVersion(metadata, message);
        }

        MessageParams.reset();

        long timeout = message.expiresAtNanos() - message.createdAtNanos();
        ReadCommand command = getCommand(message.payload);
        command.setMonitoringTime(message.createdAtNanos(), message.isCrossNode(), timeout, DatabaseDescriptor.getSlowQueryTimeout(NANOSECONDS));

        if (message.trackWarnings())
            command.trackWarnings();

        performRead(message, metadata);
    }

    private ClusterMetadata checkSchemaVersion(ClusterMetadata metadata, Message<T> message)
    {
        ReadCommand readCommand = getCommand(message.payload);

        if (SchemaConstants.isSystemKeyspace(readCommand.metadata().keyspace) ||
                readCommand.serializedAtEpoch() == null) // don't try to catch up with pre-5.0 nodes
            return metadata;

        Keyspace ks = metadata.schema.getKeyspace(readCommand.metadata().keyspace);
        ColumnFamilyStore cfs = ks != null ? ks.getColumnFamilyStore(readCommand.metadata().id) : null;
        Epoch localComparisonEpoch = metadata.epoch;
        if (cfs != null)
            localComparisonEpoch = cfs.metadata().epoch;

        if (localComparisonEpoch.isBefore(readCommand.serializedAtEpoch()))
            metadata = ClusterMetadataService.instance().fetchLogFromPeerOrCMS(metadata, message.from(), message.epoch());
        else if (localComparisonEpoch.isAfter(readCommand.serializedAtEpoch()))
        {
            TCMMetrics.instance.coordinatorBehindSchema.mark();
            throw new CoordinatorBehindException(String.format("Coordinator schema for %s.%s with epoch %s is behind our schema %s",
                                                               readCommand.metadata().keyspace,
                                                               readCommand.metadata().name,
                                                               readCommand.serializedAtEpoch(),
                                                               localComparisonEpoch));
        }
        ks = metadata.schema.getKeyspace(readCommand.metadata().keyspace);
        if (ks == null || ks.getColumnFamilyStore(readCommand.metadata().id) == null)
            throw new IllegalStateException("Unknown table " + readCommand.metadata().id +" after fetching remote log entries");
        return metadata;
    }

    private ClusterMetadata checkTokenOwnership(ClusterMetadata metadata, Message<T> message)
    {
        ReadCommand command = getCommand(message.payload);
        if (command.metadata().isVirtual())
            return metadata;

        if (command.isTopK())
            return metadata;

        if (command instanceof SinglePartitionReadCommand)
        {
            Token token = ((SinglePartitionReadCommand) command).partitionKey().getToken();
            Replica localReplica = getLocalReplica(metadata, token, command.metadata().keyspace);
            if (localReplica == null)
            {
                metadata = ClusterMetadataService.instance().fetchLogFromPeerOrCMS(metadata, message.from(), message.epoch());
                localReplica = getLocalReplica(metadata, token, command.metadata().keyspace);
            }
            if (localReplica == null)
            {
                StorageService.instance.incOutOfRangeOperationCount();
                Keyspace.open(command.metadata().keyspace).metric.outOfRangeTokenReads.inc();
                throw InvalidRoutingException.forTokenRead(message.from(), token, metadata.epoch, command);
            }

            if (!command.acceptsTransient() && localReplica.isTransient())
            {
                MessagingService.instance().metrics.recordDroppedMessage(message, message.elapsedSinceCreated(NANOSECONDS), NANOSECONDS);
                throw new InvalidRequestException(String.format("Attempted to serve %s data request from %s node in %s",
                                                                command.acceptsTransient() ? "transient" : "full",
                                                                localReplica.isTransient() ? "transient" : "full",
                                                                this));
            }
        }
        else
        {
            AbstractBounds<PartitionPosition> range = command.dataRange().keyRange();

            // TODO: preexisting issue: for the range queries or queries that span multiple replicas, we can only make requests where the right token is owned, but not the left one
            Replica maxTokenLocalReplica = getLocalReplica(metadata, range.right.getToken(), command.metadata().keyspace);
            if (maxTokenLocalReplica == null)
            {
                metadata = ClusterMetadataService.instance().fetchLogFromPeerOrCMS(metadata, message.from(), message.epoch());
                maxTokenLocalReplica = getLocalReplica(metadata, range.right.getToken(), command.metadata().keyspace);
            }
            if (maxTokenLocalReplica == null)
            {
                StorageService.instance.incOutOfRangeOperationCount();
                Keyspace.open(command.metadata().keyspace).metric.outOfRangeTokenReads.inc();
                throw InvalidRoutingException.forRangeRead(message.from(), range, metadata.epoch, command);
            }

            // TODO: preexisting issue: we should change the whole range for transient-ness, not just the right token
            if (command.acceptsTransient() != maxTokenLocalReplica.isTransient())
            {
                MessagingService.instance().metrics.recordDroppedMessage(message, message.elapsedSinceCreated(NANOSECONDS), NANOSECONDS);
                throw new InvalidRequestException(String.format("Attempted to serve %s data request from %s node in %s",
                                                                command.acceptsTransient() ? "transient" : "full",
                                                                maxTokenLocalReplica.isTransient() ? "transient" : "full",
                                                                this));
            }
        }
        return metadata;
    }

    private static Replica getLocalReplica(ClusterMetadata metadata, Token token, String keyspace)
    {
        return metadata.placements
                .get(metadata.schema.getKeyspaces().getNullable(keyspace).params.replication)
                .reads
                .forToken(token)
                .get()
                .lookup(FBUtilities.getBroadcastAddressAndPort());
    }
}
