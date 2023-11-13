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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.CoordinatorBehindException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.InvalidRoutingException;
import org.apache.cassandra.exceptions.QueryCancelledException;
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
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ReadCommandVerbHandler implements IVerbHandler<ReadCommand>
{
    public static final ReadCommandVerbHandler instance = new ReadCommandVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ReadCommandVerbHandler.class);

    public ReadResponse doRead(ReadCommand command, boolean trackRepairedData)
    {
        ReadResponse response;
        try (ReadExecutionController controller = command.executionController(trackRepairedData);
             UnfilteredPartitionIterator iterator = command.executeLocally(controller))
        {
            response = command.createResponse(iterator, controller.getRepairedDataInfo());
        }

        return response;
    }

    public void doVerb(Message<ReadCommand> message)
    {
        if (message.epoch().isAfter(Epoch.EMPTY))
        {
            ClusterMetadata metadata = ClusterMetadata.current();
            metadata = checkTokenOwnership(metadata, message);
            metadata = checkSchemaVersion(metadata, message);
        }

        MessageParams.reset();

        long timeout = message.expiresAtNanos() - message.createdAtNanos();
        ReadCommand command = message.payload;
        command.setMonitoringTime(message.createdAtNanos(), message.isCrossNode(), timeout, DatabaseDescriptor.getSlowQueryTimeout(NANOSECONDS));

        if (message.trackWarnings())
            command.trackWarnings();

        ReadResponse response;
        try
        {
            response = doRead(command, message.trackRepairedData());
        }
        catch (RejectException e)
        {
            if (!command.isTrackingWarnings())
                throw e;

            // make sure to log as the exception is swallowed
            logger.error(e.getMessage());

            response = command.createEmptyResponse();
            Message<ReadResponse> reply = message.responseWith(response);
            reply = MessageParams.addToMessage(reply);

            MessagingService.instance().send(reply, message.from());
            return;
        }
        catch (AssertionError t)
        {
            throw new AssertionError(String.format("Caught an error while trying to process the command: %s", command.toCQLString()), t);
        }
        catch (QueryCancelledException e)
        {
            logger.debug("Query cancelled (timeout)", e);
            response = null;
            assert !command.isCompleted() : "Read marked as completed despite being aborted by timeout to table " + command.metadata();
        }

        if (command.complete())
        {
            Tracing.trace("Enqueuing response to {}", message.from());
            Message<ReadResponse> reply = message.responseWith(response);
            reply = MessageParams.addToMessage(reply);
            MessagingService.instance().send(reply, message.from());
        }
        else
        {
            Tracing.trace("Discarding partial response to {} (timed out)", message.from());
            MessagingService.instance().metrics.recordDroppedMessage(message, message.elapsedSinceCreated(NANOSECONDS), NANOSECONDS);
        }
    }

    private ClusterMetadata checkSchemaVersion(ClusterMetadata metadata, Message<ReadCommand> message)
    {
        ReadCommand readCommand = message.payload;

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
                                                               message.payload.metadata().keyspace,
                                                               message.payload.metadata().name,
                                                               readCommand.serializedAtEpoch(),
                                                               localComparisonEpoch));
        }
        ks = metadata.schema.getKeyspace(readCommand.metadata().keyspace);
        if (ks == null || ks.getColumnFamilyStore(readCommand.metadata().id) == null)
            throw new IllegalStateException("Unknown table " + readCommand.metadata().id +" after fetching remote log entries");
        return metadata;
    }

    private ClusterMetadata checkTokenOwnership(ClusterMetadata metadata, Message<ReadCommand> message)
    {
        ReadCommand command = message.payload;

        if (command.metadata().isVirtual())
            return metadata;

        // Some read commands may be sent using an older Epoch intentionally so validating using the current Epoch
        // doesn't work
        if (command.allowsOutOfRangeReads())
            return metadata;

        if (command instanceof SinglePartitionReadCommand)
        {
            Token token = ((SinglePartitionReadCommand)command).partitionKey().getToken();
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
                throw InvalidRoutingException.forTokenRead(message.from(), token, metadata.epoch, message.payload);
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
            AbstractBounds<PartitionPosition> range = ((PartitionRangeReadCommand) command).dataRange().keyRange();

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
                throw InvalidRoutingException.forRangeRead(message.from(), range, metadata.epoch, message.payload);
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
