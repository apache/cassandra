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
import org.apache.cassandra.exceptions.InvalidRoutingException;
import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ReadCommandVerbHandler implements IVerbHandler<ReadCommand>
{
    public static final ReadCommandVerbHandler instance = new ReadCommandVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ReadCommandVerbHandler.class);

    public void doVerb(Message<ReadCommand> message)
    {
        ClusterMetadataService.instance().maybeCatchup(message.epoch());

        if (StorageService.instance.isBootstrapMode())
            throw new RuntimeException("Cannot service reads while bootstrapping!");

        ReadCommand command = message.payload;
        ClusterMetadata metadata = ClusterMetadata.current();

        checkTokenOwnership(metadata, message);
        MessageParams.reset();

        long timeout = message.expiresAtNanos() - message.createdAtNanos();
        command.setMonitoringTime(message.createdAtNanos(), message.isCrossNode(), timeout, DatabaseDescriptor.getSlowQueryTimeout(NANOSECONDS));

        if (message.trackWarnings())
            command.trackWarnings();

        ReadResponse response;
        try (ReadExecutionController controller = command.executionController(message.trackRepairedData());
             UnfilteredPartitionIterator iterator = command.executeLocally(controller))
        {
            response = command.createResponse(iterator, controller.getRepairedDataInfo());
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

    private void checkTokenOwnership(ClusterMetadata metadata, Message<ReadCommand> message)
    {
        ReadCommand command = message.payload;
        if (command.metadata().isVirtual())
            return;

        if (command instanceof SinglePartitionReadCommand)
        {
            Token token = ((SinglePartitionReadCommand) command).partitionKey().getToken();
            Replica localReplica = getLocalReplica(metadata, token, command.metadata().keyspace);
            if (localReplica == null)
            {
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
    }

    private static Replica getLocalReplica(ClusterMetadata metadata, Token token, String keyspace)
    {
        return metadata.placements
               .get(metadata.schema.getKeyspaces().getNullable(keyspace).params.replication)
               .reads
               .forToken(token)
               .lookup(FBUtilities.getBroadcastAddressAndPort());
    }
}
