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
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ReadCommandVerbHandler implements IVerbHandler<ReadCommand>
{
    public static final ReadCommandVerbHandler instance = new ReadCommandVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ReadCommandVerbHandler.class);

    public void doVerb(Message<ReadCommand> message)
    {
        if (StorageService.instance.isBootstrapMode())
        {
            throw new RuntimeException("Cannot service reads while bootstrapping!");
        }

        ReadCommand command = message.payload;
        validateTransientStatus(message);
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

    private void validateTransientStatus(Message<ReadCommand> message)
    {
        ReadCommand command = message.payload;
        if (command.metadata().isVirtual())
            return;
        Token token;

        if (command instanceof SinglePartitionReadCommand)
            token = ((SinglePartitionReadCommand) command).partitionKey().getToken();
        else
            token = ((PartitionRangeReadCommand) command).dataRange().keyRange().right.getToken();

        Replica replica = Keyspace.open(command.metadata().keyspace)
                                  .getReplicationStrategy()
                                  .getLocalReplicaFor(token);

        if (replica == null)
        {
            if (command.isTopK())
                return;

            logger.warn("Received a read request from {} for a range that is not owned by the current replica {}.",
                        message.from(),
                        command);
            return;
        }

        if (!command.acceptsTransient() && replica.isTransient())
        {
            MessagingService.instance().metrics.recordDroppedMessage(message, message.elapsedSinceCreated(NANOSECONDS), NANOSECONDS);
            throw new InvalidRequestException(String.format("Attempted to serve %s data request from %s node in %s",
                                                            command.acceptsTransient() ? "transient" : "full",
                                                            replica.isTransient() ? "transient" : "full",
                                                            this));
        }
    }
}
