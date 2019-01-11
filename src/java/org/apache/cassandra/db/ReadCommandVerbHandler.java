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

import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;

public class ReadCommandVerbHandler implements IVerbHandler<ReadCommand>
{
    private static final Logger logger = LoggerFactory.getLogger(ReadCommandVerbHandler.class);

    protected IVersionedSerializer<ReadResponse> serializer()
    {
        return ReadResponse.serializer;
    }

    public void doVerb(MessageIn<ReadCommand> message, int id)
    {
        if (StorageService.instance.isBootstrapMode())
        {
            throw new RuntimeException("Cannot service reads while bootstrapping!");
        }

        ReadCommand command = message.payload;
        validateTransientStatus(message);

        command.setMonitoringTime(message.constructionTime, message.isCrossNode(), message.getTimeout(), message.getSlowQueryTimeout());

        if (message.parameters.containsKey(ParameterType.TRACK_REPAIRED_DATA))
            command.trackRepairedStatus();

        ReadResponse response;
        try (ReadExecutionController executionController = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(executionController))
        {
            response = command.createResponse(iterator);
        }

        if (!command.complete())
        {
            Tracing.trace("Discarding partial response to {} (timed out)", message.from);
            MessagingService.instance().incrementDroppedMessages(message, message.getLifetimeInMS());
            return;
        }

        Tracing.trace("Enqueuing response to {}", message.from);
        MessageOut<ReadResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, response, serializer());
        MessagingService.instance().sendReply(reply, id, message.from);
    }

    private void validateTransientStatus(MessageIn<ReadCommand> message)
    {
        ReadCommand command = message.payload;
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
            logger.warn("Received a read request from {} for a range that is not owned by the current replica {}.",
                        message.from,
                        command);
            return;
        }

        if (!command.acceptsTransient() && replica.isTransient())
        {
            MessagingService.instance().incrementDroppedMessages(message, message.getLifetimeInMS());
            throw new InvalidRequestException(String.format("Attempted to serve %s data request from %s node in %s",
                                                            command.acceptsTransient() ? "transient" : "full",
                                                            replica.isTransient() ? "transient" : "full",
                                                            this));
        }
    }
}
