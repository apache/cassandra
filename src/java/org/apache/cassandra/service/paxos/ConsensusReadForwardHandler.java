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

package org.apache.cassandra.service.paxos;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;

/**
 * Handler for forwarded consensus read operations.
 * Executes the consensus read operation on behalf of the original coordinator,
 * ensuring proper coordination for tracked keyspaces on a replica coordinator.
 *
 * TODO (expected): more comprehensive testing
 */
public class ConsensusReadForwardHandler implements IVerbHandler<ConsensusReadForwardRequest>
{
    public static final ConsensusReadForwardHandler instance = new ConsensusReadForwardHandler();
    private static final Logger logger = LoggerFactory.getLogger(ConsensusReadForwardHandler.class);

    @Override
    public void doVerb(Message<ConsensusReadForwardRequest> message)
    {
        ConsensusReadForwardRequest request = message.payload;
        SinglePartitionReadCommand command = request.command;
        Tracing.trace("Executing forwarded consensus read operation for {}", command.partitionKey());

        // Start capturing client warnings for the forwarded operation
        ClientWarn.instance.captureWarnings();
        try
        {
            // Validate keyspace exists and is tracked
            String keyspaceName = command.metadata().keyspace;
            KeyspaceMetadata ksMetadata = Schema.instance.getKeyspaceMetadata(keyspaceName);
            if (ksMetadata == null)
            {
                MessagingService.instance().respondWithFailure(RequestFailureReason.INCOMPATIBLE_SCHEMA, message);
                logger.error("Failed to forward consensus read operation for non-existent keyspace {}", keyspaceName);
                return;
            }

            if (!ksMetadata.params.replicationType.isTracked())
            {
                MessagingService.instance().respondWithFailure(RequestFailureReason.INCOMPATIBLE_SCHEMA, message);
                logger.error("Asked to perform forwarded consensus read operation, but keyspace {} is not tracked", keyspaceName);
                return;
            }

            // Create a Group from the single command for reading
            SinglePartitionReadCommand.Group group = SinglePartitionReadCommand.Group.one(command);

            // Execute the read using StorageProxy.read() which will:
            // 1. Check forwarding (returns null since we're on a replica)
            // 2. Execute the consensus read with the appropriate protocol
            logger.debug("Executing consensus read operation for table {}.{} with key {}",
                         keyspaceName, command.metadata().name, command.partitionKey());

            Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();
            PartitionIterator result = StorageProxy.readWithConsensusForwarded(group, request.consistencyLevel, requestTime);

            // Create response with the read result and captured warnings
            List<String> warnings = ClientWarn.instance.getWarnings();
            CasForwardResponse response = new CasForwardResponse(result, warnings);
            MessagingService.instance().respond(response, message);
            logger.debug("Completed forwarded consensus read operation for {}", command.partitionKey());
        }
        catch (CassandraException ce)
        {
            // Forward the exception back to the original coordinator with warnings
            List<String> warnings = ClientWarn.instance.getWarnings();
            CasForwardResponse response = new CasForwardResponse(ce, warnings);
            MessagingService.instance().respond(response, message);
        }
        catch (Throwable t)
        {
            try
            {
                MessagingService.instance().respondWithFailure(RequestFailure.forException(t), message);
            }
            finally
            {
                throw t;
            }
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }
}
