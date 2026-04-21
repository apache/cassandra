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

import org.apache.cassandra.db.rows.RowIterator;
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
 * Handler for forwarded CAS (Compare-And-Set) operations.
 * Executes the CAS operation on behalf of the original coordinator,
 * ensuring that MutationId generation happens on a replica coordinator.
 *
 * TODO (expected): more comprehensive testing
 */
public class CasForwardHandler implements IVerbHandler<CasForwardRequest>
{
    public static final CasForwardHandler instance = new CasForwardHandler();
    private static final Logger logger = LoggerFactory.getLogger(CasForwardHandler.class);

    @Override
    public void doVerb(Message<CasForwardRequest> message)
    {
        CasForwardRequest request = message.payload;

        Tracing.trace("Executing forwarded CAS operation for {}", request.key);

        // Start capturing client warnings for the forwarded operation
        ClientWarn.instance.captureWarnings();
        try
        {
            KeyspaceMetadata ksMetadata = Schema.instance.getKeyspaceMetadata(request.keyspaceName);
            if (ksMetadata == null)
            {
                MessagingService.instance().respondWithFailure(RequestFailureReason.INCOMPATIBLE_SCHEMA, message);
                logger.error("Failed to forward CAS operation for non-existent keyspace {}", request.keyspaceName);
                return;
            }

            // Execute the forwarded CAS operation
            logger.debug("Executing CAS operation for table {}.{} with key {}",
                         request.keyspaceName, request.cfName, request.key);

            // Execute the CAS request using StorageProxy with the forwarded client state
            Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();
            RowIterator result = StorageProxy.casForwarded(request.keyspaceName,
                                                           request.cfName,
                                                           request.key,
                                                           request.casRequest,
                                                           request.consistencyForPaxos,
                                                           request.consistencyForCommit,
                                                           request.clientState,
                                                           request.nowInSeconds,
                                                           requestTime);

            // Create response with the CAS result and captured warnings
            List<String> warnings = ClientWarn.instance.getWarnings();
            CasForwardResponse response = new CasForwardResponse(result, warnings);
            MessagingService.instance().respond(response, message);
            logger.debug("Completed forwarded CAS operation for {}", request.key);
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
