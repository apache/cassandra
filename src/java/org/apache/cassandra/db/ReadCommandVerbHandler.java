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

import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.service.reads.IReadResponse;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ReadCommandVerbHandler extends AbstractReadCommandVerbHandler<ReadCommand>
{
    public static final ReadCommandVerbHandler instance = new ReadCommandVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ReadCommandVerbHandler.class);

    @Override
    protected ReadCommand getCommand(ReadCommand payload)
    {
        return payload;
    }

    @Override
    protected void performRead(Message<ReadCommand> message, ClusterMetadata metadata)
    {
        IReadResponse response;
        ReadCommand command = message.payload;
        MutationSummary initialSummary = command.createMutationSummary(false);
        try (ReadExecutionController controller = command.executionController(message.trackRepairedData());
             UnfilteredPartitionIterator iterator = command.executeLocally(controller))
        {
            response = command.createResponse(iterator, controller.getRepairedDataInfo(), initialSummary);
        }
        catch (RejectException e)
        {
            if (!command.isTrackingWarnings())
                throw e;

            // make sure to log as the exception is swallowed
            logger.error(e.getMessage());

            response = command.createEmptyResponse();
            Message<IReadResponse> reply = message.responseWith(response);
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
            Message<IReadResponse> reply = message.responseWith(response);
            reply = MessageParams.addToMessage(reply);
            MessagingService.instance().send(reply, message.from());
        }
        else
        {
            Tracing.trace("Discarding partial response to {} (timed out)", message.from());
            MessagingService.instance().metrics.recordDroppedMessage(message, message.elapsedSinceCreated(NANOSECONDS), NANOSECONDS);
        }
    }
}
