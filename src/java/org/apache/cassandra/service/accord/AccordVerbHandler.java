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
package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.messages.Request;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.NoSpamLogger;

public class AccordVerbHandler<T extends Request> implements IVerbHandler<T>
{
    private static final Logger logger = LoggerFactory.getLogger(AccordVerbHandler.class);
    private static final NoSpamLogger.NoSpamLogStatement dropping = NoSpamLogger.getStatement(logger, "Dropping message {} from {}", 1L, TimeUnit.SECONDS);

    private final Node node;
    private final AccordEndpointMapper endpointMapper;

    public AccordVerbHandler(Node node, AccordEndpointMapper endpointMapper)
    {
        this.node = node;
        this.endpointMapper = endpointMapper;
    }

    @Override
    public void doVerb(Message<T> message) throws IOException
    {
        if (!AccordService.instance().shouldAcceptMessages())
        {
            dropping.debug(message.verb(), message.from());
            return;
        }

        logger.trace("Receiving {} from {}", message.payload, message.from());
        T request = message.payload;

        /*
         * TODO (desired): messages without side-effects don't go through the journal,
         *  and as such are retained on heap until the node catches up to waitForEpoch,
         *  which can be problematic in absense of proper Accord<->Messaging backpressure
         */
        Node.Id fromNodeId = endpointMapper.mappedId(message.from());
        long waitForEpoch = request.waitForEpoch();

        if (node.topology().hasAtLeastEpoch(waitForEpoch))
            request.process(node, fromNodeId, message.header);
        else
            node.withEpoch(waitForEpoch, (ignored, withEpochFailure) -> {
                if (withEpochFailure != null)
                    throw new RuntimeException("Timed out waiting for epoch when processing message from " + fromNodeId + " to " + node + " message " + message, withEpochFailure);
                request.process(node, fromNodeId, message.header);
            });
    }
}
