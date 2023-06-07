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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.messages.Request;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

public class AccordVerbHandler<T extends Request> implements IVerbHandler<T>
{
    private static final Logger logger = LoggerFactory.getLogger(AccordVerbHandler.class);

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
        // TODO (desired): need a non-blocking way to inform CMS of an unknown epoch and add callback to it's receipt
//        ClusterMetadataService.instance().maybeCatchup(message.epoch());
        logger.debug("Receiving {} from {}", message.payload, message.from());
        T request = message.payload;
        Node.Id from = endpointMapper.mappedId(message.from());
        long knownEpoch = request.knownEpoch();
        if (!node.topology().hasEpoch(knownEpoch))
        {
            node.configService().fetchTopologyForEpoch(knownEpoch);
            long waitForEpoch = request.waitForEpoch();
            if (!node.topology().hasEpoch(waitForEpoch))
            {
                node.withEpoch(waitForEpoch, () -> request.process(node, from, message));
                return;
            }
        }
        request.process(node, from, message);
    }
}
