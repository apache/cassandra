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

    public AccordVerbHandler(Node node)
    {
        this.node = node;
    }

    @Override
    public void doVerb(Message<T> message) throws IOException
    {
        logger.debug("Receiving {} from {}", message.payload, message.from());
        message.payload.process(node, EndpointMapping.getId(message.from()), message);
    }
}
