package org.apache.cassandra.service;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EchoVerbHandler implements IVerbHandler<NoPayload>
{
    public static final EchoVerbHandler instance = new EchoVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(EchoVerbHandler.class);

    public void doVerb(Message<NoPayload> message)
    {
        // only respond if we are not shutdown
        if (!StorageService.instance.isShutdown())
        {
            logger.trace("Sending ECHO_RSP to {}", message.from());
            MessagingService.instance().send(message.emptyResponse(), message.from());
        }
        else
        {
            logger.trace("Not sending ECHO_RSP to {} - we are shutting down", message.from());
        }
    }
}
