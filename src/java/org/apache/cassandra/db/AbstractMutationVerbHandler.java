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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.NoSpamLogger;

public abstract class AbstractMutationVerbHandler<T extends IMutation> implements IVerbHandler<T>
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractMutationVerbHandler.class);
    private static final String logMessageTemplate = "Received mutation from {} for token {} outside valid range for keyspace {}";

    public void doVerb(Message<T> message) throws IOException
    {
        processMessage(message, message.from());
    }

    public void processMessage(Message<T> message, InetAddressAndPort respondTo)
    {
        DecoratedKey key = message.payload.key();
        if (isOutOfRangeMutation(message.payload.getKeyspaceName(), key))
        {
            StorageService.instance.incOutOfRangeOperationCount();
            Keyspace.open(message.payload.getKeyspaceName()).metric.outOfRangeTokenWrites.inc();

            // Log at most 1 message per second
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS, logMessageTemplate, respondTo, key.getToken(), message.payload.getKeyspaceName());
            sendFailureResponse(message, respondTo);
        }
        else
        {
            applyMutation(message, respondTo);
        }
    }

    abstract void applyMutation(Message<T> message, InetAddressAndPort respondToAddress);

    private void sendFailureResponse(Message<T> respondTo, InetAddressAndPort respondToAddress)
    {
        MessagingService.instance().send(respondTo.failureResponse(RequestFailureReason.INVALID_ROUTING), respondToAddress);
    }

    private static boolean isOutOfRangeMutation(String keyspace, DecoratedKey key)
    {
        return !StorageService.instance.isEndpointValidForWrite(keyspace, key.getToken());
    }
}
