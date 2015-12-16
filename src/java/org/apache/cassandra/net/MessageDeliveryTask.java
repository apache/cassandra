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
package org.apache.cassandra.net;

import java.io.IOException;
import java.util.EnumSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.index.IndexNotAvailableException;

public class MessageDeliveryTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(MessageDeliveryTask.class);

    private final MessageIn message;
    private final int id;

    public MessageDeliveryTask(MessageIn message, int id)
    {
        assert message != null;
        this.message = message;
        this.id = id;
    }

    public void run()
    {
        MessagingService.Verb verb = message.verb;
        long timeTaken = System.currentTimeMillis() - message.constructionTime.timestamp;
        if (MessagingService.DROPPABLE_VERBS.contains(verb)
            && timeTaken > message.getTimeout())
        {
            MessagingService.instance().incrementDroppedMessages(message, timeTaken);
            return;
        }

        IVerbHandler verbHandler = MessagingService.instance().getVerbHandler(verb);
        if (verbHandler == null)
        {
            logger.trace("Unknown verb {}", verb);
            return;
        }

        try
        {
            verbHandler.doVerb(message, id);
        }
        catch (IOException ioe)
        {
            handleFailure(ioe);
            throw new RuntimeException(ioe);
        }
        catch (TombstoneOverwhelmingException | IndexNotAvailableException e)
        {
            handleFailure(e);
            logger.error(e.getMessage());
        }
        catch (Throwable t)
        {
            handleFailure(t);
            throw t;
        }

        if (GOSSIP_VERBS.contains(message.verb))
            Gossiper.instance.setLastProcessedMessageAt(message.constructionTime.timestamp);
    }

    private void handleFailure(Throwable t)
    {
        if (message.doCallbackOnFailure())
        {
            MessageOut response = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE)
                                                .withParameter(MessagingService.FAILURE_RESPONSE_PARAM, MessagingService.ONE_BYTE);
            MessagingService.instance().sendReply(response, id, message.from);
        }
    }

    private static final EnumSet<MessagingService.Verb> GOSSIP_VERBS = EnumSet.of(MessagingService.Verb.GOSSIP_DIGEST_ACK,
                                                                                  MessagingService.Verb.GOSSIP_DIGEST_ACK2,
                                                                                  MessagingService.Verb.GOSSIP_DIGEST_SYN);
}
