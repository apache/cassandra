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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Shorts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.io.DummyByteVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;

public class MessageDeliveryTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(MessageDeliveryTask.class);

    private final MessageIn message;
    private final int id;
    private final long enqueueTime;

    public MessageDeliveryTask(MessageIn message, int id)
    {
        assert message != null;
        this.message = message;
        this.id = id;
        this.enqueueTime = ApproximateTime.currentTimeMillis();
    }

    public void run()
    {
        process();
    }

    /**
     * A helper function for making unit testing reasonable.
     *
     * @return true if the message was processed; else false.
     */
    @VisibleForTesting
    boolean process()
    {
        MessagingService.Verb verb = message.verb;
        if (verb == null)
        {
            logger.trace("Unknown verb {}", verb);
            return false;
        }

        MessagingService.instance().metrics.addQueueWaitTime(verb.toString(),
                                                             ApproximateTime.currentTimeMillis() - enqueueTime);

        long timeTaken = message.getLifetimeInMS();
        if (MessagingService.DROPPABLE_VERBS.contains(verb)
            && timeTaken > message.getTimeout())
        {
            MessagingService.instance().incrementDroppedMessages(message, timeTaken);
            return false;
        }

        IVerbHandler verbHandler = MessagingService.instance().getVerbHandler(verb);
        if (verbHandler == null)
        {
            logger.trace("No handler for verb {}", verb);
            return false;
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
            Gossiper.instance.setLastProcessedMessageAt(message.constructionTime);
        return true;
    }

    private void handleFailure(Throwable t)
    {
        if (message.doCallbackOnFailure())
        {
            MessageOut response = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE)
                                                .withParameter(ParameterType.FAILURE_RESPONSE, MessagingService.ONE_BYTE);

            if (t instanceof TombstoneOverwhelmingException)
            {
                response = response.withParameter(ParameterType.FAILURE_REASON, Shorts.checkedCast(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code));
            }

            MessagingService.instance().sendReply(response, id, message.from);
        }
    }

    private static final EnumSet<MessagingService.Verb> GOSSIP_VERBS = EnumSet.of(MessagingService.Verb.GOSSIP_DIGEST_ACK,
                                                                                  MessagingService.Verb.GOSSIP_DIGEST_ACK2,
                                                                                  MessagingService.Verb.GOSSIP_DIGEST_SYN);
}
