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

import java.util.Map;

import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.tracing.Tracing;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.db.commitlog.CommitLogSegment.ENTRY_OVERHEAD_SIZE;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

public class MutationVerbHandler extends AbstractMutationVerbHandler<Mutation>
{
    public static final MutationVerbHandler instance = new MutationVerbHandler();

    private void respond(MessageDelivery messaging, Message<?> respondTo, InetAddressAndPort respondToAddress, Map<ParamType, Object> params)
    {
        Tracing.trace("Enqueuing response to {}", respondToAddress);
        Message<?> response = respondTo.emptyResponse();
        if (!params.isEmpty())
            response = response.withParams(params);
        messaging.send(response, respondToAddress);
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    public void doVerb(MessageDelivery messaging, Message<Mutation> message)
    {
        if (approxTime.now() > message.expiresAtNanos())
        {
            Tracing.trace("Discarding mutation from {} (timed out)", message.from());
            MessagingService.instance().metrics.recordDroppedMessage(message, message.elapsedSinceCreated(NANOSECONDS), NANOSECONDS);
            return;
        }

        MessageParams.reset();
        message.payload.validateSize(MessagingService.current_version, ENTRY_OVERHEAD_SIZE);
        WriteThresholds.checkWriteThresholds(message.payload);

        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null)
            forwardToLocalNodes(messaging, message, forwardTo);

        InetAddressAndPort respondToAddress = message.respondTo();
        try
        {
            processMessage(messaging, message, respondToAddress);
        }
        catch (WriteTimeoutException wto)
        {
            failed();
        }
    }

    protected void applyMutation(MessageDelivery messaging, Message<Mutation> message, InetAddressAndPort respondToAddress)
    {
        Map<ParamType, Object> params = MessageParams.capture();
        message.payload.applyFuture().addCallback(o -> respond(messaging, message, respondToAddress, params), wto -> failed());
    }

    private static void forwardToLocalNodes(MessageDelivery messaging, Message<Mutation> originalMessage, ForwardingInfo forwardTo)
    {
        Message.Builder<Mutation> builder =
            Message.builder(originalMessage)
                   .withParam(ParamType.RESPOND_TO, originalMessage.from())
                   .withoutParam(ParamType.FORWARD_TO);

        // reuse the same Message if all ids are identical (as they will be for 4.0+ node originated messages)
        Message<Mutation> message = builder.build();

        forwardTo.forEach((id, target) ->
        {
            Tracing.trace("Enqueuing forwarded write to {}", target);
            messaging.send(message, target);
        });
    }
}
