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

import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.*;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.ENTRY_OVERHEAD_SIZE;

public class MutationVerbHandler implements IVerbHandler<Mutation>
{
    public static final MutationVerbHandler instance = new MutationVerbHandler();

    private void respond(Message<?> respondTo, InetAddressAndPort respondToAddress)
    {
        Tracing.trace("Enqueuing response to {}", respondToAddress);
        MessagingService.instance().send(respondTo.emptyResponse(), respondToAddress);
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    public void doVerb(Message<Mutation> message)
    {
        message.payload.validateSize(MessagingService.current_version, ENTRY_OVERHEAD_SIZE);

        // Check if there were any forwarding headers in this message
        ForwardingInfo forwardTo = message.forwardTo();
        if (forwardTo != null)
            forwardToLocalNodes(message, forwardTo);

        InetAddressAndPort respondToAddress = message.respondTo();
        try
        {
            message.payload.applyFuture().addCallback(o -> respond(message, respondToAddress), wto -> failed());
        }
        catch (WriteTimeoutException wto)
        {
            failed();
        }
    }

    private static void forwardToLocalNodes(Message<Mutation> originalMessage, ForwardingInfo forwardTo)
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
            MessagingService.instance().send(message, target);
        });
    }
}
