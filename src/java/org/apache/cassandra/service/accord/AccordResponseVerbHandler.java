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

import accord.coordinate.Timeout;
import accord.impl.RequestCallbacks;
import accord.local.Node;
import accord.messages.Reply;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

class AccordResponseVerbHandler<T extends Reply> implements IVerbHandler<T>
{
    private final RequestCallbacks callbacks;
    private final AccordEndpointMapper endpointMapper;

    AccordResponseVerbHandler(RequestCallbacks callbacks, AccordEndpointMapper endpointMapper)
    {
        this.callbacks = callbacks;
        this.endpointMapper = endpointMapper;
    }

    @Override
    public void doVerb(Message message)
    {
        Node.Id from = endpointMapper.mappedId(message.from());
        if (message.isFailureResponse())
        {
            Tracing.trace("Processing failure response from {}", message.from());
            callbacks.onFailure(message.id(), from, convertFailureMessage((RequestFailure) message.payload));
        }
        else
        {
            Tracing.trace("Processing response from {}", message.from());
            boolean remove = !(message.payload instanceof Reply) || ((Reply) message.payload).isFinal();
            RequestCallbacks.CallbackEntry cbe = callbacks.onSuccess(message.id(), from, message.payload, remove);
            if (cbe == null)
                return;

            long latencyNanos = approxTime.now() - cbe.registeredAt(NANOSECONDS);
            MessagingService.instance().latencySubscribers.add(message.from(), latencyNanos, NANOSECONDS);
        }
    }

    private static Throwable convertFailureMessage(RequestFailure failure)
    {
        return failure.reason == RequestFailureReason.TIMEOUT ?
               new Timeout(null, null) :
               new RuntimeException(failure.failure);
    }

}
