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
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import org.junit.Test;

import org.apache.cassandra.net.ResourceLimits.Basic;
import org.apache.cassandra.net.ResourceLimits.Limit;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractMessageHandlerTest
{
    @Test
    public void internodeReactivationDoesNotUseUninitializedClientMetrics() throws IOException
    {
        FrameDecoder decoder = mock(FrameDecoder.class);
        when(decoder.isActive()).thenReturn(true);

        EventLoop eventLoop = mock(EventLoop.class);
        when(eventLoop.inEventLoop()).thenReturn(true);
        Channel channel = mock(Channel.class);
        when(channel.eventLoop()).thenReturn(eventLoop);

        Limit endpointReserve = new Basic(1);
        Limit globalReserve = new Basic(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        InboundMessageHandler handler = new InboundMessageHandler(decoder,
                                                                  ConnectionType.LARGE_MESSAGES,
                                                                  channel,
                                                                  null,
                                                                  null,
                                                                  MessagingService.current_version,
                                                                  1,
                                                                  1,
                                                                  endpointReserve,
                                                                  globalReserve,
                                                                  AbstractMessageHandler.WaitQueue.endpoint(endpointReserve),
                                                                  AbstractMessageHandler.WaitQueue.global(globalReserve),
                                                                  ignored -> {},
                                                                  null,
                                                                  null)
        {
            @Override
            protected boolean processUpToOneMessage(Limit endpoint, Limit global)
            {
                return true;
            }

            @Override
            protected void fatalExceptionCaught(Throwable cause)
            {
                failure.set(cause);
            }
        };

        handler.onReserveCapacityRegained(endpointReserve, globalReserve, 0);

        assertNull(failure.get());
        verify(decoder).reactivate();
    }
}
