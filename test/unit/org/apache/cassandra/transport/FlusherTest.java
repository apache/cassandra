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

package org.apache.cassandra.transport;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.FrameEncoder;

import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;

public class FlusherTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testPreV5ResponseIsReleasedWhenChannelIsClosed()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.close().syncUninterruptibly();

        Envelope request = envelope(ProtocolVersion.V4, 16);
        Envelope response = envelope(ProtocolVersion.V4, 1024);
        AtomicInteger cleanups = new AtomicInteger();

        Flusher flusher = Flusher.immediate(channel.eventLoop());
        flusher.enqueue(new Flusher.FlushItem.Unframed(channel, response, request, item -> cleanups.incrementAndGet()));
        flusher.run();

        Assertions.assertThat(response.body.refCnt()).as("response body should have been released").isZero();
        Assertions.assertThat(cleanups.get()).as("flush item should have been cleaned up exactly once").isOne();

        request.release();
    }

    @Test
    public void testFramedResponseAllocatesNoPayloadWhenChannelIsClosed()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.close().syncUninterruptibly();

        Envelope request = envelope(ProtocolVersion.V5, 16);
        Envelope response = envelope(ProtocolVersion.V5, 1024);
        List<FrameEncoder.Payload> allocated = new ArrayList<>();

        Flusher flusher = Flusher.immediate(channel.eventLoop());
        flusher.enqueue(new Flusher.FlushItem.Framed(channel,
                                                     response,
                                                     request,
                                                     recordingAllocator(allocated),
                                                     item -> item.responseEnvelope.release()));
        flusher.run();

        Assertions.assertThat(allocated).as("no pooled payload should be allocated for a closed channel").isEmpty();
        Assertions.assertThat(response.body.refCnt()).as("response body should have been released").isZero();

        request.release();
    }

    @Test
    public void testFramedPayloadIsReleasedWhenChannelClosesAfterStaging()
    {
        EmbeddedChannel channel = new EmbeddedChannel();

        Envelope request = envelope(ProtocolVersion.V5, 16);
        Envelope response = envelope(ProtocolVersion.V5, 1024);
        List<FrameEncoder.Payload> allocated = new ArrayList<>();

        // the legacy flusher defers flushWrittenChannels(), so the response is staged in a FlushBuffer while
        // the channel is still open and its payload is only allocated once the channel has already closed
        Flusher flusher = Flusher.legacy(channel.eventLoop());
        flusher.enqueue(new Flusher.FlushItem.Framed(channel,
                                                     response,
                                                     request,
                                                     recordingAllocator(allocated),
                                                     item -> item.responseEnvelope.release()));
        flusher.run();
        Assertions.assertThat(response.body.refCnt()).as("response should still be staged, not yet flushed").isOne();
        Assertions.assertThat(allocated).as("no payload should be allocated before the flush").isEmpty();

        ChannelPromise closed = channel.newPromise();
        channel.unsafe().close(closed);
        closed.syncUninterruptibly();
        flusher.run();

        Assertions.assertThat(allocated).as("flush should have allocated one payload").hasSize(1);
        Assertions.assertThat(response.body.refCnt()).as("response body should have been released").isZero();
        // The FrameEncoder.Payload is not refCounted, so a payload handed to a closed channel is dropped
        // without its buffer returning to the pool. finish() transfers ownership to the encoder, so an
        // unfinished payload is one the flusher released itself. remaining() asserts on !isFinished,
        // so it only throws when a finish has completed
        Assertions.assertThatCode(() -> allocated.get(0).remaining())
                  .as("payload must be released rather than written to a closed channel")
                  .doesNotThrowAnyException();

        request.release();
    }

    private static Envelope envelope(ProtocolVersion version, int bodySize)
    {
        return Envelope.create(Message.Type.RESULT,
                               1,
                               version,
                               EnumSet.noneOf(Envelope.Header.Flag.class),
                               CBUtil.allocator.buffer(bodySize));
    }

    private static FrameEncoder.PayloadAllocator recordingAllocator(List<FrameEncoder.Payload> allocated)
    {
        return (boolean selfContained, int capacity) ->
        {
            FrameEncoder.Payload payload = FrameEncoder.PayloadAllocator.simple.allocate(selfContained, capacity);
            allocated.add(payload);
            return payload;
        };
    }
}
