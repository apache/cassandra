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
import java.nio.ByteBuffer;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class AsyncStreamingOutputPlusTest
{

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testSuccess() throws IOException
    {
        EmbeddedChannel channel = new TestChannel(4);
        ByteBuf read;
        try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel))
        {
            out.writeInt(1);
            assertEquals(0, out.flushed());
            assertEquals(0, out.flushedToNetwork());
            assertEquals(4, out.position());

            out.doFlush(0);
            assertEquals(4, out.flushed());
            assertEquals(4, out.flushedToNetwork());

            out.writeInt(2);
            assertEquals(8, out.position());
            assertEquals(4, out.flushed());
            assertEquals(4, out.flushedToNetwork());

            out.doFlush(0);
            assertEquals(8, out.position());
            assertEquals(8, out.flushed());
            assertEquals(4, out.flushedToNetwork());

            read = channel.readOutbound();
            assertEquals(4, read.readableBytes());
            assertEquals(1, read.getInt(0));
            assertEquals(8, out.flushed());
            assertEquals(8, out.flushedToNetwork());

            read = channel.readOutbound();
            assertEquals(4, read.readableBytes());
            assertEquals(2, read.getInt(0));

            out.write(new byte[16]);
            assertEquals(24, out.position());
            assertEquals(8, out.flushed());
            assertEquals(8, out.flushedToNetwork());

            out.doFlush(0);
            assertEquals(24, out.position());
            assertEquals(24, out.flushed());
            assertEquals(24, out.flushedToNetwork());

            read = channel.readOutbound();
            assertEquals(16, read.readableBytes());
            assertEquals(0, read.getLong(0));
            assertEquals(0, read.getLong(8));
            assertEquals(24, out.position());
            assertEquals(24, out.flushed());
            assertEquals(24, out.flushedToNetwork());

            out.writeToChannel(alloc -> {
                ByteBuffer buffer = alloc.get(16);
                buffer.putLong(1);
                buffer.putLong(2);
                buffer.flip();
            }, new StreamManager.StreamRateLimiter(FBUtilities.getBroadcastAddressAndPort()));

            assertEquals(40, out.position());
            assertEquals(40, out.flushed());
            assertEquals(40, out.flushedToNetwork());

            read = channel.readOutbound();
            assertEquals(16, read.readableBytes());
            assertEquals(1, read.getLong(0));
            assertEquals(2, read.getLong(8));
        }

    }

}
