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
package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Envelope;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.assertj.core.api.Assertions;

/**
 * If a client sends a message that can not be parsed by the server then we need to detect this and update metrics
 * for monitoring.
 * <p>
 * An issue was found between 2.1 to 3.0 upgrades with regards to paging serialization. Since
 * this is a serialization issue we hit similar paths by sending bad bytes to the server, so can simulate the mixed-mode
 * paging issue without needing to send proper messages.
 */
@RunWith(Parameterized.class)
public class UnableToParseClientMessageTest extends TestBaseImpl
{
    /** Used by {@link #badBody()} */
    private static boolean BAD_BODY_SEEN_LOGS = false;
    private static Cluster CLUSTER;

    @Parameterized.Parameter(0)
    public ProtocolVersion version;

    @Parameterized.Parameters(name = "version={0}")
    public static Iterable<ProtocolVersion> params()
    {
        return ProtocolVersion.SUPPORTED;
    }

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(1).withConfig(c -> c.with(Feature.values())).start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void badHeader() throws IOException
    {
        byte expectedVersion = (byte) (80 + version.asInt());
        String expectedError = "Invalid or unsupported protocol version (" + expectedVersion + ")";
        CustomHeaderMessage msg = new CustomHeaderMessage(new byte[]{ expectedVersion, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        test(expectedError, msg, ignore -> true);
    }

    @Test
    public void badBody() throws IOException
    {
        String expectedError = "Not enough bytes to read an UTF8 serialized string preceded by its 4 bytes length";
        CustomBodyMessage msg = new CustomBodyMessage(Message.Type.QUERY, Unpooled.wrappedBuffer("this is not correct format".getBytes(StandardCharsets.UTF_8)));
        test(expectedError, msg, results -> {
            if (BAD_BODY_SEEN_LOGS && results.isEmpty())
            {
                // ignore as there are no logs but they have been seen before
                return false;
            }
            else
            {
                if (!results.isEmpty())
                    BAD_BODY_SEEN_LOGS = true;
                return true;
            }
        });
    }

    private void test(String expectedError, Message.Request request, Predicate<List<String>> shouldCheckLogs) throws IOException
    {
        // write gibberish to the native protocol
        IInvokableInstance node = CLUSTER.get(1);

        // maintance note: this error isn't required to be consistent cross release, so if this changes its ok to update the test to reflect the new exception.

        long currentCount = getProtocolExceptionCount(node);
        long logStart = node.logs().mark();
        try (SimpleClient client = SimpleClient.builder("127.0.0.1", 9042).protocolVersion(version).useBeta().build())
        {
            client.connect(false, true);

            // this should return a failed response
            // in pre-v5 the connection isn't closed, so use `false` to avoid waiting
            Message.Response response = client.execute(request, false);
            Assert.assertEquals(Message.Type.ERROR, response.type);
            Assert.assertTrue(response.toString(), response.toString().contains(expectedError));

            node.runOnInstance(() -> {
                // channelRead throws then channelInactive throws after trying to read remaining bytes
                // using spinAssertEquals as the metric is updated AFTER replying back to the client
                // so there is a race where we check the metric before it gets updated
                Util.spinAssertEquals(currentCount + 1L,
                                      () -> CassandraMetricsRegistry.Metrics.getMeters()
                                                                            .get("org.apache.cassandra.metrics.Client.ProtocolException")
                                                                            .getCount(),
                                      10);
                Assert.assertEquals(0, CassandraMetricsRegistry.Metrics.getMeters()
                                                                       .get("org.apache.cassandra.metrics.Client.UnknownException")
                                                                       .getCount());
            });
            // the logs are noSpamLogger, so each iteration may not produce a new log; only valid if present and not seen before
            List<String> results = node.logs().grep(logStart, "Protocol exception with client networking").getResult();
            if (shouldCheckLogs.test(results))
            {
                Assertions.assertThat(results).isNotEmpty();
                results.forEach(s -> Assertions.assertThat(s).contains(expectedError));
            }
        }
    }

    private static long getProtocolExceptionCount(IInvokableInstance node)
    {
        return node.callOnInstance(() -> CassandraMetricsRegistry.Metrics.getMeters()
                                                                         .get("org.apache.cassandra.metrics.Client.ProtocolException")
                                                                         .getCount());
    }

    public static class CustomHeaderMessage extends OptionsMessage
    {
        private final ByteBuf headerEncoded;

        public CustomHeaderMessage(byte[] headerEncoded)
        {
            this(Unpooled.wrappedBuffer(headerEncoded));
        }

        public CustomHeaderMessage(ByteBuf headerEncoded)
        {
            this.headerEncoded = Objects.requireNonNull(headerEncoded);
        }

        @Override
        public Envelope encode(ProtocolVersion version)
        {
            Envelope base = super.encode(version);
            return new CustomHeaderEnvelope(base.header, base.body, headerEncoded);
        }
    }

    private static class CustomHeaderEnvelope extends Envelope
    {
        private final ByteBuf headerEncoded;

        public CustomHeaderEnvelope(Header header, ByteBuf body, ByteBuf headerEncoded)
        {
            super(header, body);
            this.headerEncoded = Objects.requireNonNull(headerEncoded);
        }

        // for V4 and below
        @Override
        public ByteBuf encodeHeader()
        {
            return headerEncoded;
        }

        // for V5 and above
        @Override
        public void encodeHeaderInto(ByteBuffer buf)
        {
            buf.put(headerEncoded.nioBuffer());
        }
    }

    private static class CustomBodyMessage extends Message.Request
    {
        private final ByteBuf body;

        protected CustomBodyMessage(Type type, ByteBuf body)
        {
            super(type);
            this.body = Objects.requireNonNull(body);
        }

        @Override
        public Envelope encode(ProtocolVersion version)
        {
            Codec<?> originalCodec = type.codec;
            try
            {
                setCodec(type, new Codec<Message>()
                {
                    @Override
                    public Message decode(ByteBuf body, ProtocolVersion version)
                    {
                        return originalCodec.decode(body, version);
                    }

                    @Override
                    public void encode(Message message, ByteBuf dest, ProtocolVersion version)
                    {

                        dest.writeBytes(body);
                    }

                    @Override
                    public int encodedSize(Message message, ProtocolVersion version)
                    {
                        return body.readableBytes();
                    }
                });

                return super.encode(version);
            }
            finally
            {
                setCodec(type, originalCodec);
            }
        }

        @Override
        protected Response execute(QueryState queryState, long queryStartNanoTime, boolean traceRequest)
        {
            throw new AssertionError("execute not supported");
        }
    }

    private static void setCodec(Message.Type type, Message.Codec<?> codec)
    {
        try
        {
            type.unsafeSetCodec(codec);
        }
        catch (NoSuchFieldException | IllegalAccessException e)
        {
            throw new AssertionError(e);
        }
    }
}
