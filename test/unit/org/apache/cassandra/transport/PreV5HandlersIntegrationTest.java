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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.StartupMessage;

import static org.junit.Assert.*;

/**
 * Integration test for PreV5Handlers that uses actual pre-V5 protocol connections
 * to achieve code coverage for queue overload scenarios.
 */
public class PreV5HandlersIntegrationTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(PreV5HandlersIntegrationTest.class);

    private static final ProtocolVersion PRE_V5_PROTOCOL = ProtocolVersion.V4;
    private EventLoopGroup eventLoopGroup;

    @Before
    public void setUp() throws Exception
    {
        // Start embedded Cassandra + native transport
        requireNetwork();

        // Configure for QUEUE_TIME testing:
        // - Ensure we're in throwing mode (not backpressure mode)
        DatabaseDescriptor.setNativeTransportThrowOnOverload(true);

        // - Disable rate limiting to avoid REQUESTS overload
        DatabaseDescriptor.setNativeTransportRateLimitingEnabled(false);

        // - Set high byte limits to avoid BYTES_IN_FLIGHT overload  
        DatabaseDescriptor.setNativeTransportMaxFrameSize(256 * 1024); // 256KB frames
        DatabaseDescriptor.setNativeTransportReceiveQueueCapacityInBytes(10 * 1024 * 1024); // 10MB
        DatabaseDescriptor.setNativeTransportMaxRequestDataInFlightPerIpInBytes(10 * 1024 * 1024); // 10MB

        // - Configure very small timeout and threshold to trigger QUEUE_TIME quickly
        DatabaseDescriptor.setMaxWaitTimeInTransportQueue(100); // 100ms max wait time
        DatabaseDescriptor.setNativeTransportMaxQueueItemAgeThreshold(0.1); // 10% threshold means trigger at 10ms

        eventLoopGroup = new NioEventLoopGroup(1);
    }

    @After
    public void tearDown()
    {
        if (eventLoopGroup != null)
        {
            eventLoopGroup.shutdownGracefully();
        }
    }

    @Test
    public void testPreV5QueueOverloadHandling() throws Exception
    {
        // This test specifically targets lines 262-265 in PreV5Handlers.discardAndThrow()
        // by creating a pre-V5 connection and triggering queue overload conditions

        CountDownLatch errorReceived = new CountDownLatch(1);
        CountDownLatch connectionEstablished = new CountDownLatch(1);

        Bootstrap bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<Channel>()
                {
                    @Override
                    protected void initChannel(Channel ch)
                    {
                        ch.pipeline().addLast("envelope-decoder", new Envelope.Decoder())
                                .addLast("envelope-encoder", Envelope.Encoder.instance)
                                .addLast("test-handler", new PreV5TestHandler(connectionEstablished, errorReceived));
                    }
                });

        ChannelFuture connectFuture = bootstrap.connect(nativeAddr, nativePort);
        Channel channel = connectFuture.sync().channel();

        try
        {
            // Wait for connection to be established
            assertTrue("Connection should be established", connectionEstablished.await(5, TimeUnit.SECONDS));

            // Send multiple rapid requests to trigger queue overload
            // This should eventually trigger the QUEUE_TIME case in PreV5Handlers.discardAndThrow()
            // Detect JaCoCo instrumentation and adjust request loop count for reliability under instrumentation
            boolean isInstrumented = java.lang.management.ManagementFactory.getRuntimeMXBean()
                    .getInputArguments().stream()
                    .anyMatch(s -> s.contains("jacocoagent"));
            int loops = isInstrumented ? 2000 : 200;
            for (int i = 0; i < loops; i++)
            {
                QueryMessage query = new QueryMessage("SELECT * FROM system.local", QueryOptions.DEFAULT);
                Envelope envelope = query.encode(PRE_V5_PROTOCOL);
                channel.writeAndFlush(envelope);

                // Don't wait between requests to maximize queue buildup
                if (i % 20 == 0)
                {
                    // Occasionally add a tiny delay to let some requests accumulate in queue
                    Thread.sleep(1);
                }
            }

            // Wait for overload error to be received
            assertTrue("Should receive overload error", errorReceived.await(10, TimeUnit.SECONDS));

            logger.info("Successfully triggered pre-V5 queue overload handling");
        }
        finally
        {
            channel.close().sync();
        }
    }

    private static class PreV5TestHandler extends SimpleChannelInboundHandler<Envelope>
    {
        private final CountDownLatch connectionEstablished;
        private final CountDownLatch errorReceived;
        private boolean startupSent = false;

        PreV5TestHandler(CountDownLatch connectionEstablished, CountDownLatch errorReceived)
        {
            this.connectionEstablished = connectionEstablished;
            this.errorReceived = errorReceived;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx)
        {
            // Send STARTUP message with pre-V5 protocol
            Map<String, String> options = new HashMap<>();
            options.put(StartupMessage.CQL_VERSION, "3.0.0");
            StartupMessage startup = new StartupMessage(options);
            Envelope startupEnvelope = startup.encode(PRE_V5_PROTOCOL);
            ctx.writeAndFlush(startupEnvelope);
            startupSent = true;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Envelope response)
        {
            logger.debug("Received response: {}", response.header);

            if (startupSent && connectionEstablished.getCount() > 0)
            {
                // STARTUP response received, connection established
                connectionEstablished.countDown();
            }

            // Check if this is an overload error (which we're trying to trigger)
            if (response.header.type == Message.Type.ERROR)
            {
                try
                {
                    Message message = Message.Decoder.decodeMessage(ctx.channel(), response);
                    if (message instanceof ErrorMessage)
                    {
                        ErrorMessage error = (ErrorMessage) message;
                        logger.info("Received error: {}", error.error.getMessage());

                        // Check if this is the overload exception we're targeting
                        if (error.error.getMessage().contains("overload") ||
                                error.error.getMessage().contains("queue") ||
                                error.error instanceof OverloadedException)
                        {
                            errorReceived.countDown();
                        }
                    }
                }
                catch (Exception e)
                {
                    logger.warn("Error decoding message", e);
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        {
            logger.error("Exception in test handler", cause);
            ctx.close();
        }
    }
}
