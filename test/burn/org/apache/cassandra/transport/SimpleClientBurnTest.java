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
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.AllowAllNetworkAuthorizer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.AssertUtil;

import static org.apache.cassandra.config.EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED;
import static org.apache.cassandra.transport.BurnTestUtil.SizeCaps;
import static org.apache.cassandra.transport.BurnTestUtil.generateQueryMessage;
import static org.apache.cassandra.transport.BurnTestUtil.generateRows;
import static org.assertj.core.api.Assertions.assertThat;

public class SimpleClientBurnTest
{

    private static final Logger logger = LoggerFactory.getLogger(CQLConnectionTest.class);

    private InetAddress address;
    private int port;

    @Before
    public void setup()
    {
        DatabaseDescriptor.toolInitialization();
        DatabaseDescriptor.setAuthenticator(new AllowAllAuthenticator());
        DatabaseDescriptor.setAuthorizer(new AllowAllAuthorizer());
        DatabaseDescriptor.setNetworkAuthorizer(new AllowAllNetworkAuthorizer());
        long seed = new SecureRandom().nextLong();
        logger.info("seed: {}", seed);
        address = InetAddress.getLoopbackAddress();
        try
        {
            try (ServerSocket serverSocket = new ServerSocket(0))
            {
                port = serverSocket.getLocalPort();
            }
            Thread.sleep(250);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test() throws Throwable
    {
        SizeCaps smallMessageCap = new SizeCaps(5, 10, 5, 5);
        SizeCaps largeMessageCap = new SizeCaps(1000, 2000, 5, 150);
        int largeMessageFrequency = 1000;

        CQLConnectionTest.AllocationObserver allocationObserver = new CQLConnectionTest.AllocationObserver();
        PipelineConfigurator configurator = new PipelineConfigurator(NativeTransportService.useEpoll(), false, false, UNENCRYPTED)
        {
            protected ClientResourceLimits.ResourceProvider resourceProvider(ClientResourceLimits.Allocator allocator)
            {
                return BurnTestUtil.observableResourceProvider(allocationObserver).apply(allocator);
            }
        };

        Server server = new Server.Builder().withHost(address)
                                            .withPort(port)
                                            .withPipelineConfigurator(configurator)
                                            .build();
        ClientMetrics.instance.init(Collections.singleton(server));
        server.start();

        Message.Type.QUERY.unsafeSetCodec(new Message.Codec<QueryMessage>()
        {
            public QueryMessage decode(ByteBuf body, ProtocolVersion version)
            {
                QueryMessage queryMessage = QueryMessage.codec.decode(body, version);
                return new QueryMessage(queryMessage.query, queryMessage.options)
                {
                    protected Message.Response execute(QueryState state, long queryStartNanoTime, boolean traceRequest)
                    {
                        int idx = Integer.parseInt(queryMessage.query);
                        SizeCaps caps = idx % largeMessageFrequency == 0 ? largeMessageCap : smallMessageCap;
                        return generateRows(idx, caps);
                    }
                };
            }

            public void encode(QueryMessage queryMessage, ByteBuf dest, ProtocolVersion version)
            {
                QueryMessage.codec.encode(queryMessage, dest, version);
            }

            public int encodedSize(QueryMessage queryMessage, ProtocolVersion version)
            {
                return QueryMessage.codec.encodedSize(queryMessage, version);
            }
        });

        List<AssertUtil.ThrowingSupplier<SimpleClient>> suppliers =
        Arrays.asList(
        () -> new SimpleClient(address.getHostAddress(),
                               port, ProtocolVersion.V5, true,
                               new EncryptionOptions())
              .connect(false),
        () -> new SimpleClient(address.getHostAddress(),
                               port, ProtocolVersion.V4, false,
                               new EncryptionOptions())
              .connect(false)
        );

        int threads = 3;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch signal = new CountDownLatch(1);

        // TODO: exercise client -> server large messages
        for (int t = 0; t < threads; t++)
        {
            int threadId = t;
            executor.execute(() -> {
                try (SimpleClient client = suppliers.get(threadId % suppliers.size()).get())
                {
                    int counter = 0;
                    while (!executor.isShutdown() && error.get() == null)
                    {
                        if (counter % 100 == 0)
                            System.out.println("idx = " + counter);
                        List<Message.Request> messages = new ArrayList<>();
                        for (int j = 0; j < 10; j++)
                        {
                            int descriptor = counter + j * 100 + threadId * 10000;
                            SizeCaps caps = descriptor % largeMessageFrequency == 0 ? largeMessageCap : smallMessageCap;
                            QueryMessage query = generateQueryMessage(descriptor, caps, client.connection.getVersion());
                            messages.add(query);
                        }

                        Map<Message.Request, Message.Response> responses = client.execute(messages);
                        for (Map.Entry<Message.Request, Message.Response> entry : responses.entrySet())
                        {
                            int idx = Integer.parseInt(((QueryMessage) entry.getKey()).query);
                            SizeCaps caps = idx % largeMessageFrequency == 0 ? largeMessageCap : smallMessageCap;
                            ResultMessage.Rows actual = ((ResultMessage.Rows) entry.getValue());

                            ResultMessage.Rows expected = generateRows(idx, caps);
                            Assert.assertEquals(expected.result.rows.size(), actual.result.rows.size());
                            for (int i = 0; i < expected.result.rows.size(); i++)
                            {
                                List<ByteBuffer> expectedRow = expected.result.rows.get(i);
                                List<ByteBuffer> actualRow = actual.result.rows.get(i);
                                Assert.assertEquals(expectedRow.size(), actualRow.size());
                                for (int col = 0; col < expectedRow.size(); col++)
                                    Assert.assertEquals(expectedRow.get(col), actualRow.get(col));
                            }
                        }
                        counter++;
                        System.gc(); // try to trigger leak detector
                    }
                }
                catch (Throwable e)
                {
                    e.printStackTrace();
                    error.set(e);
                    signal.countDown();
                }
            });
        }

        Assert.assertFalse(signal.await(120, TimeUnit.SECONDS));
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        assertThat(allocationObserver.endpointAllocationTotal()).isEqualTo(allocationObserver.endpointReleaseTotal());
        assertThat(allocationObserver.globalAllocationTotal()).isEqualTo(allocationObserver.globalReleaseTotal());

        server.stop();
    }

}
