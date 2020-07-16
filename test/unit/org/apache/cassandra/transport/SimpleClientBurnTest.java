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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import net.bytebuddy.implementation.bytecode.Throw;
import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.AllowAllNetworkAuthorizer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.AssertUtil;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

public class SimpleClientBurnTest
{

    private static final Logger logger = LoggerFactory.getLogger(CQLConnectionTest.class);

    private Random random;
    private InetAddress address;
    private int port;

    @BeforeClass
    public void setup()
    {
        DatabaseDescriptor.toolInitialization();
        DatabaseDescriptor.setAuthenticator(new AllowAllAuthenticator());
        DatabaseDescriptor.setAuthorizer(new AllowAllAuthorizer());
        DatabaseDescriptor.setNetworkAuthorizer(new AllowAllNetworkAuthorizer());
        long seed = new SecureRandom().nextLong();
        logger.info("seed: {}", seed);
        random = new Random(seed);
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

    private static class SizeCaps
    {
        private final int valueMinSize;
        private final int valueMaxSize;
        private final int columnCountCap;
        private final int rowsCountCap;

        private SizeCaps(int valueMinSize, int valueMaxSize, int columnCountCap, int rowsCountCap)
        {
            this.valueMinSize = valueMinSize;
            this.valueMaxSize = valueMaxSize;
            this.columnCountCap = columnCountCap;
            this.rowsCountCap = rowsCountCap;
        }
    }

    @Test
    public void test() throws Throwable
    {
        SizeCaps smallMessageCap = new SizeCaps(5, 10, 5, 5);
        SizeCaps largeMessageCap = new SizeCaps(1000, 2000, 5, 150);
        int largeMessageFrequency = 1000;

        Server server = new Server.Builder().withHost(address)
                                            .withPort(port)
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
                        return getRows(idx, caps);
                    }
                };
            }

            public void encode(QueryMessage queryMessage, ByteBuf dest, ProtocolVersion version)
            {
                QueryMessage.codec.encode(queryMessage, dest, version);
            }

            public int encodedSize(QueryMessage queryMessage, ProtocolVersion version)
            {
                return 0;
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

        int threads = 10;
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
                            QueryMessage query = getQueryMessage(descriptor, caps);
                            messages.add(query);
                        }

                        Map<Message.Request, Message.Response> responses = client.execute(messages);
                        for (Map.Entry<Message.Request, Message.Response> entry : responses.entrySet())
                        {
                            int idx = Integer.parseInt(((QueryMessage) entry.getKey()).query);
                            SizeCaps caps = idx % largeMessageFrequency == 0 ? largeMessageCap : smallMessageCap;
                            ResultMessage.Rows actual = ((ResultMessage.Rows) entry.getValue());

                            ResultMessage.Rows expected = getRows(idx, caps);
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

        Assert.assertFalse(signal.await(600, TimeUnit.SECONDS));
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        server.stop();
    }

    public static QueryMessage getQueryMessage(int idx, SizeCaps sizeCaps)
    {
        Random rnd = new Random(idx);
        List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < sizeCaps.columnCountCap * sizeCaps.rowsCountCap; i++)
            values.add(bytes(rnd, sizeCaps.valueMinSize, sizeCaps.valueMaxSize));

        QueryOptions queryOptions = QueryOptions.create(ConsistencyLevel.ONE,
                                                        values,
                                                        true,
                                                        10,
                                                        null,
                                                        null,
                                                        ProtocolVersion.V4,
                                                        "KEYSPACE");

        return new QueryMessage(Integer.toString(idx), queryOptions);
    }

    public static ResultMessage.Rows getRows(int idx, SizeCaps sizeCaps)
    {
        Random rnd = new Random(idx);
        List<ColumnSpecification> columns = new ArrayList<>();
        for (int i = 0; i < sizeCaps.columnCountCap; i++)
        {
            columns.add(new ColumnSpecification("ks", "cf",
                                                new ColumnIdentifier(bytes(rnd, 5, 10), BytesType.instance),
                                                BytesType.instance));
        }

        List<List<ByteBuffer>> rows = new ArrayList<>();
        int count = rnd.nextInt(sizeCaps.rowsCountCap);
        for (int i = 0; i < count; i++)
        {
            List<ByteBuffer> row = new ArrayList<>();
            for (int j = 0; j < sizeCaps.columnCountCap; j++)
                row.add(bytes(rnd, sizeCaps.valueMinSize, sizeCaps.valueMaxSize));
            rows.add(row);
        }

        ResultSet resultSet = new ResultSet(new ResultSet.ResultMetadata(columns), rows);
        return new ResultMessage.Rows(resultSet);
    }

    public static ByteBuffer bytes(Random rnd, int minSize, int maxSize)
    {
        byte[] bytes = new byte[rnd.nextInt(maxSize) + minSize];
        rnd.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    public static void main(String... args) throws Throwable
    {
        SimpleClientBurnTest test = new SimpleClientBurnTest();
        test.setup();
        test.test();
    }
}