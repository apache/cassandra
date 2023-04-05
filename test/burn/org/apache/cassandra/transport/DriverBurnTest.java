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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.*;
import io.netty.buffer.ByteBuf;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.AssertUtil;

import static org.apache.cassandra.config.EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED;
import static org.apache.cassandra.transport.BurnTestUtil.SizeCaps;
import static org.apache.cassandra.transport.BurnTestUtil.generateQueryMessage;
import static org.apache.cassandra.transport.BurnTestUtil.generateQueryStatement;
import static org.apache.cassandra.transport.BurnTestUtil.generateRows;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.assertj.core.api.Assertions.assertThat;

public class DriverBurnTest extends CQLTester
{
    private final CQLConnectionTest.AllocationObserver allocationObserver = new CQLConnectionTest.AllocationObserver();

    @Before
    public void setup()
    {
        PipelineConfigurator configurator = new PipelineConfigurator(NativeTransportService.useEpoll(), false, false, UNENCRYPTED)
        {
            protected ClientResourceLimits.ResourceProvider resourceProvider(ClientResourceLimits.Allocator allocator)
            {
                return BurnTestUtil.observableResourceProvider(allocationObserver).apply(allocator);
            }
        };

        requireNetwork(builder -> builder.withPipelineConfigurator(configurator), builder -> {});
    }

    @Test
    public void test() throws Throwable
    {
        final SizeCaps smallMessageCap = new SizeCaps(10, 20, 5, 10);
        final SizeCaps largeMessageCap = new SizeCaps(1000, 2000, 5, 150);
        int largeMessageFrequency = 1000;

        Message.Type.QUERY.unsafeSetCodec(new Message.Codec<QueryMessage>() {
            public QueryMessage decode(ByteBuf body, ProtocolVersion version)
            {
                QueryMessage queryMessage = QueryMessage.codec.decode(body, version);
                return new QueryMessage(queryMessage.query, queryMessage.options) {
                    protected Message.Response execute(QueryState state, long queryStartNanoTime, boolean traceRequest)
                    {
                        try
                        {
                            int idx = Integer.parseInt(queryMessage.query);
                            SizeCaps caps = idx % largeMessageFrequency == 0 ? largeMessageCap : smallMessageCap;
                            return generateRows(idx, caps);
                        }
                        catch (NumberFormatException e)
                        {
                            // for the requests driver issues under the hood
                            return super.execute(state, queryStartNanoTime, traceRequest);
                        }
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

        List<AssertUtil.ThrowingSupplier<Cluster.Builder>> suppliers =
        Arrays.asList(() -> Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                                   .withProtocolVersion(com.datastax.driver.core.ProtocolVersion.V4)
                                   .withPort(nativePort),
                      () -> Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                                   .allowBetaProtocolVersion()
                                   .withPort(nativePort),
                      () -> Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                                   .withCompression(ProtocolOptions.Compression.LZ4)
                                   .allowBetaProtocolVersion()
                                   .withPort(nativePort),
                      () -> Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                                   .withCompression(ProtocolOptions.Compression.LZ4)
                                   .withProtocolVersion(com.datastax.driver.core.ProtocolVersion.V4)
                                   .withPort(nativePort)
        );

        int threads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch signal = new CountDownLatch(1);

        for (int t = 0; t < threads; t++)
        {
            int threadId = t;
            executor.execute(() -> {
                try (Cluster driver = suppliers.get(threadId % suppliers.size()).get().build();
                     Session session = driver.connect())
                {
                    int counter = 0;
                    while(!Thread.interrupted())
                    {
                        Map<Integer, ResultSetFuture> futures = new HashMap<>();

                        for (int j = 0; j < 10; j++)
                        {
                            int descriptor = counter + j * 100 + threadId * 10000;
                            SizeCaps caps = descriptor % largeMessageFrequency == 0 ? largeMessageCap : smallMessageCap;
                            futures.put(j, session.executeAsync(generateQueryStatement(descriptor, caps)));
                        }

                        for (Map.Entry<Integer, ResultSetFuture> e : futures.entrySet())
                        {
                            final int j = e.getKey().intValue();
                            final int descriptor = counter + j * 100 + threadId * 10000;
                            SizeCaps caps = descriptor % largeMessageFrequency == 0 ? largeMessageCap : smallMessageCap;
                            ResultMessage.Rows expectedRS = generateRows(descriptor, caps);
                            List<Row> actualRS = e.getValue().get().all();

                            for (int i = 0; i < actualRS.size(); i++)
                            {
                                List<ByteBuffer> expected = expectedRS.result.rows.get(i);
                                Row actual = actualRS.get(i);

                                for (int col = 0; col < expected.size(); col++)
                                    Assert.assertEquals(actual.getBytes(col), expected.get(col));
                            }
                        }
                        counter++;
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
    }

    @Test
    public void measureSmallV6() throws Throwable
    {
        perfTest(new SizeCaps(10, 20, 5, 10),
                 new SizeCaps(10, 20, 5, 10),
                 Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                        .allowBetaProtocolVersion()
                        .withPort(nativePort),
                 ProtocolVersion.V6);
    }

    @Test
    public void measureSmallV5() throws Throwable
    {
        perfTest(new SizeCaps(10, 20, 5, 10),
                 new SizeCaps(10, 20, 5, 10),
                 Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                        .withProtocolVersion(com.datastax.driver.core.ProtocolVersion.V5)
                        .withPort(nativePort),
                 ProtocolVersion.V5);
    }

    @Test
    public void measureSmallV4() throws Throwable
    {
        perfTest(new SizeCaps(10, 20, 5, 10),
                 new SizeCaps(10, 20, 5, 10),
                 Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                        .withProtocolVersion(com.datastax.driver.core.ProtocolVersion.V4)
                        .withPort(nativePort),
                 ProtocolVersion.V4);
    }

    @Test
    public void measureLargeV6() throws Throwable
    {
        perfTest(new SizeCaps(1000, 2000, 5, 150),
                 new SizeCaps(1000, 2000, 5, 150),
                 Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                        .allowBetaProtocolVersion()
                        .withPort(nativePort),
                 ProtocolVersion.V6);
    }

    @Test
    public void measureLargeV5() throws Throwable
    {
        perfTest(new SizeCaps(1000, 2000, 5, 150),
                 new SizeCaps(1000, 2000, 5, 150),
                 Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                        .withProtocolVersion(com.datastax.driver.core.ProtocolVersion.V5)
                        .withPort(nativePort),
                 ProtocolVersion.V5);
    }

    @Test
    public void measureLargeV4() throws Throwable
    {
        perfTest(new SizeCaps(1000, 2000, 5, 150),
                 new SizeCaps(1000, 2000, 5, 150),
                 Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                        .withProtocolVersion(com.datastax.driver.core.ProtocolVersion.V4)
                        .withPort(nativePort),
                 ProtocolVersion.V4);
    }

    @Test
    public void measureSmallV6WithCompression() throws Throwable
    {
        perfTest(new SizeCaps(10, 20, 5, 10),
                 new SizeCaps(10, 20, 5, 10),
                 Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                        .allowBetaProtocolVersion()
                        .withCompression(ProtocolOptions.Compression.LZ4)
                        .withPort(nativePort),
                 ProtocolVersion.V6);
    }

    @Test
    public void measureSmallV5WithCompression() throws Throwable
    {
        perfTest(new SizeCaps(10, 20, 5, 10),
                 new SizeCaps(10, 20, 5, 10),
                 Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                        .withProtocolVersion(com.datastax.driver.core.ProtocolVersion.V5)
                        .withCompression(ProtocolOptions.Compression.LZ4)
                        .withPort(nativePort),
                 ProtocolVersion.V5);
    }

    @Test
    public void measureSmallV4WithCompression() throws Throwable
    {
        perfTest(new SizeCaps(10, 20, 5, 10),
                 new SizeCaps(10, 20, 5, 10),
                 Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                        .withProtocolVersion(com.datastax.driver.core.ProtocolVersion.V4)
                        .withCompression(ProtocolOptions.Compression.LZ4)
                        .withPort(nativePort),
                 ProtocolVersion.V4);
    }

    @Test
    public void measureLargeV6WithCompression() throws Throwable
    {
        perfTest(new SizeCaps(1000, 2000, 5, 150),
                 new SizeCaps(1000, 2000, 5, 150),
                 Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                        .allowBetaProtocolVersion()
                        .withCompression(ProtocolOptions.Compression.LZ4)
                        .withPort(nativePort),
                 ProtocolVersion.V6);
    }

    @Test
    public void measureLargeV5WithCompression() throws Throwable
    {
        perfTest(new SizeCaps(1000, 2000, 5, 150),
                 new SizeCaps(1000, 2000, 5, 150),
                 Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                        .withProtocolVersion(com.datastax.driver.core.ProtocolVersion.V5)
                        .withCompression(ProtocolOptions.Compression.LZ4)
                        .withPort(nativePort),
                 ProtocolVersion.V5);
    }

    @Test
    public void measureLargeV4WithCompression() throws Throwable
    {
        perfTest(new SizeCaps(1000, 2000, 5, 150),
                 new SizeCaps(1000, 2000, 5, 150),
                 Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
                        .withProtocolVersion(com.datastax.driver.core.ProtocolVersion.V4)
                        .withCompression(ProtocolOptions.Compression.LZ4)
                        .withPort(nativePort),
                 ProtocolVersion.V4);
    }

    public void perfTest(SizeCaps requestCaps, SizeCaps responseCaps, Cluster.Builder builder, ProtocolVersion version) throws Throwable
    {
        SimpleStatement request = generateQueryStatement(0, requestCaps);
        ResultMessage.Rows response = generateRows(0, responseCaps);
        QueryMessage requestMessage = generateQueryMessage(0, requestCaps, version);
        Envelope message = requestMessage.encode(version);
        int requestSize = message.body.readableBytes();
        message.release();
        message = response.encode(version);
        int responseSize = message.body.readableBytes();
        message.release();
        Message.Type.QUERY.unsafeSetCodec(new Message.Codec<QueryMessage>() {
            public QueryMessage decode(ByteBuf body, ProtocolVersion version)
            {
                QueryMessage queryMessage = QueryMessage.codec.decode(body, version);
                return new QueryMessage(queryMessage.query, queryMessage.options) {
                    protected Message.Response execute(QueryState state, long queryStartNanoTime, boolean traceRequest)
                    {
                        try
                        {
                            int idx = Integer.parseInt(queryMessage.query); // unused
                            return generateRows(idx, responseCaps);
                        }
                        catch (NumberFormatException e)
                        {
                            // for the requests driver issues under the hood
                            return super.execute(state, queryStartNanoTime, traceRequest);
                        }
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

        int threads = 10;
        int perThread = 30;
        ExecutorService executor = Executors.newFixedThreadPool(threads + 10);
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch signal = new CountDownLatch(1);

        AtomicBoolean measure = new AtomicBoolean(false);
        DescriptiveStatistics stats = new DescriptiveStatistics();
        Lock lock = new ReentrantLock();
        for (int t = 0; t < threads; t++)
        {
            executor.execute(() -> {
                try (Cluster driver = builder.build();
                     Session session = driver.connect())
                {
                    while (!executor.isShutdown() && error.get() == null)
                    {
                        Map<Integer, ResultSetFuture> futures = new HashMap<>();

                        for (int j = 0; j < perThread; j++)
                        {
                            long startNanos = nanoTime();
                            ResultSetFuture future = session.executeAsync(request);
                            future.addListener(() -> {
                                long diff = nanoTime() - startNanos;
                                if (measure.get())
                                {
                                    lock.lock();
                                    try
                                    {
                                        stats.addValue(TimeUnit.MICROSECONDS.toMillis(diff));
                                    }
                                    finally
                                    {
                                        lock.unlock();
                                    }
                                }
                            }, executor);
                            futures.put(j, future);
                        }

                        for (Map.Entry<Integer, ResultSetFuture> e : futures.entrySet())
                        {
                            Assert.assertEquals(response.result.size(),
                                                e.getValue().get().all().size());
                        }
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

        Assert.assertFalse(signal.await(30, TimeUnit.SECONDS));
        measure.set(true);
        Assert.assertFalse(signal.await(60, TimeUnit.SECONDS));
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        System.out.println("requestSize = " + requestSize);
        System.out.println("responseSize = " + responseSize);
        System.out.println("Mean:     " + stats.getMean());
        System.out.println("Variance: " + stats.getVariance());
        System.out.println("Median:   " + stats.getPercentile(0.5));
        System.out.println("90p:      " + stats.getPercentile(0.90));
        System.out.println("95p:      " + stats.getPercentile(0.95));
        System.out.println("99p:      " + stats.getPercentile(0.99));
    }
}
// TODO: test disconnecting and reconnecting constantly
