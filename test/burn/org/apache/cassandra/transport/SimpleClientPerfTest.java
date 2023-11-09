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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.AllowAllNetworkAuthorizer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.AssertUtil;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.transport.BurnTestUtil.SizeCaps;
import static org.apache.cassandra.transport.BurnTestUtil.generateQueryMessage;
import static org.apache.cassandra.transport.BurnTestUtil.generateRows;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

@RunWith(Parameterized.class)
public class SimpleClientPerfTest
{
    @Parameterized.Parameter
    public ProtocolVersion version;

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> versions()
    {
        return ProtocolVersion.SUPPORTED.stream()
                                        .map(v -> new Object[]{v})
                                        .collect(Collectors.toList());
    }

    private InetAddress address;
    private int port;

    @Before
    public void setup()
    {
        DatabaseDescriptor.toolInitialization();
        DatabaseDescriptor.setAuthenticator(new AllowAllAuthenticator());
        DatabaseDescriptor.setAuthorizer(new AllowAllAuthorizer());
        DatabaseDescriptor.setNetworkAuthorizer(new AllowAllNetworkAuthorizer());
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
    public void measureSmall() throws Throwable
    {
        perfTest(new SizeCaps(10, 20, 5, 10),
                 new SizeCaps(10, 20, 5, 10),
                 () -> new SimpleClient(address.getHostAddress(),
                                        port, version, true,
                                        new EncryptionOptions())
                       .connect(false),
                 version);
    }

    @Test
    public void measureSmallWithCompression() throws Throwable
    {
        perfTest(new SizeCaps(10, 20, 5, 10),
                 new SizeCaps(10, 20, 5, 10),
                 () -> new SimpleClient(address.getHostAddress(),
                                        port, version, true,
                                        new EncryptionOptions())
                       .connect(true),
                 version);
    }

    @Test
    public void measureLarge() throws Throwable
    {
        perfTest(new SizeCaps(1000, 2000, 5, 150),
                 new SizeCaps(1000, 2000, 5, 150),
                 () -> new SimpleClient(address.getHostAddress(),
                                        port, version, true,
                                        new EncryptionOptions())
                       .connect(false),
                 version);
    }

    @Test
    public void measureLargeWithCompression() throws Throwable
    {
        perfTest(new SizeCaps(1000, 2000, 5, 150),
                 new SizeCaps(1000, 2000, 5, 150),
                 () -> new SimpleClient(address.getHostAddress(),
                                        port, version, true,
                                        new EncryptionOptions())
                       .connect(true),
                 version);
    }

    @SuppressWarnings({"UnstableApiUsage", "UseOfSystemOutOrSystemErr", "ResultOfMethodCallIgnored"})
    public void perfTest(SizeCaps requestCaps, SizeCaps responseCaps, AssertUtil.ThrowingSupplier<SimpleClient> clientSupplier, ProtocolVersion version) throws Throwable
    {
        ResultMessage.Rows response = generateRows(0, responseCaps);
        QueryMessage requestMessage = generateQueryMessage(0, requestCaps, version);
        Envelope message = requestMessage.encode(version);
        int requestSize = message.body.readableBytes();
        message.release();
        message = response.encode(version);
        int responseSize = message.body.readableBytes();
        message.release();

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
                        int idx = Integer.parseInt(queryMessage.query); // unused
                        return generateRows(idx, responseCaps);
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

        int threads = 1;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch signal = new CountDownLatch(1);

        AtomicBoolean measure = new AtomicBoolean(false);
        DescriptiveStatistics stats = new DescriptiveStatistics();
        Lock lock = new ReentrantLock();
        RateLimiter limiter = RateLimiter.create(2000);
        AtomicLong overloadedExceptions = new AtomicLong(0);
        
        // TODO: exercise client -> server large messages
        for (int t = 0; t < threads; t++)
        {
            executor.execute(() -> {
                try (SimpleClient client = clientSupplier.get())
                {
                    while (!executor.isShutdown() && error.get() == null)
                    {
                        List<Message.Request> messages = new ArrayList<>();
                        for (int j = 0; j < 1; j++)
                            messages.add(requestMessage);

                            if (measure.get())
                            {
                                try
                                {
                                    limiter.acquire();
                                    long nanoStart = nanoTime();
                                    client.execute(messages);
                                    long elapsed = nanoTime() - nanoStart;

                                    lock.lock();
                                    try
                                    {
                                        stats.addValue(TimeUnit.NANOSECONDS.toMicros(elapsed));
                                    }
                                    finally
                                    {
                                        lock.unlock();
                                    }
                                }
                                catch (RuntimeException e)
                                {
                                    if (Throwables.anyCauseMatches(e, cause -> cause instanceof OverloadedException))
                                    {
                                        overloadedExceptions.incrementAndGet();
                                    }
                                    else
                                    {
                                        throw e;
                                    }
                                }
                            }
                            else
                            {
                                try
                                {
                                    limiter.acquire();
                                    client.execute(messages); // warm-up
                                }
                                catch (RuntimeException e)
                                {
                                    // Ignore overloads during warmup...
                                    if (!Throwables.anyCauseMatches(e, cause -> cause instanceof OverloadedException))
                                    {
                                        throw e;
                                    }
                                }
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

        System.out.println("Latencies (in microseconds)");
        System.out.println("Elements: " + stats.getN());
        System.out.println("Mean:     " + stats.getMean());
        System.out.println("Variance: " + stats.getVariance());
        System.out.println("Median:   " + stats.getPercentile(0.5));
        System.out.println("90p:      " + stats.getPercentile(0.90));
        System.out.println("95p:      " + stats.getPercentile(0.95));
        System.out.println("99p:      " + stats.getPercentile(0.99));
        System.out.println("Max:      " + stats.getMax());
        
        System.out.println("Failed due to overload: " + overloadedExceptions.get());

        server.stop();
    }
}
