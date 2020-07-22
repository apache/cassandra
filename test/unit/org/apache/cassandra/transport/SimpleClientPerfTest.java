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
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.SimpleStatement;
import io.netty.buffer.ByteBuf;
import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.AllowAllNetworkAuthorizer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.AssertUtil;

import static org.apache.cassandra.transport.BurnTestUtil.SizeCaps;
import static org.apache.cassandra.transport.BurnTestUtil.generateQueryMessage;
import static org.apache.cassandra.transport.BurnTestUtil.generateQueryStatement;
import static org.apache.cassandra.transport.BurnTestUtil.generateRows;

@Ignore
public class SimpleClientPerfTest extends SimpleClientBurnTest
{

    private static final Logger logger = LoggerFactory.getLogger(CQLConnectionTest.class);

    private Random random;
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

    @Test
    public void measureSmallV5() throws Throwable
    {
        perfTest(new SizeCaps(10, 20, 5, 10),
                 new SizeCaps(10, 20, 5, 10),
                 () -> new SimpleClient(address.getHostAddress(),
                                        port, ProtocolVersion.V5, true,
                                        new EncryptionOptions())
                       .connect(false));
    }


    @Test
    public void measureSmallV4() throws Throwable
    {
        perfTest(new SizeCaps(10, 20, 5, 10),
                 new SizeCaps(10, 20, 5, 10),
                 () -> new SimpleClient(address.getHostAddress(),
                                  port, ProtocolVersion.V4, false,
                                  new EncryptionOptions())
                 .connect(false));
    }

    public void perfTest(SizeCaps requestCaps, SizeCaps responseCaps, AssertUtil.ThrowingSupplier<SimpleClient> clientSupplier) throws Throwable
    {
        ResultMessage.Rows response = generateRows(0, responseCaps);
        QueryMessage requestMessage = generateQueryMessage(0, requestCaps);
        Frame frame = requestMessage.encode(ProtocolVersion.V4);
        int requestSize = frame.body.readableBytes();
        frame.release();
        frame = response.encode(ProtocolVersion.V4);
        int responseSize = frame.body.readableBytes();
        frame.release();

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
                            long nanoStart = System.nanoTime();
                            client.execute(messages);
                            long diff = System.nanoTime() - nanoStart;

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
                        else
                            client.execute(messages); // warm-up
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

        server.stop();
    }
}

