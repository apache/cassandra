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
import java.net.ConnectException;
import java.net.InetAddress;
import java.nio.channels.ClosedChannelException;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Ticker;
import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result.MessagingSuccess;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.NonBlockingRateLimiter;

import static org.apache.cassandra.net.ConnectionType.SMALL_MESSAGES;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.minimum_version;
import static org.apache.cassandra.net.OutboundConnectionInitiator.initiateMessaging;

public class OutboundConnectionInitiatorTest
{
    private static SocketFactory factory;
    private static final InetAddressAndPort BIND_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.1"), 7016);
    private InboundSockets inbound;
    private InetAddressAndPort endpoint;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
        ClusterMetadataTestHelper.register(BIND_ADDR);
        CommitLog.instance.start();
        factory = new SocketFactory();
    }

    @AfterClass
    public static void afterClass()
    {
        factory.shutdownNow();
    }

    @Before
    public void setup() throws Exception
    {
        inbound = new InboundSockets(new InboundConnectionSettings().withAcceptMessaging(new AcceptVersions(minimum_version, current_version)));
        inbound.open();
        endpoint = inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();
    }

    @After
    public void tearDown() throws Exception
    {
        if (inbound != null)
            inbound.close().await(2L, TimeUnit.SECONDS);
    }

    private OutboundConnectionSettings createSettings(SocketFactory socketFactory)
    {
        return new OutboundConnectionSettings(endpoint)
               .withAcceptVersions(new AcceptVersions(minimum_version, current_version))
               .withSocketFactory(socketFactory)
               .withDefaults(ConnectionCategory.MESSAGING);
    }

    @Test
    public void testRateLimiterDisabledFastPath() throws Exception
    {
        DatabaseDescriptor.setInternodeMaxOutboundConnectionsPerSecond(0);
        try
        {
            EventLoop loop = factory.defaultGroup().next();
            long startNanos = System.nanoTime();
            Future<Result<MessagingSuccess>> future = initiateMessaging(loop, SMALL_MESSAGES,
                                                                        OutboundConnectionInitiator.SslFallbackConnectionType.SERVER_CONFIG,
                                                                        createSettings(factory), AsyncPromise.withExecutor(loop));

            Result<MessagingSuccess> result = future.get(2, TimeUnit.SECONDS);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            Assert.assertTrue("Connection should succeed", result.isSuccess());
            Assert.assertTrue("Fast path should connect almost instantaneously (< 200ms), took " + elapsedMillis + "ms",
                              elapsedMillis < 200);
            result.success().channel.close();
        }
        finally
        {
            DatabaseDescriptor.setInternodeMaxOutboundConnectionsPerSecond(250);
        }
    }

    @Test
    public void testCancellationWhileDelayedInQueue() throws Exception
    {
        SocketFactory testFactory = new SocketFactory(new NonBlockingRateLimiter(1, 0, Ticker.systemTicker()));
        try
        {
            EventLoop loop1 = testFactory.defaultGroup().next();
            Future<Result<MessagingSuccess>> f1 = initiateMessaging(loop1, SMALL_MESSAGES,
                                                                    OutboundConnectionInitiator.SslFallbackConnectionType.SERVER_CONFIG,
                                                                    createSettings(testFactory), AsyncPromise.withExecutor(loop1));
            Result<MessagingSuccess> r1 = f1.get(2, TimeUnit.SECONDS);
            Assert.assertTrue(r1.isSuccess());

            EventLoop loop2 = testFactory.defaultGroup().next();
            AsyncPromise<Result<MessagingSuccess>> promise2 = AsyncPromise.withExecutor(loop2);
            Future<Result<MessagingSuccess>> f2 = initiateMessaging(loop2, SMALL_MESSAGES,
                                                                    OutboundConnectionInitiator.SslFallbackConnectionType.SERVER_CONFIG,
                                                                    createSettings(testFactory), promise2);

            Assert.assertFalse("Second connection should be waiting in delay queue", f2.isDone());
            promise2.cancel(true);
            Assert.assertTrue("Future must reflect cancellation", f2.isCancelled());
            Thread.sleep(300);
            Assert.assertTrue("Cancelled connection must remain cancelled and never succeed", f2.isCancelled());

            r1.success().channel.close();
        }
        finally
        {
            testFactory.shutdownNow();
        }
    }

    @Test
    public void testRejectionOnEventLoopShutdown() throws Exception
    {
        EventLoop dyingLoop = new DefaultEventLoop();
        dyingLoop.shutdownGracefully().syncUninterruptibly();

        AsyncPromise<Result<MessagingSuccess>> promise = AsyncPromise.withExecutor(dyingLoop);
        Future<Result<MessagingSuccess>> future = initiateMessaging(dyingLoop, SMALL_MESSAGES,
                                                                    OutboundConnectionInitiator.SslFallbackConnectionType.SERVER_CONFIG,
                                                                    createSettings(factory), promise);

        Assert.assertTrue("Should fail immediately on shutdown event loop", future.isDone());
        Assert.assertFalse(future.isSuccess());
        Assert.assertTrue(future.cause() instanceof ConnectException);
    }

    @Test
    public void testSynchronousAuthFailurePropagation() throws Exception
    {
        SocketFactory testFactory = new SocketFactory(new NonBlockingRateLimiter(2, 0, Ticker.systemTicker()));
        try
        {
            EventLoop loop = testFactory.defaultGroup().next();
            OutboundConnectionSettings rejectingSettings = createSettings(testFactory).withAuthenticator(new IInternodeAuthenticator()
            {
                public boolean authenticate(InetAddress toAddress, int toPort) { return false; }
                public boolean authenticate(InetAddress toAddress, int toPort, Certificate[] certificates, InternodeConnectionDirection direction) { return false; }
                public void validateConfiguration() {}
            });

            AsyncPromise<Result<MessagingSuccess>> p1 = AsyncPromise.withExecutor(loop);
            initiateMessaging(loop, SMALL_MESSAGES, OutboundConnectionInitiator.SslFallbackConnectionType.SERVER_CONFIG, rejectingSettings, p1);

            AsyncPromise<Result<MessagingSuccess>> p2 = AsyncPromise.withExecutor(loop);
            Future<Result<MessagingSuccess>> f2 = initiateMessaging(loop, SMALL_MESSAGES, OutboundConnectionInitiator.SslFallbackConnectionType.SERVER_CONFIG, rejectingSettings, p2);

            f2.await(2, TimeUnit.SECONDS);
            Assert.assertTrue("Request 2 must be completed", f2.isDone());
            Assert.assertFalse("Request 2 must fail due to auth rejection", f2.isSuccess());
            Assert.assertTrue("Must be IOException from failed authentication", f2.cause() instanceof IOException);
            Assert.assertTrue("Error message must mention Authentication failed", f2.cause().getMessage().contains("Authentication failed"));
        }
        finally
        {
            testFactory.shutdownNow();
        }
    }

    @Test
    public void testConcurrentConnectionRateThrottling() throws Exception
    {
        final int targetRate = 5;
        final int totalConnections = 10;
        SocketFactory testFactory = new SocketFactory(new NonBlockingRateLimiter(targetRate, 0, Ticker.systemTicker()));
        try
        {
            List<Future<Result<MessagingSuccess>>> futures = new ArrayList<>();
            List<Result<MessagingSuccess>> results = new ArrayList<>();
            long startNanos = System.nanoTime();
            try
            {
                for (int i = 0; i < totalConnections; i++)
                {
                    EventLoop loop = testFactory.defaultGroup().next();
                    futures.add(initiateMessaging(loop, SMALL_MESSAGES,
                                                  OutboundConnectionInitiator.SslFallbackConnectionType.SERVER_CONFIG,
                                                  createSettings(testFactory), AsyncPromise.withExecutor(loop)));
                }

                for (Future<Result<MessagingSuccess>> f : futures)
                {
                    Result<MessagingSuccess> r = f.get(10, TimeUnit.SECONDS);
                    Assert.assertTrue("Connection handshake must succeed", r.isSuccess());
                    results.add(r);
                }
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                Assert.assertTrue("10 connections at 5/sec should take at least 1700ms, took " + elapsedMillis + "ms",
                                  elapsedMillis >= 1700);
            }
            finally
            {
                for (Result<MessagingSuccess> r : results)
                {
                    if (r != null && r.isSuccess())
                        r.success().channel.close();
                }
            }
        }
        finally
        {
            testFactory.shutdownNow();
        }
    }

    @Test
    public void testShutdownOccurringDuringDelay() throws Exception
    {
        SocketFactory testFactory = new SocketFactory(new NonBlockingRateLimiter(1, 0, Ticker.systemTicker()));
        EventLoop loop = new DefaultEventLoop();
        try
        {
            testFactory.outboundConnectionRateLimiter().reserveAndGetDelay(TimeUnit.SECONDS);

            AsyncPromise<Result<MessagingSuccess>> promise = AsyncPromise.withExecutor(loop);
            Future<Result<MessagingSuccess>> future = initiateMessaging(loop, SMALL_MESSAGES,
                                                                        OutboundConnectionInitiator.SslFallbackConnectionType.SERVER_CONFIG,
                                                                        createSettings(testFactory), promise);
            Assert.assertFalse("Connection must be waiting in the delay queue", future.isDone());

            loop.shutdownGracefully();

            future.await(3, TimeUnit.SECONDS);
            Assert.assertTrue("Connection must terminate promptly on loop shutdown", future.isDone());
            Assert.assertFalse("Connection must not succeed", future.isSuccess());
            Assert.assertTrue("Must fail with ClosedChannelException", future.cause() instanceof ClosedChannelException);
        }
        finally
        {
            testFactory.shutdownNow();
            if (!loop.isTerminated())
                loop.shutdownGracefully().syncUninterruptibly();
        }
    }

}
