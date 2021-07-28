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

import java.nio.channels.ClosedChannelException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result.MessagingSuccess;

import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.minimum_version;
import static org.apache.cassandra.net.ConnectionType.SMALL_MESSAGES;
import static org.apache.cassandra.net.OutboundConnectionInitiator.*;

// TODO: test failure due to exception, timeout, etc
public class HandshakeTest
{
    private static final SocketFactory factory = new SocketFactory();

    @BeforeClass
    public static void startup()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @AfterClass
    public static void cleanup() throws InterruptedException
    {
        factory.shutdownNow();
    }

    private Result handshake(int req, int outMin, int outMax) throws ExecutionException, InterruptedException
    {
        return handshake(req, new AcceptVersions(outMin, outMax), null);
    }
    private Result handshake(int req, int outMin, int outMax, int inMin, int inMax) throws ExecutionException, InterruptedException
    {
        return handshake(req, new AcceptVersions(outMin, outMax), new AcceptVersions(inMin, inMax));
    }
    private Result handshake(int req, AcceptVersions acceptOutbound, AcceptVersions acceptInbound) throws ExecutionException, InterruptedException
    {
        InboundSockets inbound = new InboundSockets(new InboundConnectionSettings().withAcceptMessaging(acceptInbound));
        try
        {
            inbound.open();
            InetAddressAndPort endpoint = inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();
            EventLoop eventLoop = factory.defaultGroup().next();
            Future<Result<MessagingSuccess>> future =
            initiateMessaging(eventLoop,
                              SMALL_MESSAGES,
                              new OutboundConnectionSettings(endpoint)
                                                    .withAcceptVersions(acceptOutbound)
                                                    .withDefaults(ConnectionCategory.MESSAGING),
                              req, AsyncPromise.withExecutor(eventLoop));
            return future.get();
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testBothCurrentVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(current_version, minimum_version, current_version);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        result.success().channel.close();
    }

    @Test
    public void testSendCompatibleOldVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(current_version, current_version, current_version + 1, current_version +1, current_version + 2);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(current_version + 1, result.success().messagingVersion);
        result.success().channel.close();
    }

    @Test
    public void testSendCompatibleFutureVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(current_version + 1, current_version - 1, current_version + 1);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(current_version, result.success().messagingVersion);
        result.success().channel.close();
    }

    @Test
    public void testSendIncompatibleFutureVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(current_version + 1, current_version + 1, current_version + 1);
        Assert.assertEquals(Result.Outcome.INCOMPATIBLE, result.outcome);
        Assert.assertEquals(current_version, result.incompatible().closestSupportedVersion);
        Assert.assertEquals(current_version, result.incompatible().maxMessagingVersion);
    }

    @Test
    public void testSendIncompatibleOldVersion() throws InterruptedException, ExecutionException
    {
        Result result = handshake(current_version + 1, current_version + 1, current_version + 1, current_version + 2, current_version + 3);
        Assert.assertEquals(Result.Outcome.INCOMPATIBLE, result.outcome);
        Assert.assertEquals(current_version + 2, result.incompatible().closestSupportedVersion);
        Assert.assertEquals(current_version + 3, result.incompatible().maxMessagingVersion);
    }

    @Test
    public void testSendCompatibleMaxVersionPre40() throws InterruptedException, ExecutionException
    {
        Result result = handshake(VERSION_3014, VERSION_30, VERSION_3014, VERSION_30, VERSION_3014);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(VERSION_3014, result.success().messagingVersion);
        result.success().channel.close();
    }

    @Test
    public void testSendCompatibleFutureVersionPre40() throws InterruptedException, ExecutionException
    {
        Result result = handshake(VERSION_3014, VERSION_30, VERSION_3014, VERSION_30, VERSION_30);
        Assert.assertEquals(Result.Outcome.RETRY, result.outcome);
        Assert.assertEquals(VERSION_30, result.retry().withMessagingVersion);
    }

    @Test
    public void testSendIncompatibleFutureVersionPre40() throws InterruptedException, ExecutionException
    {
        Result result = handshake(VERSION_3014, VERSION_3014, VERSION_3014, VERSION_30, VERSION_30);
        Assert.assertEquals(Result.Outcome.INCOMPATIBLE, result.outcome);
        Assert.assertEquals(-1, result.incompatible().closestSupportedVersion);
        Assert.assertEquals(VERSION_30, result.incompatible().maxMessagingVersion);
    }

    @Test
    public void testSendCompatibleOldVersionPre40() throws InterruptedException
    {
        try
        {
            handshake(VERSION_30, VERSION_30, VERSION_3014, VERSION_3014, VERSION_3014);
            Assert.fail("Should have thrown");
        }
        catch (ExecutionException e)
        {
            Assert.assertTrue(e.getCause() instanceof ClosedChannelException);
        }
    }

    @Test
    public void testSendIncompatibleOldVersionPre40() throws InterruptedException
    {
        try
        {
            handshake(VERSION_30, VERSION_30, VERSION_30, VERSION_3014, VERSION_3014);
            Assert.fail("Should have thrown");
        }
        catch (ExecutionException e)
        {
            Assert.assertTrue(e.getCause() instanceof ClosedChannelException);
        }
    }

    @Test
    public void testSendCompatibleOldVersion40() throws InterruptedException, ExecutionException
    {
        Result result = handshake(VERSION_30, VERSION_30, VERSION_30, VERSION_30, current_version);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(VERSION_30, result.success().messagingVersion);
    }

    @Test
    public void testSendIncompatibleOldVersion40() throws InterruptedException
    {
        try
        {
            Assert.fail(Objects.toString(handshake(VERSION_30, VERSION_30, VERSION_30, current_version, current_version)));
        }
        catch (ExecutionException e)
        {
            Assert.assertTrue(e.getCause() instanceof ClosedChannelException);
        }
    }

    @Test // fairly contrived case, but since we introduced logic for testing we need to be careful it doesn't make us worse
    public void testSendToFuturePost40BelievedToBePre40() throws InterruptedException, ExecutionException
    {
        Result result = handshake(VERSION_30, VERSION_30, current_version, VERSION_30, current_version + 1);
        Assert.assertEquals(Result.Outcome.SUCCESS, result.outcome);
        Assert.assertEquals(VERSION_30, result.success().messagingVersion);
    }
}
