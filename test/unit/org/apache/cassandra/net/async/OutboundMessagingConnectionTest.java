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

package org.apache.cassandra.net.async;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLHandshakeException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.auth.AllowAllInternodeAuthenticator;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingServiceTest;
import org.apache.cassandra.net.async.OutboundHandshakeHandler.HandshakeResult;
import org.apache.cassandra.net.async.OutboundMessagingConnection.State;

import static org.apache.cassandra.net.MessagingService.Verb.ECHO;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.CLOSED;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.CREATING_CHANNEL;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.NOT_READY;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.READY;

public class OutboundMessagingConnectionTest
{
    private static final InetSocketAddress LOCAL_ADDR = new InetSocketAddress("127.0.0.1", 9998);
    private static final InetSocketAddress REMOTE_ADDR = new InetSocketAddress("127.0.0.2", 9999);
    private static final InetSocketAddress RECONNECT_ADDR = new InetSocketAddress("127.0.0.3", 9999);
    private static final int MESSAGING_VERSION = MessagingService.current_version;

    private OutboundConnectionIdentifier connectionId;
    private OutboundMessagingConnection omc;
    private EmbeddedChannel channel;

    private IEndpointSnitch snitch;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        connectionId = OutboundConnectionIdentifier.small(LOCAL_ADDR, REMOTE_ADDR);
        omc = new OutboundMessagingConnection(connectionId, null, Optional.empty(), new AllowAllInternodeAuthenticator());
        channel = new EmbeddedChannel();
        omc.setChannelWriter(ChannelWriter.create(channel, omc::handleMessageResult, Optional.empty()));

        snitch = DatabaseDescriptor.getEndpointSnitch();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setEndpointSnitch(snitch);
        channel.finishAndReleaseAll();
    }

    @Test
    public void sendMessage_CreatingChannel()
    {
        Assert.assertEquals(0, omc.backlogSize());
        omc.setState(CREATING_CHANNEL);
        Assert.assertTrue(omc.sendMessage(new MessageOut<>(ECHO), 1));
        Assert.assertEquals(1, omc.backlogSize());
        Assert.assertEquals(1, omc.getPendingMessages().intValue());
    }

    @Test
    public void sendMessage_HappyPath()
    {
        Assert.assertEquals(0, omc.backlogSize());
        omc.setState(READY);
        Assert.assertTrue(omc.sendMessage(new MessageOut<>(ECHO), 1));
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void sendMessage_Closed()
    {
        Assert.assertEquals(0, omc.backlogSize());
        omc.setState(CLOSED);
        Assert.assertFalse(omc.sendMessage(new MessageOut<>(ECHO), 1));
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertFalse(channel.releaseOutbound());
    }

    @Test
    public void shouldCompressConnection_None()
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.none);
        Assert.assertFalse(OutboundMessagingConnection.shouldCompressConnection(LOCAL_ADDR.getAddress(), REMOTE_ADDR.getAddress()));
    }

    @Test
    public void shouldCompressConnection_All()
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.all);
        Assert.assertTrue(OutboundMessagingConnection.shouldCompressConnection(LOCAL_ADDR.getAddress(), REMOTE_ADDR.getAddress()));
    }

    @Test
    public void shouldCompressConnection_SameDc()
    {
        TestSnitch snitch = new TestSnitch();
        snitch.add(LOCAL_ADDR.getAddress(), "dc1");
        snitch.add(REMOTE_ADDR.getAddress(), "dc1");
        DatabaseDescriptor.setEndpointSnitch(snitch);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertFalse(OutboundMessagingConnection.shouldCompressConnection(LOCAL_ADDR.getAddress(), REMOTE_ADDR.getAddress()));
    }

    private static class TestSnitch extends AbstractEndpointSnitch
    {
        private Map<InetAddress, String> nodeToDc = new HashMap<>();

        void add(InetAddress node, String dc)
        {
            nodeToDc.put(node, dc);
        }

        public String getRack(InetAddress endpoint)
        {
            return null;
        }

        public String getDatacenter(InetAddress endpoint)
        {
            return nodeToDc.get(endpoint);
        }

        public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
        {
            return 0;
        }
    }

    @Test
    public void shouldCompressConnection_DifferentDc()
    {
        TestSnitch snitch = new TestSnitch();
        snitch.add(LOCAL_ADDR.getAddress(), "dc1");
        snitch.add(REMOTE_ADDR.getAddress(), "dc2");
        DatabaseDescriptor.setEndpointSnitch(snitch);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertTrue(OutboundMessagingConnection.shouldCompressConnection(LOCAL_ADDR.getAddress(), REMOTE_ADDR.getAddress()));
    }

    @Test
    public void close_softClose()
    {
        close(true);
    }

    @Test
    public void close_hardClose()
    {
        close(false);
    }

    private void close(boolean softClose)
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());
        Assert.assertEquals(count, omc.getPendingMessages().intValue());

        ScheduledFuture<?> connectionTimeoutFuture = new TestScheduledFuture();
        Assert.assertFalse(connectionTimeoutFuture.isCancelled());
        omc.setConnectionTimeoutFuture(connectionTimeoutFuture);
        ChannelWriter channelWriter = ChannelWriter.create(channel, omc::handleMessageResult, Optional.empty());
        omc.setChannelWriter(channelWriter);

        omc.close(softClose);
        Assert.assertFalse(channel.isActive());
        Assert.assertEquals(State.CLOSED, omc.getState());
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertEquals(0, omc.getPendingMessages().intValue());
        int sentMessages = channel.outboundMessages().size();

        if (softClose)
            Assert.assertTrue(count <= sentMessages);
        else
            Assert.assertEquals(0, sentMessages);
        Assert.assertTrue(connectionTimeoutFuture.isCancelled());
        Assert.assertTrue(channelWriter.isClosed());
    }

    @Test
    public void connect_IInternodeAuthFail()
    {
        IInternodeAuthenticator auth = new IInternodeAuthenticator()
        {
            public boolean authenticate(InetAddress remoteAddress, int remotePort)
            {
                return false;
            }

            public void validateConfiguration() throws ConfigurationException
            {

            }
        };

        MessageOut messageOut = new MessageOut(MessagingService.Verb.GOSSIP_DIGEST_ACK);
        OutboundMessagingPool pool = new OutboundMessagingPool(REMOTE_ADDR, LOCAL_ADDR, null,
                                                               new MessagingServiceTest.MockBackPressureStrategy(null).newState(REMOTE_ADDR.getAddress()), auth);
        omc = pool.getConnection(messageOut);
        Assert.assertSame(State.NOT_READY, omc.getState());
        Assert.assertFalse(omc.connect());
    }

    @Test
    public void connect_ConnectionAlreadyStarted()
    {
        omc.setState(State.CREATING_CHANNEL);
        Assert.assertFalse(omc.connect());
        Assert.assertSame(State.CREATING_CHANNEL, omc.getState());
    }

    @Test
    public void connect_ConnectionClosed()
    {
        omc.setState(State.CLOSED);
        Assert.assertFalse(omc.connect());
        Assert.assertSame(State.CLOSED, omc.getState());
    }

    @Test
    public void connectionTimeout_StateIsReady()
    {
        omc.setState(READY);
        ChannelFuture channelFuture = channel.newPromise();
        Assert.assertFalse(omc.connectionTimeout(channelFuture));
        Assert.assertEquals(READY, omc.getState());
    }

    @Test
    public void connectionTimeout_StateIsClosed()
    {
        omc.setState(CLOSED);
        ChannelFuture channelFuture = channel.newPromise();
        Assert.assertTrue(omc.connectionTimeout(channelFuture));
        Assert.assertEquals(CLOSED, omc.getState());
    }

    @Test
    public void connectionTimeout_AssumeConnectionTimedOut()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());
        Assert.assertEquals(count, omc.getPendingMessages().intValue());

        omc.setState(CREATING_CHANNEL);
        ChannelFuture channelFuture = channel.newPromise();
        Assert.assertTrue(omc.connectionTimeout(channelFuture));
        Assert.assertEquals(NOT_READY, omc.getState());
        Assert.assertEquals(0, omc.backlogSize());
        Assert.assertEquals(0, omc.getPendingMessages().intValue());
    }

    @Test
    public void connectCallback_FutureIsSuccess()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setSuccess();
        Assert.assertTrue(omc.connectCallback(promise));
    }

    @Test
    public void connectCallback_Closed()
    {
        ChannelPromise promise = channel.newPromise();
        omc.setState(State.CLOSED);
        Assert.assertFalse(omc.connectCallback(promise));
    }

    @Test
    public void connectCallback_FailCauseIsSslHandshake()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new SSLHandshakeException("test is only a test"));
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertSame(State.NOT_READY, omc.getState());
    }

    @Test
    public void connectCallback_FailCauseIsNPE()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new NullPointerException("test is only a test"));
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertSame(State.NOT_READY, omc.getState());
    }

    @Test
    public void connectCallback_FailCauseIsIOException()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("test is only a test"));
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertSame(State.NOT_READY, omc.getState());
    }

    @Test
    public void connectCallback_FailedAndItsClosed()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("test is only a test"));
        omc.setState(CLOSED);
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertSame(State.CLOSED, omc.getState());
    }

    @Test
    public void finishHandshake_GOOD()
    {
        ChannelWriter channelWriter = ChannelWriter.create(channel, omc::handleMessageResult, Optional.empty());
        HandshakeResult result = HandshakeResult.success(channelWriter, MESSAGING_VERSION);
        ScheduledFuture<?> connectionTimeoutFuture = new TestScheduledFuture();
        Assert.assertFalse(connectionTimeoutFuture.isCancelled());

        omc.setChannelWriter(null);
        omc.setConnectionTimeoutFuture(connectionTimeoutFuture);
        omc.finishHandshake(result);
        Assert.assertFalse(channelWriter.isClosed());
        Assert.assertEquals(channelWriter, omc.getChannelWriter());
        Assert.assertEquals(READY, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR.getAddress()));
        Assert.assertNull(omc.getConnectionTimeoutFuture());
        Assert.assertTrue(connectionTimeoutFuture.isCancelled());
    }

    @Test
    public void finishHandshake_GOOD_ButClosed()
    {
        ChannelWriter channelWriter = ChannelWriter.create(channel, omc::handleMessageResult, Optional.empty());
        HandshakeResult result = HandshakeResult.success(channelWriter, MESSAGING_VERSION);
        ScheduledFuture<?> connectionTimeoutFuture = new TestScheduledFuture();
        Assert.assertFalse(connectionTimeoutFuture.isCancelled());

        omc.setChannelWriter(null);
        omc.setState(CLOSED);
        omc.setConnectionTimeoutFuture(connectionTimeoutFuture);
        omc.finishHandshake(result);
        Assert.assertTrue(channelWriter.isClosed());
        Assert.assertNull(omc.getChannelWriter());
        Assert.assertEquals(CLOSED, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR.getAddress()));
        Assert.assertNull(omc.getConnectionTimeoutFuture());
        Assert.assertTrue(connectionTimeoutFuture.isCancelled());
    }

    @Test
    public void finishHandshake_DISCONNECT()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());

        HandshakeResult result = HandshakeResult.disconnect(MESSAGING_VERSION);
        omc.finishHandshake(result);
        Assert.assertNotNull(omc.getChannelWriter());
        Assert.assertEquals(CREATING_CHANNEL, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR.getAddress()));
        Assert.assertEquals(count, omc.backlogSize());
    }

    @Test
    public void finishHandshake_CONNECT_FAILURE()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        Assert.assertEquals(count, omc.backlogSize());

        HandshakeResult result = HandshakeResult.failed();
        omc.finishHandshake(result);
        Assert.assertEquals(NOT_READY, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR.getAddress()));
        Assert.assertEquals(0, omc.backlogSize());
    }

    @Test
    public void setStateIfNotClosed_AlreadyClosed()
    {
        AtomicReference<State> state = new AtomicReference<>(CLOSED);
        OutboundMessagingConnection.setStateIfNotClosed(state, NOT_READY);
        Assert.assertEquals(CLOSED, state.get());
    }

    @Test
    public void setStateIfNotClosed_NotClosed()
    {
        AtomicReference<State> state = new AtomicReference<>(READY);
        OutboundMessagingConnection.setStateIfNotClosed(state, NOT_READY);
        Assert.assertEquals(NOT_READY, state.get());
    }

    @Test
    public void reconnectWithNewIp_HappyPath()
    {
        ChannelWriter channelWriter = ChannelWriter.create(channel, omc::handleMessageResult, Optional.empty());
        omc.setChannelWriter(channelWriter);
        omc.setState(READY);
        OutboundConnectionIdentifier originalId = omc.getConnectionId();
        omc.reconnectWithNewIp(RECONNECT_ADDR);
        Assert.assertFalse(omc.getConnectionId().equals(originalId));
        Assert.assertTrue(channelWriter.isClosed());
        Assert.assertNotSame(CLOSED, omc.getState());
    }

    @Test
    public void reconnectWithNewIp_Closed()
    {
        omc.setState(CLOSED);
        OutboundConnectionIdentifier originalId = omc.getConnectionId();
        omc.reconnectWithNewIp(RECONNECT_ADDR);
        Assert.assertSame(omc.getConnectionId(), originalId);
        Assert.assertSame(CLOSED, omc.getState());
    }

    @Test
    public void reconnectWithNewIp_UnsedConnection()
    {
        omc.setState(NOT_READY);
        OutboundConnectionIdentifier originalId = omc.getConnectionId();
        omc.reconnectWithNewIp(RECONNECT_ADDR);
        Assert.assertNotSame(omc.getConnectionId(), originalId);
        Assert.assertSame(NOT_READY, omc.getState());
    }

    private static class TestScheduledFuture implements ScheduledFuture<Object>
    {
        private boolean cancelled = false;

        public long getDelay(TimeUnit unit)
        {
            return 0;
        }

        public int compareTo(Delayed o)
        {
            return 0;
        }

        public boolean cancel(boolean mayInterruptIfRunning)
        {
            cancelled = true;
            return false;
        }

        public boolean isCancelled()
        {
            return cancelled;
        }

        public boolean isDone()
        {
            return false;
        }

        public Object get() throws InterruptedException, ExecutionException
        {
            return null;
        }

        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            return null;
        }
    }
}
