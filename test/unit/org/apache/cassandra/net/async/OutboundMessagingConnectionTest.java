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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.auth.AllowAllInternodeAuthenticator;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingServiceTest;
import org.apache.cassandra.net.async.OutboundHandshakeHandler.HandshakeResult;

import static org.apache.cassandra.net.MessagingService.Verb.ECHO;
import static org.apache.cassandra.net.MessagingService.Verb.REQUEST_RESPONSE;

public class OutboundMessagingConnectionTest
{
    private static final InetAddressAndPort LOCAL_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.1"), 9998);
    private static final InetAddressAndPort REMOTE_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.2"), 9999);
    private static final InetAddressAndPort RECONNECT_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.3"), 9999);
    private static final int MESSAGING_VERSION = MessagingService.current_version;

    private static final int BACKLOG_PURGE_SIZE = 1;
    private static final int BACKLOG_MAX_DEPTH = 1000;
    private final static MessagingService.Verb VERB_DROPPABLE = MessagingService.Verb.MUTATION; // Droppable, 2s timeout
    private final static MessagingService.Verb VERB_NONDROPPABLE = MessagingService.Verb.GOSSIP_DIGEST_ACK; // Not droppable

    private OutboundConnectionIdentifier connectionId;
    private OutboundMessagingConnection omc;
    private EmbeddedChannel channel;

    private IEndpointSnitch snitch;
    private ServerEncryptionOptions encryptionOptions;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        connectionId = OutboundConnectionIdentifier.small(LOCAL_ADDR, REMOTE_ADDR);
        omc = new OutboundMessagingConnection(connectionId, null, null, new AllowAllInternodeAuthenticator(), BACKLOG_PURGE_SIZE, BACKLOG_MAX_DEPTH);
        channel = new EmbeddedChannel();
        channel.attr(OutboundMessagingConnection.PURGE_MESSAGES_CHANNEL_ATTR).set(false);
        omc.setChannel(channel);
        snitch = DatabaseDescriptor.getEndpointSnitch();
        encryptionOptions = DatabaseDescriptor.getInternodeMessagingEncyptionOptions();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setEndpointSnitch(snitch);
        DatabaseDescriptor.setInternodeMessagingEncyptionOptions(encryptionOptions);
        channel.finishAndReleaseAll();
    }

    @Test
    public void sendMessage_CreatingChannel()
    {
        assertBacklogSizes(0);
        Assert.assertTrue(omc.sendMessage(new MessageOut<>(ECHO), 1));
        assertBacklogSizes(1);
    }

    private void assertBacklogSizes(int size)
    {
        Assert.assertEquals(size, omc.getPendingMessages().intValue());
        Assert.assertEquals(size, omc.backlogSize.get());
    }

    @Test
    public void sendMessage_HappyPath()
    {
        assertBacklogSizes(0);
        Assert.assertTrue(omc.sendMessage(new MessageOut<>(ECHO), 1));
        assertBacklogSizes(1);
    }


    @Test
    public void sendMessage_BacklogTooDeep()
    {
        int backlogDepth = BACKLOG_MAX_DEPTH * 2;
        omc.backlogSize.set(backlogDepth);
        Assert.assertFalse(omc.sendMessage(new MessageOut<>(REQUEST_RESPONSE), 1));
        Assert.assertEquals(backlogDepth, omc.backlogSize.get());
    }

    @Test
    public void sendMessage_Closed()
    {
        assertBacklogSizes(0);
        omc.unsafeSetClosed(true);
        Assert.assertFalse(omc.sendMessage(new MessageOut<>(ECHO), 1));
        assertBacklogSizes(0);
        Assert.assertFalse(channel.releaseOutbound());
    }

    //TODO:JEB fix this
//    @Test
//    public void dequeueMessages_AllExpired()
//    {
//        int msgCount = OutboundMessagingConnection.MAX_DEQUEUE_COUNT;
//        for (int i = 0; i < msgCount; i++)
//            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i, 0, true, true));
//
//        assertBacklogSizes(msgCount);
//        Assert.assertEquals(0, omc.getDroppedMessages().intValue());
//        Assert.assertEquals(0, omc.getCompletedMessages().intValue());
//        Assert.assertFalse(omc.dequeueMessages());
//        Assert.assertFalse(channel.releaseOutbound());
//        Assert.assertEquals(msgCount, omc.getDroppedMessages().intValue());
//        Assert.assertEquals(0, omc.getCompletedMessages().intValue());
//        assertBacklogSizes(0);
//    }
//
//    @Test
//    public void dequeueMessages_HappyPathWithFlush()
//    {
//        int msgCount = 4;
//        for (int i = 0; i < msgCount; i++)
//            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
//
//        assertBacklogSizes(msgCount);
//        Assert.assertEquals(0, omc.getDroppedMessages().intValue());
//        Assert.assertEquals(0, omc.getCompletedMessages().intValue());
//        Assert.assertEquals(0, channel.unsafe().outboundBuffer().totalPendingWriteBytes());
//
//        Assert.assertTrue(omc.dequeueMessages());
//        Assert.assertEquals(0, channel.unsafe().outboundBuffer().totalPendingWriteBytes());
//        Assert.assertEquals(1, channel.outboundMessages().size());  // messages should be written into a single buffer
//        Assert.assertTrue(channel.releaseOutbound());
//        Assert.assertEquals(0, omc.getDroppedMessages().intValue());
//        Assert.assertEquals(msgCount, omc.getCompletedMessages().intValue());
//        assertBacklogSizes(0);
//    }

    @Test
    public void shouldCompressConnection_None()
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.none);
        Assert.assertFalse(OutboundMessagingConnection.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_All()
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.all);
        Assert.assertTrue(OutboundMessagingConnection.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_SameDc()
    {
        TestSnitch snitch = new TestSnitch();
        snitch.add(LOCAL_ADDR, "dc1");
        snitch.add(REMOTE_ADDR, "dc1");
        DatabaseDescriptor.setEndpointSnitch(snitch);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertFalse(OutboundMessagingConnection.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    private static class TestSnitch extends AbstractEndpointSnitch
    {
        private final Map<InetAddressAndPort, String> nodeToDc = new HashMap<>();

        void add(InetAddressAndPort node, String dc)
        {
            nodeToDc.put(node, dc);
        }

        public String getRack(InetAddressAndPort endpoint)
        {
            return null;
        }

        public String getDatacenter(InetAddressAndPort endpoint)
        {
            return nodeToDc.get(endpoint);
        }

        public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
        {
            return 0;
        }
    }

    @Test
    public void shouldCompressConnection_DifferentDc()
    {
        TestSnitch snitch = new TestSnitch();
        snitch.add(LOCAL_ADDR, "dc1");
        snitch.add(REMOTE_ADDR, "dc2");
        DatabaseDescriptor.setEndpointSnitch(snitch);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertTrue(OutboundMessagingConnection.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
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
        Assert.assertFalse(omc.isClosed());
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        assertBacklogSizes(count);

        ScheduledFuture<?> connectionTimeoutFuture = new TestScheduledFuture();
        Assert.assertFalse(connectionTimeoutFuture.isCancelled());
        omc.connectionTimeoutFuture = connectionTimeoutFuture;

        omc.close(softClose);
        Assert.assertFalse(channel.isActive());
        if (!softClose)
            Assert.assertTrue(omc.isClosed());
        assertBacklogSizes(softClose ? count : 0);
        Assert.assertTrue(connectionTimeoutFuture.isCancelled());
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
            { }
        };

        MessageOut messageOut = new MessageOut(MessagingService.Verb.GOSSIP_DIGEST_ACK);
        OutboundMessagingPool pool = new OutboundMessagingPool(REMOTE_ADDR, LOCAL_ADDR, null,
                                                               new MessagingServiceTest.MockBackPressureStrategy(null).newState(REMOTE_ADDR), auth);

        // make sure we also remove the pool object in MS on IAuth fail
        MessagingService ms = MessagingService.instance();
        ms.channelManagers.put(REMOTE_ADDR, pool);

        OutboundMessagingConnection omc = pool.getConnection(messageOut);
        Assert.assertFalse(omc.connect(true));
        Assert.assertFalse(ms.channelManagers.containsKey(REMOTE_ADDR));
    }

    @Test
    public void connect_ConnectionClosed()
    {
        omc.unsafeSetClosed(true);
        omc.connect(true);
        Assert.assertTrue(omc.isClosed());
    }

    @Test
    public void connectionTimeout()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        assertBacklogSizes(count);

        ChannelFuture channelFuture = channel.newPromise();
        omc.connectionTimeout(channelFuture);
        assertBacklogSizes(0);
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
        Assert.assertTrue(channel.isOpen());
        omc.unsafeSetClosed(true);
        ChannelPromise promise = channel.newPromise();
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertFalse(channel.isOpen());
    }

    @Test
    public void connectCallback_FailCauseIsNPE()
    {
        Assert.assertEquals(0, omc.connectAttemptCount);
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new NullPointerException("test is only a test"));
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertEquals(0, omc.connectAttemptCount);
    }

    @Test
    public void connectCallback_FailCauseIsIOException()
    {
        Assert.assertNull(omc.connectionRetryFuture);
        Assert.assertEquals(0, omc.connectAttemptCount);
        ScheduledFuture<?> connectionTimeoutFuture = new TestScheduledFuture();
        omc.connectionTimeoutFuture = connectionTimeoutFuture;

        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("test is only a test"));
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertEquals(1, omc.connectAttemptCount);

        // should not change the timeout future
        Assert.assertSame(connectionTimeoutFuture, omc.connectionTimeoutFuture);
        Assert.assertNotNull(omc.connectionRetryFuture);
    }

    @Test
    public void connectCallback_FailCauseIsIOException_NoRetry()
    {
        Assert.assertNull(omc.connectionRetryFuture);
        omc.connectAttemptCount = OutboundMessagingConnection.MAX_RECONNECT_ATTEMPTS;
        ScheduledFuture<?> connectionTimeoutFuture = new TestScheduledFuture();
        omc.connectionTimeoutFuture = connectionTimeoutFuture;

        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("test is only a test"));
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertEquals(OutboundMessagingConnection.MAX_RECONNECT_ATTEMPTS, omc.connectAttemptCount);

        // should not change the timeout future
        Assert.assertSame(connectionTimeoutFuture, omc.connectionTimeoutFuture);
        Assert.assertNull(omc.connectionRetryFuture);
    }

    @Test
    public void maybeReconnect_NoMessages()
    {
        assertBacklogSizes(0);
        Assert.assertNull(omc.connectionTimeoutFuture);
        omc.maybeReconnect();
        Assert.assertNull(omc.connectionTimeoutFuture);
    }

    @Test
    public void maybeReconnect_WithExpiredMessages()
    {
        omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), 1, 0, true, true));
        assertBacklogSizes(1);
        Assert.assertNull(omc.connectionTimeoutFuture);
        omc.maybeReconnect();
        assertBacklogSizes(0);
        Assert.assertNull(omc.connectionTimeoutFuture);
    }

    @Test
    public void maybeReconnect_WithMessages()
    {
        omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), 1, System.nanoTime(), false, true));
        assertBacklogSizes(1);
        Assert.assertNull(omc.connectionTimeoutFuture);
        omc.maybeReconnect();
        assertBacklogSizes(1);
        Assert.assertNotNull(omc.connectionTimeoutFuture);
    }

    @Test
    public void finishHandshake_GOOD()
    {
        HandshakeResult result = HandshakeResult.success(channel, MESSAGING_VERSION);
        ScheduledFuture<?> connectionTimeoutFuture = new TestScheduledFuture();
        Assert.assertFalse(connectionTimeoutFuture.isCancelled());

        OutboundConnectionParams params = OutboundConnectionParams.builder().connectionId(connectionId).protocolVersion(MESSAGING_VERSION).build();
        omc.connectionTimeoutFuture = connectionTimeoutFuture;
        omc.finishHandshake(result);
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR));
        Assert.assertNull(omc.connectionTimeoutFuture);
        Assert.assertTrue(connectionTimeoutFuture.isCancelled());
    }

    @Test
    public void finishHandshake_Closed()
    {
        HandshakeResult result = HandshakeResult.success(channel, MESSAGING_VERSION);
        ScheduledFuture<?> connectionTimeoutFuture = new TestScheduledFuture();
        Assert.assertFalse(connectionTimeoutFuture.isCancelled());

        omc.unsafeSetClosed(true);
        omc.connectionTimeoutFuture = connectionTimeoutFuture;
        omc.finishHandshake(result);
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR));
        Assert.assertNull(omc.connectionTimeoutFuture);
        Assert.assertTrue(connectionTimeoutFuture.isCancelled());
    }

    @Test
    public void finishHandshake_DISCONNECT()
    {
        omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), 1));
        assertBacklogSizes(1);
        Assert.assertNull(omc.connectionTimeoutFuture);
        HandshakeResult result = HandshakeResult.disconnect(MESSAGING_VERSION);
        omc.finishHandshake(result);
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR));
        Assert.assertNotNull(omc.connectionTimeoutFuture); // this is the flag that we've started a connect attempt
    }

    @Test
    public void finishHandshake_NEGOTIATION_FAILURE()
    {
        omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), 1));
        assertBacklogSizes(1);
        HandshakeResult result = HandshakeResult.failed();
        omc.finishHandshake(result);
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR));
        assertBacklogSizes(0);
    }

    @Test
    public void reconnectWithNewIp_Closed()
    {
        omc.unsafeSetClosed(true);
        OutboundConnectionIdentifier originalId = omc.getConnectionId();
        Assert.assertNull(omc.reconnectWithNewIp(RECONNECT_ADDR));
        Assert.assertEquals(omc.getConnectionId(), originalId);
    }

    @Test
    public void reconnectWithNewIp() throws InterruptedException, ExecutionException, TimeoutException
    {
        OutboundConnectionIdentifier originalId = omc.getConnectionId();
        Future<?> future = omc.reconnectWithNewIp(RECONNECT_ADDR);
        Assert.assertNotNull(future);
        future.get(2, TimeUnit.SECONDS);
        Assert.assertNotEquals(omc.getConnectionId(), originalId);
    }

    @Test
    public void maybeUpdateConnectionId_NoEncryption()
    {
        OutboundConnectionIdentifier connectionId = omc.getConnectionId();
        int version = omc.getTargetVersion();
        omc.maybeUpdateConnectionId();
        Assert.assertEquals(connectionId, omc.getConnectionId());
        Assert.assertEquals(version, omc.getTargetVersion());
    }

    @Test
    public void maybeUpdateConnectionId_SameVersion()
    {
        ServerEncryptionOptions encryptionOptions = new ServerEncryptionOptions();
        omc = new NonSendingOutboundMessagingConnection(connectionId, encryptionOptions, null);
        OutboundConnectionIdentifier connectionId = omc.getConnectionId();
        int version = omc.getTargetVersion();
        omc.maybeUpdateConnectionId();
        Assert.assertEquals(connectionId, omc.getConnectionId());
        Assert.assertEquals(version, omc.getTargetVersion());
    }

    @Test
    public void maybeUpdateConnectionId_3_X_Version()
    {
        ServerEncryptionOptions encryptionOptions = new ServerEncryptionOptions();
        encryptionOptions.enabled = true;
        encryptionOptions.internode_encryption = ServerEncryptionOptions.InternodeEncryption.all;
        DatabaseDescriptor.setInternodeMessagingEncyptionOptions(encryptionOptions);
        omc = new NonSendingOutboundMessagingConnection(connectionId, encryptionOptions, null);
        int peerVersion = MessagingService.VERSION_30;
        MessagingService.instance().setVersion(connectionId.remote(), MessagingService.VERSION_30);

        OutboundConnectionIdentifier connectionId = omc.getConnectionId();
        omc.maybeUpdateConnectionId();
        Assert.assertNotEquals(connectionId, omc.getConnectionId());
        Assert.assertEquals(InetAddressAndPort.getByAddressOverrideDefaults(REMOTE_ADDR.address, DatabaseDescriptor.getSSLStoragePort()), omc.getConnectionId().remote());
        Assert.assertEquals(InetAddressAndPort.getByAddressOverrideDefaults(REMOTE_ADDR.address, DatabaseDescriptor.getSSLStoragePort()), omc.getConnectionId().connectionAddress());
        Assert.assertEquals(peerVersion, omc.getTargetVersion());
    }

    @Test
    public void handleMessageResult_FutureIsCancelled()
    {
        ChannelPromise promise = channel.newPromise();
        promise.cancel(false);
        omc.handleMessageResult(promise);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
    }

    @Test
    public void handleMessageResult_NonIOException()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new NullPointerException("this is a test"));
        omc.handleMessageResult(promise);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
        assertBacklogSizes(0);
    }

    @Test
    public void handleMessageResult_IOException_ChannelNotClosed_RetryMsg()
    {
        omc.setMessageDequeuer(omc.new SimpleMessageDequeuer());
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("this is a test"));
        Assert.assertTrue(channel.isActive());
        omc.handleMessageResult(promise);
        Assert.assertFalse(channel.isActive());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
        assertBacklogSizes(0);
    }

    @Test
    public void handleMessageResult_Cancelled()
    {
        ChannelPromise promise = channel.newPromise();
        promise.cancel(false);
        Assert.assertTrue(channel.isActive());
        omc.handleMessageResult(promise);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
        assertBacklogSizes(0);
    }

    @Test
    public void maybeScheduleMessageExpiry_EmptyBacklog()
    {
        long now = System.nanoTime();
        Assert.assertFalse(omc.maybeScheduleMessageExpiry(now));
    }

    @Test
    public void maybeScheduleMessageExpiry_NotTimeYet()
    {
        long now = System.nanoTime();
        omc.backlogNextExpirationTime = now + TimeUnit.MINUTES.toNanos(10);
        omc.backlogSize.set(BACKLOG_PURGE_SIZE + 1);
        Assert.assertFalse(omc.maybeScheduleMessageExpiry(now));
    }

    @Test
    public void maybeScheduleMessageExpiry_AlreadyScheduled()
    {
        long now = System.nanoTime();
        omc.backlogNextExpirationTime = now - TimeUnit.MINUTES.toNanos(10);
        omc.backlogSize.set(BACKLOG_PURGE_SIZE + 1);
        omc.backlogExpirationActive.set(true);
        Assert.assertFalse(omc.maybeScheduleMessageExpiry(now));
    }

    @Test
    public void maybeScheduleMessageExpiry_CanSchedule()
    {
        long now = System.nanoTime();
        omc.backlogNextExpirationTime = now - TimeUnit.MINUTES.toNanos(10);
        omc.backlogSize.set(BACKLOG_PURGE_SIZE + 1);
        omc.backlogExpirationActive.set(false);
        Assert.assertTrue(omc.maybeScheduleMessageExpiry(now));
    }

    @Test
    public void expireMessages_Closed()
    {
        omc.close(false);
        omc.backlogSize.set(10);
        omc.backlogExpirationActive.set(true);
        Assert.assertEquals(0, omc.expireMessages());
        Assert.assertEquals(10, omc.backlogSize.get());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
        Assert.assertTrue(omc.backlogExpirationActive.get());
    }

    @Test
    public void expireMessages()
    {
        int droppableCount = 10;

        long currentTime = System.nanoTime();
        for (int i = 0; i < droppableCount; i++)
        {
            // add an expired message
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i, 0, true, true));
        }


        // add a non-droppable message
        omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), droppableCount + 1, currentTime, false, true));

        // add an expired message - but won't be removed
        omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), droppableCount + 2, 0, true, true));

        omc.backlogNextExpirationTime = currentTime;
        Assert.assertEquals(droppableCount + 2, omc.backlogSize.get());
        Assert.assertEquals(droppableCount, omc.expireMessages());
        Assert.assertEquals(droppableCount, omc.getDroppedMessages().longValue());
        Assert.assertEquals(2, omc.backlogSize.get());
        Assert.assertFalse(omc.backlogExpirationActive.get());
        Assert.assertTrue(currentTime < omc.backlogNextExpirationTime);
    }

    /**
     * Verifies our assumptions whether a Verb can be dropped or not. The tests make use of droppabilty, and
     * may produce wrong test results if their droppabilty is changed.
     */
    @Test
    public void assertDroppability()
    {
        Assert.assertTrue(MessagingService.DROPPABLE_VERBS.contains(VERB_DROPPABLE));
        Assert.assertFalse(MessagingService.DROPPABLE_VERBS.contains(VERB_NONDROPPABLE));
    }


    @Test
    public void bufSize()
    {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(14);
        buf.writeLong(42L);
        buf.writeLong(42L);
        Assert.assertEquals(42, buf.readLong());
        Assert.assertEquals(42, buf.readLong());
    }
}
