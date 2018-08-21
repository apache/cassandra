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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLHandshakeException;

import com.google.common.net.InetAddresses;
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
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.STATE_CLOSED;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.STATE_CONSUME_ENQUEUED;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.STATE_CONSUME_RUNNING;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.STATE_CREATING_CONNECTION;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.STATE_IDLE;
import static org.apache.cassandra.net.async.OutboundMessagingConnection.State.isValidTransition;

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
    private ChannelWriter channelWriter;
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
        OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                  .protocolVersion(MESSAGING_VERSION)
                                                                  .build();
        channelWriter = ChannelWriter.create(channel, params);
        omc.setChannelWriter(channelWriter);

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
        omc.setState(STATE_CONSUME_RUNNING);
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
        omc.setState(STATE_IDLE);
        Assert.assertTrue(omc.sendMessage(new MessageOut<>(ECHO), 1));
        assertBacklogSizes(1);
        omc.setState(STATE_CONSUME_RUNNING);
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
        omc.setState(STATE_CLOSED);
        Assert.assertFalse(omc.sendMessage(new MessageOut<>(ECHO), 1));
        assertBacklogSizes(0);
        Assert.assertFalse(channel.releaseOutbound());
    }

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
        omc.setConnectionTimeoutFuture(connectionTimeoutFuture);
        omc.setChannelWriter(channelWriter);

        omc.close(softClose);
        Assert.assertFalse(channel.isActive());
        if (!softClose)
            Assert.assertEquals(STATE_CLOSED, omc.getState());
        assertBacklogSizes(softClose ? count : 0);
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
                                                               new MessagingServiceTest.MockBackPressureStrategy(null).newState(REMOTE_ADDR), auth);

        // make sure we also remove the pool object in MS on IAuth fail
        MessagingService ms = MessagingService.instance();
        ms.channelManagers.put(REMOTE_ADDR, pool);

        OutboundMessagingConnection omc = pool.getConnection(messageOut);
        Assert.assertEquals(STATE_IDLE, omc.getState());
        Assert.assertFalse(omc.connect(true));
        Assert.assertFalse(ms.channelManagers.containsKey(REMOTE_ADDR));
    }

    @Test
    public void connect_ConnectionAlreadyStarted()
    {
        omc.setState(STATE_CONSUME_RUNNING);
        omc.reconnect();
        Assert.assertSame(STATE_CONSUME_RUNNING, omc.getState());
    }

    @Test
    public void connect_ConnectionClosed()
    {
        omc.setState(STATE_CLOSED);
        omc.reconnect();
        Assert.assertEquals(STATE_CLOSED, omc.getState());
    }

    @Test
    public void connectionTimeout_StateIsReady()
    {
        omc.setState(STATE_CONSUME_RUNNING);
        ChannelFuture channelFuture = channel.newPromise();
        Assert.assertTrue(omc.connectionTimeout(channelFuture));
        Assert.assertTrue(channelWriter.isClosed());
        Assert.assertEquals(STATE_IDLE, omc.getState());
    }

    @Test
    public void connectionTimeout_StateIsClosed()
    {
        omc.setState(STATE_CLOSED);
        ChannelFuture channelFuture = channel.newPromise();
        Assert.assertFalse(omc.connectionTimeout(channelFuture));
        Assert.assertEquals(STATE_CLOSED, omc.getState());
    }

    @Test
    public void connectionTimeout_AssumeConnectionTimedOut()
    {
        int count = 32;
        for (int i = 0; i < count; i++)
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i));
        assertBacklogSizes(count);

        omc.setState(STATE_CONSUME_RUNNING);
        ChannelFuture channelFuture = channel.newPromise();
        Assert.assertTrue(omc.connectionTimeout(channelFuture));
        Assert.assertEquals(STATE_IDLE, omc.getState());
        assertBacklogSizes(0);
    }

    @Test
    public void connectCallback_FutureIsSuccess()
    {
        omc.setState(STATE_CONSUME_RUNNING);
        ChannelPromise promise = channel.newPromise();
        promise.setSuccess();
        Assert.assertTrue(omc.connectCallback(promise));
    }

    @Test
    public void connectCallback_Closed()
    {
        ChannelPromise promise = channel.newPromise();
        omc.setState(STATE_CLOSED);
        Assert.assertFalse(omc.connectCallback(promise));
    }

    @Test
    public void connectCallback_FailCauseIsSslHandshake()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new SSLHandshakeException("test is only a test"));
        omc.setState(STATE_CONSUME_RUNNING);
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertEquals(STATE_CONSUME_RUNNING, omc.getState());
    }

    @Test
    public void connectCallback_FailCauseIsNPE()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new NullPointerException("test is only a test"));
        omc.setState(STATE_CONSUME_RUNNING);
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertEquals(STATE_IDLE, omc.getState());
    }

    @Test
    public void connectCallback_FailCauseIsIOException()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("test is only a test"));
        omc.setState(STATE_CONSUME_RUNNING);
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertEquals(STATE_CONSUME_RUNNING, omc.getState());
    }

    @Test
    public void connectCallback_FailedAndItsClosed()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("test is only a test"));
        omc.setState(STATE_CLOSED);
        Assert.assertFalse(omc.connectCallback(promise));
        Assert.assertTrue(omc.isClosed());
    }

    @Test
    public void finishHandshake_GOOD()
    {
        HandshakeResult result = HandshakeResult.success(channelWriter, MESSAGING_VERSION);
        ScheduledFuture<?> connectionTimeoutFuture = new TestScheduledFuture();
        Assert.assertFalse(connectionTimeoutFuture.isCancelled());

        omc.setChannelWriter(null);
        omc.setConnectionTimeoutFuture(connectionTimeoutFuture);
        omc.finishHandshake(result);
        Assert.assertFalse(channelWriter.isClosed());
        Assert.assertEquals(channelWriter, omc.getChannelWriter());
        Assert.assertEquals(STATE_IDLE, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR));
        Assert.assertNull(omc.getConnectionTimeoutFuture());
        Assert.assertTrue(connectionTimeoutFuture.isCancelled());
    }

    @Test
    public void finishHandshake_GOOD_ButClosed()
    {
        HandshakeResult result = HandshakeResult.success(channelWriter, MESSAGING_VERSION);
        ScheduledFuture<?> connectionTimeoutFuture = new TestScheduledFuture();
        Assert.assertFalse(connectionTimeoutFuture.isCancelled());

        omc.setChannelWriter(null);
        omc.setState(STATE_CLOSED);
        omc.setConnectionTimeoutFuture(connectionTimeoutFuture);
        omc.finishHandshake(result);
        Assert.assertTrue(channelWriter.isClosed());
        Assert.assertNull(omc.getChannelWriter());
        Assert.assertEquals(STATE_CLOSED, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR));
        Assert.assertNull(omc.getConnectionTimeoutFuture());
        Assert.assertTrue(connectionTimeoutFuture.isCancelled());
    }

    @Test
    public void finishHandshake_DISCONNECT()
    {
        HandshakeResult result = HandshakeResult.disconnect(MESSAGING_VERSION);
        omc.finishHandshake(result);
        Assert.assertNotNull(omc.getChannelWriter());
        Assert.assertEquals(STATE_CREATING_CONNECTION, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR));
    }

    @Test
    public void finishHandshake_CONNECT_FAILURE()
    {
        HandshakeResult result = HandshakeResult.failed();
        omc.finishHandshake(result);
        Assert.assertEquals(STATE_IDLE, omc.getState());
        Assert.assertEquals(MESSAGING_VERSION, MessagingService.instance().getVersion(REMOTE_ADDR));
    }

    @Test
    public void setState_IDLE()
    {
        Assert.assertTrue(isValidTransition(STATE_IDLE, STATE_IDLE));
        Assert.assertTrue(isValidTransition(STATE_IDLE, STATE_CONSUME_ENQUEUED));
        Assert.assertFalse(isValidTransition(STATE_IDLE, STATE_CONSUME_RUNNING));
        Assert.assertTrue(isValidTransition(STATE_IDLE, STATE_CREATING_CONNECTION));
        Assert.assertTrue(isValidTransition(STATE_IDLE, STATE_CLOSED));
    }

    @Test
    public void setState_CONSUME_ENQUEUED()
    {
        Assert.assertTrue(isValidTransition(STATE_CONSUME_ENQUEUED, STATE_IDLE));
        Assert.assertTrue(isValidTransition(STATE_CONSUME_ENQUEUED, STATE_CONSUME_ENQUEUED));
        Assert.assertTrue(isValidTransition(STATE_CONSUME_ENQUEUED, STATE_CONSUME_RUNNING));
        Assert.assertFalse(isValidTransition(STATE_CONSUME_ENQUEUED, STATE_CREATING_CONNECTION));
        Assert.assertTrue(isValidTransition(STATE_CONSUME_ENQUEUED, STATE_CLOSED));
    }

    @Test
    public void setState_CONSUME_RUNNING()
    {
        Assert.assertTrue(isValidTransition(STATE_CONSUME_RUNNING, STATE_IDLE));
        Assert.assertFalse(isValidTransition(STATE_CONSUME_RUNNING, STATE_CONSUME_ENQUEUED));
        Assert.assertTrue(isValidTransition(STATE_CONSUME_RUNNING, STATE_CONSUME_RUNNING));
        Assert.assertFalse(isValidTransition(STATE_CONSUME_RUNNING, STATE_CREATING_CONNECTION));
        Assert.assertTrue(isValidTransition(STATE_CONSUME_RUNNING, STATE_CLOSED));
    }

    @Test
    public void setState_CREATING_CONNECTION()
    {
        Assert.assertTrue(isValidTransition(STATE_CREATING_CONNECTION, STATE_IDLE));
        Assert.assertTrue(isValidTransition(STATE_CREATING_CONNECTION, STATE_CONSUME_ENQUEUED));
        Assert.assertFalse(isValidTransition(STATE_CREATING_CONNECTION, STATE_CONSUME_RUNNING));
        Assert.assertTrue(isValidTransition(STATE_CREATING_CONNECTION, STATE_CREATING_CONNECTION));
        Assert.assertTrue(isValidTransition(STATE_CREATING_CONNECTION, STATE_CLOSED));
    }

    @Test
    public void setState_CLOSED()
    {
        Assert.assertFalse(isValidTransition(STATE_CLOSED, STATE_IDLE));
        Assert.assertFalse(isValidTransition(STATE_CLOSED, STATE_CONSUME_ENQUEUED));
        Assert.assertFalse(isValidTransition(STATE_CLOSED, STATE_CONSUME_RUNNING));
        Assert.assertFalse(isValidTransition(STATE_CLOSED, STATE_CREATING_CONNECTION));
        Assert.assertTrue(isValidTransition(STATE_CLOSED, STATE_CLOSED));
    }

    @Test
    public void reconnectWithNewIp_HappyPath()
    {
        omc.setChannelWriter(channelWriter);
        omc.setState(STATE_CONSUME_RUNNING);
        OutboundConnectionIdentifier originalId = omc.getConnectionId();
        omc.reconnectWithNewIp(RECONNECT_ADDR);
        Assert.assertFalse(omc.getConnectionId().equals(originalId));
        Assert.assertTrue(channelWriter.isClosed());
        Assert.assertNotSame(STATE_CLOSED, omc.getState());
    }

    @Test
    public void reconnectWithNewIp_Closed()
    {
        omc.setState(STATE_CLOSED);
        OutboundConnectionIdentifier originalId = omc.getConnectionId();
        omc.reconnectWithNewIp(RECONNECT_ADDR);
        Assert.assertSame(omc.getConnectionId(), originalId);
        Assert.assertEquals(STATE_CLOSED, omc.getState());
    }

    @Test
    public void reconnectWithNewIp_UnsedConnection()
    {
        omc.setState(STATE_CONSUME_RUNNING);
        OutboundConnectionIdentifier originalId = omc.getConnectionId();
        omc.reconnectWithNewIp(RECONNECT_ADDR);
        Assert.assertNotSame(omc.getConnectionId(), originalId);
        Assert.assertSame(STATE_CONSUME_RUNNING, omc.getState());
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
        omc.handleMessageResult(new QueuedMessage(new MessageOut<>(ECHO), 1), promise);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
    }

    @Test
    public void handleMessageResult_NonIOException()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new NullPointerException("this is a test"));
        omc.handleMessageResult(new QueuedMessage(new MessageOut<>(ECHO), 1), promise);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
        assertBacklogSizes(0);
    }

    @Test
    public void handleMessageResult_IOException_ChannelNotClosed_RetryMsg()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("this is a test"));
        Assert.assertTrue(channel.isActive());
        omc.handleMessageResult(new QueuedMessage(new MessageOut<>(ECHO), 1, 0, true, true), promise);
        Assert.assertFalse(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
        assertBacklogSizes(1);
    }

    @Test
    public void handleMessageResult_Cancelled()
    {
        ChannelPromise promise = channel.newPromise();
        promise.cancel(false);
        Assert.assertTrue(channel.isActive());
        omc.handleMessageResult(new QueuedMessage(new MessageOut<>(ECHO), 1, 0, true, true), promise);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
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
            // add a non-expired, droppable message
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i, currentTime, true, true));

            // add an expired message
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i, 0, true, true));

            // add a non-droppable message
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i, currentTime, false, true));

            // add an expired, non-droppable message
            omc.addToBacklog(new QueuedMessage(new MessageOut<>(ECHO), i, 0, false, true));
        }

        omc.backlogNextExpirationTime = currentTime;
        Assert.assertEquals(droppableCount * 4, omc.backlogSize.get());
        Assert.assertEquals(droppableCount, omc.expireMessages());
        Assert.assertEquals(droppableCount, omc.getDroppedMessages().longValue());
        Assert.assertEquals(droppableCount * 3, omc.backlogSize.get());
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
}
