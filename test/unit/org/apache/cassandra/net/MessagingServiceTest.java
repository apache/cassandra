/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.*;
import java.util.regex.Matcher;

import com.google.common.collect.Iterables;
import com.google.common.net.InetAddresses;

import com.codahale.metrics.Timer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.MessagingService.ServerChannel;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.net.async.OutboundConnectionParams;
import org.apache.cassandra.net.async.OutboundMessagingPool;
import org.apache.cassandra.utils.FBUtilities;
import org.caffinitas.ohc.histo.EstimatedHistogram;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class MessagingServiceTest
{
    private final static long ONE_SECOND = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);
    private final static long[] bucketOffsets = new EstimatedHistogram(160).getBucketOffsets();
    public static final IInternodeAuthenticator ALLOW_NOTHING_AUTHENTICATOR = new IInternodeAuthenticator()
    {
        public boolean authenticate(InetAddress remoteAddress, int remotePort)
        {
            return false;
        }

        public void validateConfiguration() throws ConfigurationException
        {

        }
    };
    private static IInternodeAuthenticator originalAuthenticator;
    private static ServerEncryptionOptions originalServerEncryptionOptions;
    private static InetAddress originalListenAddress;

    private final MessagingService messagingService = MessagingService.test();

    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setBackPressureStrategy(new MockBackPressureStrategy(Collections.emptyMap()));
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.0.0.1"));
        originalAuthenticator = DatabaseDescriptor.getInternodeAuthenticator();
        originalServerEncryptionOptions = DatabaseDescriptor.getServerEncryptionOptions();
        originalListenAddress = DatabaseDescriptor.getListenAddress();
    }

    private static int metricScopeId = 0;

    @Before
    public void before() throws UnknownHostException
    {
        messagingService.resetDroppedMessagesMap(Integer.toString(metricScopeId++));
        MockBackPressureStrategy.applied = false;
        messagingService.destroyConnectionPool(InetAddress.getByName("127.0.0.2"));
        messagingService.destroyConnectionPool(InetAddress.getByName("127.0.0.3"));
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setInternodeAuthenticator(originalAuthenticator);
        DatabaseDescriptor.setServerEncryptionOptions(originalServerEncryptionOptions);
        DatabaseDescriptor.setShouldListenOnBroadcastAddress(false);
        DatabaseDescriptor.setListenAddress(originalListenAddress);
        FBUtilities.reset();
    }

    @Test
    public void testDroppedMessages()
    {
        MessagingService.Verb verb = MessagingService.Verb.READ;

        for (int i = 1; i <= 5000; i++)
            messagingService.incrementDroppedMessages(verb, i, i % 2 == 0);

        List<String> logs = messagingService.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        Pattern regexp = Pattern.compile("READ messages were dropped in last 5000 ms: (\\d+) internal and (\\d+) cross node. Mean internal dropped latency: (\\d+) ms and Mean cross-node dropped latency: (\\d+) ms");
        Matcher matcher = regexp.matcher(logs.get(0));
        assertTrue(matcher.find());
        assertEquals(2500, Integer.parseInt(matcher.group(1)));
        assertEquals(2500, Integer.parseInt(matcher.group(2)));
        assertTrue(Integer.parseInt(matcher.group(3)) > 0);
        assertTrue(Integer.parseInt(matcher.group(4)) > 0);
        assertEquals(5000, (int) messagingService.getDroppedMessages().get(verb.toString()));

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.incrementDroppedMessages(verb, i, i % 2 == 0);

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        matcher = regexp.matcher(logs.get(0));
        assertTrue(matcher.find());
        assertEquals(1250, Integer.parseInt(matcher.group(1)));
        assertEquals(1250, Integer.parseInt(matcher.group(2)));
        assertTrue(Integer.parseInt(matcher.group(3)) > 0);
        assertTrue(Integer.parseInt(matcher.group(4)) > 0);
        assertEquals(7500, (int) messagingService.getDroppedMessages().get(verb.toString()));
    }

    @Test
    public void testDCLatency() throws Exception
    {
        int latency = 100;
        ConcurrentHashMap<String, Timer> dcLatency = MessagingService.instance().metrics.dcLatency;
        dcLatency.clear();

        long now = ApproximateTime.currentTimeMillis();
        long sentAt = now - latency;
        assertNull(dcLatency.get("datacenter1"));
        addDCLatency(sentAt, now);
        assertNotNull(dcLatency.get("datacenter1"));
        assertEquals(1, dcLatency.get("datacenter1").getCount());
        long expectedBucket = bucketOffsets[Math.abs(Arrays.binarySearch(bucketOffsets, TimeUnit.MILLISECONDS.toNanos(latency))) - 1];
        assertEquals(expectedBucket, dcLatency.get("datacenter1").getSnapshot().getMax());
    }

    @Test
    public void testNegativeDCLatency() throws Exception
    {
        // if clocks are off should just not track anything
        int latency = -100;

        ConcurrentHashMap<String, Timer> dcLatency = MessagingService.instance().metrics.dcLatency;
        dcLatency.clear();

        long now = ApproximateTime.currentTimeMillis();
        long sentAt = now - latency;

        assertNull(dcLatency.get("datacenter1"));
        addDCLatency(sentAt, now);
        assertNull(dcLatency.get("datacenter1"));
    }

    @Test
    public void testQueueWaitLatency() throws Exception
    {
        int latency = 100;
        String verb = MessagingService.Verb.MUTATION.toString();

        ConcurrentHashMap<String, Timer> queueWaitLatency = MessagingService.instance().metrics.queueWaitLatency;
        queueWaitLatency.clear();

        assertNull(queueWaitLatency.get(verb));
        MessagingService.instance().metrics.addQueueWaitTime(verb, latency);
        assertNotNull(queueWaitLatency.get(verb));
        assertEquals(1, queueWaitLatency.get(verb).getCount());
        long expectedBucket = bucketOffsets[Math.abs(Arrays.binarySearch(bucketOffsets, TimeUnit.MILLISECONDS.toNanos(latency))) - 1];
        assertEquals(expectedBucket, queueWaitLatency.get(verb).getSnapshot().getMax());
    }

    @Test
    public void testNegativeQueueWaitLatency() throws Exception
    {
        int latency = -100;
        String verb = MessagingService.Verb.MUTATION.toString();

        ConcurrentHashMap<String, Timer> queueWaitLatency = MessagingService.instance().metrics.queueWaitLatency;
        queueWaitLatency.clear();

        assertNull(queueWaitLatency.get(verb));
        MessagingService.instance().metrics.addQueueWaitTime(verb, latency);
        assertNull(queueWaitLatency.get(verb));
    }

    @Test
    public void testUpdatesBackPressureOnSendWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getBackPressureState(InetAddress.getByName("127.0.0.2"));
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();
        MessageOut<?> ignored = null;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnSend(InetAddress.getByName("127.0.0.2"), noCallback, ignored);
        assertFalse(backPressureState.onSend);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnSend(InetAddress.getByName("127.0.0.2"), bpCallback, ignored);
        assertFalse(backPressureState.onSend);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnSend(InetAddress.getByName("127.0.0.2"), bpCallback, ignored);
        assertTrue(backPressureState.onSend);
    }

    @Test
    public void testUpdatesBackPressureOnReceiveWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getBackPressureState(InetAddress.getByName("127.0.0.2"));
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();
        boolean timeout = false;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), noCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), bpCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), bpCallback, timeout);
        assertTrue(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);
    }

    @Test
    public void testUpdatesBackPressureOnTimeoutWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getBackPressureState(InetAddress.getByName("127.0.0.2"));
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();
        boolean timeout = true;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), noCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), bpCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), bpCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertTrue(backPressureState.onTimeout);
    }

    @Test
    public void testAppliesBackPressureWhenEnabled() throws UnknownHostException
    {
        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.2")), ONE_SECOND);
        assertFalse(MockBackPressureStrategy.applied);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.2")), ONE_SECOND);
        assertTrue(MockBackPressureStrategy.applied);
    }

    @Test
    public void testDoesntApplyBackPressureToBroadcastAddress() throws UnknownHostException
    {
        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.1")), ONE_SECOND);
        assertFalse(MockBackPressureStrategy.applied);
    }

    private static void addDCLatency(long sentAt, long nowTime) throws IOException
    {
        MessageIn.deriveConstructionTime(InetAddress.getLocalHost(), (int)sentAt, nowTime);
    }

    public static class MockBackPressureStrategy implements BackPressureStrategy<MockBackPressureStrategy.MockBackPressureState>
    {
        public static volatile boolean applied = false;

        public MockBackPressureStrategy(Map<String, Object> args)
        {
        }

        @Override
        public void apply(Set<MockBackPressureState> states, long timeout, TimeUnit unit)
        {
            if (!Iterables.isEmpty(states))
                applied = true;
        }

        @Override
        public MockBackPressureState newState(InetAddress host)
        {
            return new MockBackPressureState(host);
        }

        public static class MockBackPressureState implements BackPressureState
        {
            private final InetAddress host;
            public volatile boolean onSend = false;
            public volatile boolean onReceive = false;
            public volatile boolean onTimeout = false;

            private MockBackPressureState(InetAddress host)
            {
                this.host = host;
            }

            @Override
            public void onMessageSent(MessageOut<?> message)
            {
                onSend = true;
            }

            @Override
            public void onResponseReceived()
            {
                onReceive = true;
            }

            @Override
            public void onResponseTimeout()
            {
                onTimeout = true;
            }

            @Override
            public double getBackPressureRateLimit()
            {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public InetAddress getHost()
            {
                return host;
            }
        }
    }

    private static class BackPressureCallback implements IAsyncCallback
    {
        @Override
        public boolean supportsBackPressure()
        {
            return true;
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }

        @Override
        public void response(MessageIn msg)
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }

    private static class NoBackPressureCallback implements IAsyncCallback
    {
        @Override
        public boolean supportsBackPressure()
        {
            return false;
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }

        @Override
        public void response(MessageIn msg)
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }

    /**
     * Make sure that if internode authenticatino fails for an outbound connection that all the code that relies
     * on getting the connection pool handles the null return
     * @throws Exception
     */
    @Test
    public void testFailedInternodeAuth() throws Exception
    {
        MessagingService ms = MessagingService.instance();
        DatabaseDescriptor.setInternodeAuthenticator(ALLOW_NOTHING_AUTHENTICATOR);
        InetAddress address = InetAddress.getByName("127.0.0.250");

        //Should return null
        MessageOut messageOut = new MessageOut(MessagingService.Verb.GOSSIP_DIGEST_ACK);
        assertFalse(ms.isConnected(address, messageOut));

        //Should tolerate null
        ms.convict(address);
        ms.sendOneWay(messageOut, address);
    }

    @Test
    public void testOutboundMessagingConnectionCleansUp() throws Exception
    {
        MessagingService ms = MessagingService.instance();
        InetSocketAddress local = new InetSocketAddress("127.0.0.1", 9876);
        InetSocketAddress remote = new InetSocketAddress("127.0.0.2", 9876);

        OutboundMessagingPool pool = new OutboundMessagingPool(remote, local, null, new MockBackPressureStrategy(null).newState(remote.getAddress()), ALLOW_NOTHING_AUTHENTICATOR);
        ms.channelManagers.put(remote.getAddress(), pool);
        pool.sendMessage(new MessageOut(MessagingService.Verb.GOSSIP_DIGEST_ACK), 0);
        assertFalse(ms.channelManagers.containsKey(remote.getAddress()));
    }

    @Test
    public void reconnectWithNewIp() throws UnknownHostException
    {
        InetAddress publicIp = InetAddress.getByName("127.0.0.2");
        InetAddress privateIp = InetAddress.getByName("127.0.0.3");

        // reset the preferred IP value, for good test hygene
        SystemKeyspace.updatePreferredIP(publicIp, publicIp);

        // create pool/conn with public addr
        Assert.assertEquals(publicIp, messagingService.getCurrentEndpoint(publicIp));
        messagingService.reconnectWithNewIp(publicIp, privateIp);
        Assert.assertEquals(privateIp, messagingService.getCurrentEndpoint(publicIp));

        messagingService.destroyConnectionPool(publicIp);

        // recreate the pool/conn, and make sure the preferred ip addr is used
        Assert.assertEquals(privateIp, messagingService.getCurrentEndpoint(publicIp));
    }

    @Test
    public void testCloseInboundConnections() throws UnknownHostException, InterruptedException
    {
        try
        {
            messagingService.listen();
            Assert.assertTrue(messagingService.isListening());
            Assert.assertTrue(messagingService.serverChannels.size() > 0);
            for (ServerChannel serverChannel : messagingService.serverChannels)
                Assert.assertEquals(0, serverChannel.size());

            // now, create a connection and make sure it's in a channel group
            InetSocketAddress server = new InetSocketAddress(FBUtilities.getBroadcastAddress(), DatabaseDescriptor.getStoragePort());
            OutboundConnectionIdentifier id = OutboundConnectionIdentifier.small(new InetSocketAddress(InetAddress.getByName("127.0.0.2"), 0), server);

            CountDownLatch latch = new CountDownLatch(1);
            OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                      .mode(NettyFactory.Mode.MESSAGING)
                                                                      .sendBufferSize(1 << 10)
                                                                      .connectionId(id)
                                                                      .callback(handshakeResult -> latch.countDown())
                                                                      .protocolVersion(MessagingService.current_version)
                                                                      .build();
            Bootstrap bootstrap = NettyFactory.instance.createOutboundBootstrap(params);
            Channel channel = bootstrap.connect().awaitUninterruptibly().channel();
            Assert.assertNotNull(channel);
            latch.await(1, TimeUnit.SECONDS); // allow the netty pipeline/c* handshake to get set up

            int connectCount = 0;
            for (ServerChannel serverChannel : messagingService.serverChannels)
                connectCount += serverChannel.size();
            Assert.assertTrue(connectCount > 0);
        }
        finally
        {
            // last, shutdown the MS and make sure connections are removed
            messagingService.shutdown(true);
            for (ServerChannel serverChannel : messagingService.serverChannels)
                Assert.assertEquals(0, serverChannel.size());
            messagingService.clearServerChannels();
        }
    }

    @Test
    public void listenPlainConnection()
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions();
        serverEncryptionOptions.enabled = false;
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenPlainConnectionWithBroadcastAddr()
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions();
        serverEncryptionOptions.enabled = false;
        listen(serverEncryptionOptions, true);
    }

    @Test
    public void listenRequiredSecureConnection()
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions();
        serverEncryptionOptions.enabled = true;
        serverEncryptionOptions.optional = false;
        serverEncryptionOptions.enable_legacy_ssl_storage_port = false;
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenRequiredSecureConnectionWithBroadcastAddr()
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions();
        serverEncryptionOptions.enabled = true;
        serverEncryptionOptions.optional = false;
        serverEncryptionOptions.enable_legacy_ssl_storage_port = false;
        listen(serverEncryptionOptions, true);
    }

    @Test
    public void listenRequiredSecureConnectionWithLegacyPort()
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions();
        serverEncryptionOptions.enabled = true;
        serverEncryptionOptions.optional = false;
        serverEncryptionOptions.enable_legacy_ssl_storage_port = true;
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenRequiredSecureConnectionWithBroadcastAddrAndLegacyPort()
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions();
        serverEncryptionOptions.enabled = true;
        serverEncryptionOptions.optional = false;
        serverEncryptionOptions.enable_legacy_ssl_storage_port = true;
        listen(serverEncryptionOptions, true);
    }

    @Test
    public void listenOptionalSecureConnection()
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions();
        serverEncryptionOptions.enabled = true;
        serverEncryptionOptions.optional = true;
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenOptionalSecureConnectionWithBroadcastAddr()
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions();
        serverEncryptionOptions.enabled = true;
        serverEncryptionOptions.optional = true;
        listen(serverEncryptionOptions, true);
    }

    private void listen(ServerEncryptionOptions serverEncryptionOptions, boolean listenOnBroadcastAddr)
    {
        InetAddress listenAddress = null;
        if (listenOnBroadcastAddr)
        {
            DatabaseDescriptor.setShouldListenOnBroadcastAddress(true);
            listenAddress = InetAddresses.increment(FBUtilities.getBroadcastAddress());
            DatabaseDescriptor.setListenAddress(listenAddress);
            FBUtilities.reset();
        }

        try
        {
            messagingService.listen(serverEncryptionOptions);
            Assert.assertTrue(messagingService.isListening());
            int expectedListeningCount = NettyFactory.determineAcceptGroupSize(serverEncryptionOptions);
            Assert.assertEquals(expectedListeningCount, messagingService.serverChannels.size());

            if (!serverEncryptionOptions.enabled)
            {
                // make sure no channel is using TLS
                for (ServerChannel serverChannel : messagingService.serverChannels)
                    Assert.assertEquals(ServerChannel.SecurityLevel.NONE, serverChannel.getSecurityLevel());
            }
            else
            {
                final int legacySslPort = DatabaseDescriptor.getSSLStoragePort();
                boolean foundLegacyListenSslAddress = false;
                for (ServerChannel serverChannel : messagingService.serverChannels)
                {
                    if (serverEncryptionOptions.optional)
                        Assert.assertEquals(ServerChannel.SecurityLevel.OPTIONAL, serverChannel.getSecurityLevel());
                    else
                        Assert.assertEquals(ServerChannel.SecurityLevel.REQUIRED, serverChannel.getSecurityLevel());

                    if (serverEncryptionOptions.enable_legacy_ssl_storage_port)
                    {
                        if (legacySslPort == serverChannel.getAddress().getPort())
                        {
                            foundLegacyListenSslAddress = true;
                            Assert.assertEquals(ServerChannel.SecurityLevel.REQUIRED, serverChannel.getSecurityLevel());
                        }
                    }
                }

                if (serverEncryptionOptions.enable_legacy_ssl_storage_port && !foundLegacyListenSslAddress)
                    Assert.fail("failed to find legacy ssl listen address");
            }

            // check the optional listen address
            if (listenOnBroadcastAddr)
            {
                int expectedCount = (serverEncryptionOptions.enabled && serverEncryptionOptions.enable_legacy_ssl_storage_port) ? 2 : 1;
                int found = 0;
                for (ServerChannel serverChannel : messagingService.serverChannels)
                {
                    if (serverChannel.getAddress().getAddress().equals(listenAddress))
                        found++;
                }

                Assert.assertEquals(expectedCount, found);
            }
        }
        finally
        {
            messagingService.shutdown(true);
            messagingService.clearServerChannels();
            Assert.assertEquals(0, messagingService.serverChannels.size());
        }
    }
}
