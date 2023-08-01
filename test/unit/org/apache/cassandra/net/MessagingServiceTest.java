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
import java.nio.channels.AsynchronousSocketChannel;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Timer;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.MessagingMetrics;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.awaitility.Awaitility;
import org.caffinitas.ohc.histo.EstimatedHistogram;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MessagingServiceTest
{
    private final static long[] bucketOffsets = new EstimatedHistogram(160).getBucketOffsets();
    public static AtomicInteger rejectedConnections = new AtomicInteger();
    public static final IInternodeAuthenticator ALLOW_NOTHING_AUTHENTICATOR = new IInternodeAuthenticator()
    {
        public boolean authenticate(InetAddress remoteAddress, int remotePort,
                                    Certificate[] certificates, InternodeConnectionDirection connectionType)
        {
            rejectedConnections.incrementAndGet();
            return false;
        }

        public void validateConfiguration() throws ConfigurationException
        {

        }
    };

    public static final IInternodeAuthenticator REJECT_OUTBOUND_AUTHENTICATOR = new IInternodeAuthenticator()
    {
        public boolean authenticate(InetAddress remoteAddress, int remotePort,
                                    Certificate[] certificates, InternodeConnectionDirection connectionType)
        {
            if (connectionType == InternodeConnectionDirection.OUTBOUND)
            {
                rejectedConnections.incrementAndGet();
                return false;
            }
            return true;
        }

        public void validateConfiguration() throws ConfigurationException
        {

        }
    };
    private static IInternodeAuthenticator originalAuthenticator;
    private static ServerEncryptionOptions originalServerEncryptionOptions;
    private static InetAddressAndPort originalListenAddress;

    private final MessagingService messagingService = new MessagingService(true);

    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.0.0.1"));
        originalAuthenticator = DatabaseDescriptor.getInternodeAuthenticator();
        originalServerEncryptionOptions = DatabaseDescriptor.getInternodeMessagingEncyptionOptions();
        originalListenAddress = InetAddressAndPort.getByAddressOverrideDefaults(DatabaseDescriptor.getListenAddress(), DatabaseDescriptor.getStoragePort());
    }

    @Before
    public void before() throws UnknownHostException
    {
        messagingService.metrics.resetDroppedMessages();
        messagingService.closeOutbound(InetAddressAndPort.getByName("127.0.0.2"));
        messagingService.closeOutbound(InetAddressAndPort.getByName("127.0.0.3"));
        DatabaseDescriptor.setInternodeAuthenticator(originalAuthenticator);
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setInternodeAuthenticator(originalAuthenticator);
        DatabaseDescriptor.setInternodeMessagingEncyptionOptions(originalServerEncryptionOptions);
        DatabaseDescriptor.setShouldListenOnBroadcastAddress(false);
        DatabaseDescriptor.setListenAddress(originalListenAddress.getAddress());
        FBUtilities.reset();
    }

    @Test
    public void testDroppedMessages()
    {
        Verb verb = Verb.READ_REQ;

        for (int i = 1; i <= 5000; i++)
            messagingService.metrics.recordDroppedMessage(verb, i, MILLISECONDS, i % 2 == 0);

        List<String> logs = new ArrayList<>();
        messagingService.metrics.resetAndConsumeDroppedErrors(logs::add);
        assertEquals(1, logs.size());
        Pattern regexp = Pattern.compile("READ_REQ messages were dropped in last 5000 ms: (\\d+) internal and (\\d+) cross node. Mean internal dropped latency: (\\d+) ms and Mean cross-node dropped latency: (\\d+) ms");
        Matcher matcher = regexp.matcher(logs.get(0));
        assertTrue(matcher.find());
        assertEquals(2500, Integer.parseInt(matcher.group(1)));
        assertEquals(2500, Integer.parseInt(matcher.group(2)));
        assertTrue(Integer.parseInt(matcher.group(3)) > 0);
        assertTrue(Integer.parseInt(matcher.group(4)) > 0);
        assertEquals(5000, (int) messagingService.metrics.getDroppedMessages().get(verb.toString()));

        logs.clear();
        messagingService.metrics.resetAndConsumeDroppedErrors(logs::add);
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.metrics.recordDroppedMessage(verb, i, MILLISECONDS, i % 2 == 0);

        logs.clear();
        messagingService.metrics.resetAndConsumeDroppedErrors(logs::add);
        assertEquals(1, logs.size());
        matcher = regexp.matcher(logs.get(0));
        assertTrue(matcher.find());
        assertEquals(1250, Integer.parseInt(matcher.group(1)));
        assertEquals(1250, Integer.parseInt(matcher.group(2)));
        assertTrue(Integer.parseInt(matcher.group(3)) > 0);
        assertTrue(Integer.parseInt(matcher.group(4)) > 0);
        assertEquals(7500, (int) messagingService.metrics.getDroppedMessages().get(verb.toString()));
    }

    @Test
    public void testDCLatency()
    {
        int latency = 100;
        ConcurrentHashMap<String, MessagingMetrics.DCLatencyRecorder> dcLatency = MessagingService.instance().metrics.dcLatency;
        dcLatency.clear();

        long now = System.currentTimeMillis();
        long sentAt = now - latency;
        assertNull(dcLatency.get("datacenter1"));
        addDCLatency(sentAt, now);
        assertNotNull(dcLatency.get("datacenter1"));
        assertEquals(1, dcLatency.get("datacenter1").dcLatency.getCount());
        long expectedBucket = bucketOffsets[Math.abs(Arrays.binarySearch(bucketOffsets, MILLISECONDS.toMicros(latency))) - 1];
        assertEquals(expectedBucket, dcLatency.get("datacenter1").dcLatency.getSnapshot().getMax());
    }

    @Test
    public void testNegativeDCLatency()
    {
        MessagingMetrics.DCLatencyRecorder updater = MessagingService.instance().metrics.internodeLatencyRecorder(InetAddressAndPort.getLocalHost());

        // if clocks are off should just not track anything
        int latency = -100;

        long now = System.currentTimeMillis();
        long sentAt = now - latency;

        long count = updater.dcLatency.getCount();
        updater.accept(now - sentAt, MILLISECONDS);
        // negative value shoudln't be recorded
        assertEquals(count, updater.dcLatency.getCount());
    }

    @Test
    public void testQueueWaitLatency()
    {
        int latency = 100;
        Verb verb = Verb.MUTATION_REQ;

        Map<Verb, Timer> queueWaitLatency = MessagingService.instance().metrics.internalLatency;
        MessagingService.instance().metrics.recordInternalLatency(verb, latency, MILLISECONDS);
        assertEquals(1, queueWaitLatency.get(verb).getCount());
        long expectedBucket = bucketOffsets[Math.abs(Arrays.binarySearch(bucketOffsets, MILLISECONDS.toMicros(latency))) - 1];
        assertEquals(expectedBucket, queueWaitLatency.get(verb).getSnapshot().getMax());
    }

    @Test
    public void testNegativeQueueWaitLatency()
    {
        int latency = -100;
        Verb verb = Verb.MUTATION_REQ;

        Map<Verb, Timer> queueWaitLatency = MessagingService.instance().metrics.internalLatency;
        queueWaitLatency.clear();

        assertNull(queueWaitLatency.get(verb));
        MessagingService.instance().metrics.recordInternalLatency(verb, latency, MILLISECONDS);
        assertNull(queueWaitLatency.get(verb));
    }

    private static void addDCLatency(long sentAt, long nowTime)
    {
        MessagingService.instance().metrics.internodeLatencyRecorder(InetAddressAndPort.getLocalHost()).accept(nowTime - sentAt, MILLISECONDS);
    }

    /**
     * Make sure that if internode authenticatino fails for an outbound connection that all the code that relies
     * on getting the connection pool handles the null return
     *
     * @throws Exception
     */
    @Test
    public void testFailedOutboundInternodeAuth() throws Exception
    {
        // Listen on serverside for connections
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
        .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.none);

        DatabaseDescriptor.setInternodeAuthenticator(REJECT_OUTBOUND_AUTHENTICATOR);
        InetAddress listenAddress = FBUtilities.getJustLocalAddress();

        InboundConnectionSettings settings = new InboundConnectionSettings().withEncryption(serverEncryptionOptions);
        InboundSockets connections = new InboundSockets(settings);

        try
        {
            connections.open().await();
            Assert.assertTrue(connections.isListening());

            MessagingService ms = MessagingService.instance();
            //Should return null
            int rejectedBefore = rejectedConnections.get();
            Message<?> messageOut = Message.out(Verb.ECHO_REQ, NoPayload.noPayload);
            InetAddressAndPort address = InetAddressAndPort.getByAddress(listenAddress);
            ms.send(messageOut, address);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> rejectedConnections.get() > rejectedBefore);

            //Should tolerate null
            ms.closeOutbound(address);
            ms.send(messageOut, address);
        }
        finally
        {
            connections.close().await();
            Assert.assertFalse(connections.isListening());
        }
    }

    @Test
    public void testFailedInboundInternodeAuth() throws IOException, InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
            .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.none);

        DatabaseDescriptor.setInternodeAuthenticator(ALLOW_NOTHING_AUTHENTICATOR);
        InetAddress listenAddress = FBUtilities.getJustLocalAddress();

        InboundConnectionSettings settings = new InboundConnectionSettings().withEncryption(serverEncryptionOptions);
        InboundSockets connections = new InboundSockets(settings);

        try (AsynchronousSocketChannel testChannel = AsynchronousSocketChannel.open())
        {
            connections.open().await();
            Assert.assertTrue(connections.isListening());

            int rejectedBefore = rejectedConnections.get();
            Future<Void> connectFuture = testChannel.connect(new InetSocketAddress(listenAddress, DatabaseDescriptor.getStoragePort()));
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(connectFuture::isDone);

            // Since authentication doesn't happen during connect, try writing a dummy string which triggers
            // authentication handler.
            testChannel.write(ByteBufferUtil.bytes("dummy string"));
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> rejectedConnections.get() > rejectedBefore);

            connectFuture.cancel(true);
        }
        finally
        {
            connections.close().await();
            Assert.assertFalse(connections.isListening());
        }
    }

//    @Test
//    public void reconnectWithNewIp() throws Exception
//    {
//        InetAddressAndPort publicIp = InetAddressAndPort.getByName("127.0.0.2");
//        InetAddressAndPort privateIp = InetAddressAndPort.getByName("127.0.0.3");
//
//        // reset the preferred IP value, for good test hygene
//        SystemKeyspace.updatePreferredIP(publicIp, publicIp);
//
//        // create pool/conn with public addr
//        Assert.assertEquals(publicIp, messagingService.getCurrentEndpoint(publicIp));
//        messagingService.maybeReconnectWithNewIp(publicIp, privateIp).await(1L, TimeUnit.SECONDS);
//        Assert.assertEquals(privateIp, messagingService.getCurrentEndpoint(publicIp));
//
//        messagingService.closeOutbound(publicIp);
//
//        // recreate the pool/conn, and make sure the preferred ip addr is used
//        Assert.assertEquals(privateIp, messagingService.getCurrentEndpoint(publicIp));
//    }

    @Test
    public void listenPlainConnection() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.none);
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenPlainConnectionWithBroadcastAddr() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.none);
        listen(serverEncryptionOptions, true);
    }

    @Test
    public void listenRequiredSecureConnection() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withOptional(false)
                                                          .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                                          .withLegacySslStoragePort(false);
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenRequiredSecureConnectionWithBroadcastAddr() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withOptional(false)
                                                          .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                                          .withLegacySslStoragePort(false);
        listen(serverEncryptionOptions, true);
    }

    @Test
    public void listenRequiredSecureConnectionWithLegacyPort() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                                          .withOptional(false)
                                                          .withLegacySslStoragePort(true);
        listen(serverEncryptionOptions, false);
    }

    @Test
    public void listenRequiredSecureConnectionWithBroadcastAddrAndLegacyPort() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withInternodeEncryption(ServerEncryptionOptions.InternodeEncryption.all)
                                                          .withOptional(false)
                                                          .withLegacySslStoragePort(true);
        listen(serverEncryptionOptions, true);
    }

    @Test
    public void listenOptionalSecureConnection() throws InterruptedException
    {
        for (int i = 0; i < 500; i++) // test used to be flaky, so run in a loop to make sure stable (see CASSANDRA-17033)
        {
            ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                              .withOptional(true);
            listen(serverEncryptionOptions, false);
        }
    }

    @Test
    public void listenOptionalSecureConnectionWithBroadcastAddr() throws InterruptedException
    {
        ServerEncryptionOptions serverEncryptionOptions = new ServerEncryptionOptions()
                                                          .withOptional(true);
        listen(serverEncryptionOptions, true);
    }

    private void listen(ServerEncryptionOptions serverEncryptionOptions, boolean listenOnBroadcastAddr) throws InterruptedException
    {
        InetAddress listenAddress = FBUtilities.getJustLocalAddress();
        if (listenOnBroadcastAddr)
        {
            DatabaseDescriptor.setShouldListenOnBroadcastAddress(true);
            listenAddress = InetAddresses.increment(FBUtilities.getBroadcastAddressAndPort().getAddress());
            DatabaseDescriptor.setListenAddress(listenAddress);
            FBUtilities.reset();
        }

        InboundConnectionSettings settings = new InboundConnectionSettings()
                                             .withEncryption(serverEncryptionOptions);
        InboundSockets connections = new InboundSockets(settings);
        try
        {
            connections.open().sync();
            Assert.assertTrue("connections is not listening", connections.isListening());

            Set<InetAddressAndPort> expect = new HashSet<>();
            expect.add(InetAddressAndPort.getByAddressOverrideDefaults(listenAddress, DatabaseDescriptor.getStoragePort()));
            if (settings.encryption.legacy_ssl_storage_port_enabled)
                expect.add(InetAddressAndPort.getByAddressOverrideDefaults(listenAddress, DatabaseDescriptor.getSSLStoragePort()));
            if (listenOnBroadcastAddr)
            {
                expect.add(InetAddressAndPort.getByAddressOverrideDefaults(FBUtilities.getBroadcastAddressAndPort().getAddress(), DatabaseDescriptor.getStoragePort()));
                if (settings.encryption.legacy_ssl_storage_port_enabled)
                    expect.add(InetAddressAndPort.getByAddressOverrideDefaults(FBUtilities.getBroadcastAddressAndPort().getAddress(), DatabaseDescriptor.getSSLStoragePort()));
            }

            Assert.assertEquals(expect.size(), connections.sockets().size());

            final int legacySslPort = DatabaseDescriptor.getSSLStoragePort();
            for (InboundSockets.InboundSocket socket : connections.sockets())
            {
                Assert.assertEquals(serverEncryptionOptions.getEnabled(), socket.settings.encryption.getEnabled());
                Assert.assertEquals(serverEncryptionOptions.getOptional(), socket.settings.encryption.getOptional());
                if (!serverEncryptionOptions.getEnabled())
                    assertNotEquals(legacySslPort, socket.settings.bindAddress.getPort());
                if (legacySslPort == socket.settings.bindAddress.getPort())
                    Assert.assertFalse(socket.settings.encryption.getOptional());
                Assert.assertTrue(socket.settings.bindAddress.toString(), expect.remove(socket.settings.bindAddress));
            }
        }
        finally
        {
            connections.close().await();
            Assert.assertFalse(connections.isListening());
        }
    }


//    @Test
//    public void getPreferredRemoteAddrUsesPrivateIp() throws UnknownHostException
//    {
//        MessagingService ms = MessagingService.instance();
//        InetAddressAndPort remote = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.151", 7000);
//        InetAddressAndPort privateIp = InetAddressAndPort.getByName("127.0.0.6");
//
//        OutboundConnectionSettings template = new OutboundConnectionSettings(remote)
//                                              .withConnectTo(privateIp)
//                                              .withAuthenticator(ALLOW_NOTHING_AUTHENTICATOR);
//        OutboundConnections pool = new OutboundConnections(template, new MockBackPressureStrategy(null).newState(remote));
//        ms.channelManagers.put(remote, pool);
//
//        Assert.assertEquals(privateIp, ms.getPreferredRemoteAddr(remote));
//    }
//
//    @Test
//    public void getPreferredRemoteAddrUsesPreferredIp() throws UnknownHostException
//    {
//        MessagingService ms = MessagingService.instance();
//        InetAddressAndPort remote = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.115", 7000);
//
//        InetAddressAndPort preferredIp = InetAddressAndPort.getByName("127.0.0.16");
//        SystemKeyspace.updatePreferredIP(remote, preferredIp);
//
//        Assert.assertEquals(preferredIp, ms.getPreferredRemoteAddr(remote));
//    }
//
//    @Test
//    public void getPreferredRemoteAddrUsesPrivateIpOverridesPreferredIp() throws UnknownHostException
//    {
//        MessagingService ms = MessagingService.instance();
//        InetAddressAndPort local = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.4", 7000);
//        InetAddressAndPort remote = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.105", 7000);
//        InetAddressAndPort privateIp = InetAddressAndPort.getByName("127.0.0.6");
//
//        OutboundConnectionSettings template = new OutboundConnectionSettings(remote)
//                                              .withConnectTo(privateIp)
//                                              .withAuthenticator(ALLOW_NOTHING_AUTHENTICATOR);
//
//        OutboundConnections pool = new OutboundConnections(template, new MockBackPressureStrategy(null).newState(remote));
//        ms.channelManagers.put(remote, pool);
//
//        InetAddressAndPort preferredIp = InetAddressAndPort.getByName("127.0.0.16");
//        SystemKeyspace.updatePreferredIP(remote, preferredIp);
//
//        Assert.assertEquals(privateIp, ms.getPreferredRemoteAddr(remote));
//    }
}
