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
package org.apache.cassandra.distributed.test;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.auth.AllowAllInternodeAuthenticator;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.InboundMessageHandlers;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundConnections;
import org.apache.cassandra.net.PingRequest;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.transport.TlsTestUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.awaitility.Awaitility;

import static com.google.common.collect.Iterables.getOnlyElement;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// Nodes start communicating rather early with TCM, which means that we will encouter the expected exceptions already on startup. This test should simply be rewritten to check
// communication between non-CMS nodes.
public final class InternodeEncryptionEnforcementTest extends TestBaseImpl
{
    @Test
    public void testInboundConnectionsAreRejectedWhenAuthFails() throws IOException, TimeoutException
    {
        // RejectInboundConnections authenticator is configured only for instance 1 of the cluster
        Cluster.Builder builder = createCluster(RejectInboundConnections.class);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try (Cluster cluster = builder.withInstanceInitializer(BB::install).start();
             Closeable es = executorService::shutdown)
        {
            openConnections(cluster, true);

            /*
             * Instance (1) should be able to make outbound connections to instance (2) but Instance (1) should not be
             * accepting any inbound connections. we should wait for the authentication failure log on Instance (1)
             */
            SerializableRunnable runnable = () ->
            {
                // There should be no inbound handlers as authentication fails and we remove handlers.
                assertTrue(MessagingService.instance().messageHandlers.isEmpty());

                // Verify that the failure is due to authentication failure
                final RejectInboundConnections authenticator = (RejectInboundConnections) DatabaseDescriptor.getInternodeAuthenticator();
                assertTrue(authenticator.authenticationFailed);
            };

            // Wait for authentication to fail
            cluster.get(1).logs().watchFor("Unable to authenticate peer");
            cluster.get(1).runOnInstance(runnable);

        }
    }

    @Test
    public void testOutboundConnectionsAreRejectedWhenAuthFails() throws IOException, TimeoutException
    {
        Cluster.Builder builder = createCluster(RejectOutboundAuthenticator.class);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try (Cluster cluster = builder.withInstanceInitializer(BB::install).start();
             Closeable es = executorService::shutdown)
        {
            openConnections(cluster, true);

            /*
             * Instance (1) should not be able to make outbound connections to instance (2) but Instance (2) should be
             * accepting outbound connections from Instance (1)
             */
            SerializableRunnable runnable = () ->
            {
                // There should be no outbound connections as authentication fails on Instance (1).
                OutboundConnections outbound = getOnlyElement(MessagingService.instance().channelManagers.values());
                assertTrue(!outbound.small.isConnected() && !outbound.large.isConnected() && !outbound.urgent.isConnected());

                // Verify that the failure is due to authentication failure
                final RejectOutboundAuthenticator authenticator = (RejectOutboundAuthenticator) DatabaseDescriptor.getInternodeAuthenticator();
                assertTrue(authenticator.authenticationFailed);
            };

            // Wait for authentication to fail
            cluster.get(1).logs().watchFor("Authentication failed");
            cluster.get(1).runOnInstance(runnable);
        }
    }

    @Test
    public void testOutboundConnectionsAreInterruptedWhenAuthFails() throws IOException, TimeoutException
    {
        Cluster.Builder builder = createCluster(AllowFirstAndRejectOtherOutboundAuthenticator.class);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try (Cluster cluster = builder.withInstanceInitializer(BB::install).start();
             Closeable es = executorService::shutdown)
        {
            long mark = cluster.get(1).logs().mark();
            executorService.submit(() -> {
                openConnections(cluster, 2, 1, Verb.PING_REQ, ConnectionType.LARGE_MESSAGES, false);
                openConnections(cluster, 1, 2, Verb.PING_REQ, ConnectionType.SMALL_MESSAGES, true);

            });

            // Verify that authentication is failed and Interrupt is called on outbound connections.
            cluster.get(1).logs().watchFor(mark, "Authentication failed to");
            cluster.get(1).logs().watchFor(mark, "Interrupted outbound connections to");

            /*
             * Check if outbound connections are zero
             */
            SerializableRunnable runnable = () ->
            {
                // Verify that there is only one successful outbound connection
                final AllowFirstAndRejectOtherOutboundAuthenticator authenticator = (AllowFirstAndRejectOtherOutboundAuthenticator) DatabaseDescriptor.getInternodeAuthenticator();
                assertEquals(1, authenticator.successfulOutbound.get());
                assertTrue(authenticator.failedOutbound.get() > 0);

                // There should be no outbound connections as authentication fails.
                OutboundConnections outbound = getOnlyElement(MessagingService.instance().channelManagers.values());
                Awaitility.await().until(() -> !outbound.small.isConnected() && !outbound.large.isConnected() && !outbound.urgent.isConnected());
            };
            cluster.get(1).runOnInstance(runnable);
        }
    }

    @Test
    public void testConnectionsAreAcceptedWhenAuthSucceds() throws IOException
    {
        verifyAuthenticationSucceeds(AllowAllInternodeAuthenticator.class);
    }

    @Test
    public void testAuthenticationWithCertificateAuthenticator() throws IOException
    {
        verifyAuthenticationSucceeds(CertificateVerifyAuthenticator.class);
    }

    @Test
    public void testConnectionsAreRejectedWithInvalidConfig() throws Throwable
    {
        Cluster.Builder builder = builder()
            .withNodes(2)
            .withConfig(c ->
            {
                c.with(Feature.NETWORK);
                c.with(Feature.NATIVE_PROTOCOL);

                HashMap<String, Object> encryption = new HashMap<>();
                encryption.put("optional", "false");
                encryption.put("internode_encryption", "none");
                if (c.num() == 1)
                {
                    encryption.put("keystore", TlsTestUtils.SERVER_KEYSTORE_PATH);
                    encryption.put("keystore_password", TlsTestUtils.SERVER_KEYSTORE_PASSWORD);
                    encryption.put("truststore", TlsTestUtils.SERVER_TRUSTSTORE_PATH);
                    encryption.put("truststore_password", TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD);
                    encryption.put("internode_encryption", "all");
                }
                c.set("server_encryption_options", encryption);
            })
            .withNodeIdTopology(ImmutableMap.of(1, NetworkTopology.dcAndRack("dc1", "r1a"),
                                                2, NetworkTopology.dcAndRack("dc2", "r2a")));

        try (Cluster cluster = builder.withInstanceInitializer(BB::install).start())
        {
            openConnections(cluster, true);

            /*
             * instance (1) won't connect to (2), since (2) won't have a TLS listener;
             * instance (2) won't connect to (1), since inbound check will reject
             * the unencrypted connection attempt;
             *
             * without the patch, instance (2) *CAN* connect to (1), without encryption,
             * despite being in a different dc.
             */

            cluster.get(1).runOnInstance(() ->
            {
                assertTrue(MessagingService.instance().messageHandlers.isEmpty());

                OutboundConnections outbound = getOnlyElement(MessagingService.instance().channelManagers.values());
                assertFalse(outbound.small.isConnected() || outbound.large.isConnected() || outbound.urgent.isConnected());
            });

            cluster.get(2).runOnInstance(() ->
            {
                assertTrue(MessagingService.instance().messageHandlers.isEmpty());

                OutboundConnections outbound = getOnlyElement(MessagingService.instance().channelManagers.values());
                assertFalse(outbound.small.isConnected() || outbound.large.isConnected() || outbound.urgent.isConnected());
            });
        }
    }

    /**
     * Some tests require nodes to be fully started up before they open connections to other nodes (i.e. cluster has to be
     * fully started). This allows each node to fully start up thinking it is the only seed in the cluster. Since tests
     * do not expect nodes to successfully commit, this is the simplest way to test such things.
     */
    public static class BB
    {
        public static void install(ClassLoader classLoader, Integer num)
        {
            new ByteBuddy().rebase(DatabaseDescriptor.class)
                           .method(named("getSeeds"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

        }

        @SuppressWarnings("unused")
        public static Set<InetAddressAndPort> getSeeds()
        {
            return Collections.singleton(FBUtilities.getBroadcastAddressAndPort());
        }
    }


    @Test
    public void testConnectionsAreAcceptedWithValidConfig() throws Throwable
    {
        Cluster.Builder builder = builder()
            .withNodes(2)
            .withConfig(c ->
            {
                c.with(Feature.NETWORK);
                c.with(Feature.NATIVE_PROTOCOL);

                HashMap<String, Object> encryption = new HashMap<>(); encryption.put("keystore", TlsTestUtils.SERVER_KEYSTORE_PATH);
                encryption.put("keystore_password", TlsTestUtils.SERVER_KEYSTORE_PASSWORD);
                encryption.put("truststore", TlsTestUtils.SERVER_TRUSTSTORE_PATH);
                encryption.put("truststore_password", TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD);
                encryption.put("internode_encryption", "dc");
                c.set("server_encryption_options", encryption);
            })
            .withNodeIdTopology(ImmutableMap.of(1, NetworkTopology.dcAndRack("dc1", "r1a"),
                                                2, NetworkTopology.dcAndRack("dc2", "r2a")));

        try (Cluster cluster = builder.start())
        {
            openConnections(cluster);

            /*
             * instance (1) should connect to instance (2) without any issues;
             * instance (2) should connect to instance (1) without any issues.
             */

            SerializableRunnable runnable = () ->
            {
                InboundMessageHandlers inbound = getOnlyElement(MessagingService.instance().messageHandlers.values());
                assertTrue(inbound.count() > 0);

                OutboundConnections outbound = getOnlyElement(MessagingService.instance().channelManagers.values());
                assertTrue(outbound.small.isConnected() || outbound.large.isConnected() || outbound.urgent.isConnected());
            };

            cluster.get(1).runOnInstance(runnable);
            cluster.get(2).runOnInstance(runnable);
        }
    }

    private void openConnections(Cluster cluster, boolean expectFail)
    {
        openConnections(cluster, Verb.PING_REQ, ConnectionType.SMALL_MESSAGES, expectFail);
    }

    private void openConnections(Cluster cluster, Verb verb, ConnectionType connectionType, boolean expectFail)
    {
        Pair[] connections = new Pair[] { Pair.create(1, 2), Pair.create(2, 1) };
        for (Pair<Integer, Integer> connection : connections)
            openConnections(cluster, connection.left, connection.right, verb, connectionType, expectFail);
    }

    private void openConnections(Cluster cluster, int from, int to, Verb verb, ConnectionType connectionType, boolean expectFail)
    {
        boolean failed = false;
        try
        {
            cluster.get(from).acceptsOnInstance((Integer p, Integer ct) -> {
                try
                {
                    MessagingService.instance().sendWithResponse(InetAddressAndPort.getByName("127.0.0." + p),
                                                                 Message.out(verb, PingRequest.get(ConnectionType.fromId(ct))))
                                    .get(5, TimeUnit.SECONDS);
                }
                catch (Throwable e)
                {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }).accept(to, connectionType.id);
        }
        catch (Throwable t)
        {
            failed = true;
        }
        if (expectFail != failed)
            fail(String.format("Should %shave failed", expectFail ? "" : "not "));
    }

    private void openConnections(Cluster cluster)
    {
        cluster.schemaChange("CREATE KEYSPACE test_connections_from_1 " +
                             "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};", false, cluster.get(1), 5, TimeUnit.SECONDS);

        cluster.schemaChange("CREATE KEYSPACE test_connections_from_2 " +
                             "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};", false, cluster.get(2), 5, TimeUnit.SECONDS);
    }

    private void verifyAuthenticationSucceeds(final Class authenticatorClass) throws IOException
    {
        Cluster.Builder builder = createCluster(authenticatorClass);
        try (Cluster cluster = builder.start())
        {
            openConnections(cluster);

            /*
             * instance (1) should connect to instance (2) without any issues;
             * instance (2) should connect to instance (1) without any issues.
             */

            SerializableRunnable runnable = () ->
            {
                // There should be inbound connections as authentication succeeds.
                InboundMessageHandlers inbound = getOnlyElement(MessagingService.instance().messageHandlers.values());
                assertTrue(inbound.count() > 0);

                // There should be outbound connections as authentication succeeds.
                OutboundConnections outbound = getOnlyElement(MessagingService.instance().channelManagers.values());
                assertTrue(outbound.small.isConnected() || outbound.large.isConnected() || outbound.urgent.isConnected());
            };

            cluster.get(1).runOnInstance(runnable);
            cluster.get(2).runOnInstance(runnable);
        }
    }

    private Cluster.Builder createCluster(final Class authenticatorClass)
    {
        return builder()
        .withNodes(2)
        .withConfig(c ->
                    {
                        c.with(Feature.NETWORK);
                        c.with(Feature.NATIVE_PROTOCOL);

                        HashMap<String, Object> encryption = new HashMap<>();
                        encryption.put("keystore", TlsTestUtils.SERVER_KEYSTORE_PATH);
                        encryption.put("keystore_password", TlsTestUtils.SERVER_KEYSTORE_PASSWORD);
                        encryption.put("truststore", TlsTestUtils.SERVER_TRUSTSTORE_PATH);
                        encryption.put("truststore_password", TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD);
                        encryption.put("internode_encryption", "all");
                        encryption.put("require_client_auth", "true");
                        c.set("server_encryption_options", encryption);
                        if (c.num() == 1)
                        {
                            c.set("internode_authenticator", authenticatorClass.getName());
                        }
                        else
                        {
                            c.set("internode_authenticator", AllowAllInternodeAuthenticator.class.getName());
                        }
                    })
        .withNodeIdTopology(ImmutableMap.of(1, NetworkTopology.dcAndRack("dc1", "r1a"),
                                            2, NetworkTopology.dcAndRack("dc2", "r2a")));
    }

    // Authenticator that validates certificate authentication
    public static class CertificateVerifyAuthenticator implements IInternodeAuthenticator
    {
        @Override
        public boolean authenticate(InetAddress remoteAddress, int remotePort, Certificate[] certificates, InternodeConnectionDirection connectionType)
        {
            if (connectionType == InternodeConnectionDirection.OUTBOUND_PRECONNECT)
            {
                return true;
            }
            try
            {
                // Check if the presented certificates during internode authentication are the ones in the keystores
                // configured in the cassandra.yaml configuration.
                KeyStore keyStore = KeyStore.getInstance("JKS");
                char[] keyStorePassword = TlsTestUtils.SERVER_KEYSTORE_PASSWORD.toCharArray();
                InputStream keyStoreData = new FileInputStream(TlsTestUtils.SERVER_KEYSTORE_PATH);
                keyStore.load(keyStoreData, keyStorePassword);
                return certificates != null && certificates.length != 0 && keyStore.getCertificate("cassandra_ssl_test").equals(certificates[0]);
            }
            catch (Exception e)
            {
                return false;
            }
        }

        @Override
        public void validateConfiguration() throws ConfigurationException
        {

        }
    }

    public static class RejectConnectionsAuthenticator implements IInternodeAuthenticator
    {
        boolean authenticationFailed = false;

        @Override
        public boolean authenticate(InetAddress remoteAddress, int remotePort, Certificate[] certificates, InternodeConnectionDirection connectionType)
        {
            authenticationFailed = true;
            return false;
        }

        @Override
        public void validateConfiguration() throws ConfigurationException
        {

        }
    }

    public static class RejectInboundConnections extends RejectConnectionsAuthenticator
    {
        @Override
        public boolean authenticate(InetAddress remoteAddress, int remotePort, Certificate[] certificates, InternodeConnectionDirection connectionType)
        {
            if (connectionType == InternodeConnectionDirection.INBOUND)
            {
                return super.authenticate(remoteAddress, remotePort, certificates, connectionType);
            }
            return true;
        }
    }

    public static class RejectOutboundAuthenticator extends RejectConnectionsAuthenticator
    {
        @Override
        public boolean authenticate(InetAddress remoteAddress, int remotePort, Certificate[] certificates, InternodeConnectionDirection connectionType)
        {
            if (connectionType == InternodeConnectionDirection.OUTBOUND)
            {
                return super.authenticate(remoteAddress, remotePort, certificates, connectionType);
            }
            return true;
        }
    }

    public static class AllowFirstAndRejectOtherOutboundAuthenticator extends RejectOutboundAuthenticator
    {
        AtomicInteger successfulOutbound = new AtomicInteger();
        AtomicInteger failedOutbound = new AtomicInteger();

        @Override
        public boolean authenticate(InetAddress remoteAddress, int remotePort, Certificate[] certificates, InternodeConnectionDirection connectionType)
        {
            if (connectionType == InternodeConnectionDirection.OUTBOUND)
            {
                if (successfulOutbound.compareAndSet(0, 1))
                {
                    return true;
                }
                else
                {
                    failedOutbound.incrementAndGet();
                    authenticationFailed = true;
                    return false;
                }
            }
            return true;
        }
    }
}
