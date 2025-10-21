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

package org.apache.cassandra.metrics;

import javax.net.ssl.SSLException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.auth.IAuthenticator.AuthenticationMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.transport.TlsTestUtils;

import static org.apache.cassandra.auth.AuthTestUtils.waitForExistingRoles;
import static org.apache.cassandra.config.CassandraRelevantProperties.SUPERUSER_SETUP_DELAY_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ClientMetricsTest
{
    private static EmbeddedCassandraService cassandra;

    private static final ClientMetrics clientMetrics = ClientMetrics.instance;

    private static final String USER1 = "user1";
    private static final String USER1PW = "user1pw";

    @BeforeClass
    public static void setUp() throws Throwable
    {
        OverrideConfigurationLoader.override(TlsTestUtils::configureWithMutualTlsWithPasswordFallbackAuthenticator);

        SUPERUSER_SETUP_DELAY_MS.setLong(0);

        cassandra = ServerTestUtils.startEmbeddedCassandraService();

        waitForExistingRoles();

        // Allow client to connect as cassandra using an mTLS identity.
        try (Cluster cluster = clusterBuilder()
                               .withCredentials("cassandra", "cassandra")
                               .build())
        {
            try (Session session = cluster.connect())
            {
                session.execute(String.format("CREATE ROLE %s WITH password = '%s' AND LOGIN = true", USER1, USER1PW));
                session.execute(String.format("ADD IDENTITY '%s' TO ROLE '%s'", TlsTestUtils.CLIENT_SPIFFE_IDENTITY, USER1));
            }
        }
    }

    public static void tearDown()
    {
        if (cassandra != null)
        {
            cassandra.stop();
        }
    }

    private static Cluster.Builder clusterBuilder()
    {
        return Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort());
    }

    /**
     * Validates the behavior of metrics tied to connectivity and authentication by doing the following:
     *
     * <ol>
     *     <li>Attempt to connect with password authentication and ensure ConnectedNativeClients and AuthSuccess metrics
     *     are incremented, in addition to the Password specific metrics for these.</li>
     *     <li>Attempt to connect with mtls authentication and ensure ConnectedNativeClients and AuthSuccess metrics
     *     are incremented, in addition to the MTLS specific metrics.</li>
     *     <li>Attempt to connect with mtls authentication using a non-permitted identity and ensure AuthFailure
     *     metrics are incremented.</li>
     * </ol>
     * @throws SSLException
     */
    @Test
    public void testConnectedClientsAndAuthMetrics() throws SSLException
    {
        // Should be no connections at this time.
        assertEquals(0, clientMetrics.connectedNativeClients.getValue().intValue());
        assertEquals(0, clientMetrics.encryptedConnectedNativeClients.getValue().intValue());
        assertEquals(0, clientMetrics.unencryptedConnectedNativeClients.getValue().intValue());

        long initialAuthSuccess = clientMetrics.authSuccess.getCount();
        long initialAuthFailure = clientMetrics.authFailure.getCount();

        // For each authentication mode, we should have connectedNativeClientsByAuthMode, authSuccessByMode, and authFailureByMode entries.
        Gauge<Integer> passwordConnections = clientMetrics.connectedNativeClientsByAuthMode.get(AuthenticationMode.PASSWORD);
        assertNotNull(passwordConnections);

        Meter authSuccessPassword = clientMetrics.authSuccessByMode.get(AuthenticationMode.PASSWORD);
        assertNotNull(authSuccessPassword);
        long initialAuthSuccessPassword = authSuccessPassword.getCount();

        Meter authFailurePassword = clientMetrics.authFailureByMode.get(AuthenticationMode.PASSWORD);
        assertNotNull(authFailurePassword);
        long initialAuthFailurePassword = authFailurePassword.getCount();

        Gauge<Integer> mtlsConnections = clientMetrics.connectedNativeClientsByAuthMode.get(AuthenticationMode.MTLS);
        assertNotNull(mtlsConnections);

        Meter authSuccessMtls = clientMetrics.authSuccessByMode.get(AuthenticationMode.MTLS);
        assertNotNull(authSuccessMtls);
        long initialAuthSuccessMtls = authSuccessMtls.getCount();

        Meter authFailureMtls = clientMetrics.authFailureByMode.get(AuthenticationMode.MTLS);
        assertNotNull(authFailureMtls);
        long initialAuthFailureMtls = authFailureMtls.getCount();

        try (Cluster cluster = clusterBuilder().withCredentials("cassandra", "cassandra").build();
             Session session = cluster.connect())
        {
            // Connected native clients should be 2 (1 for core connection, 1 for control connection)
            assertEquals(2, clientMetrics.connectedNativeClients.getValue().intValue());
            assertEquals(2, clientMetrics.unencryptedConnectedNativeClients.getValue().intValue());
            assertEquals(0, clientMetrics.encryptedConnectedNativeClients.getValue().intValue());

            // Should be two connections on the "cassandra" user.
            Integer cassandraConnections = clientMetrics.connectedNativeClientsByUser.getValue().get("cassandra");
            assertNotNull(cassandraConnections);
            assertEquals(2, cassandraConnections.intValue());

            // Should be two password-authed connections.
            assertEquals(2, passwordConnections.getValue().intValue());

            // auth success should increment by 2.
            assertEquals(initialAuthSuccess+2, clientMetrics.authSuccess.getCount());
            assertEquals(initialAuthFailure, clientMetrics.authFailure.getCount());

            // should increment by 2 for password mode specific metric as well.
            assertEquals(initialAuthSuccessPassword+2L, authSuccessPassword.getCount());
            assertEquals(initialAuthFailurePassword, authFailurePassword.getCount());

            // Should be no mtls connections.
            assertEquals(0, mtlsConnections.getValue().intValue());

            // Should be no mtls attempts.
            assertEquals(initialAuthSuccessMtls, authSuccessMtls.getCount());
            // No auth failure for mtls
            assertEquals(initialAuthFailureMtls, authFailureMtls.getCount());

            Cluster.Builder user1ClusterBuilder = clusterBuilder()
                                                  .withSSL(TlsTestUtils.getSSLOptions(true))
                                                  .withPoolingOptions(new PoolingOptions()
                                                                      .setConnectionsPerHost(HostDistance.LOCAL, 4, 4));
            // Authenticate with 'user1' over mtls with a connection pool of size 4
            try (Cluster user1Cluster = user1ClusterBuilder.build();
                 Session user1Session = user1Cluster.connect())
            {
                // Connected native clients should increment by 5 for new cluster (4 core + 1 control)
                assertEquals(7, clientMetrics.connectedNativeClients.getValue().intValue());
                assertEquals(2, clientMetrics.unencryptedConnectedNativeClients.getValue().intValue());
                assertEquals(5, clientMetrics.encryptedConnectedNativeClients.getValue().intValue());

                // auth success should increment by 5 (accounting for the 2 initial password connections).
                assertEquals(initialAuthSuccess + 2 + 5, clientMetrics.authSuccess.getCount());

                // Should still be two connections on the "cassandra" user.
                cassandraConnections = clientMetrics.connectedNativeClientsByUser.getValue().get("cassandra");
                assertNotNull(cassandraConnections);
                assertEquals(2, cassandraConnections.intValue());

                // User1 should have 5 connections.
                Integer user1Connections = clientMetrics.connectedNativeClientsByUser.getValue().get(USER1);
                assertNotNull(user1Connections);
                assertEquals(5, user1Connections.intValue());

                // Expect 5 mtls connections, password connections should remain unchanged.
                assertEquals(5, mtlsConnections.getValue().intValue());
                assertEquals(2, passwordConnections.getValue().intValue());
                assertEquals(initialAuthSuccessMtls + 5L, authSuccessMtls.getCount());
                assertEquals(initialAuthFailureMtls, authFailureMtls.getCount());
            }

            // after user1's session is closed, metrics should drop to what they were before.
            assertEquals(2, clientMetrics.connectedNativeClients.getValue().intValue());
            assertEquals(2, clientMetrics.unencryptedConnectedNativeClients.getValue().intValue());
            assertEquals(0, clientMetrics.encryptedConnectedNativeClients.getValue().intValue());

            // user1 should not be present in map
            Integer user1Connections = clientMetrics.connectedNativeClientsByUser.getValue().get(USER1);
            assertNull(user1Connections);
            // No mTLS connections should be present.
            assertEquals(0, mtlsConnections.getValue().intValue());

            // Drop the identity used for mTLS to create an auth failure scenario for mTLS.
            session.execute(String.format("DROP IDENTITY '%s'", TlsTestUtils.CLIENT_SPIFFE_IDENTITY));

            // Attempt to connect with mTLS again, which should fail.
            try (Cluster user1Cluster = user1ClusterBuilder.build())
            {
                user1Cluster.connect();
            } catch (Exception e)
            {
                // expected.
            }
            // No mTLS connections should be present.
            assertEquals(0, mtlsConnections.getValue().intValue());
            // No new successful mTLS attempts.
            assertEquals(initialAuthSuccessMtls + 5L, authSuccessMtls.getCount());
            assertEquals(initialAuthSuccess + 2 + 5, clientMetrics.authSuccess.getCount());
            // 1 failed mTLS attempt (control connection cannot establish)
            assertEquals(initialAuthFailureMtls + 1L, authFailureMtls.getCount());
            assertEquals(initialAuthFailure+1, clientMetrics.authFailure.getCount());
        }

        // Should be no connections as all clusters are closed at this point.
        assertEquals(0, clientMetrics.connectedNativeClients.getValue().intValue());
        assertEquals(0, clientMetrics.unencryptedConnectedNativeClients.getValue().intValue());
        assertEquals(0, clientMetrics.encryptedConnectedNativeClients.getValue().intValue());
        assertEquals(0, passwordConnections.getValue().intValue());
    }
}
