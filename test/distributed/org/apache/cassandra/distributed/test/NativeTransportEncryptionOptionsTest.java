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

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.security.KeyStore;
import java.util.Collections;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.shaded.netty.handler.ssl.SslContext;
import com.datastax.shaded.netty.handler.ssl.SslContextBuilder;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

public class NativeTransportEncryptionOptionsTest extends AbstractEncryptionOptionsImpl
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void nodeWillNotStartWithBadKeystore() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL);
            c.set("client_encryption_options",
                  ImmutableMap.of("optional", true,
                                  "keystore", "/path/to/bad/keystore/that/should/not/exist",
                                  "truststore", "/path/to/bad/truststore/that/should/not/exist"));
        }).createWithoutStarting())
        {
            assertCannotStartDueToConfigurationException(cluster);
        }
    }

    @Test
    public void optionalTlsConnectionDisabledWithoutKeystoreTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> c.with(Feature.NATIVE_PROTOCOL)).createWithoutStarting())
        {
            InetAddress address = cluster.get(1).config().broadcastAddress().getAddress();
            int port = (int) cluster.get(1).config().get("native_transport_port");

            TlsConnection tlsConnection = new TlsConnection(address.getHostAddress(), port);
            tlsConnection.assertCannotConnect();

            cluster.startup();

            Assert.assertEquals("TLS connection should not be possible without keystore",
                                ConnectResult.FAILED_TO_NEGOTIATE, tlsConnection.connect());
        }
    }


    @Test
    public void optionalTlsConnectionAllowedWithKeystoreTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL);
            c.set("client_encryption_options", validKeystore);
        }).createWithoutStarting())
        {
            InetAddress address = cluster.get(1).config().broadcastAddress().getAddress();
            int port = (int) cluster.get(1).config().get("native_transport_port");

            TlsConnection tlsConnection = new TlsConnection(address.getHostAddress(), port);
            tlsConnection.assertCannotConnect();

            cluster.startup();

            Assert.assertEquals("TLS native connection should be possible with keystore by default",
                                ConnectResult.NEGOTIATED, tlsConnection.connect());
        }
    }

    @Test
    public void optionalTlsConnectionAllowedToRegularPortTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL);
            c.set("native_transport_port_ssl", 9043);
            c.set("client_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("enabled", false)
                              .put("optional", true)
                              .build());
        }).createWithoutStarting())
        {
            InetAddress address = cluster.get(1).config().broadcastAddress().getAddress();
            int unencrypted_port = (int) cluster.get(1).config().get("native_transport_port");
            int ssl_port = (int) cluster.get(1).config().get("native_transport_port_ssl");

            // Create the connections and prove they cannot connect before server start
            TlsConnection connectionToUnencryptedPort = new TlsConnection(address.getHostAddress(), unencrypted_port);
            connectionToUnencryptedPort.assertCannotConnect();

            TlsConnection connectionToEncryptedPort = new TlsConnection(address.getHostAddress(), ssl_port);
            connectionToEncryptedPort.assertCannotConnect();

            cluster.startup();

            Assert.assertEquals("TLS native connection should be possible to native_transport_port_ssl",
                                ConnectResult.NEGOTIATED, connectionToEncryptedPort.connect());
            Assert.assertEquals("TLS native connection should not be possible on the regular port if an SSL port is specified",
                                ConnectResult.FAILED_TO_NEGOTIATE, connectionToUnencryptedPort.connect()); // but did connect
        }
    }

    @Test
    public void unencryptedNativeConnectionNotlisteningOnTlsPortTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL);
            c.set("native_transport_port_ssl", 9043);
            c.set("client_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("enabled", false)
                              .put("optional", false)
                              .build());
        }).createWithoutStarting())
        {
            assertCannotStartDueToConfigurationException(cluster);
        }
    }


    /**
     * Tests that the negotiated protocol is the highest common protocol between the client and server.
     * <p> 
     * Note: This test uses TLSV1.1, which is disabled by default in JDK 8 and higher. If the test fails with 
     * FAILED_TO_NEGOTIATE, it may be necessary to check the java.security file in your JDK installation and remove 
     * TLSv1.1 from the jdk.tls.disabledAlgorithms.
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-18540">CASSANDRA-18540</a>
     * @see <a href="https://senthilnayagan.medium.com/tlsv1-and-tlsv1-1-protocols-disabled-by-default-in-javas-latest-patch-released-on-april-20-2021-52c309f6b16d">
     *     TLSv1 and TLSv1.1 Protocols are Disabled in Java!</a>
     */
    @Test
    public void negotiatedProtocolMustBeAcceptedProtocolTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL);
            c.set("client_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("enabled", true)
                              .put("accepted_protocols", ImmutableList.of("TLSv1.1", "TLSv1.2", "TLSv1.3"))
                              .build());
        }).start())
        {
            InetAddress address = cluster.get(1).config().broadcastAddress().getAddress();
            int port = (int) cluster.get(1).config().get("native_transport_port");

            TlsConnection tls10Connection = new TlsConnection(address.getHostAddress(), port, Collections.singletonList("TLSv1"));
            Assert.assertEquals("Should not be possible to establish a TLSv1 connection",
                                ConnectResult.FAILED_TO_NEGOTIATE, tls10Connection.connect());
            tls10Connection.assertReceivedHandshakeException();

            TlsConnection tls11Connection = new TlsConnection(address.getHostAddress(), port, Collections.singletonList("TLSv1.1"));
            Assert.assertEquals("Should be possible to establish a TLSv1.1 connection",
                                ConnectResult.NEGOTIATED, tls11Connection.connect());
            Assert.assertEquals("TLSv1.1", tls11Connection.lastProtocol());

            TlsConnection tls12Connection = new TlsConnection(address.getHostAddress(), port, Collections.singletonList("TLSv1.2"));
            Assert.assertEquals("Should be possible to establish a TLSv1.2 connection",
                                ConnectResult.NEGOTIATED, tls12Connection.connect());
            Assert.assertEquals("TLSv1.2", tls12Connection.lastProtocol());

            TlsConnection tls13Connection = new TlsConnection(address.getHostAddress(), port, Collections.singletonList("TLSv1.3"));
            Assert.assertEquals("Should be possible to establish a TLSv1.3 connection",
                                ConnectResult.NEGOTIATED, tls13Connection.connect());
            Assert.assertEquals("TLSv1.3", tls13Connection.lastProtocol());
        }
    }

    @Test
    public void connectionCannotAgreeOnClientAndServerTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL);
            c.set("client_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("enabled", true)
                              .put("accepted_protocols", Collections.singletonList("TLSv1.2"))
                              .put("cipher_suites", Collections.singletonList("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"))
                              .build());
        }).start())
        {
            InetAddress address = cluster.get(1).config().broadcastAddress().getAddress();
            int port = (int) cluster.get(1).config().get("native_transport_port");

            TlsConnection connection = new TlsConnection(address.getHostAddress(), port,
                                                         Collections.singletonList("TLSv1.2"),
                                                         Collections.singletonList("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"));
            Assert.assertEquals("Should not be possible to establish a TLSv1.2 connection with different ciphers",
                                ConnectResult.FAILED_TO_NEGOTIATE, connection.connect());
            connection.assertReceivedHandshakeException();
        }
    }

    @Test
    public void nodeMustNotStartWithNonExistantProtocolTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL);
            c.set("client_encryption_options",
                  ImmutableMap.<String,Object>builder().putAll(nonExistantProtocol).put("enabled", true).build());
        }).createWithoutStarting())
        {
            assertCannotStartDueToConfigurationException(cluster);
        }
    }

    @Test
    public void nodeMustNotStartWithNonExistantCiphersTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL);
            c.set("client_encryption_options",
                  ImmutableMap.<String,Object>builder().putAll(nonExistantCipher).put("enabled", true).build());
        }).createWithoutStarting())
        {
            // Should also log "Dropping unsupported cipher_suite NoCipherIKnow from from native transport configuration"
            assertCannotStartDueToConfigurationException(cluster);
        }
    }

    @Test
    public void testEndpointVerificationDisabledIpNotInSAN() throws Throwable
    {
        // When required_endpoint_verification is set to false, client certificate Ip/hostname should be validated
        // The certificate in cassandra_ssl_test_outbound.keystore does not have IP/hostname embeded, so when
        // require_endpoint_verification is false, the connection should be established
       testEndpointVerification(false, true);
    }

    @Test
    public void testEndpointVerificationEnabledIpNotInSAN() throws Throwable
    {
        // When required_endpoint_verification is set to true, client certificate Ip/hostname should be validated
        // The certificate in cassandra_ssl_test_outbound.keystore does not have IP/hostname emebeded, so when
        // require_endpoint_verification is true, the connection should not be established
        testEndpointVerification(true, false);
    }

    @Test
    public void testEndpointVerificationEnabledWithIPInSan() throws Throwable
    {
        // When required_endpoint_verification is set to true, client certificate Ip/hostname should be validated
        // The certificate in cassandra_ssl_test_outbound.keystore have IP/hostname emebeded, so when
        // require_endpoint_verification is true, the connection should be established
        testEndpointVerification(true, true);
    }

    private void testEndpointVerification(boolean requireEndpointVerification, boolean ipInSAN) throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL);
            c.set("client_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("enabled", true)
                              .put("require_client_auth", true)
                              .put("require_endpoint_verification", requireEndpointVerification)
                              .build());
        }).start())
        {
            InetAddress address = cluster.get(1).config().broadcastAddress().getAddress();
            SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
            if (ipInSAN)
                sslContextBuilder.keyManager(createKeyManagerFactory("test/conf/cassandra_ssl_test_endpoint_verify.keystore", "cassandra"));
            else
                sslContextBuilder.keyManager(createKeyManagerFactory("test/conf/cassandra_ssl_test_outbound.keystore", "cassandra"));

            SslContext sslContext = sslContextBuilder.trustManager(createTrustManagerFactory("test/conf/cassandra_ssl_test.truststore", "cassandra"))
                                                     .build();
            final SSLOptions sslOptions = socketChannel -> sslContext.newHandler(socketChannel.alloc());
            com.datastax.driver.core.Cluster driverCluster = com.datastax.driver.core.Cluster.builder()
                                                                                             .addContactPoint(address.getHostAddress())
                                                                                             .withSSL(sslOptions)
                                                                                             .build();

            if (!ipInSAN)
            {
                expectedException.expect(NoHostAvailableException.class);
            }

            driverCluster.connect();
        }
    }

    private KeyManagerFactory createKeyManagerFactory(final String keyStorePath,
                                                     final String keyStorePassword) throws Exception
    {
        final InputStream stream = new FileInputStream(keyStorePath);
        final KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(stream, keyStorePassword.toCharArray());
        final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, keyStorePassword.toCharArray());
        return kmf;
    }

    private TrustManagerFactory createTrustManagerFactory(final String trustStorePath,
                                                          final String trustStorePassword) throws Exception
    {
        final InputStream stream = new FileInputStream(trustStorePath);
        final KeyStore ts = KeyStore.getInstance("JKS");
        ts.load(stream, trustStorePassword.toCharArray());
        final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        return tmf;
    }

}
