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

import java.net.InetAddress;
import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

public class InternodeEncryptionOptionsTest extends AbstractEncryptionOptionsImpl
{
    @Test
    public void nodeWillNotStartWithBadKeystoreTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NETWORK);
            c.set("server_encryption_options",
                  ImmutableMap.of("optional", true,
                                  "keystore", "/path/to/bad/keystore/that/should/not/exist",
                                  "truststore", "/path/to/bad/truststore/that/should/not/exist"));
        }).createWithoutStarting())
        {
            assertCannotStartDueToConfigurationException(cluster);
        }
    }

    @Test
    public void legacySslPortProvidedWithEncryptionNoneWillNotStartTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NETWORK);
            c.set("ssl_storage_port", 7013);
            c.set("server_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                  .put("internode_encryption", "none")
                  .put("optional", false)
                  .put("enable_legacy_ssl_storage_port", "true")
                  .build());
        }).createWithoutStarting())
        {
            assertCannotStartDueToConfigurationException(cluster);
        }
    }

    @Test
    public void optionalTlsConnectionDisabledWithoutKeystoreTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> c.with(Feature.NETWORK)).createWithoutStarting())
        {
            InetAddress address = cluster.get(1).config().broadcastAddress().getAddress();
            int port = cluster.get(1).config().broadcastAddress().getPort();

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
            c.with(Feature.NETWORK);
            c.set("server_encryption_options", validKeystore);
        }).createWithoutStarting())
        {
            InetAddress address = cluster.get(1).config().broadcastAddress().getAddress();
            int port = cluster.get(1).config().broadcastAddress().getPort();

            TlsConnection tlsConnection = new TlsConnection(address.getHostAddress(), port);
            tlsConnection.assertCannotConnect();

            cluster.startup();

            Assert.assertEquals("TLS connection should be possible with keystore by default",
                                ConnectResult.NEGOTIATED, tlsConnection.connect());
        }
    }

    @Test
    public void optionalTlsConnectionAllowedToStoragePortTest() throws Throwable
    {
        try (Cluster  cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NETWORK);
            c.set("storage_port", 7012);
            c.set("ssl_storage_port", 7013);
            c.set("server_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("internode_encryption", "none")
                              .put("optional", true)
                              .put("enable_legacy_ssl_storage_port", "true")
                              .build());
        }).createWithoutStarting())
        {
            InetAddress address = cluster.get(1).config().broadcastAddress().getAddress();
            int regular_port = (int) cluster.get(1).config().get("storage_port");
            int ssl_port = (int) cluster.get(1).config().get("ssl_storage_port");

            // Create the connections and prove they cannot connect before server start
            TlsConnection connectToRegularPort = new TlsConnection(address.getHostAddress(), regular_port);
            connectToRegularPort.assertCannotConnect();

            TlsConnection connectToSslStoragePort = new TlsConnection(address.getHostAddress(), ssl_port);
            connectToSslStoragePort.assertCannotConnect();

            cluster.startup();

            Assert.assertEquals("TLS native connection should be possible to ssl_storage_port",
                                ConnectResult.NEGOTIATED, connectToSslStoragePort.connect());
            Assert.assertEquals("TLS native connection should be possible with valid keystore by default",
                                ConnectResult.NEGOTIATED, connectToRegularPort.connect());
        }
    }

    @Test
    public void legacySslStoragePortEnabledWithSameRegularAndSslPortTest() throws Throwable
    {
        try (Cluster  cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NETWORK);
            c.set("storage_port", 7012); // must match in-jvm dtest assigned ports
            c.set("ssl_storage_port", 7012);
            c.set("server_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("internode_encryption", "none")
                              .put("optional", true)
                              .put("enable_legacy_ssl_storage_port", "true")
                              .build());
        }).createWithoutStarting())
        {
            InetAddress address = cluster.get(1).config().broadcastAddress().getAddress();
            int ssl_port = (int) cluster.get(1).config().get("ssl_storage_port");

            // Create the connections and prove they cannot connect before server start
            TlsConnection connectToSslStoragePort = new TlsConnection(address.getHostAddress(), ssl_port);
            connectToSslStoragePort.assertCannotConnect();

            cluster.startup();

            Assert.assertEquals("TLS native connection should be possible to ssl_storage_port",
                                ConnectResult.NEGOTIATED, connectToSslStoragePort.connect());
        }
    }


    @Test
    public void tlsConnectionRejectedWhenUnencrypted() throws Throwable
    {
        try (Cluster  cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NETWORK);
            c.set("server_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("internode_encryption", "none")
                              .put("optional", false)
                              .build());
        }).createWithoutStarting())
        {
            InetAddress address = cluster.get(1).config().broadcastAddress().getAddress();
            int regular_port = (int) cluster.get(1).config().get("storage_port");

            // Create the connections and prove they cannot connect before server start
            TlsConnection connection = new TlsConnection(address.getHostAddress(), regular_port);
            connection.assertCannotConnect();

            cluster.startup();

            Assert.assertEquals("TLS native connection should be possible with valid keystore by default",
                                ConnectResult.FAILED_TO_NEGOTIATE, connection.connect());
        }
    }

    @Test
    public void allInternodeEncryptionEstablishedTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(2).withConfig(c -> {
            c.with(Feature.NETWORK)
             .with(Feature.GOSSIP) // To make sure AllMembersAliveMonitor checks gossip (which uses internode conns)
             .with(Feature.NATIVE_PROTOCOL); // For virtual tables
            c.set("server_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("internode_encryption", "all")
                              .build());
        }).start())
        {
            // Just check startup - cluster should not be able to establish internode connections xwithout encrypted connections
            for (int i = 1; i <= cluster.size(); i++)
            {
                Object[][] result = cluster.get(i).executeInternal("SELECT successful_connection_attempts, address, port FROM system_views.internode_outbound");
                Assert.assertEquals(1, result.length);
                long successfulConnectionAttempts = (long) result[0][0];
                Assert.assertTrue("At least one connection: " + successfulConnectionAttempts, successfulConnectionAttempts > 0);
            }
        }
    }

    @Test
    public void negotiatedProtocolMustBeAcceptedProtocolTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NETWORK);
            c.set("server_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("internode_encryption", "all")
                              .put("accepted_protocols", Collections.singletonList("TLSv1.1"))
                              .build());
        }).start())
        {
            InetAddress address = cluster.get(1).config().broadcastAddress().getAddress();
            int port = cluster.get(1).config().broadcastAddress().getPort();

            TlsConnection tls10Connection = new TlsConnection(address.getHostAddress(), port, Collections.singletonList("TLSv1"));
            Assert.assertEquals("Should not be possible to establish a TLSv1 connection",
                                ConnectResult.FAILED_TO_NEGOTIATE, tls10Connection.connect());
            tls10Connection.assertReceivedHandshakeException();

            TlsConnection tls11Connection = new TlsConnection(address.getHostAddress(), port, Collections.singletonList("TLSv1.1"));
            Assert.assertEquals("Should be possible to establish a TLSv1.1 connection",
                                ConnectResult.NEGOTIATED, tls11Connection.connect());
            Assert.assertEquals("TLSv1.1", tls11Connection.lastProtocol());

            TlsConnection tls12Connection = new TlsConnection(address.getHostAddress(), port, Collections.singletonList("TLSv1.2"));
            Assert.assertEquals("Should not be possible to establish a TLSv1.2 connection",
                                ConnectResult.FAILED_TO_NEGOTIATE, tls12Connection.connect());
            tls12Connection.assertReceivedHandshakeException();
        }
    }

    @Test
    public void connectionCannotAgreeOnClientAndServer() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NETWORK);
            c.set("server_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("internode_encryption", "all")
                              .put("accepted_protocols", Collections.singletonList("TLSv1.2"))
                              .put("cipher_suites", Collections.singletonList("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"))
                              .build());
        }).start())
        {
            InetAddress address = cluster.get(1).config().broadcastAddress().getAddress();
            int port = cluster.get(1).config().broadcastAddress().getPort();

            TlsConnection connection = new TlsConnection(address.getHostAddress(), port,
                                                         Collections.singletonList("TLSv1.2"),
                                                         Collections.singletonList("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"));
            Assert.assertEquals("Should not be possible to establish a TLSv1.2 connection with different ciphers",
                                ConnectResult.FAILED_TO_NEGOTIATE, connection.connect());
            connection.assertReceivedHandshakeException();
        }
    }

    @Test
    public void nodeMustNotStartWithNonExistantProtocol() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NETWORK);
            c.set("server_encryption_options",
                  ImmutableMap.<String,Object>builder().putAll(nonExistantProtocol)
                                                       .put("internode_encryption", "all").build());
        }).createWithoutStarting())
        {
            assertCannotStartDueToConfigurationException(cluster);
        }
    }

    @Test
    public void nodeMustNotStartWithNonExistantCipher() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NETWORK);
            c.set("server_encryption_options",
                  ImmutableMap.<String,Object>builder().putAll(nonExistantCipher)
                                                       .put("internode_encryption", "all").build());
        }).createWithoutStarting())
        {
            assertCannotStartDueToConfigurationException(cluster);
        }
    }
}
