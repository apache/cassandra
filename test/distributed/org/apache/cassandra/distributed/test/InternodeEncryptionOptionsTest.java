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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

public class InternodeEncryptionOptionsTest extends AbstractEncryptionOptionsTest
{
    @Test
    public void nodeWillNotStartWithBadKeystoreTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NETWORK);
            c.set("server_encryption_options",
                  "optional: true\n" +
                  "keystore: /path/to/bad/keystore/that/should/not/exist\n" +
                  "keystore_password: cassandra\n" +
                  "truststore: /path/to/bad/truststore/that/should/not/exist\n" +
                  "truststore_password: cassandra\n");
        }).createWithoutStarting())
        {
            assertCannotStartDueToConfigurationException(cluster);
        }
    }

    @Test
    public void supplyingEnabledWillNotStartTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(2).withConfig(c ->
                                                                 c.set("server_encryption_options",
                                                                       "enabled: false\n" +
                                                                       validKeystoreYaml)).createWithoutStarting())
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
                  "internode_encryption: none\n" +
                  "optional: false\n" +
                  "enable_legacy_ssl_storage_port: true\n" +
                  validKeystoreYaml);
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
            c.set("server_encryption_options", validKeystoreYaml);
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
                  "internode_encryption: none\n" +
                  "optional: true\n" +
                  "enable_legacy_ssl_storage_port: true\n" +
                  validKeystoreYaml);
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
                  "internode_encryption: none\n" +
                  "optional: true\n" +
                  "enable_legacy_ssl_storage_port: true\n" +
                  validKeystoreYaml);
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
                  "internode_encryption: none\n" +
                  "optional: false\n" +
                  validKeystoreYaml);
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
                  "internode_encryption: all\n" +
                  validKeystoreYaml);
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
}
