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

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

public class NativeTransportEncryptionOptionsTest extends AbstractEncryptionOptionsImpl
{
    @Test
    public void nodeWillNotStartWithBadKeystore() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL);
            c.set("client_encryption_options",
                  ImmutableMap.of("enabled", true,
                                   "optional", true,
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
            Assert.assertEquals("TLS native connection should be possible with valid keystore by default",
                                ConnectResult.NEGOTIATED, connectionToUnencryptedPort.connect());
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
}
