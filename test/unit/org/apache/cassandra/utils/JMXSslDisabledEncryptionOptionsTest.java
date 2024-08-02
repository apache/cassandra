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

package org.apache.cassandra.utils;

import java.net.InetAddress;
import java.util.Map;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.net.ssl.SSLException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS;

/**
 * Tests for disabling jmx_encryption_options in the cassandra.yaml.
 */
public class JMXSslDisabledEncryptionOptionsTest
{
    static WithProperties properties;

    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        properties = new WithProperties().set(CASSANDRA_CONFIG, "cassandra-jmx-disabled-sslconfig.yaml");
        DatabaseDescriptor.daemonInitialization();
    }

    @AfterClass
    public static void tearDownDatabaseDescriptor()
    {
        properties.close();
    }

    /**
     * Tests absence of all JMX SSL configurations,
     * 1. local only JMX server
     * 2. System properties set for remote JMX SSL
     * 3. jmx_encryption_options in the cassandra.yaml
     */
    @Test
    public void testAbsenceOfAllJmxSslConfigs() throws SSLException
    {
        InetAddress serverAddress = InetAddress.getLoopbackAddress();
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL.setBoolean(false);
        Map<String, Object> env = JMXServerUtils.configureJmxSocketFactories(serverAddress, false);
        Assert.assertTrue("no properties must be set", env.isEmpty());
        Assert.assertFalse("com.sun.management.jmxremote.ssl must be false", COM_SUN_MANAGEMENT_JMXREMOTE_SSL.getBoolean());
        Assert.assertNull("javax.rmi.ssl.client.enabledProtocols must be null", JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS.getString());
        Assert.assertNull("javax.rmi.ssl.client.enabledCipherSuites must be null", JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES.getString());
    }

    /**
     * Tests fallback to the `local only` JMX server when jmx_encryption_options are disabled in the cassandra.yaml
     * and no System settings provided for the remote SSL config.
     */
    @Test
    public void testFallbackToLocalJmxServer() throws SSLException
    {
        InetAddress serverAddress = InetAddress.getLoopbackAddress();
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL.setBoolean(false);
        Map<String, Object> env = JMXServerUtils.configureJmxSocketFactories(serverAddress, true);
        Assert.assertFalse("com.sun.management.jmxremote.ssl must be false", COM_SUN_MANAGEMENT_JMXREMOTE_SSL.getBoolean());
        Assert.assertNull("com.sun.jndi.rmi.factory.socket must be null", env.get("com.sun.jndi.rmi.factory.socket"));
        Assert.assertNotNull("ServerSocketFactory must not be null", env.get(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE));
        Assert.assertFalse("com.sun.management.jmxremote.ssl must be false", COM_SUN_MANAGEMENT_JMXREMOTE_SSL.getBoolean());
        Assert.assertNull("javax.rmi.ssl.client.enabledProtocols must be null", JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS.getString());
        Assert.assertNull("javax.rmi.ssl.client.enabledCipherSuites must be null", JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES.getString());
    }
}
