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

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS;

/**
 * This class tests for JMX SSL config using keystore/trusstore in the PEM format with jmx_encryption_options in the
 * cassandra.yaml.
 */
public class JMXSslPEMConfigTest
{
    static WithProperties properties;

    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        properties = new WithProperties().set(CASSANDRA_CONFIG, "cassandra-jmx-pem-sslconfig.yaml");
        DatabaseDescriptor.daemonInitialization();
    }

    @AfterClass
    public static void tearDownDatabaseDescriptor()
    {
        properties.close();
    }

    @Test
    public void testJMXPluggableConfig() throws SSLException
    {
        EncryptionOptions jmxEncryptionOptions = DatabaseDescriptor.getJmxEncryptionOptions();
        String expectedProtocols = StringUtils.join(jmxEncryptionOptions.getAcceptedProtocols(), ",");
        String expectedCipherSuites = StringUtils.join(jmxEncryptionOptions.cipherSuitesArray(), ",");

        InetAddress serverAddress = InetAddress.getLoopbackAddress();
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL.setBoolean(false);
        Map<String, Object> env = JMXServerUtils.configureJmxSocketFactories(serverAddress, false);

        Assert.assertTrue("com.sun.management.jmxremote.ssl must be true", COM_SUN_MANAGEMENT_JMXREMOTE_SSL.getBoolean());
        Assert.assertNotNull("ServerSocketFactory must not be null", env.get(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE));
        Assert.assertTrue("RMI_SERVER_SOCKET_FACTORY must be of JMXSslRMIServerSocketFactory type", env.get(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE) instanceof JMXSslRMIServerSocketFactory);
        Assert.assertNotNull("ClientSocketFactory must not be null", env.get(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE));
        Assert.assertNotNull("com.sun.jndi.rmi.factory.socket must be set in the env", env.get("com.sun.jndi.rmi.factory.socket"));
        Assert.assertEquals("javax.rmi.ssl.client.enabledProtocols must match", expectedProtocols, JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS.getString());
        Assert.assertEquals("javax.rmi.ssl.client.enabledCipherSuites must match", expectedCipherSuites, JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES.getString() );
    }
}
