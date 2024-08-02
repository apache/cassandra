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
import javax.rmi.ssl.SslRMIServerSocketFactory;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS;

/**
 * This class tests for Local JMX settings and the SSL configuration via System Properties.
 */
public class JMXSslConfigTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testRemoteJMXSystemConfig() throws SSLException
    {
        InetAddress serverAddress = InetAddress.getLoopbackAddress();
        String enabledProtocols = "TLSv1.2,TLSv1.3,TLSv1.1";
        String cipherSuites = "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256";
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL.setBoolean(true);
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH.setBoolean(true);
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS.setString(enabledProtocols);
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES.setString(cipherSuites);
        Map<String, Object> env = JMXServerUtils.configureJmxSocketFactories(serverAddress, false);

        Assert.assertNotNull("ServerSocketFactory must not be null", env.get(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE));
        Assert.assertTrue("RMI_SERVER_SOCKET_FACTORY must be of SslRMIServerSocketFactory type", env.get(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE) instanceof SslRMIServerSocketFactory);
        Assert.assertNotNull("ClientSocketFactory must not be null", env.get(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE));
        Assert.assertNotNull("com.sun.jndi.rmi.factory.socket must be set in the env", env.get("com.sun.jndi.rmi.factory.socket"));
        Assert.assertEquals("protocols must match", enabledProtocols, JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS.getString());
        Assert.assertEquals("cipher-suites must match", cipherSuites, JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES.getString());
    }

    @Test
    public void testLocalJMXConfig() throws SSLException
    {
        InetAddress serverAddress = InetAddress.getLoopbackAddress();
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL.setBoolean(false);
        Map<String, Object> env = JMXServerUtils.configureJmxSocketFactories(serverAddress, true);

        Assert.assertNull("ClientSocketFactory must be null", env.get(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE));
        Assert.assertNull("com.sun.jndi.rmi.factory.socket must not be set in the env", env.get("com.sun.jndi.rmi.factory.socket"));
        Assert.assertNotNull("ServerSocketFactory must not be null", env.get(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE));
        Assert.assertNull("protocols must be empty", JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS.getString());
        Assert.assertNull("cipher-suites must empty", JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES.getString());
    }
}
