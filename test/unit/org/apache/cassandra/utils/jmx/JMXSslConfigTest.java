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

package org.apache.cassandra.utils.jmx;

import java.net.InetAddress;
import java.util.Map;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.net.ssl.SSLException;
import javax.rmi.ssl.SslRMIServerSocketFactory;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.JMXServerOptions;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.JMXServerUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_NET_SSL_KEYSTORE;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_NET_SSL_KEYSTOREPASSWORD;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_NET_SSL_TRUSTSTORE;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_NET_SSL_TRUSTSTOREPASSWORD;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS;

/**
 * Tests for Local JMX server, the remote JMX SSL configuration via System properties in absence of jmx_encryption_options
 * in the cassandra.yaml. This is the behavior before CASSANDRA-18508.
 */
public class JMXSslConfigTest
{
    static WithProperties properties;

    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        properties = new WithProperties().set(CASSANDRA_CONFIG, "cassandra.yaml");
        DatabaseDescriptor.daemonInitialization();
    }

    @AfterClass
    public static void tearDownDatabaseDescriptor()
    {
        properties.close();
    }

    /**
     * Tests for remote JMX SSL configuration specified via the System properties.
     */
    @Test
    public void testRemoteJmxSystemConfig() throws SSLException
    {
        InetAddress serverAddress = InetAddress.getLoopbackAddress();
        String enabledProtocols = "TLSv1.2,TLSv1.3,TLSv1.1";
        String cipherSuites = "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256";

        try (WithProperties ignored = JMXSslPropertiesUtil.use(true, true, enabledProtocols, cipherSuites)
                                      .set(JAVAX_NET_SSL_KEYSTORE, "test/conf/cassandra_ssl_test.keystore")
                                      .set(JAVAX_NET_SSL_TRUSTSTORE, "test/conf/cassandra_ssl_test.truststore")
                                      .set(JAVAX_NET_SSL_KEYSTOREPASSWORD, "cassandra")
                                      .set(JAVAX_NET_SSL_TRUSTSTOREPASSWORD, "cassandra"))
        {
            JMXServerOptions options = JMXServerOptions.createParsingSystemProperties();
            options.jmx_encryption_options.applyConfig();
            Map<String, Object> env = JMXServerUtils.configureJmxSocketFactories(serverAddress, options);
            Assert.assertNotNull("ServerSocketFactory must not be null", env.get(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE));
            Assert.assertTrue("RMI_SERVER_SOCKET_FACTORY must be of SslRMIServerSocketFactory type", env.get(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE) instanceof SslRMIServerSocketFactory);
            Assert.assertNotNull("ClientSocketFactory must not be null", env.get(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE));
            Assert.assertNotNull("com.sun.jndi.rmi.factory.socket must be set in the env", env.get("com.sun.jndi.rmi.factory.socket"));
            Assert.assertEquals("protocols must match", enabledProtocols, JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS.getString());
            Assert.assertEquals("cipher-suites must match", cipherSuites, JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES.getString());
        }
    }

    /**
     * Tests for {@code localOnly} JMX Server configuration.
     */
    @Test
    public void testLocalJmxServer() throws SSLException
    {
        InetAddress serverAddress = InetAddress.getLoopbackAddress();
        try (WithProperties ignored = JMXSslPropertiesUtil.use(false))
        {
            JMXServerOptions options = JMXServerOptions.fromDescriptor(true, true, 7199);
            Map<String, Object> env = JMXServerUtils.configureJmxSocketFactories(serverAddress, options);

            Assert.assertNull("ClientSocketFactory must be null", env.get(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE));
            Assert.assertNull("com.sun.jndi.rmi.factory.socket must not be set in the env", env.get("com.sun.jndi.rmi.factory.socket"));
            Assert.assertNotNull("ServerSocketFactory must not be null", env.get(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE));
            Assert.assertNull("protocols must be empty", JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS.getString());
            Assert.assertNull("cipher-suites must empty", JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES.getString());
        }
    }
}
