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

package org.apache.cassandra.distributed.test.jmx;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.net.ssl.SSLException;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.impl.JmxTestClientSslContextFactory;
import org.apache.cassandra.distributed.impl.JmxTestClientSslSocketFactory;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.AbstractEncryptionOptionsImpl;
import org.apache.cassandra.utils.jmx.JMXSslPropertiesUtil;

import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES;

/**
 * Distributed tests for JMX SSL configuration via the system properties OR the encryption options in the cassandra.yaml.
 */
public class JMXSslConfigDistributedTest extends AbstractEncryptionOptionsImpl
{
    @Test
    public void testDefaultEncryptionOptions() throws Throwable
    {
        // We must set the keystore in the system variable to make sure that the call to SSLContext.getDefault()
        // uses it when Client SSL Socketfactory is initialized even if we don't need it here.
        // The same default SSLContext.getDefault() will be used by other methods like testSystemSettings() in this test
        // for the Server SSL Socketfactory and at that time we will need the keystore to be available
        // All of the above is the issue because we run everything (JMX Server, Client) in the same JVM, multiple times
        // and the SSLContext.getDefault() relies on static initialization that is reused
        try (WithProperties withProperties = JMXSslPropertiesUtil.preserveAllProperties())
        {
            setKeystoreProperties(withProperties);
            ImmutableMap<String, Object> encryptionOptionsMap = ImmutableMap.<String, Object>builder().putAll(validFileBasedKeystores)
                                                                            .put("enabled", true)
                                                                            .put("accepted_protocols", Arrays.asList("TLSv1.2", "TLSv1.3", "TLSv1.1"))
                                                                            .build();

            try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
                c.with(Feature.JMX).set("jmx_encryption_options", encryptionOptionsMap);
            }).start())
            {
                Map<String, Object> jmxEnv = new HashMap<>();
                configureClientSocketFactory(jmxEnv, encryptionOptionsMap);
                JMXTestsUtil.testAllValidGetters(cluster, jmxEnv);
            }
        }
    }

    @Test
    public void testClientAuth() throws Throwable
    {
        try (WithProperties withProperties = JMXSslPropertiesUtil.preserveAllProperties())
        {
            setKeystoreProperties(withProperties);
            ImmutableMap<String, Object> encryptionOptionsMap = ImmutableMap.<String, Object>builder().putAll(validFileBasedKeystores)
                                                                            .put("enabled", true)
                                                                            .put("require_client_auth", true)
                                                                            .put("accepted_protocols", Arrays.asList("TLSv1.2", "TLSv1.3", "TLSv1.1"))
                                                                            .build();

            try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
                c.with(Feature.JMX).set("jmx_encryption_options", encryptionOptionsMap);
            }).start())
            {
                Map<String, Object> jmxEnv = new HashMap<>();
                configureClientSocketFactory(jmxEnv, encryptionOptionsMap);
                JMXTestsUtil.testAllValidGetters(cluster, jmxEnv);
            }
        }
    }

    @Test
    public void testSystemSettings() throws Throwable
    {
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES.reset();
        try (WithProperties withProperties = JMXSslPropertiesUtil.use(true, false,
                                                                      "TLSv1.2,TLSv1.3,TLSv1.1"))
        {
            setKeystoreProperties(withProperties);
            try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
                c.with(Feature.JMX);
            }).start())
            {
                Map<String, Object> jmxEnv = new HashMap<>();
                SslRMIClientSocketFactory clientFactory = new SslRMIClientSocketFactory();
                jmxEnv.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, clientFactory);
                jmxEnv.put("com.sun.jndi.rmi.factory.socket", clientFactory);
                JMXTestsUtil.testAllValidGetters(cluster, jmxEnv);
            }
        }
    }

    @Test
    public void testInvalidKeystorePath() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.JMX).set("jmx_encryption_options",
                                    ImmutableMap.<String, Object>builder()
                                                .put("enabled", true)
                                                .put("keystore", "/path/to/bad/keystore/that/should/not/exist")
                                                .put("keystore_password", "cassandra")
                                                .put("accepted_protocols", Arrays.asList("TLSv1.2", "TLSv1.3", "TLSv1.1"))
                                                .build());
        }).createWithoutStarting())
        {
            assertCannotStartDueToConfigurationException(cluster);
        }
    }

    /**
     * Tests {@code disabled} jmx_encryption_options. Here even if the configured {@code keystore} is invalid, it will
     * not matter and the JMX server/client should start.
     */
    @Test
    public void testDisabledEncryptionOptions() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.JMX).set("jmx_encryption_options",
                                    ImmutableMap.builder()
                                                .put("enabled", false)
                                                .put("keystore", "/path/to/bad/keystore/that/should/not/exist")
                                                .put("keystore_password", "cassandra")
                                                .build());
        }).start())
        {
            JMXTestsUtil.testAllValidGetters(cluster, null);
        }
    }

    private void setKeystoreProperties(WithProperties properties)
    {
        properties.with("javax.net.ssl.trustStore", (String) validFileBasedKeystores.get("truststore"),
                        "javax.net.ssl.trustStorePassword", (String) validFileBasedKeystores.get("truststore_password"),
                        "javax.net.ssl.keyStore", (String) validFileBasedKeystores.get("keystore"),
                        "javax.net.ssl.keyStorePassword", (String) validFileBasedKeystores.get("keystore_password"));
    }

    @SuppressWarnings("unchecked")
    private void configureClientSocketFactory(Map<String, Object> jmxEnv, Map<String, Object> encryptionOptionsMap) throws SSLException
    {
        JmxTestClientSslContextFactory clientSslContextFactory = new JmxTestClientSslContextFactory(encryptionOptionsMap);
        List<String> cipherSuitesList = (List<String>) encryptionOptionsMap.get("cipher_suites");
        String[] cipherSuites = cipherSuitesList == null ? null : cipherSuitesList.toArray(new String[0]);
        List<String> acceptedProtocolList = (List<String>) encryptionOptionsMap.get("accepted_protocols");
        String[] acceptedProtocols = acceptedProtocolList == null ? null : acceptedProtocolList.toArray(new String[0]);
        JmxTestClientSslSocketFactory clientFactory = new JmxTestClientSslSocketFactory(clientSslContextFactory.createSSLContext(),
                                                                                        cipherSuites, acceptedProtocols);
        jmxEnv.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, clientFactory);
        jmxEnv.put("com.sun.jndi.rmi.factory.socket", clientFactory);
    }
}
