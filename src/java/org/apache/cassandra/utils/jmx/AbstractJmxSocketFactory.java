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
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.JMXServerOptions;

/**
 * Abstracts out the most common workflow in setting up the SSL client and server socket factorires for JMX.
 * First, it checks the system properties (see <a href="https://docs.oracle.com/en/java/javase/17/management/monitoring-and-management-using-jmx-technology.html#GUID-F08985BB-629A-4FBF-A0CB-8762DF7590E0">Java Documentation</a> to read the SSL configuration.
 * Next, it checks the provided {@code jmxEncryptionOptions} to read the SSL configuration.
 * If none of them is enabled, it checks the provided {@code localOnly} flag to configure the JMX server socket
 * factory for the local JMX connection.
 */
abstract public class AbstractJmxSocketFactory
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractJmxSocketFactory.class);

    /**
     * Configures the client and server socket factories for the JMX connection.
     * Specifically it configures below properties as applicable,
     * <pre>
     *     jmx.remote.rmi.client.socket.factory
     *     jmx.remote.rmi.server.socket.factory
     *     com.sun.jndi.rmi.factory.socket
     * </pre>
     * <p>
     * In case of remote connection enabled, this also sets the following system properties,
     * <pre>
     *     com.sun.management.jmxremote.ssl=true
     *     javax.rmi.ssl.client.enabledCipherSuites=&lt;applicable cipher suites provided in the configuration&gt;
     *     javax.rmi.ssl.client.enabledProtocols=&lt;applicable protocols provided in the configuration&gt;
     * </pre>
     *
     * @param serverAddress        the JMX server is bound to
     * @param serverOptions        JMX server options
     * @param jmxEncryptionOptions {@link EncryptionOptions} used for the SSL configuration in case of the remote
     *                             connections. Could be {@code null} if system properties are
     *                             used instead as per <a href="https://docs.oracle.com/en/java/javase/17/management/monitoring-and-management-using-jmx-technology.html#GUID-F08985BB-629A-4FBF-A0CB-8762DF7590E0">Java Documentation</a>
     * @return Map&lt;String, Object@gt; containing {@code jmx.remote.rmi.client.socket.factory}, {@code jmx.remote.rmi.server.socket.factory}
     * and {@code com.sun.jndi.rmi.factory.socket} properties for the client and server socket factories.
     * @throws SSLException if it fails to configure the socket factories with the given input
     */
    public Map<String, Object> configure(InetAddress serverAddress,
                                         JMXServerOptions serverOptions,
                                         EncryptionOptions jmxEncryptionOptions) throws SSLException
    {
        Map<String, Object> env = new HashMap<>();

        // We check for the enabled jmx_encryption_options here because in case of no configuration provided in cassandra.yaml
        // it will default to empty/non-null encryption options. Hence, we consider it set only if 'enabled' flag is set to true
        boolean jmxEncryptionOptionsProvided = jmxEncryptionOptions != null && jmxEncryptionOptions.getEnabled() != null && jmxEncryptionOptions.getEnabled();

        if (jmxEncryptionOptionsProvided)
        {
            if (jmxEncryptionOptions.getEnabled())
                JMXServerOptions.setJmxSystemProperties(jmxEncryptionOptions.getAcceptedProtocols(), jmxEncryptionOptions.getCipherSuites());

            logger.info("Enabling JMX SSL using jmx_encryption_options");
            boolean requireClientAuth = jmxEncryptionOptions.getClientAuth() == EncryptionOptions.ClientAuth.REQUIRED;
            String[] ciphers = jmxEncryptionOptions.cipherSuitesArray();
            String[] protocols = jmxEncryptionOptions.acceptedProtocolsArray();
            SSLContext sslContext = jmxEncryptionOptions.sslContextFactoryInstance.createJSSESslContext(jmxEncryptionOptions.getClientAuth());
            configureSslClientSocketFactory(env, serverAddress);
            configureSslServerSocketFactory(env, serverAddress, ciphers, protocols, requireClientAuth, sslContext);
        }
        else if (!serverOptions.remote)
        {
            configureLocalSocketFactories(env, serverAddress);
        }

        return env;
    }

    /**
     * Configures the non-SSL socket factories for the local JMX.
     *
     * @param env           output param containing the configured socket factories
     * @param serverAddress the JMX server is bound to
     */
    abstract public void configureLocalSocketFactories(Map<String, Object> env, InetAddress serverAddress);

    /**
     * Configures SSL based client socket factory.
     *
     * @param env           output param containing the configured socket factories
     * @param serverAddress the JMX server is bound to
     */
    abstract public void configureSslClientSocketFactory(Map<String, Object> env, InetAddress serverAddress);

    /**
     * Configures SSL based server socket factory based on custom SSLContext.
     *
     * @param env                 output param containing the configured socket factories
     * @param serverAddress       the JMX server is bound to
     * @param enabledCipherSuites for the SSL communication
     * @param enabledProtocols    for the SSL communication
     * @param needClientAuth      {@code true} if it requires the client-auth; {@code false} otherwise
     * @param sslContext          for the SSL communication
     */
    abstract public void configureSslServerSocketFactory(Map<String, Object> env, InetAddress serverAddress,
                                                         String[] enabledCipherSuites, String[] enabledProtocols,
                                                         boolean needClientAuth, SSLContext sslContext);
}
