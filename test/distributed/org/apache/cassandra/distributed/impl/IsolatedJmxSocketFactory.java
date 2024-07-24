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

package org.apache.cassandra.distributed.impl;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.RMIClientSocketFactoryImpl;
import org.apache.cassandra.utils.jmx.AbstractJmxSocketFactory;

/**
 * JMX Socket factory used for the isolated JMX testing.
 */
public class IsolatedJmxSocketFactory extends AbstractJmxSocketFactory
{
    private static final Logger logger = LoggerFactory.getLogger(IsolatedJmxSocketFactory.class);

    @Override
    public void configureLocalSocketFactories(Map<String, Object> env, InetAddress serverAddress)
    {
        CollectingRMIServerSocketFactoryImpl serverSocketFactory = new CollectingRMIServerSocketFactoryImpl(serverAddress);
        env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE,
                serverSocketFactory);
        RMIClientSocketFactoryImpl clientSocketFactory = new RMIClientSocketFactoryImpl(serverAddress);
        env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE,
                clientSocketFactory);
    }

    @Override
    public void configureSslClientSocketFactory(Map<String, Object> env, InetAddress serverAddress)
    {
        RMISslClientSocketFactoryImpl clientFactory = new RMISslClientSocketFactoryImpl(serverAddress,
                                                                                        CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES.getString(),
                                                                                        CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS.getString());
        env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, clientFactory);
        env.put("com.sun.jndi.rmi.factory.socket", clientFactory);
    }

    @Override
    public void configureSslServerSocketFactory(Map<String, Object> env, InetAddress serverAddress, String[] enabledCipherSuites,
                                                String[] enabledProtocols, boolean needClientAuth)
    {
        CollectingSslRMIServerSocketFactoryImpl serverFactory = new CollectingSslRMIServerSocketFactoryImpl(serverAddress,
                                                                                                            enabledCipherSuites,
                                                                                                            enabledProtocols,
                                                                                                            needClientAuth);
        env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, serverFactory);
        logJmxSslConfig(serverFactory);
    }

    @Override
    public void configureSslServerSocketFactory(Map<String, Object> env, InetAddress serverAddress, String[] enabledCipherSuites,
                                                String[] enabledProtocols, boolean needClientAuth, SSLContext sslContext)
    {
        CollectingSslRMIServerSocketFactoryImpl serverFactory = new CollectingSslRMIServerSocketFactoryImpl(serverAddress,
                                                                                                            enabledCipherSuites,
                                                                                                            enabledProtocols,
                                                                                                            needClientAuth,
                                                                                                            sslContext);
        env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, serverFactory);
        logJmxSslConfig(serverFactory);
    }

    private void logJmxSslConfig(CollectingSslRMIServerSocketFactoryImpl serverFactory)
    {
        if (logger.isDebugEnabled())
            logger.debug("JMX SSL configuration. { protocols: [{}], cipher_suites: [{}], require_client_auth: {} }",
                         serverFactory.getEnabledProtocols() == null ? "'JVM defaults'" : Arrays.stream(serverFactory.getEnabledProtocols()).collect(Collectors.joining("','", "'", "'")),
                         serverFactory.getEnabledCipherSuites() == null ? "'JVM defaults'" : Arrays.stream(serverFactory.getEnabledCipherSuites()).collect(Collectors.joining("','", "'", "'")),
                         serverFactory.isNeedClientAuth());
    }
}
