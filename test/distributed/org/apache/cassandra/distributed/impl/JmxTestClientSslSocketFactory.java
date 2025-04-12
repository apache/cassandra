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

import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.rmi.ssl.SslRMIClientSocketFactory;

/**
 * {@code RMIClientSocketFactory} for testing SSL based JMX clients.
 */
public class JmxTestClientSslSocketFactory extends SslRMIClientSocketFactory implements Serializable
{
    private static final long serialVersionUID = 818579127759449333L;
    private final SocketFactory defaultSocketFactory;
    private final String[] cipherSuites;
    private final String[] acceptedProtocols;

    public JmxTestClientSslSocketFactory(SSLContext sslContext, String[] cipherSuites, String[] acceptedProtocols)
    {
        this.cipherSuites = cipherSuites;
        this.acceptedProtocols = acceptedProtocols;
        defaultSocketFactory = sslContext == null ? SSLSocketFactory.getDefault() : sslContext.getSocketFactory();
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException
    {
        final SSLSocket sslSocket = (SSLSocket) defaultSocketFactory.createSocket(host, port);

        if (cipherSuites != null)
            sslSocket.setEnabledCipherSuites(cipherSuites);

        if (acceptedProtocols != null)
            sslSocket.setEnabledProtocols(acceptedProtocols);

        return sslSocket;
    }
}
