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
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.cassandra.utils.RMICloseableClientSocketFactory;

/**
 * {@code RMIClientSocketFactory} for testing SSL based JMX clients.
 * This class is used to override the local address the JMX client calculates when trying to connect,
 * which can otherwise be influenced by the system property "java.rmi.server.hostname" in strange and
 * unpredictable ways.
 */
public class RMISslClientSocketFactoryImpl implements Serializable, RMICloseableClientSocketFactory
{
    private static final long serialVersionUID = 9054380061905145241L;
    private static final Pattern COMMA_SPLITTER = Pattern.compile(",");
    private static final List<Socket> sockets = new ArrayList<>();
    private final InetAddress localAddress;
    private final String[] enabledCipherSuites;
    private final String[] enabledProtocols;

    public RMISslClientSocketFactoryImpl(InetAddress localAddress, String enabledCipherSuites, String enabledProtocls)
    {
        this.localAddress = localAddress;
        this.enabledCipherSuites = splitCommaSeparatedString(enabledCipherSuites);
        this.enabledProtocols = splitCommaSeparatedString(enabledProtocls);
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException
    {
        Socket socket = createSslSocket(port);
        sockets.add(socket);
        return socket;
    }

    private Socket createSslSocket(int port) throws IOException
    {
        final SocketFactory sslSocketFactory = SSLSocketFactory.getDefault();
        final SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(localAddress, port);
        if (enabledCipherSuites != null)
        {
            try
            {
                sslSocket.setEnabledCipherSuites(enabledCipherSuites);
            }
            catch (IllegalArgumentException e)
            {
                throw new IOException(e.getMessage(), e);
            }
        }
        if (enabledProtocols != null)
        {
            try
            {
                sslSocket.setEnabledProtocols(enabledProtocols);
            }
            catch (IllegalArgumentException e)
            {
                throw new IOException(e.getMessage(), e);
            }
        }
        return sslSocket;
    }

    @Override
    public void close() throws IOException
    {
        for (Socket socket : sockets)
        {
            try
            {
                socket.close();
            }
            catch (IOException ignored)
            {
                // intentionally ignored
            }
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RMISslClientSocketFactoryImpl that = (RMISslClientSocketFactoryImpl) o;
        return Objects.equals(localAddress, that.localAddress);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(localAddress);
    }

    private String[] splitCommaSeparatedString(String stringToSplit)
    {
        if (stringToSplit == null)
            return null;
        return COMMA_SPLITTER.split(stringToSplit);
    }
}
