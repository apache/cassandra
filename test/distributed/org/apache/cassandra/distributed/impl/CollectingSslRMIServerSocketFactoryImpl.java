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
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.cassandra.utils.RMICloseableServerSocketFactory;


/**
 * This class is used to keep track of SSL based RMI servers created during a cluster creation to
 * later close the sockets, which would otherwise be left with a thread running waiting for
 * connections that would never show up as the server was otherwise closed.
 */
class CollectingSslRMIServerSocketFactoryImpl implements RMICloseableServerSocketFactory
{
    private final InetAddress bindAddress;
    private final String[] enabledCipherSuites;
    private final String[] enabledProtocols;
    private final boolean needClientAuth;
    private final SSLSocketFactory sslSocketFactory;
    List<ServerSocket> sockets = new ArrayList<>();

    public CollectingSslRMIServerSocketFactoryImpl(InetAddress bindAddress, String[] enabledCipherSuites,
                                                   String[] enabledProtocols, boolean needClientAuth, SSLContext sslContext)
    {
        this.bindAddress = bindAddress;
        this.enabledCipherSuites = enabledCipherSuites;
        this.enabledProtocols = enabledProtocols;
        this.needClientAuth = needClientAuth;
        this.sslSocketFactory = sslContext.getSocketFactory();
    }

    public String[] getEnabledCipherSuites()
    {
        return enabledCipherSuites;
    }

    public String[] getEnabledProtocols()
    {
        return enabledProtocols;
    }

    public boolean isNeedClientAuth()
    {
        return needClientAuth;
    }

    @Override
    public ServerSocket createServerSocket(int pPort) throws IOException
    {
        ServerSocket result = createSslServerSocket(pPort);
        try
        {
            result.setReuseAddress(true);
        }
        catch (SocketException e)
        {
            result.close();
            throw e;
        }
        sockets.add(result);
        return result;
    }

    private ServerSocket createSslServerSocket(int pPort) throws IOException
    {
        return new ServerSocket(pPort, 0, bindAddress)
        {
            public Socket accept() throws IOException
            {
                Socket socket = super.accept();
                SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(
                socket, socket.getInetAddress().getHostName(),
                socket.getPort(), true);
                sslSocket.setUseClientMode(false);
                if (enabledCipherSuites != null)
                {
                    sslSocket.setEnabledCipherSuites(enabledCipherSuites);
                }
                if (enabledProtocols != null)
                {
                    sslSocket.setEnabledProtocols(enabledProtocols);
                }
                sslSocket.setNeedClientAuth(needClientAuth);
                return sslSocket;
            }
        };
    }

    @Override
    public void close() throws IOException
    {
        for (ServerSocket socket : sockets)
        {
            socket.close();
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CollectingSslRMIServerSocketFactoryImpl that = (CollectingSslRMIServerSocketFactoryImpl) o;
        return Objects.equals(bindAddress, that.bindAddress);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bindAddress);
    }

    private static SSLSocketFactory defaultSSLSocketFactory = null;

    private static synchronized SSLSocketFactory getDefaultSSLSocketFactory()
    {
        if (defaultSSLSocketFactory == null)
            defaultSSLSocketFactory =
            (SSLSocketFactory) SSLSocketFactory.getDefault();
        return defaultSSLSocketFactory;
    }
}
