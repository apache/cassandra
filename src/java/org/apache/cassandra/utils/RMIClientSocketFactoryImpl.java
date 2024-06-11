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

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.rmi.server.RMIClientSocketFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This class is used to override the local address the JMX client calculates when trying to connect,
 * which can otherwise be influenced by the system property "java.rmi.server.hostname" in strange and
 * unpredictable ways.
 */
public class RMIClientSocketFactoryImpl implements RMIClientSocketFactory, Serializable
{
    List<Socket> sockets = new ArrayList<>();
    private final InetAddress localAddress;

    public RMIClientSocketFactoryImpl(InetAddress localAddress)
    {
        this.localAddress = localAddress;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException
    {
        Socket socket = new Socket(localAddress, port);
        sockets.add(socket);
        return socket;
    }

    public void close() throws IOException
    {
        for (Socket socket: sockets) {
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
        RMIClientSocketFactoryImpl that = (RMIClientSocketFactoryImpl) o;
        return Objects.equals(localAddress, that.localAddress);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(localAddress);
    }
}
