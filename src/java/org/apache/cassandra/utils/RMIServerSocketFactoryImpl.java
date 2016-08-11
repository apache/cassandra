/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.rmi.server.RMIServerSocketFactory;
import javax.net.ServerSocketFactory;

public class RMIServerSocketFactoryImpl implements RMIServerSocketFactory
{
    // Address to bind server sockets too, may be null indicating all local interfaces are to be bound
    private final InetAddress bindAddress;

    public RMIServerSocketFactoryImpl(InetAddress bindAddress)
    {
        this.bindAddress = bindAddress;
    }

    public ServerSocket createServerSocket(final int pPort) throws IOException
    {
        ServerSocket socket = ServerSocketFactory.getDefault().createServerSocket(pPort, 0, bindAddress);
        try
        {
            socket.setReuseAddress(true);
            return socket;
        }
        catch (SocketException e)
        {
            socket.close();
            throw e;
        }
    }

    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }

        return obj.getClass().equals(getClass());
    }

    public int hashCode()
    {
        return RMIServerSocketFactoryImpl.class.hashCode();
    }
}

