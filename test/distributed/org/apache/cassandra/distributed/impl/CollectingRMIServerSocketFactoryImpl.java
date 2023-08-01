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
import java.net.SocketException;
import java.rmi.server.RMIServerSocketFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.net.ServerSocketFactory;


/**
 * This class is used to keep track of RMI servers created during a cluster creation so we can
 * later close the sockets, which would otherwise be left with a thread running waiting for
 * connections that would never show up as the server was otherwise closed.
 */
class CollectingRMIServerSocketFactoryImpl implements RMIServerSocketFactory
{
    private final InetAddress bindAddress;
    List<ServerSocket> sockets = new ArrayList<>();

    public CollectingRMIServerSocketFactoryImpl(InetAddress bindAddress)
    {
        this.bindAddress = bindAddress;
    }

    @Override
    public ServerSocket createServerSocket(int pPort) throws IOException
    {
        ServerSocket result = ServerSocketFactory.getDefault().createServerSocket(pPort, 0, bindAddress);
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
        CollectingRMIServerSocketFactoryImpl that = (CollectingRMIServerSocketFactoryImpl) o;
        return Objects.equals(bindAddress, that.bindAddress);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bindAddress);
    }
}
