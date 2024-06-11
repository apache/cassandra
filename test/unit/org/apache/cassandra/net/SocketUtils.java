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

package org.apache.cassandra.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

public class SocketUtils
{
    /**
     * Returns an available port for the given {@code bindAddress}. When an {@link IOException} occurs when opening a
     * socket or if a {@link SecurityException} is raised because a manager exists and its checkListen method does
     * not allow the operation, the {@code fallbackPort} is returned.
     *
     * @param bindAddress  the ip address for the interface where we need an available port number
     * @param fallbackPort a port to return in case {@link SecurityException} or {@link IOException} is encountered
     * @return an available port the given {@code bindAddress} when succeeds, otherwise the {@code fallbackPort}
     * @throws RuntimeException if no IP address for the {@code bindAddress} could be found
     */
    public static synchronized int findAvailablePort(String bindAddress, int fallbackPort) throws RuntimeException
    {
        try
        {
            return findAvailablePort(InetAddress.getByName(bindAddress), fallbackPort);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns an available port for the given {@code bindAddress}. When an {@link IOException} occurs when opening a
     * socket or if a {@link SecurityException} is raised because a manager exists and its checkListen method does
     * not allow the operation, the {@code fallbackPort} is returned.
     *
     * @param bindAddress  the ip address for the interface where we need an available port number
     * @param fallbackPort a port to return in case {@link SecurityException} or {@link IOException} is encountered
     * @return an available port the given {@code bindAddress} when succeeds, otherwise the {@code fallbackPort}
     */
    public static synchronized int findAvailablePort(InetAddress bindAddress, int fallbackPort)
    {
        try (ServerSocket socket = new ServerSocket(0, 50, bindAddress))
        {
            return socket.getLocalPort();
        }
        catch (SecurityException | IOException exception)
        {
            return fallbackPort;
        }
    }
}
