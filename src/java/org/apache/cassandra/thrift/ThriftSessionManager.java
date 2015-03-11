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
package org.apache.cassandra.thrift;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the current client state (session).
 *
 * We rely on the Thrift server to tell us what socket it is
 * executing a request for via setCurrentSocket, after which currentSession can do its job anywhere.
 */
public class ThriftSessionManager
{
    private static final Logger logger = LoggerFactory.getLogger(ThriftSessionManager.class);
    public final static ThriftSessionManager instance = new ThriftSessionManager();

    private final ThreadLocal<SocketAddress> remoteSocket = new ThreadLocal<>();
    private final Map<SocketAddress, ThriftClientState> activeSocketSessions = new ConcurrentHashMap<>();

    /**
     * @param socket the address on which the current thread will work on requests for until further notice
     */
    public void setCurrentSocket(SocketAddress socket)
    {
        remoteSocket.set(socket);
    }

    /**
     * @return the current session for the most recently given socket on this thread
     */
    public ThriftClientState currentSession()
    {
        SocketAddress socket = remoteSocket.get();
        assert socket != null;

        ThriftClientState cState = activeSocketSessions.get(socket);
        if (cState == null)
        {
            cState = new ThriftClientState((InetSocketAddress)socket);
            activeSocketSessions.put(socket, cState);
        }
        return cState;
    }

    /**
     * The connection associated with @param socket is permanently finished.
     */
    public void connectionComplete(SocketAddress socket)
    {
        assert socket != null;
        activeSocketSessions.remove(socket);
        if (logger.isTraceEnabled())
            logger.trace("ClientState removed for socket addr {}", socket);
    }
    
    public int getConnectedClients()
    {
        return activeSocketSessions.size();
    }
}
