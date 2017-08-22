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

package org.apache.cassandra.net.async;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Identifies an outbound messaging connection.
 *
 * This mainly hold the remote address and the type (small/large messages or gossip) of connection used, but with the
 * additional detail that in some case (typically public EC2 address across regions) the address to which we connect
 * to the remote is different from the address by which the node is known by the rest of the C*.
 */
public class OutboundConnectionIdentifier
{
    enum ConnectionType
    {
        GOSSIP, LARGE_MESSAGE, SMALL_MESSAGE
    }

    /**
     * Memoization of the local node's broadcast address.
     */
    private final InetSocketAddress localAddr;

    /**
     * The address by which the remote is identified. This may be different from {@link #remoteConnectionAddr} for
     * something like EC2 public IP address which need to be used for communication between EC2 regions.
     */
    private final InetSocketAddress remoteAddr;

    /**
     * The address to which we're connecting to the node (often the same as {@link #remoteAddr} but not always).
     */
    private final InetSocketAddress remoteConnectionAddr;

    private final ConnectionType connectionType;

    private OutboundConnectionIdentifier(InetSocketAddress localAddr,
                                         InetSocketAddress remoteAddr,
                                         InetSocketAddress remoteConnectionAddr,
                                         ConnectionType connectionType)
    {
        this.localAddr = localAddr;
        this.remoteAddr = remoteAddr;
        this.remoteConnectionAddr = remoteConnectionAddr;
        this.connectionType = connectionType;
    }

    private OutboundConnectionIdentifier(InetSocketAddress localAddr,
                                         InetSocketAddress remoteAddr,
                                         ConnectionType connectionType)
    {
        this(localAddr, remoteAddr, remoteAddr, connectionType);
    }

    /**
     * Creates an identifier for a small message connection and using the remote "identifying" address as its connection
     * address.
     */
    public static OutboundConnectionIdentifier small(InetSocketAddress localAddr, InetSocketAddress remoteAddr)
    {
        return new OutboundConnectionIdentifier(localAddr, remoteAddr, ConnectionType.SMALL_MESSAGE);
    }

    /**
     * Creates an identifier for a large message connection and using the remote "identifying" address as its connection
     * address.
     */
    public static OutboundConnectionIdentifier large(InetSocketAddress localAddr, InetSocketAddress remoteAddr)
    {
        return new OutboundConnectionIdentifier(localAddr, remoteAddr, ConnectionType.LARGE_MESSAGE);
    }

    /**
     * Creates an identifier for a gossip connection and using the remote "identifying" address as its connection
     * address.
     */
    public static OutboundConnectionIdentifier gossip(InetSocketAddress localAddr, InetSocketAddress remoteAddr)
    {
        return new OutboundConnectionIdentifier(localAddr, remoteAddr, ConnectionType.GOSSIP);
    }

    /**
     * Returns a newly created connection identifier to the same remote that this identifier, but using the provided
     * address as connection address.
     *
     * @param remoteConnectionAddr the address to use for connection to the remote in the new identifier.
     * @return a newly created connection identifier that differs from this one only by using {@code remoteConnectionAddr}
     * as connection address to the remote.
     */
    OutboundConnectionIdentifier withNewConnectionAddress(InetSocketAddress remoteConnectionAddr)
    {
        return new OutboundConnectionIdentifier(localAddr, remoteAddr, remoteConnectionAddr, connectionType);
    }

    /**
     * The local node address.
     */
    InetAddress local()
    {
        return localAddr.getAddress();
    }

    /**
     * The remote node identifying address (the one to use for anything else than connecting to the node).
     */
    InetSocketAddress remoteAddress()
    {
        return remoteAddr;
    }

    /**
     * The remote node identifying address (the one to use for anything else than connecting to the node).
     */
    InetAddress remote()
    {
        return remoteAddr.getAddress();
    }

    /**
     * The remote node connection address (the one to use to actually connect to the remote, and only that).
     */
    InetSocketAddress connectionAddress()
    {
        return remoteConnectionAddr;
    }

    /**
     * The type of this connection.
     */
    ConnectionType type()
    {
        return connectionType;
    }

    @Override
    public String toString()
    {
        return remoteAddr.equals(remoteConnectionAddr)
               ? String.format("%s (%s)", remoteAddr, connectionType)
               : String.format("%s on %s (%s)", remoteAddr, remoteConnectionAddr, connectionType);
    }
}
