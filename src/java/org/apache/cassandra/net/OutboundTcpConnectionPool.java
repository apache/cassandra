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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.metrics.ConnectionMetrics;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.utils.FBUtilities;

public class OutboundTcpConnectionPool
{
    private final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
    // pointer for the real Address.
    private final InetAddress id;
    public final OutboundTcpConnection cmdCon;
    public final OutboundTcpConnection ackCon;
    // pointer to the reseted Address.
    private InetAddress resetedEndpoint;
    private ConnectionMetrics metrics;

    OutboundTcpConnectionPool(InetAddress remoteEp)
    {
        id = remoteEp;
        cmdCon = new OutboundTcpConnection(this);
        cmdCon.start();
        ackCon = new OutboundTcpConnection(this);
        ackCon.start();

        metrics = new ConnectionMetrics(id, this);
    }

    /**
     * returns the appropriate connection based on message type.
     * returns null if a connection could not be established.
     */
    OutboundTcpConnection getConnection(MessageOut msg)
    {
        Stage stage = msg.getStage();
        return stage == Stage.REQUEST_RESPONSE || stage == Stage.INTERNAL_RESPONSE || stage == Stage.GOSSIP
               ? ackCon
               : cmdCon;
    }

    void reset()
    {
        for (OutboundTcpConnection conn : new OutboundTcpConnection[] { cmdCon, ackCon })
            conn.closeSocket();
    }

    public void resetToNewerVersion(int version)
    {
        for (OutboundTcpConnection conn : new OutboundTcpConnection[] { cmdCon, ackCon })
        {
            if (version > conn.getTargetVersion())
                conn.softCloseSocket();
        }
    }

    /**
     * reconnect to @param remoteEP (after the current message backlog is exhausted).
     * Used by Ec2MultiRegionSnitch to force nodes in the same region to communicate over their private IPs.
     * @param remoteEP
     */
    public void reset(InetAddress remoteEP)
    {
        resetedEndpoint = remoteEP;
        for (OutboundTcpConnection conn : new OutboundTcpConnection[] { cmdCon, ackCon })
            conn.softCloseSocket();

        // release previous metrics and create new one with reset address
        metrics.release();
        metrics = new ConnectionMetrics(resetedEndpoint, this);
    }

    public long getTimeouts()
    {
       return metrics.timeouts.count();
    }

    public long getRecentTimeouts()
    {
        return metrics.getRecentTimeout();
    }

    public void incrementTimeout()
    {
        metrics.timeouts.mark();
    }

    public Socket newSocket() throws IOException
    {
        // zero means 'bind on any available port.'
        if (isEncryptedChannel())
        {
            if (Config.getOutboundBindAny())
                return SSLFactory.getSocket(DatabaseDescriptor.getEncryptionOptions(), endPoint(), DatabaseDescriptor.getSSLStoragePort());
            else
                return SSLFactory.getSocket(DatabaseDescriptor.getEncryptionOptions(), endPoint(), DatabaseDescriptor.getSSLStoragePort(), FBUtilities.getLocalAddress(), 0);
        }
        else
        {
            Socket socket = SocketChannel.open(new InetSocketAddress(endPoint(), DatabaseDescriptor.getStoragePort())).socket();
            if (Config.getOutboundBindAny())
                socket.bind(new InetSocketAddress(FBUtilities.getLocalAddress(), 0));
            return socket;
        }
    }

    InetAddress endPoint()
    {
        return resetedEndpoint == null ? id : resetedEndpoint;
    }

    boolean isEncryptedChannel()
    {
        switch (DatabaseDescriptor.getEncryptionOptions().internode_encryption)
        {
            case none:
                return false; // if nothing needs to be encrypted then return immediately.
            case all:
                break;
            case dc:
                if (snitch.getDatacenter(id).equals(snitch.getDatacenter(FBUtilities.getBroadcastAddress())))
                    return false;
                break;
            case rack:
                // for rack then check if the DC's are the same.
                if (snitch.getRack(id).equals(snitch.getRack(FBUtilities.getBroadcastAddress()))
                        && snitch.getDatacenter(id).equals(snitch.getDatacenter(FBUtilities.getBroadcastAddress())))
                    return false;
                break;
        }
        return true;
    }
}
