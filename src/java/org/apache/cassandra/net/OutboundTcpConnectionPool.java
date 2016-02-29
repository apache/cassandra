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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.metrics.ConnectionMetrics;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.utils.FBUtilities;

public class OutboundTcpConnectionPool
{
    public static final long LARGE_MESSAGE_THRESHOLD =
            Long.getLong(Config.PROPERTY_PREFIX + "otcp_large_message_threshold", 1024 * 64);

    // pointer for the real Address.
    private final InetAddress id;
    private final CountDownLatch started;
    public final OutboundTcpConnection smallMessages;
    public final OutboundTcpConnection largeMessages;
    public final OutboundTcpConnection gossipMessages;

    // pointer to the reset Address.
    private InetAddress resetEndpoint;
    private ConnectionMetrics metrics;

    OutboundTcpConnectionPool(InetAddress remoteEp)
    {
        id = remoteEp;
        resetEndpoint = SystemKeyspace.getPreferredIP(remoteEp);
        started = new CountDownLatch(1);

        smallMessages = new OutboundTcpConnection(this);
        largeMessages = new OutboundTcpConnection(this);
        gossipMessages = new OutboundTcpConnection(this);
    }

    /**
     * returns the appropriate connection based on message type.
     * returns null if a connection could not be established.
     */
    OutboundTcpConnection getConnection(MessageOut msg)
    {
        if (Stage.GOSSIP == msg.getStage())
            return gossipMessages;
        return msg.payloadSize(smallMessages.getTargetVersion()) > LARGE_MESSAGE_THRESHOLD
               ? largeMessages
               : smallMessages;
    }

    void reset()
    {
        for (OutboundTcpConnection conn : new OutboundTcpConnection[] { smallMessages, largeMessages, gossipMessages })
            conn.closeSocket(false);
    }

    public void resetToNewerVersion(int version)
    {
        for (OutboundTcpConnection conn : new OutboundTcpConnection[] { smallMessages, largeMessages, gossipMessages })
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
        SystemKeyspace.updatePreferredIP(id, remoteEP);
        resetEndpoint = remoteEP;
        for (OutboundTcpConnection conn : new OutboundTcpConnection[] { smallMessages, largeMessages, gossipMessages })
            conn.softCloseSocket();

        // release previous metrics and create new one with reset address
        metrics.release();
        metrics = new ConnectionMetrics(resetEndpoint, this);
    }

    public long getTimeouts()
    {
       return metrics.timeouts.getCount();
    }


    public void incrementTimeout()
    {
        metrics.timeouts.mark();
    }

    public Socket newSocket() throws IOException
    {
        return newSocket(endPoint());
    }

    @SuppressWarnings("resource") // Closing the socket will close the underlying channel.
    public static Socket newSocket(InetAddress endpoint) throws IOException
    {
        // zero means 'bind on any available port.'
        if (isEncryptedChannel(endpoint))
        {
            if (Config.getOutboundBindAny())
                return SSLFactory.getSocket(DatabaseDescriptor.getServerEncryptionOptions(), endpoint, DatabaseDescriptor.getSSLStoragePort());
            else
                return SSLFactory.getSocket(DatabaseDescriptor.getServerEncryptionOptions(), endpoint, DatabaseDescriptor.getSSLStoragePort(), FBUtilities.getLocalAddress(), 0);
        }
        else
        {
            SocketChannel channel = SocketChannel.open();
            if (!Config.getOutboundBindAny())
                channel.bind(new InetSocketAddress(FBUtilities.getLocalAddress(), 0));
            channel.connect(new InetSocketAddress(endpoint, DatabaseDescriptor.getStoragePort()));
            return channel.socket();
        }
    }

    public InetAddress endPoint()
    {
        if (id.equals(FBUtilities.getBroadcastAddress()))
            return FBUtilities.getLocalAddress();
        return resetEndpoint;
    }

    public static boolean isEncryptedChannel(InetAddress address)
    {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        switch (DatabaseDescriptor.getServerEncryptionOptions().internode_encryption)
        {
            case none:
                return false; // if nothing needs to be encrypted then return immediately.
            case all:
                break;
            case dc:
                if (snitch.getDatacenter(address).equals(snitch.getDatacenter(FBUtilities.getBroadcastAddress())))
                    return false;
                break;
            case rack:
                // for rack then check if the DC's are the same.
                if (snitch.getRack(address).equals(snitch.getRack(FBUtilities.getBroadcastAddress()))
                        && snitch.getDatacenter(address).equals(snitch.getDatacenter(FBUtilities.getBroadcastAddress())))
                    return false;
                break;
        }
        return true;
    }

    public void start()
    {
        smallMessages.start();
        largeMessages.start();
        gossipMessages.start();

        metrics = new ConnectionMetrics(id, this);

        started.countDown();
    }

    public void waitForStarted()
    {
        if (started.getCount() == 0)
            return;

        boolean error = false;
        try
        {
            if (!started.await(1, TimeUnit.MINUTES))
                error = true;
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            error = true;
        }
        if (error)
            throw new IllegalStateException(String.format("Connections to %s are not started!", id.getHostAddress()));
    }

    public void close()
    {
        // these null guards are simply for tests
        if (largeMessages != null)
            largeMessages.closeSocket(true);
        if (smallMessages != null)
            smallMessages.closeSocket(true);
        if (gossipMessages != null)
            gossipMessages.closeSocket(true);

        metrics.release();
    }
}
