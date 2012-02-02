/**
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
import java.net.Socket;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.utils.FBUtilities;

public class OutboundTcpConnectionPool
{
    private IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
    // pointer for the real Address.
    private final InetAddress id;
    public final OutboundTcpConnection cmdCon;
    public final OutboundTcpConnection ackCon;
    // pointer to the reseted Address.
    private InetAddress resetedEndpoint;

    OutboundTcpConnectionPool(InetAddress remoteEp)
    {
        id = remoteEp;
        cmdCon = new OutboundTcpConnection(this);
        cmdCon.start();
        ackCon = new OutboundTcpConnection(this);
        ackCon.start();
    }

    /**
     * returns the appropriate connection based on message type.
     * returns null if a connection could not be established.
     */
    OutboundTcpConnection getConnection(Message msg)
    {
        Stage stage = msg.getMessageType();
        return stage == Stage.REQUEST_RESPONSE || stage == Stage.INTERNAL_RESPONSE || stage == Stage.GOSSIP
               ? ackCon
               : cmdCon;
    }

    synchronized void reset()
    {
        for (OutboundTcpConnection con : new OutboundTcpConnection[] { cmdCon, ackCon })
            con.closeSocket();
    }
    
    public void reset(InetAddress remoteEP)
    {
        resetedEndpoint = remoteEP;
        reset();
    }
    
    public Socket newSocket() throws IOException
    {
        // zero means 'bind on any available port.'
        if (isEncryptedChannel())
        {
            return SSLFactory.getSocket(DatabaseDescriptor.getEncryptionOptions(), endPoint(), DatabaseDescriptor.getSSLStoragePort(), FBUtilities.getLocalAddress(), 0);
        }
        else {
            return new Socket(endPoint(), DatabaseDescriptor.getStoragePort(), FBUtilities.getLocalAddress(), 0);
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
