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

import java.net.InetAddress;

import org.apache.cassandra.concurrent.StageManager;

class OutboundTcpConnectionPool
{
    private final OutboundTcpConnection cmdCon;
    private final OutboundTcpConnection ackCon;

    OutboundTcpConnectionPool(InetAddress remoteEp)
    {
         cmdCon = new OutboundTcpConnection(remoteEp);
         ackCon = new OutboundTcpConnection(remoteEp);                                             
    }

    /**
     * returns the appropriate connection based on message type.
     * returns null if a connection could not be established.
     */
    OutboundTcpConnection getConnection(Message msg)
    {
        return msg.getMessageType().equals(StageManager.RESPONSE_STAGE) || msg.getMessageType().equals(StageManager.GOSSIP_STAGE)
               ? ackCon
               : cmdCon;
    }

    synchronized void reset()
    {
        for (OutboundTcpConnection con : new OutboundTcpConnection[] { cmdCon, ackCon })
            con.closeSocket();
    }
}
