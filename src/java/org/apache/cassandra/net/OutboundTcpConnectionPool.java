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

import org.apache.cassandra.concurrent.StageManager;

class OutboundTcpConnectionPool
{
    private InetAddress localEp_;
    private InetAddress remoteEp_;
    private OutboundTcpConnection cmdCon;
    private OutboundTcpConnection ackCon;

    // TODO localEp is ignored, get rid of it
    OutboundTcpConnectionPool(InetAddress localEp, InetAddress remoteEp)
    {
        localEp_ = localEp;
        remoteEp_ = remoteEp;
    }

    private OutboundTcpConnection newCon()
    {
        return new OutboundTcpConnection(this, localEp_, remoteEp_);
    }

    /**
     * returns the appropriate connection based on message type.
     */
    synchronized OutboundTcpConnection getConnection(Message msg)
    {
        if (StageManager.RESPONSE_STAGE.equals(msg.getMessageType())
            || StageManager.GOSSIP_STAGE.equals(msg.getMessageType()))
        {
            if (ackCon == null)
                ackCon = newCon();
            return ackCon;
        }
        else
        {
            if (cmdCon == null)
                cmdCon = newCon();
            return cmdCon;
        }
    }

    synchronized void reset()
    {
        for (OutboundTcpConnection con : new OutboundTcpConnection[] { cmdCon, ackCon })
            if (con != null)
                con.closeSocket();
        cmdCon = null;
        ackCon = null;
    }
}
