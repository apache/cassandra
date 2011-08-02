package org.apache.cassandra.thrift;
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


import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import org.apache.cassandra.service.SocketSessionManagementService;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCustomNonblockingServerSocket extends TNonblockingServerSocket
{
    private static final Logger logger = LoggerFactory.getLogger(TCustomNonblockingServerSocket.class);
    private final boolean keepAlive;
    private final Integer sendBufferSize;
    private final Integer recvBufferSize;

    public TCustomNonblockingServerSocket(InetSocketAddress bindAddr, boolean keepAlive, Integer sendBufferSize, Integer recvBufferSize) throws TTransportException
    {
        super(bindAddr);
        this.keepAlive = keepAlive;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
    }

    @Override
    protected TNonblockingSocket acceptImpl() throws TTransportException
    {
        TNonblockingSocket tsocket = super.acceptImpl();
        if (tsocket == null || tsocket.getSocketChannel() == null)
            return tsocket;
        Socket socket = tsocket.getSocketChannel().socket();
        // clean up the old information.
        SocketSessionManagementService.instance.remove(socket.getRemoteSocketAddress());
        try
        {
            socket.setKeepAlive(this.keepAlive);
        } catch (SocketException se)
        {
            logger.warn("Failed to set keep-alive on Thrift socket.", se);
        }
        
        if (this.sendBufferSize != null)
        {
            try
            {
                socket.setSendBufferSize(this.sendBufferSize.intValue());
            }
            catch (SocketException se)
            {
                logger.warn("Failed to set send buffer size on Thrift socket.", se);
            }
        }

        if (this.recvBufferSize != null)
        {
            try
            {
                socket.setReceiveBufferSize(this.recvBufferSize.intValue());
            }
            catch (SocketException se)
            {
                logger.warn("Failed to set receive buffer size on Thrift socket.", se);
            }
        }
        return tsocket;
    }
}
