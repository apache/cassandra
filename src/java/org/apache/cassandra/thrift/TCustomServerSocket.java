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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * Extends Thrift's TServerSocket to allow customization of various desirable TCP properties.
 */
public class TCustomServerSocket extends TServerTransport
{

    private static final Logger logger = LoggerFactory.getLogger(TCustomServerSocket.class);

    /**
     * Underlying serversocket object
     */
    private ServerSocket serverSocket_ = null;

    private final boolean keepAlive;
    private final Integer sendBufferSize;
    private final Integer recvBufferSize;

    /**
     * Allows fine-tuning of the server socket including keep-alive, reuse of addresses, send and receive buffer sizes.
     * 
     * @param bindAddr
     * @param keepAlive
     * @param sendBufferSize
     * @param recvBufferSize
     * @throws TTransportException
     */
    public TCustomServerSocket(InetSocketAddress bindAddr, boolean keepAlive, Integer sendBufferSize,
            Integer recvBufferSize)
            throws TTransportException
    {
        try
        {
            // Make server socket
            serverSocket_ = new ServerSocket();
            // Prevent 2MSL delay problem on server restarts
            serverSocket_.setReuseAddress(true);
            // Bind to listening port
            serverSocket_.bind(bindAddr);
        }
        catch (IOException ioe)
        {
            serverSocket_ = null;
            throw new TTransportException("Could not create ServerSocket on address " + bindAddr.toString() + ".");
        }

        this.keepAlive = keepAlive;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
    }

    @Override
    protected TCustomSocket acceptImpl() throws TTransportException
    {

        if (serverSocket_ == null)
            throw new TTransportException(TTransportException.NOT_OPEN, "No underlying server socket.");

        TCustomSocket tsocket = null;
        Socket socket = null;
        try
        {
            socket = serverSocket_.accept();
            tsocket = new TCustomSocket(socket);
            tsocket.setTimeout(0);
        }
        catch (IOException iox)
        {
            throw new TTransportException(iox);
        }

        try
        {
            socket.setKeepAlive(this.keepAlive);
        }
        catch (SocketException se)
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

    @Override
    public void listen() throws TTransportException
    {
        // Make sure not to block on accept
        if (serverSocket_ != null)
        {
            try
            {
                serverSocket_.setSoTimeout(0);
            }
            catch (SocketException sx)
            {
                logger.error("Could not set socket timeout.", sx);
            }
        }
    }

    @Override
    public void close()
    {
        if (serverSocket_ != null)
        {
            try
            {
                serverSocket_.close();
            }
            catch (IOException iox)
            {
                logger.warn("Could not close server socket.", iox);
            }
            serverSocket_ = null;
        }
    }

    @Override
    public void interrupt()
    {
        // The thread-safeness of this is dubious, but Java documentation suggests
        // that it is safe to do this from a different thread context
        close();
    }
}
