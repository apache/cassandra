package org.apache.cassandra.net;
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


import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.utils.FBUtilities;

public class OutboundTcpConnection extends Thread
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundTcpConnection.class);

    private static final ByteBuffer CLOSE_SENTINEL = ByteBuffer.allocate(0);
    private static final int OPEN_RETRY_DELAY = 100; // ms between retries

    private final InetAddress endpoint;
    private final BlockingQueue<ByteBuffer> queue = new LinkedBlockingQueue<ByteBuffer>();
    private DataOutputStream output;
    private Socket socket;
    private long completedCount;

    public OutboundTcpConnection(InetAddress remoteEp)
    {
        super("WRITE-" + remoteEp);
        this.endpoint = remoteEp;
    }

    public void write(ByteBuffer buffer)
    {
        try
        {
            queue.put(buffer);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    void closeSocket()
    {
        queue.clear();
        write(CLOSE_SENTINEL);
    }

    public void run()
    {
        while (true)
        {
            ByteBuffer bb = take();
            if (bb == CLOSE_SENTINEL)
            {
                disconnect();
                continue;
            }
            if (socket != null || connect())
                writeConnected(bb);
            else
                // clear out the queue, else gossip messages back up.
                queue.clear();            
        }
    }

    public int getPendingMessages()
    {
        return queue.size();
    }

    public long getCompletedMesssages()
    {
        return completedCount;
    }

    private void writeConnected(ByteBuffer bb)
    {
        try
        {
            ByteBufferUtil.write(bb, output);
            if (queue.peek() == null)
            {
                output.flush();
            }
        }
        catch (IOException e)
        {
            if (logger.isDebugEnabled())
                logger.debug("error writing to " + endpoint, e);
            disconnect();
        }
    }

    private void disconnect()
    {
        if (socket != null)
        {
            try
            {
                socket.close();
            }
            catch (IOException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("exception closing connection to " + endpoint, e);
            }
            output = null;
            socket = null;
        }
    }

    private ByteBuffer take()
    {
        ByteBuffer bb;
        try
        {
            bb = queue.take();
            completedCount++;
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        return bb;
    }

    private boolean connect()
    {
        if (logger.isDebugEnabled())
            logger.debug("attempting to connect to " + endpoint);
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() < start + DatabaseDescriptor.getRpcTimeout())
        {
            try
            {
                // zero means 'bind on any available port.'
                EncryptionOptions options = DatabaseDescriptor.getEncryptionOptions();
                if (options != null && options.internode_encryption == EncryptionOptions.InternodeEncryption.all)
                {
                    socket = SSLFactory.getSocket(options, endpoint, DatabaseDescriptor.getStoragePort(), FBUtilities.getLocalAddress(), 0);
                }
                else {
                    socket = new Socket(endpoint, DatabaseDescriptor.getStoragePort(), FBUtilities.getLocalAddress(), 0);
                }

                socket.setKeepAlive(true);
                socket.setTcpNoDelay(true);
                output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 4096));
                return true;
            }
            catch (IOException e)
            {
                socket = null;
                if (logger.isTraceEnabled())
                    logger.trace("unable to connect to " + endpoint, e);
                try
                {
                    Thread.sleep(OPEN_RETRY_DELAY);
                }
                catch (InterruptedException e1)
                {
                    throw new AssertionError(e1);
                }
            }
        }
        return false;
    }
}
